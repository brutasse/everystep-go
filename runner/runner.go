// Package runner is the replay engine.
//
// A claimed workflow's body is re-run from the top on every claim. Step
// calls whose outcome is already recorded are served from the step table;
// execution resumes at the first unrecorded step.
//
// SetFault is a test hook: when set, the fault function is called (ctx,
// stepID) after a step's side effect has run but before its outcome is
// recorded. Returning the SimulatedCrash error simulates a worker process
// dying at that point.
package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"gorm.io/gorm"

	esc "github.com/brutasse/everystep-go/internal/ctx"
	errs "github.com/brutasse/everystep-go/internal/errors"
	"github.com/brutasse/everystep-go/internal/registry"
	"github.com/brutasse/everystep-go/metrics"
	"github.com/brutasse/everystep-go/serde"
	"github.com/brutasse/everystep-go/store"
	"github.com/brutasse/everystep-go/traces"
)

// FailureMeta identifies the run a failure reporter is called for.
type FailureMeta struct {
	WorkflowID uuid.UUID
	Workflow   string
}

var (
	reporterPtr atomic.Pointer[func(context.Context, error, FailureMeta)]
	faultPtr    atomic.Pointer[func(context.Context, string) error]
)

// SetFailureReporter installs the hook called for unhandled workflow
// failures. Control-flow errors (Terminal, EffectUncertain, DrainOrphan,
// SimulatedCrash) are never reported.
func SetFailureReporter(f func(context.Context, error, FailureMeta)) {
	if f == nil {
		reporterPtr.Store(nil)
		return
	}
	reporterPtr.Store(&f)
}

func getReporter() func(context.Context, error, FailureMeta) {
	if p := reporterPtr.Load(); p != nil {
		return *p
	}
	return nil
}

// SetFault installs the test fault hook; SetFault(nil) removes it. The hook
// returns an error to abort the run at that point; returning the
// SimulatedCrash error simulates a process death, so nothing after the hook
// is recorded.
func SetFault(f func(context.Context, string) error) {
	if f == nil {
		faultPtr.Store(nil)
		return
	}
	faultPtr.Store(&f)
}

func getFault() func(context.Context, string) error {
	if p := faultPtr.Load(); p != nil {
		return *p
	}
	return nil
}

// ReportFailure calls the installed failure reporter for an unhandled
// workflow failure. Control-flow errors (SimulatedCrash, DrainOrphan,
// Terminal, EffectUncertain) are never reported.
func ReportFailure(ctx context.Context, err error, meta FailureMeta) {
	var (
		crash     *errs.SimulatedCrash
		orphan    *errs.DrainOrphan
		terminal  *errs.Terminal
		uncertain *errs.EffectUncertain
	)
	if errors.As(err, &crash) || errors.As(err, &orphan) || errors.As(err, &terminal) || errors.As(err, &uncertain) {
		return
	}
	if f := getReporter(); f != nil {
		f(ctx, err, meta)
	}
}

// RunStep executes one step within a running workflow: the outcome is
// looked up under the next dotpath id; on a hit the stored outcome is
// served (or the stored error re-raised), on a miss the side effect runs
// and the outcome is persisted. Outside a run it is not used: Step.Call
// executes the function plainly.
func RunStep[R, A any](
	ctx context.Context,
	name string,
	unsafeToRepeat bool,
	a A,
	namedID *string,
	fn func(context.Context, A) (R, error),
) (R, error) {
	var zero R
	rs := esc.From(ctx)
	stepID, err := rs.NextID(namedID)
	if err != nil {
		return zero, err
	}

	stored, hit := rs.Outcomes().Get(stepID)
	if hit {
		if stored.Name != name {
			return zero, &errs.WorkflowCodeError{Msg: fmt.Sprintf(
				"workflow code changed at step %q: previously %q, now %q (workflow %s). "+
					"Workflow bodies must be deterministic across runs.",
				stepID, stored.Name, name, rs.WorkflowID())}
		}
		switch stored.Status {
		case store.StepFailed:
			var raw []byte
			if stored.Error != nil {
				raw, _ = json.Marshal(stored.Error)
			}
			return zero, serde.DecodeError(raw)
		case store.StepStarted:
			if unsafeToRepeat {
				return zero, &errs.EffectUncertain{Msg: fmt.Sprintf(
					"step %q (%s) is marked unsafe to repeat and its outcome is uncertain: "+
						"a previous attempt started but was not recorded, so the effect may "+
						"have happened. Verify it in the external system, then resolve run %s "+
						"with es.ResolveStep.", stepID, name, rs.WorkflowID())}
			}
			// No longer marked unsafe to repeat: the current code opts back
			// into at-least-once, so re-execute the step.
		default: // done
			var r R
			if err := serde.Unmarshal(stored.Result, &r); err != nil {
				return zero, &errs.WorkflowCodeError{Msg: fmt.Sprintf(
					"workflow code changed at step %q: stored result no longer decodes "+
						"into the step result type: %v", stepID, err)}
			}
			return r, nil
		}
	}

	if rs.Draining() != nil && rs.Draining().Load() {
		return zero, &errs.DrainOrphan{}
	}

	argsRaw, err := serde.Marshal(a)
	if err != nil {
		return zero, &errs.EverystepError{Msg: fmt.Sprintf("step %q arguments are not serializable: %v", name, err)}
	}

	if unsafeToRepeat && rs.Persistent() {
		// Claim the right to perform the effect before performing it: the
		// unique (workflow, step_id) constraint makes this atomic, and the
		// started row outlives a crash in the effect window.
		err := store.CreateStep(ctx, rs.DB(), &store.Step{
			Workflow: store.UUID{UUID: rs.WorkflowID()},
			StepID:   stepID,
			Name:     name,
			Args:     argsRaw,
			Status:   store.StepStarted,
		})
		if err != nil {
			if errors.Is(err, gorm.ErrDuplicatedKey) {
				// A racing claimant holds the effect.
				return zero, &errs.EffectUncertain{Msg: fmt.Sprintf(
					"step %q (%s) is marked unsafe to repeat and its outcome is uncertain: "+
						"another attempt is already in flight (run %s). Verify it in the "+
						"external system, then resolve the run with es.ResolveStep.",
					stepID, name, rs.WorkflowID())}
			}
			return zero, err
		}
	}

	started := time.Now()
	ctx2, span := traces.StepSpan(ctx, name, stepID, rs.Persistent())
	var (
		result  R
		callErr error
	)
	result, callErr = fn(ctx2, a)
	if callErr != nil {
		traces.MarkError(span, callErr)
	} else {
		traces.MarkOK(span)
	}
	span.End()
	duration := time.Since(started)

	if callErr == nil {
		if _, merr := serde.Marshal(result); merr != nil {
			return zero, &errs.EverystepError{Msg: fmt.Sprintf("step %q result is not serializable: %v", name, merr)}
		}
	}

	if rs.Persistent() {
		if f := getFault(); f != nil {
			if cerr := f(ctx, stepID); cerr != nil {
				return zero, cerr
			}
		}
		status := store.StepDone
		var (
			resultRaw []byte
			errRaw    []byte
			enc       *serde.EncodedError
		)
		if callErr != nil {
			status = store.StepFailed
			e := serde.EncodeError(callErr)
			enc = &e
			errRaw, _ = json.Marshal(e)
		} else {
			resultRaw, _ = serde.Marshal(result)
		}
		if hit || unsafeToRepeat {
			// A started row already exists: either this attempt claimed the
			// effect before running it, or a previous attempt left one
			// behind and the step no longer marks itself unsafe to repeat.
			// Close it out in place.
			if err := store.FinishStep(ctx, rs.DB(), store.UUID{UUID: rs.WorkflowID()}, stepID, status, resultRaw, errRaw); err != nil {
				return zero, err
			}
		} else {
			if err := store.CreateStep(ctx, rs.DB(), &store.Step{
				Workflow: store.UUID{UUID: rs.WorkflowID()},
				StepID:   stepID,
				Name:     name,
				Args:     argsRaw,
				Status:   status,
				Result:   resultRaw,
				Error:    errRaw,
			}); err != nil {
				return zero, err
			}
		}
		metrics.RecordStep(rs.Workflow(), name, status, duration.Seconds())
		// Keep the shared outcome store current, so a later step can read
		// this step's outcome via es.Outcome/es.Result. On a replay the
		// outcome is already present from the claim-time snapshot.
		rs.Outcomes().Set(stepID, esc.Outcome{Name: name, Status: status, Result: resultRaw, Error: enc})
	}

	if callErr != nil {
		return zero, callErr
	}
	return result, nil
}

// Execute runs a claimed workflow to a terminal state, re-raising
// SimulatedCrash and DrainOrphan to the caller (the worker requeues on the
// latter). draining is the stop signal the runner checks at step boundaries.
func Execute(ctx context.Context, db *gorm.DB, runID uuid.UUID, draining *atomic.Bool) error {
	started := time.Now()
	wf, err := store.GetWorkflow(ctx, db, store.UUID{UUID: runID})
	if err != nil {
		return err
	}
	body, ok := registry.ResolveWorkflow(wf.Name)
	if !ok {
		return fmt.Errorf("everystep cannot resolve workflow %q", wf.Name)
	}
	steps, err := store.ListSteps(ctx, db, store.UUID{UUID: runID})
	if err != nil {
		return err
	}

	outcomes := esc.NewOutcomeStore()
	for _, s := range steps {
		o := esc.Outcome{Name: s.Name, Status: s.Status, Result: []byte(s.Result)}
		if len(s.Error) > 0 {
			var enc serde.EncodedError
			if uerr := json.Unmarshal(s.Error, &enc); uerr == nil {
				o.Error = &enc
			}
		}
		outcomes.Set(s.StepID, o)
	}

	rs := esc.NewRun(runID, wf.Name, db, outcomes, draining)
	ctx = esc.With(ctx, rs)

	ctx, span := traces.WorkflowSpan(ctx, wf.Name, runID)
	defer span.End()

	result, bodyErr := body(ctx, json.RawMessage(wf.Args))
	if bodyErr == nil {
		raw, merr := serde.Marshal(result)
		if merr != nil {
			bodyErr = &errs.EverystepError{Msg: fmt.Sprintf(
				"workflow result for %q is not serializable: %v", wf.Name, merr)}
		} else {
			now := time.Now().UTC()
			updated, uerr := store.SetTerminal(ctx, db, store.UUID{UUID: runID}, store.StatusCompleted, raw, nil, now)
			if uerr != nil {
				return uerr
			}
			if updated {
				metrics.RecordWorkflowTerminal(wf.Name, store.StatusCompleted, time.Since(started).Seconds())
				span.SetAttributes(attribute.String("everystep.workflow.status", store.StatusCompleted))
				traces.MarkOK(span)
			}
			return nil
		}
	}

	now := time.Now().UTC()
	var (
		crash     *errs.SimulatedCrash
		orphan    *errs.DrainOrphan
		terminal  *errs.Terminal
		uncertain *errs.EffectUncertain
	)
	switch {
	case errors.As(bodyErr, &crash):
		span.SetAttributes(attribute.String("everystep.workflow.status", store.StatusRunning))
		return bodyErr
	case errors.As(bodyErr, &orphan):
		span.SetAttributes(attribute.String("everystep.workflow.status", store.StatusRunning))
		return bodyErr
	case errors.As(bodyErr, &terminal):
		updated, uerr := store.SetTerminal(ctx, db, store.UUID{UUID: runID}, store.StatusStopped, nil, encodeRaw(bodyErr), now)
		if uerr != nil {
			return uerr
		}
		if updated {
			metrics.RecordWorkflowTerminal(wf.Name, store.StatusStopped, time.Since(started).Seconds())
			span.SetAttributes(attribute.String("everystep.workflow.status", store.StatusStopped))
			traces.MarkOK(span)
		}
		return nil
	case errors.As(bodyErr, &uncertain):
		updated, uerr := store.SetTerminal(ctx, db, store.UUID{UUID: runID}, store.StatusBlocked, nil, encodeRaw(bodyErr), now)
		if uerr != nil {
			return uerr
		}
		if updated {
			metrics.RecordWorkflowTerminal(wf.Name, store.StatusBlocked, time.Since(started).Seconds())
			span.SetAttributes(attribute.String("everystep.workflow.status", store.StatusBlocked))
			traces.MarkOK(span)
		}
		return nil
	default:
		ReportFailure(ctx, bodyErr, FailureMeta{WorkflowID: runID, Workflow: wf.Name})
		updated, uerr := store.SetTerminal(ctx, db, store.UUID{UUID: runID}, store.StatusFailed, nil, encodeRaw(bodyErr), now)
		if uerr != nil {
			return uerr
		}
		if updated {
			metrics.RecordWorkflowTerminal(wf.Name, store.StatusFailed, time.Since(started).Seconds())
			span.SetAttributes(attribute.String("everystep.workflow.status", store.StatusFailed))
			traces.MarkError(span, bodyErr)
		}
		return nil
	}
}

func encodeRaw(err error) []byte {
	enc := serde.EncodeError(err)
	raw, _ := json.Marshal(enc)
	return raw
}
