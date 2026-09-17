package tests

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"gorm.io/gorm"

	es "github.com/brutasse/everystep-go/everystep"
	"github.com/brutasse/everystep-go/store"
)

// The failure reporter is the Go replacement for the Python suite's
// sentry_sdk stand-in: everystep never reports control-flow errors, only
// unhandled workflow failures, identified by their run.

type rbEvent struct {
	err  error
	meta es.FailureMeta
}

type rbRecorder struct {
	mu     sync.Mutex
	events []rbEvent
}

func (r *rbRecorder) add(err error, meta es.FailureMeta) {
	r.mu.Lock()
	r.events = append(r.events, rbEvent{err: err, meta: meta})
	r.mu.Unlock()
}

func (r *rbRecorder) list() []rbEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]rbEvent, len(r.events))
	copy(out, r.events)
	return out
}

func (r *rbRecorder) reset() {
	r.mu.Lock()
	r.events = nil
	r.mu.Unlock()
}

var rbReporter = &rbRecorder{}

func installReporter(t *testing.T) {
	t.Helper()
	rbReporter.reset()
	es.SetFailureReporter(func(ctx context.Context, err error, meta es.FailureMeta) {
		rbReporter.add(err, meta)
	})
	t.Cleanup(func() {
		es.SetFailureReporter(nil)
		rbReporter.reset()
	})
}

type rbBoomError string

func (e rbBoomError) Error() string { return string(e) }

func rbBoomFn(ctx context.Context, a struct{}) (string, error) {
	return "", rbBoomError("kaboom")
}

var rbBoom = es.Step("rb.boom", rbBoomFn)

func rbFailingBody(ctx context.Context, a struct{}) (string, error) {
	return rbBoom.Call(ctx, struct{}{})
}

var rbFailing = es.Workflow("rb.failing", rbFailingBody)

func rbFlakyFn(ctx context.Context, a struct{}) (string, error) {
	return "", rbBoomError("handled")
}

var rbFlaky = es.Step("rb.flaky", rbFlakyFn)

func rbRescuedBody(ctx context.Context, a struct{}) (string, error) {
	_, err := rbFlaky.Call(ctx, struct{}{})
	if err != nil {
		var b rbBoomError
		if errors.As(err, &b) {
			return "rescued", nil
		}
		return "", err
	}
	return "unreachable", nil
}

var rbRescued = es.Workflow("rb.rescued", rbRescuedBody)

func rbTFirstFn(ctx context.Context, a struct{}) (string, error) {
	return "1", nil
}

func rbTSecondFn(ctx context.Context, a struct{}) (string, error) {
	return "", &es.Terminal{Reason: "stop", Data: map[string]any{"x": 1}}
}

var (
	rbTFirst  = es.Step("rb.t_first", rbTFirstFn)
	rbTSecond = es.Step("rb.t_second", rbTSecondFn)
)

func rbTFlowBody(ctx context.Context, a struct{}) (string, error) {
	if _, err := rbTFirst.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	if _, err := rbTSecond.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	return "ok", nil
}

var rbTFlow = es.Workflow("rb.t_flow", rbTFlowBody)

// rbFunc is not JSON-serializable: a workflow result of this type makes the
// runner itself fail the run (the Python blobby workflow's object() result).
type rbFunc func()

func rbBlobbyBody(ctx context.Context, a struct{}) (rbFunc, error) {
	return func() {}, nil
}

var rbBlobby = es.Workflow("rb.blobby", rbBlobbyBody)

func rbRiskyFn(ctx context.Context, a struct{}) (string, error) {
	return "r", nil
}

var rbRisky = es.Step("rb.risky", rbRiskyFn, es.UnsafeToRepeat())

func rbRiskyFlowBody(ctx context.Context, a struct{}) (string, error) {
	return rbRisky.Call(ctx, struct{}{})
}

var rbRiskyFlow = es.Workflow("rb.risky_flow", rbRiskyFlowBody)

func TestUnhandledFailureIsReported(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		installReporter(t)
		id, err := rbFailing.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		if err := runnerExecute(t, db, id); err != nil {
			t.Fatal(err)
		}
		if wf := getRun(t, db, id); wf.Status != store.StatusFailed {
			t.Fatalf("status %s, want failed", wf.Status)
		}
		events := rbReporter.list()
		if len(events) != 1 {
			t.Fatalf("got %d reported failures, want 1", len(events))
		}
		var b rbBoomError
		if !errors.As(events[0].err, &b) || string(b) != "kaboom" {
			t.Fatalf("reported error %v, want kaboom", events[0].err)
		}
		if events[0].meta.Workflow != "rb.failing" {
			t.Fatalf("meta workflow %q, want rb.failing", events[0].meta.Workflow)
		}
		if events[0].meta.WorkflowID != id {
			t.Fatalf("meta workflow id %s, want %s", events[0].meta.WorkflowID, id)
		}
	})
}

func TestHandledFailureIsNotReported(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		installReporter(t)
		id, err := rbRescued.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		wf := runToCompletion(t, db, id)
		if wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}
		if events := rbReporter.list(); len(events) != 0 {
			t.Fatalf("handled failure was reported: %v", events)
		}
	})
}

func TestSimulatedCrashIsNotReported(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		installReporter(t)
		id, err := rbFailing.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "1")
		expectCrash(t, db, id)
		if events := rbReporter.list(); len(events) != 0 {
			t.Fatalf("crash was reported: %v", events)
		}
	})
}

func TestTerminalIsNotReported(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		installReporter(t)
		id, err := rbTFlow.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		if err := runnerExecute(t, db, id); err != nil {
			t.Fatal(err)
		}
		if wf := getRun(t, db, id); wf.Status != store.StatusStopped {
			t.Fatalf("status %s, want stopped", wf.Status)
		}
		if events := rbReporter.list(); len(events) != 0 {
			t.Fatalf("terminal was reported: %v", events)
		}
	})
}

func TestRunnerEscapeIsReported(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		installReporter(t)
		id, err := rbBlobby.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		if err := runnerExecute(t, db, id); err != nil {
			t.Fatal(err)
		}
		if wf := getRun(t, db, id); wf.Status != store.StatusFailed {
			t.Fatalf("status %s, want failed", wf.Status)
		}
		events := rbReporter.list()
		if len(events) != 1 {
			t.Fatalf("got %d reported failures, want 1", len(events))
		}
		if !strings.Contains(events[0].err.Error(), "not serializable") {
			t.Fatalf("reported error %v, want the not-serializable message", events[0].err)
		}
		if events[0].meta.Workflow != "rb.blobby" {
			t.Fatalf("meta workflow %q, want rb.blobby", events[0].meta.Workflow)
		}
		if events[0].meta.WorkflowID != id {
			t.Fatalf("meta workflow id %s, want %s", events[0].meta.WorkflowID, id)
		}
	})
}

func TestBlockedRunIsNotReported(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		installReporter(t)
		id, err := rbRiskyFlow.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "1")
		expectCrash(t, db, id)

		reClaim(t, db, id)
		if err := runnerExecute(t, db, id); err != nil {
			t.Fatal(err)
		}
		if wf := getRun(t, db, id); wf.Status != store.StatusBlocked {
			t.Fatalf("status %s, want blocked", wf.Status)
		}
		if events := rbReporter.list(); len(events) != 0 {
			t.Fatalf("blocked run was reported: %v", events)
		}
	})
}

func TestNoReporterIsANoop(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		// No reporter installed: the failure is recorded in the run row and
		// nothing else happens.
		id, err := rbFailing.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		if err := runnerExecute(t, db, id); err != nil {
			t.Fatal(err)
		}
		if wf := getRun(t, db, id); wf.Status != store.StatusFailed {
			t.Fatalf("status %s, want failed", wf.Status)
		}
	})
}
