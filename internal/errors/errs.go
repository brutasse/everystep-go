// Package errs defines everystep's error types.
//
// EverystepError is the base for general engine errors (validation,
// serialization). The remaining types are either control-flow errors the
// engine raises and classifies (Terminal, EffectUncertain, DrainOrphan,
// SimulatedCrash) or replay errors (StepFailure, WorkflowCodeError).
//
// Errors that must survive a replay implement Replayable and are bound to a
// decoder with errs.Register; anything else degrades to *StepFailure on
// replay.
package errs

import (
	"encoding/json"
	"sync"
)

// Replayable marks an error that survives a replay. When the engine encodes
// a step or workflow failure it walks the error chain for this interface and
// stores the code, message and payload; on replay the decoder registered
// under the code rebuilds the error so the workflow body can match it with
// errors.Is/errors.As exactly as if the step had raised it fresh.
type Replayable interface {
	error
	// Code returns the stable identifier of the error, e.g.
	// "cloud.out_of_capacity".
	Code() string
	// Payload returns the JSON-serializable fields carried across the
	// replay, or nil.
	Payload() any
}

// Codes of the errors everystep itself raises and records. Their decoders
// are registered by serde, so they rebuild on replay without the host
// doing anything.
const (
	CodeEverystep         = "everystep.error"
	CodeTerminal          = "everystep.terminal"
	CodeWorkflowCodeError = "everystep.workflow_code_error"
	CodeStepFailure       = "everystep.step_failure"
	CodeEffectUncertain   = "everystep.effect_uncertain"
)

// EverystepError is a general engine error.
type EverystepError struct {
	Msg string
}

func (e *EverystepError) Error() string { return e.Msg }
func (e *EverystepError) Code() string  { return CodeEverystep }
func (e *EverystepError) Payload() any  { return nil }

// Terminal stops a workflow for a known reason. The step that raises it is
// recorded as failed and the run ends in the stopped status. It is a control
// flow, not a failure: it is never reported to the failure reporter.
type Terminal struct {
	Reason string
	Data   any // JSON-serializable
}

func (t *Terminal) Error() string { return t.Reason }
func (t *Terminal) Code() string  { return CodeTerminal }
func (t *Terminal) Payload() any  { return t.Data }

// WorkflowCodeError: a workflow body diverged from its previously recorded
// step identities (a step at an id has a different name, or a stored result
// no longer decodes into the step's result type).
type WorkflowCodeError struct {
	Msg string
}

func (e *WorkflowCodeError) Error() string { return e.Msg }
func (e *WorkflowCodeError) Code() string  { return CodeWorkflowCodeError }
func (e *WorkflowCodeError) Payload() any  { return nil }

// StepFailure is a recorded step failure whose original error is
// unavailable, or the failure recorded on a step resolved with an error
// message.
type StepFailure struct {
	Msg string
}

func (e *StepFailure) Error() string { return e.Msg }
func (e *StepFailure) Code() string  { return CodeStepFailure }
func (e *StepFailure) Payload() any  { return nil }

// EffectUncertain: a step marked unsafe to repeat may have performed its
// effect but its outcome was never recorded (the worker died in the effect
// window, or a racing claimant holds the effect). The engine refuses to
// re-execute it and ends the run in the blocked status — a holding state,
// never a failure: it is not reported to the failure reporter. The run is
// resolved with ResolveStep.
type EffectUncertain struct {
	Msg string
}

func (e *EffectUncertain) Error() string { return e.Msg }
func (e *EffectUncertain) Code() string  { return CodeEffectUncertain }
func (e *EffectUncertain) Payload() any  { return nil }

// DrainOrphan is raised at a step boundary when the runner is draining
// after a stop signal. The step in flight at the signal finishes and is
// recorded; no new step starts. The worker requeues the run so any runner
// can claim it and resume it from the recorded steps.
type DrainOrphan struct{}

func (e *DrainOrphan) Error() string { return "drained at a step boundary; run requeued" }

// SimulatedCrash is raised from the test fault hook to simulate a process
// death mid-step. The runner re-raises it without writing any further
// state, leaving the workflow exactly as it would be if the worker had
// died.
type SimulatedCrash struct{}

func (e *SimulatedCrash) Error() string { return "simulated crash" }

// DecodeFunc rebuilds a stored error from its message and payload. It
// returns the reconstructed error, or nil if it cannot be built.
type DecodeFunc func(message string, payload json.RawMessage) error

var (
	decMu    sync.RWMutex
	decoders = map[string]DecodeFunc{}
)

// Register binds a decoder to an error code.
func Register(code string, decode DecodeFunc) {
	decMu.Lock()
	decoders[code] = decode
	decMu.Unlock()
}

// Lookup returns the decoder bound to code.
func Lookup(code string) (DecodeFunc, bool) {
	decMu.RLock()
	f, ok := decoders[code]
	decMu.RUnlock()
	return f, ok
}
