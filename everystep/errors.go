// Package everystep is the public API: steps, workflows, parallel forks,
// scheduling and result reading for durable, replay-based execution.
package everystep

import (
	"encoding/json"

	errs "github.com/brutasse/everystep-go/internal/errors"
	"github.com/brutasse/everystep-go/serde"
)

// Error types, re-exported from the internal errors package.
type (
	// EverystepError is a general engine error: validation, serialization.
	EverystepError = errs.EverystepError
	// Terminal stops a workflow for a known reason; the run ends stopped.
	Terminal = errs.Terminal
	// WorkflowCodeError: the body diverged from its recorded identities.
	WorkflowCodeError = errs.WorkflowCodeError
	// StepFailure: a recorded failure whose original error is unavailable.
	StepFailure = errs.StepFailure
	// EffectUncertain: an unsafe-to-repeat step may have run; the run is
	// blocked until ResolveStep decides.
	EffectUncertain = errs.EffectUncertain
	// DrainOrphan: raised at a step boundary while the worker drains; the
	// run is requeued for any runner.
	DrainOrphan = errs.DrainOrphan
	// Replayable marks an error that survives a replay via a registered
	// decoder.
	Replayable = errs.Replayable
)

// RegisterError binds a decoder to an error code so errors with that code
// rebuild on replay and the workflow body can match them with
// errors.Is/errors.As exactly as if the step had raised them fresh. The
// errors everystep itself raises are already registered by serde.
func RegisterError(code string, decode func(message string, payload json.RawMessage) error) {
	serde.RegisterError(code, decode)
}
