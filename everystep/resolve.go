package everystep

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	errs "github.com/brutasse/everystep-go/internal/errors"
	"github.com/brutasse/everystep-go/serde"
	"github.com/brutasse/everystep-go/store"
)

// ResolveOpt selects and configures one ResolveStep operation; exactly
// one is required.
type ResolveOpt func(*resolveConfig)

type resolveConfig struct {
	mode        store.ResolveMode
	resultValue any
	errMsg      string
	set         bool
}

// WithResult records the effect's result on the step: the effect happened.
// The value is serialized with everystep's serde rules; a value that is
// not storable is an error.
func WithResult(v any) ResolveOpt {
	return func(c *resolveConfig) {
		c.mode = store.ResolveResult
		c.resultValue = v
		c.set = true
	}
}

// WithError records a failure on the step, so the workflow's durable
// cleanup (try/except) replays on the next pass: the effect happened and
// failed. The failure is stored as a *StepFailure carrying the message,
// so the body re-raises that type on replay.
func WithError(msg string) ResolveOpt {
	return func(c *resolveConfig) {
		c.mode = store.ResolveError
		c.errMsg = msg
		c.set = true
	}
}

// Discard deletes the started row so the step re-runs on the next claim:
// the effect did not happen. Only a human who checked the external system
// may assert that.
func Discard() ResolveOpt {
	return func(c *resolveConfig) {
		c.mode = store.ResolveDiscard
		c.set = true
	}
}

// ResolveStep resolves a run blocked on an unsafe-to-repeat step: record
// the effect's result or failure, or discard the unrecorded attempt so
// the step re-runs. Nothing here re-runs the step's side effect on its
// own. Only a blocked run's started step can be resolved; each resolution
// clears the run's error and completed_at, releases the claim, and returns
// the run to scheduled.
//
// The crash can also land before the effect — between the started insert
// and the call — leaving a blocked run whose effect never happened;
// verify in the external system and resolve with Discard.
func ResolveStep(ctx context.Context, db *gorm.DB, runID uuid.UUID, stepID string, opts ...ResolveOpt) error {
	c := &resolveConfig{}
	for _, o := range opts {
		o(c)
	}
	if !c.set {
		return &errs.EverystepError{Msg: "exactly one of WithResult, WithError or Discard is required"}
	}
	var result, errRaw []byte
	switch c.mode {
	case store.ResolveResult:
		raw, err := serde.Marshal(c.resultValue)
		if err != nil {
			return &errs.EverystepError{Msg: fmt.Sprintf("resolve: result is not storable: %v", err)}
		}
		result = raw
	case store.ResolveError:
		enc := serde.EncodeError(&errs.StepFailure{Msg: c.errMsg})
		errRaw, _ = json.Marshal(enc)
	}
	return store.ResolveStep(ctx, db, store.UUID{UUID: runID}, stepID, c.mode, result, errRaw)
}
