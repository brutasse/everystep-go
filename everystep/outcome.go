package everystep

import (
	"context"
	"fmt"

	esc "github.com/brutasse/everystep-go/internal/ctx"
	errs "github.com/brutasse/everystep-go/internal/errors"
	"github.com/brutasse/everystep-go/serde"
)

// StepOutcome is a recorded step outcome: what a step id maps to in the
// outcomes store. The shared store holds every outcome recorded before the
// current claim plus those completed earlier in this claim; treat it as
// read-only. Name the steps you read.
type StepOutcome = esc.Outcome

// Outcome returns the recorded outcome for a step id, if any.
func Outcome(ctx context.Context, id string) (*StepOutcome, bool) {
	rs := esc.From(ctx)
	if rs == nil {
		return nil, false
	}
	o, ok := rs.Outcomes().Get(id)
	if !ok {
		return nil, false
	}
	return &o, true
}

// Result decodes the recorded result of a step into dest.
func Result(ctx context.Context, id string, dest any) error {
	o, ok := Outcome(ctx, id)
	if !ok {
		return &errs.EverystepError{Msg: fmt.Sprintf("no outcome recorded for step %q", id)}
	}
	return serde.Unmarshal(o.Result, dest)
}
