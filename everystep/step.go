package everystep

import (
	"context"

	esc "github.com/brutasse/everystep-go/internal/ctx"
	"github.com/brutasse/everystep-go/internal/registry"
	"github.com/brutasse/everystep-go/runner"
)

// step is the registered form of a durable step.
type step[R, A any] struct {
	name   string
	unsafe bool
	fn     func(context.Context, A) (R, error)
}

// Step is a recorded unit of work. The name is mandatory and is the step's
// identity across replays: there is no Go equivalent of a stable function
// location, so identity is a declared contract, checked at registration —
// two steps, or a step and a workflow, with the same name panic at init.
//
// A step takes a context and one args value and returns one result.
// Zero-arg steps use struct{} for A. The step's type is unexported; a
// step is defined with Step and used through its Call method.

// StepOpt configures a step at definition time.
type StepOpt func(*stepConfig)

type stepConfig struct {
	unsafe bool
}

// UnsafeToRepeat marks a step whose side effect must not happen twice: the
// step row is recorded started before the effect runs, and the unique
// (workflow, step_id) constraint makes that insert an atomic claim — a
// racing claimant gets the constraint violation, not a second effect. A
// replay that finds started without an outcome returns *EffectUncertain,
// which parks the run blocked instead of executing the effect again.
func UnsafeToRepeat() StepOpt {
	return func(c *stepConfig) { c.unsafe = true }
}

// Step defines a durable step under a mandatory name.
func Step[R, A any](name string, fn func(context.Context, A) (R, error), opts ...StepOpt) step[R, A] {
	registry.RegisterStep(name)
	c := &stepConfig{}
	for _, o := range opts {
		o(c)
	}
	return step[R, A]{name: name, unsafe: c.unsafe, fn: fn}
}

// CallOpt configures one step call.
type CallOpt func(*callConfig)

type callConfig struct {
	name *string
}

// Named gives the call a stable step identity (the everystep_id of the
// Python version): non-empty, no dots, no whitespace, not purely numeric,
// unique per scope, and the dotpath is bounded to 300 characters. A named
// step still consumes a position, so naming never shifts other ids.
// Violations surface as *EverystepError before the step runs.
func Named(id string) CallOpt {
	return func(c *callConfig) { c.name = &id }
}

// Call runs the step. Inside a running workflow the call is recorded: the
// outcome is persisted in SQL and served from the store on replay, so the
// function body only runs for unrecorded steps. Called outside a running
// workflow it executes the function plainly, with no recording.
func (s step[R, A]) Call(ctx context.Context, a A, opts ...CallOpt) (R, error) {
	if esc.From(ctx) == nil {
		return s.fn(ctx, a)
	}
	c := &callConfig{}
	for _, o := range opts {
		o(c)
	}
	return runner.RunStep(ctx, s.name, s.unsafe, a, c.name, s.fn)
}
