package everystep

import (
	"context"
	"errors"
	"sync"

	esc "github.com/brutasse/everystep-go/internal/ctx"
	"github.com/brutasse/everystep-go/traces"
)

// Branch is one arm of a parallel fork. Results are captured in the
// enclosing scope — the standard errgroup pattern. A branch that does
// several steps is just a closure that calls them in order.
type Branch func(context.Context) error

type parallel struct {
	ctx      context.Context
	forkName *string
}

// ForkOpt configures a Parallel fork.
type ForkOpt func(*forkConfig)

type forkConfig struct {
	forkID *string
}

// ForkID gives the fork a stable name, so the branch step ids
// ("fanout.0.1", "fanout.1.1") do not shift when steps before the fork
// change. Fork ids follow the same rules and scope as step names: a fork
// named "fanout" and a step named "fanout" in the same body is an error;
// the same name in different branches is fine.
func ForkID(id string) ForkOpt {
	return func(c *forkConfig) { c.forkID = &id }
}

// Parallel starts a fork: the branches run in goroutines, each with a
// child context whose run state prefix is "forkID.index.". A fork id
// follows the step naming rules and consumes a position in the scope.
// Outside a running workflow the branches execute plainly on goroutines.
func Parallel(ctx context.Context, opts ...ForkOpt) *parallel {
	c := &forkConfig{}
	for _, o := range opts {
		o(c)
	}
	return &parallel{ctx: ctx, forkName: c.forkID}
}

// Run executes the branches concurrently and returns their errors. A lone
// branch failure is returned as-is; several are joined with errors.Join —
// Go's native ExceptionGroup. *Terminal, *EffectUncertain and *DrainOrphan
// short-circuit out of the join, as in Python.
func (p *parallel) Run(branches ...Branch) error {
	if len(branches) == 0 {
		panic("everystep: parallel requires at least one branch")
	}
	rs := esc.From(p.ctx)
	if rs == nil {
		rs = esc.NewTransient()
		p.ctx = esc.With(p.ctx, rs)
	}
	forkID, err := rs.NextID(p.forkName)
	if err != nil {
		return err
	}
	ctx, span := traces.ForkSpan(p.ctx, forkID, rs.Persistent())
	defer span.End()

	errs := make([]error, len(branches))
	var wg sync.WaitGroup
	for i, b := range branches {
		i, b := i, b
		branchCtx := esc.With(ctx, rs.Branch(forkID, i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = b(branchCtx)
		}()
	}
	wg.Wait()

	var failed []error
	for _, e := range errs {
		if e != nil {
			failed = append(failed, e)
		}
	}
	if len(failed) == 0 {
		return nil
	}
	for _, e := range failed {
		var (
			terminal  *Terminal
			uncertain *EffectUncertain
			orphan    *DrainOrphan
		)
		if errors.As(e, &terminal) || errors.As(e, &uncertain) || errors.As(e, &orphan) {
			return e
		}
	}
	if len(failed) == 1 {
		return failed[0]
	}
	return errors.Join(failed...)
}
