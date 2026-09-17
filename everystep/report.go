package everystep

import (
	"context"

	"github.com/brutasse/everystep-go/runner"
)

// FailureMeta identifies the run a failure reporter is called for.
type FailureMeta = runner.FailureMeta

// SetFailureReporter installs the hook called for every unhandled workflow
// failure. Control-flow errors (Terminal, EffectUncertain, DrainOrphan,
// SimulatedCrash) are never reported. EffectUncertain is a holding state
// (the run is blocked, awaiting ResolveStep), not a failure.
func SetFailureReporter(f func(ctx context.Context, err error, meta FailureMeta)) {
	runner.SetFailureReporter(f)
}
