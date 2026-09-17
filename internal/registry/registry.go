// Package registry holds the step and workflow name tables.
//
// Names are the identity contract of a replay engine: a step or workflow
// with a duplicate name would make two distinct functions share one
// recorded identity, which surfaces later as a WorkflowCodeError mid-replay
// of an in-flight run. So duplicates panic at registration (init time)
// instead.
package registry

import (
	"context"
	"encoding/json"
	"fmt"
)

// Body is the uniform workflow body: decode the stored args, run the typed
// body, return the result. The runner calls a body without knowing the
// args and result types.
type Body func(ctx context.Context, args json.RawMessage) (any, error)

var (
	steps     = map[string]bool{}
	workflows = map[string]Body{}
)

// RegisterStep records a step name. Panics if a step or workflow already
// uses the name.
func RegisterStep(name string) {
	if steps[name] || workflows[name] != nil {
		panic(fmt.Sprintf("everystep: duplicate step or workflow name %q", name))
	}
	steps[name] = true
}

// RegisterWorkflow records a workflow body under its name. Panics if a
// step or workflow already uses the name.
func RegisterWorkflow(name string, body Body) {
	if steps[name] || workflows[name] != nil {
		panic(fmt.Sprintf("everystep: duplicate step or workflow name %q", name))
	}
	workflows[name] = body
}

// ResolveWorkflow returns the body registered under name.
func ResolveWorkflow(name string) (Body, bool) {
	b, ok := workflows[name]
	return b, ok
}
