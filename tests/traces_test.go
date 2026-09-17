package tests

import (
	"context"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"gorm.io/gorm"

	es "github.com/brutasse/everystep-go/everystep"
	"github.com/brutasse/everystep-go/store"
)

// memSpanExporter collects finished spans in memory.
type memSpanExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (e *memSpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	e.spans = append(e.spans, spans...)
	e.mu.Unlock()
	return nil
}

func (e *memSpanExporter) Shutdown(context.Context) error { return nil }

func (e *memSpanExporter) get() []sdktrace.ReadOnlySpan {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]sdktrace.ReadOnlySpan, len(e.spans))
	copy(out, e.spans)
	return out
}

func (e *memSpanExporter) clear() {
	e.mu.Lock()
	e.spans = nil
	e.mu.Unlock()
}

var tbExporter = &memSpanExporter{}

func init() {
	// The global tracer is a delegator: a provider installed here is used
	// for spans started before or after, so install order does not matter.
	otel.SetTracerProvider(sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(tbExporter)),
	))
}

func tbSpans() []sdktrace.ReadOnlySpan { return tbExporter.get() }

func tbRoots(spans []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range spans {
		if !s.Parent().IsValid() {
			out = append(out, s)
		}
	}
	return out
}

func tbChildren(spans []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Parent().IsValid() {
			out = append(out, s)
		}
	}
	return out
}

func tbAttr(t *testing.T, span sdktrace.ReadOnlySpan, key string) string {
	t.Helper()
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	t.Fatalf("span %q has no attribute %s", span.Name(), key)
	return ""
}

func tbHasException(span sdktrace.ReadOnlySpan) bool {
	for _, e := range span.Events() {
		if e.Name == "exception" {
			return true
		}
	}
	return false
}

func tbClear(t *testing.T) {
	t.Helper()
	tbExporter.clear()
	t.Cleanup(tbExporter.clear)
}

func tbOKStepFn(ctx context.Context, a struct{}) (string, error) {
	return "ok", nil
}

func tbOtherStepFn(ctx context.Context, a struct{}) (string, error) {
	return "other", nil
}

func tbBadStepFn(ctx context.Context, a struct{}) (string, error) {
	return "", esBoomError("boom")
}

func tbStopStepFn(ctx context.Context, a struct{}) (string, error) {
	return "", &es.Terminal{Reason: "known-reason", Data: map[string]any{"k": "v"}}
}

var (
	tbOKStep    = es.Step("tb.ok_step", tbOKStepFn)
	tbOtherStep = es.Step("tb.other_step", tbOtherStepFn)
	tbBadStep   = es.Step("tb.bad_step", tbBadStepFn)
	tbStopStep  = es.Step("tb.stop_step", tbStopStepFn)
)

func tbGoodBody(ctx context.Context, a struct{}) (string, error) {
	return tbOKStep.Call(ctx, struct{}{})
}

var tbGood = es.Workflow("tb.good", tbGoodBody)

func tbBadBody(ctx context.Context, a struct{}) (string, error) {
	return tbBadStep.Call(ctx, struct{}{})
}

var tbBad = es.Workflow("tb.bad", tbBadBody)

func tbStoppedBody(ctx context.Context, a struct{}) (string, error) {
	return tbStopStep.Call(ctx, struct{}{})
}

var tbStopped = es.Workflow("tb.stopped_wf", tbStoppedBody)

func tbTwoStepsBody(ctx context.Context, a struct{}) (string, error) {
	if _, err := tbOKStep.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	if _, err := tbOtherStep.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	return "done", nil
}

var tbTwoSteps = es.Workflow("tb.two_steps", tbTwoStepsBody)

func tbFanoutBody(ctx context.Context, a struct{}) ([]string, error) {
	var av, bv string
	err := es.Parallel(ctx, es.ForkID("fan")).Run(
		func(c context.Context) error {
			var e error
			av, e = tbOKStep.Call(c, struct{}{})
			return e
		},
		func(c context.Context) error {
			var e error
			bv, e = tbOtherStep.Call(c, struct{}{})
			return e
		},
	)
	if err != nil {
		return nil, err
	}
	return []string{av, bv}, nil
}

var tbFanout = es.Workflow("tb.fanout", tbFanoutBody)

func TestCompletedRunHasRunAndStepSpans(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		tbClear(t)
		id, err := tbGood.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		wf := runToCompletion(t, db, id)
		if wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}

		finished := tbSpans()
		roots := tbRoots(finished)
		if len(roots) != 1 {
			t.Fatalf("got %d root spans, want 1", len(roots))
		}
		root := roots[0]
		if root.Name() != "tb.good" {
			t.Fatalf("root span name %q, want tb.good", root.Name())
		}
		if got := tbAttr(t, root, "everystep.workflow.id"); got != id.String() {
			t.Fatalf("workflow id attr %q, want %s", got, id)
		}
		if got := tbAttr(t, root, "everystep.workflow.status"); got != "completed" {
			t.Fatalf("workflow status attr %q, want completed", got)
		}
		if root.Status().Code != codes.Ok {
			t.Fatalf("root status %v, want OK", root.Status().Code)
		}

		steps := tbChildren(finished)
		if len(steps) != 1 {
			t.Fatalf("got %d step spans, want 1", len(steps))
		}
		stepSpan := steps[0]
		if stepSpan.Name() != "tb.ok_step" {
			t.Fatalf("step span name %q, want tb.ok_step", stepSpan.Name())
		}
		if got := tbAttr(t, stepSpan, "everystep.step.id"); got != "1" {
			t.Fatalf("step id attr %q, want 1", got)
		}
		if stepSpan.Status().Code != codes.Ok {
			t.Fatalf("step status %v, want OK", stepSpan.Status().Code)
		}
		if stepSpan.Parent().SpanID() != root.SpanContext().SpanID() {
			t.Fatal("step span is not a child of the run span")
		}
		if stepSpan.SpanContext().TraceID() != root.SpanContext().TraceID() {
			t.Fatal("step span is not in the run's trace")
		}
	})
}

func TestFailedRunMarksErrors(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		tbClear(t)
		id, err := tbBad.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, id)

		finished := tbSpans()
		root := tbRoots(finished)[0]
		if got := tbAttr(t, root, "everystep.workflow.status"); got != "failed" {
			t.Fatalf("workflow status attr %q, want failed", got)
		}
		if root.Status().Code != codes.Error {
			t.Fatalf("root status %v, want ERROR", root.Status().Code)
		}
		if !tbHasException(root) {
			t.Fatal("root span has no exception event")
		}

		stepSpan := tbChildren(finished)[0]
		if stepSpan.Status().Code != codes.Error {
			t.Fatalf("step status %v, want ERROR", stepSpan.Status().Code)
		}
		if !tbHasException(stepSpan) {
			t.Fatal("step span has no exception event")
		}
	})
}

func TestStoppedRunIsNotAnError(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		tbClear(t)
		id, err := tbStopped.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, id)

		finished := tbSpans()
		root := tbRoots(finished)[0]
		if got := tbAttr(t, root, "everystep.workflow.status"); got != "stopped" {
			t.Fatalf("workflow status attr %q, want stopped", got)
		}
		if root.Status().Code != codes.Ok {
			t.Fatalf("root status %v, want OK: Terminal is a control flow, not an error", root.Status().Code)
		}
		// The step that raised Terminal is still marked as a failed step.
		if stepSpan := tbChildren(finished)[0]; stepSpan.Status().Code != codes.Error {
			t.Fatalf("step status %v, want ERROR", stepSpan.Status().Code)
		}
	})
}

func TestReplayedStepIsNotRespanned(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		tbClear(t)
		id, err := tbTwoSteps.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "2")
		expectCrash(t, db, id)

		finished := tbSpans()
		roots := tbRoots(finished)
		if len(roots) != 1 {
			t.Fatalf("got %d root spans after first pass, want 1", len(roots))
		}
		if got := tbAttr(t, roots[0], "everystep.workflow.status"); got != "running" {
			t.Fatalf("workflow status attr %q, want running", got)
		}
		// Step 1 ran and was recorded; step 2 ran but the crash came
		// before its record, so both steps have a span from this claim.
		if n := len(tbChildren(finished)); n != 2 {
			t.Fatalf("got %d step spans after first pass, want 2", n)
		}

		reClaim(t, db, id)
		if err := runnerExecute(t, db, id); err != nil {
			t.Fatal(err)
		}
		if wf := getRun(t, db, id); wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}

		finished = tbSpans()
		roots = tbRoots(finished)
		if len(roots) != 2 {
			t.Fatalf("got %d root spans after replay, want 2", len(roots))
		}
		if got := tbAttr(t, roots[len(roots)-1], "everystep.workflow.status"); got != "completed" {
			t.Fatalf("replay status attr %q, want completed", got)
		}
		// On the second claim step 1 is served from the store and not
		// re-traced; only step 2 re-executes.
		if n := len(tbChildren(finished)); n != 3 {
			t.Fatalf("got %d step spans after replay, want 3", n)
		}
	})
}

func TestParallelStepsNestUnderTheForkSpan(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		tbClear(t)
		id, err := tbFanout.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, id)

		finished := tbSpans()
		root := tbRoots(finished)[0]
		var fork sdktrace.ReadOnlySpan
		for _, s := range finished {
			if s.Name() == "parallel" {
				fork = s
			}
		}
		if fork == nil {
			t.Fatal("no fork span")
		}
		if got := tbAttr(t, fork, "everystep.parallel.id"); got != "fan" {
			t.Fatalf("fork id attr %q, want fan", got)
		}
		if fork.Parent().SpanID() != root.SpanContext().SpanID() {
			t.Fatal("fork span is not a child of the run span")
		}

		branchSteps := make(map[string]string)
		for _, s := range tbChildren(finished) {
			if s.Parent().SpanID() == fork.SpanContext().SpanID() {
				branchSteps[tbAttr(t, s, "everystep.step.id")] = s.Name()
			}
		}
		if len(branchSteps) != 2 {
			t.Fatalf("got %d branch step spans, want 2", len(branchSteps))
		}
		if _, ok := branchSteps["fan.0.1"]; !ok {
			t.Fatalf("branch step ids %v, want fan.0.1", branchSteps)
		}
		if _, ok := branchSteps["fan.1.1"]; !ok {
			t.Fatalf("branch step ids %v, want fan.1.1", branchSteps)
		}
	})
}

func TestStepOutsideWorkflowHasNoSpans(t *testing.T) {
	tbClear(t)
	if got, err := tbOKStep.Call(context.Background(), struct{}{}); err != nil || got != "ok" {
		t.Fatalf("direct call got %q, %v", got, err)
	}
	if spans := tbSpans(); len(spans) != 0 {
		t.Fatalf("plain step call produced %d spans", len(spans))
	}
}

func TestParallelOutsideWorkflowHasNoSpans(t *testing.T) {
	tbClear(t)
	var got string
	err := es.Parallel(context.Background()).Run(
		func(c context.Context) error {
			var e error
			got, e = tbOKStep.Call(c, struct{}{})
			return e
		},
	)
	if err != nil || got != "ok" {
		t.Fatalf("plain parallel got %q, %v", got, err)
	}
	if spans := tbSpans(); len(spans) != 0 {
		t.Fatalf("plain parallel produced %d spans", len(spans))
	}
}
