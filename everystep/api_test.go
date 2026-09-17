package everystep_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	es "github.com/brutasse/everystep-go/everystep"
	esc "github.com/brutasse/everystep-go/internal/ctx"
	"github.com/google/uuid"
)

var calls []string
var callsMu sync.Mutex

func record(name string) {
	callsMu.Lock()
	calls = append(calls, name)
	callsMu.Unlock()
}

func resetCalls() {
	callsMu.Lock()
	calls = nil
	callsMu.Unlock()
}

func TestStepCallOutsideRunExecutesPlainly(t *testing.T) {
	resetCalls()
	var s = es.Step("plain.exec", func(ctx context.Context, a string) (string, error) {
		record("exec")
		return "ok:" + a, nil
	})
	out, err := s.Call(context.Background(), "x", es.Named("ignored"))
	if err != nil {
		t.Fatal(err)
	}
	if out != "ok:x" {
		t.Fatalf("out = %q", out)
	}
	if len(calls) != 1 || calls[0] != "exec" {
		t.Fatalf("calls = %v", calls)
	}
}

func TestStepCallOutsideRunPropagatesError(t *testing.T) {
	boom := errors.New("boom")
	var s = es.Step("plain.fails", func(ctx context.Context, a struct{}) (string, error) {
		return "", boom
	})
	_, err := s.Call(context.Background(), struct{}{})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestDuplicateRegistrationPanics(t *testing.T) {
	fn := func(ctx context.Context, a struct{}) (string, error) { return "", nil }
	es.Step("dup.one", fn)
	es.Workflow("dup.two", func(ctx context.Context, a struct{}) (string, error) { return "", nil })

	cases := []struct {
		name string
		fn   func()
	}{
		{"step/step", func() { es.Step("dup.one", fn) }},
		{"workflow/workflow", func() {
			es.Workflow("dup.two", func(ctx context.Context, a struct{}) (string, error) { return "", nil })
		}},
		{"step/workflow", func() {
			es.Workflow("dup.one", func(ctx context.Context, a struct{}) (string, error) { return "", nil })
		}},
		{"workflow/step", func() { es.Step("dup.two", fn) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("want panic on duplicate name")
				}
			}()
			c.fn()
		})
	}
}

func TestParallelOutsideRunRunsBranchesPlainly(t *testing.T) {
	resetCalls()
	done := make(chan struct{})
	var orderMu sync.Mutex
	var order []string

	err := es.Parallel(context.Background()).Run(
		func(ctx context.Context) error {
			record("a")
			return nil
		},
		func(ctx context.Context) error {
			record("b")
			close(done)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	<-done
	orderMu.Lock()
	order = append(order, calls...)
	orderMu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("calls = %v", calls)
	}
	// Both branches ran; order is nondeterministic.
	_, ok := es.Outcome(context.Background(), "1.0.1")
	if ok {
		t.Fatal("no outcomes may be recorded outside a run")
	}
}

func TestParallelSingleFailureReturnsAsIs(t *testing.T) {
	boom := errors.New("branch blew up")
	err := es.Parallel(context.Background()).Run(
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return boom },
	)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if err != boom {
		t.Fatal("single failure must be returned as-is, not wrapped")
	}
}

func TestParallelMultipleFailuresJoin(t *testing.T) {
	a, b := errors.New("branch a"), errors.New("branch b")
	err := es.Parallel(context.Background()).Run(
		func(ctx context.Context) error { return a },
		func(ctx context.Context) error { return b },
		func(ctx context.Context) error { return nil },
	)
	if !errors.Is(err, a) || !errors.Is(err, b) {
		t.Fatalf("err = %v", err)
	}
	var joined interface{ Unwrap() []error }
	if !errors.As(err, &joined) {
		t.Fatalf("want a joined error, got %T", err)
	}
}

func TestParallelControlFlowShortCircuits(t *testing.T) {
	term := &es.Terminal{Reason: "no capacity"}
	other := errors.New("other failure")
	err := es.Parallel(context.Background()).Run(
		func(ctx context.Context) error { return other },
		func(ctx context.Context) error { return fmt.Errorf("wrap: %w", term) },
	)
	var tt *es.Terminal
	if !errors.As(err, &tt) {
		t.Fatalf("err = %v", err)
	}
	if tt.Reason != "no capacity" {
		t.Fatalf("reason = %q", tt.Reason)
	}
}

func TestParallelRequiresABranch(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic for a fork without branches")
		}
	}()
	_ = es.Parallel(context.Background()).Run()
}

func TestNamedValidationBeforeEffect(t *testing.T) {
	resetCalls()
	var s = es.Step("named.validates", func(ctx context.Context, a struct{}) (string, error) {
		record("should-not-run")
		return "", nil
	})
	rs := esc.NewRun(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"), "wf", nil, esc.NewOutcomeStore(), nil)
	ctx := esc.With(context.Background(), rs)

	cases := []string{"", "a.b", "a b", "123"}
	for _, name := range cases {
		_, err := s.Call(ctx, struct{}{}, es.Named(name))
		var ee *es.EverystepError
		if !errors.As(err, &ee) {
			t.Fatalf("%q: err = %T %v", name, err, err)
		}
		if err == nil || !hasPrefix(err.Error(), "everystep_id") {
			t.Fatalf("%q: message = %q", name, err)
		}
	}
	if len(calls) != 0 {
		t.Fatalf("side effect must not run on invalid name, calls = %v", calls)
	}
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func TestOutcomeAndResultReads(t *testing.T) {
	// Outside a run: nothing recorded.
	if _, ok := es.Outcome(context.Background(), "1"); ok {
		t.Fatal("want no outcome outside a run")
	}
	if err := es.Result(context.Background(), "1", new(string)); err == nil {
		t.Fatal("want error outside a run")
	}

	// Inside a state: a recorded outcome is readable.
	rs := esc.NewRun(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"), "wf", nil, esc.NewOutcomeStore(), nil)
	ctx := esc.With(context.Background(), rs)
	rs.Outcomes().Set("source", esc.Outcome{Name: "source", Status: "done", Result: []byte(`"S"`)})
	o, ok := es.Outcome(ctx, "source")
	if !ok || o.Name != "source" || o.Status != "done" {
		t.Fatalf("outcome = %+v ok=%v", o, ok)
	}
	var v string
	if err := es.Result(ctx, "source", &v); err != nil {
		t.Fatal(err)
	}
	if v != "S" {
		t.Fatalf("v = %q", v)
	}
	if err := es.Result(ctx, "missing", &v); err == nil {
		t.Fatal("want error for a missing id")
	}
}

func TestParallelBranchesShareOutcomes(t *testing.T) {
	// A branch can read another branch's completed outcome through the
	// shared store — the Go counterpart of the shared outcomes dict.
	rs := esc.NewRun(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"), "wf", nil, esc.NewOutcomeStore(), nil)
	ctx := esc.With(context.Background(), rs)
	rs.Outcomes().Set("1.0.1", esc.Outcome{Name: "first", Status: "done", Result: []byte(`"a"`)})

	got := make(chan string, 1)
	err := es.Parallel(ctx).Run(
		func(ctx context.Context) error {
			var v string
			if err := es.Result(ctx, "1.0.1", &v); err != nil {
				return err
			}
			got <- v
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if v := <-got; v != "a" {
		t.Fatalf("v = %q", v)
	}
}

func TestUnsafeToRepeatOutsideRunIsIgnored(t *testing.T) {
	resetCalls()
	var s = es.Step("unsafe.plain", func(ctx context.Context, a struct{}) (string, error) {
		record("once")
		return "v", nil
	}, es.UnsafeToRepeat())
	out, err := s.Call(context.Background(), struct{}{})
	if err != nil || out != "v" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %v", calls)
	}
}

var atomicCounter atomic.Int64

func TestZeroArgStepsUseStruct(t *testing.T) {
	var s = es.Step("zero.arg", func(ctx context.Context, a struct{}) (int64, error) {
		return atomicCounter.Add(1), nil
	})
	n, err := s.Call(context.Background(), struct{}{})
	if err != nil || n == 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}
