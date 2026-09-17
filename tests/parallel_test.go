package tests

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"

	es "github.com/brutasse/everystep-go/everystep"
	"github.com/brutasse/everystep-go/runner"
	"github.com/brutasse/everystep-go/serde"
	"github.com/brutasse/everystep-go/store"
)

func parSlowAFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("slow_a")
	time.Sleep(300 * time.Millisecond)
	return "a", nil
}

func parSlowBFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("slow_b")
	time.Sleep(300 * time.Millisecond)
	return "b", nil
}

var (
	parSlowA = es.Step("par.slow_a", parSlowAFn)
	parSlowB = es.Step("par.slow_b", parSlowBFn)
)

type parBothResult struct {
	A       string  `json:"a"`
	B       string  `json:"b"`
	Elapsed float64 `json:"elapsed"`
}

func parBothBody(ctx context.Context, a struct{}) (parBothResult, error) {
	start := time.Now()
	var av, bv string
	err := es.Parallel(ctx).Run(
		func(c context.Context) error {
			var e error
			av, e = parSlowA.Call(c, struct{}{})
			return e
		},
		func(c context.Context) error {
			var e error
			bv, e = parSlowB.Call(c, struct{}{})
			return e
		},
	)
	if err != nil {
		return parBothResult{}, err
	}
	return parBothResult{A: av, B: bv, Elapsed: time.Since(start).Seconds()}, nil
}

var parBoth = es.Workflow("par.both", parBothBody)

func TestParallelBranchesRunConcurrently(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := parBoth.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		wf := runToCompletion(t, db, id)
		var result parBothResult
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result.A != "a" || result.B != "b" {
			t.Fatalf("result %+v, want a/b", result)
		}
		if result.Elapsed >= 0.5 {
			t.Fatalf("elapsed %fs: branches did not run concurrently", result.Elapsed)
		}
		if calls.count("slow_a") != 1 || calls.count("slow_b") != 1 {
			t.Fatalf("calls %v, want slow_a and slow_b once each", calls.listCopy())
		}
		assertMap(t, stepNames(t, db, id), map[string]string{
			"1.0.1": "par.slow_a",
			"1.1.1": "par.slow_b",
		})
	})
}

// parBoomError is deliberately not registered: like the Python suite's
// Boom, it is an ordinary error with no stable cross-replay identity, so
// only its message survives.
type parBoomError struct{ msg string }

func (e *parBoomError) Error() string { return e.msg }

func parOkFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("ok")
	return "ok", nil
}

func parBoomFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("boom")
	return "", &parBoomError{msg: "branch blew up"}
}

func parBoomTwoFn(ctx context.Context, a struct{}) (string, error) {
	return "", errors.New("second boom")
}

var (
	parOk      = es.Step("par.ok", parOkFn)
	parBoom    = es.Step("par.boom", parBoomFn)
	parBoomTwo = es.Step("par.boom_two", parBoomTwoFn)
)

func parOneFailsBody(ctx context.Context, a struct{}) ([]string, error) {
	var av, bv string
	err := es.Parallel(ctx).Run(
		func(c context.Context) error {
			var e error
			av, e = parOk.Call(c, struct{}{})
			return e
		},
		func(c context.Context) error {
			var e error
			bv, e = parBoom.Call(c, struct{}{})
			return e
		},
	)
	if err != nil {
		return nil, err
	}
	return []string{av, bv}, nil
}

var parOneFails = es.Workflow("par.one_fails", parOneFailsBody)

func TestParallelSingleFailureReraisesOriginal(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := parOneFails.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusFailed {
			t.Fatalf("status %s, want failed", wf.Status)
		}
		enc := runError(t, wf)
		if enc.Message != "branch blew up" {
			t.Fatalf("error message %q, want %q", enc.Message, "branch blew up")
		}
		// The healthy branch still completed and was recorded.
		assertMap(t, stepStatuses(t, db, id), map[string]string{
			"1.0.1": store.StepDone,
			"1.1.1": store.StepFailed,
		})
	})
}

func parBothFailBody(ctx context.Context, a struct{}) ([]string, error) {
	var xv, yv string
	err := es.Parallel(ctx).Run(
		func(c context.Context) error {
			var e error
			xv, e = parBoom.Call(c, struct{}{})
			return e
		},
		func(c context.Context) error {
			var e error
			yv, e = parBoomTwo.Call(c, struct{}{})
			return e
		},
	)
	if err != nil {
		return nil, err
	}
	return []string{xv, yv}, nil
}

var parBothFail = es.Workflow("par.both_fail", parBothFailBody)

func TestParallelMultipleFailuresAreJoined(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := parBothFail.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusFailed {
			t.Fatalf("status %s, want failed", wf.Status)
		}
		// errors.Join is Go's ExceptionGroup: every branch failure is kept.
		enc := runError(t, wf)
		if !strings.Contains(enc.Message, "branch blew up") || !strings.Contains(enc.Message, "second boom") {
			t.Fatalf("joined error %q must carry both branch failures", enc.Message)
		}
	})
}

func parBranchOneFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("branch_one")
	return "1", nil
}

func parBranchTwoFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("branch_two")
	return "2", nil
}

func parTailFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("tail")
	return "t", nil
}

var (
	parBranchOne = es.Step("par.branch_one", parBranchOneFn)
	parBranchTwo = es.Step("par.branch_two", parBranchTwoFn)
	parTail      = es.Step("par.tail", parTailFn)
)

func parForkedBody(ctx context.Context, a struct{}) (string, error) {
	var xv, yv string
	err := es.Parallel(ctx).Run(
		func(c context.Context) error {
			var e error
			xv, e = parBranchOne.Call(c, struct{}{})
			return e
		},
		func(c context.Context) error {
			var e error
			yv, e = parBranchTwo.Call(c, struct{}{})
			return e
		},
	)
	if err != nil {
		return "", err
	}
	tv, err := parTail.Call(ctx, struct{}{})
	if err != nil {
		return "", err
	}
	return xv + yv + tv, nil
}

var parForked = es.Workflow("par.forked", parForkedBody)

func TestParallelBranchRecoversAfterCrash(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := parForked.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "1.0.1")
		expectCrash(t, db, id)

		// branch_one's side effect happened but its outcome was not recorded.
		if calls.count("branch_one") != 1 {
			t.Fatalf("branch_one ran %d times, want 1", calls.count("branch_one"))
		}

		reClaim(t, db, id)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}
		var result string
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result != "12t" {
			t.Fatalf("result %q, want %q", result, "12t")
		}
		if calls.count("branch_one") != 2 {
			t.Fatalf("branch_one ran %d times, want 2", calls.count("branch_one"))
		}
		// branch_two may or may not have been recorded before the crash.
		if n := calls.count("branch_two"); n != 1 && n != 2 {
			t.Fatalf("branch_two ran %d times, want 1 or 2", n)
		}
		if calls.count("tail") != 1 {
			t.Fatalf("tail ran %d times, want 1", calls.count("tail"))
		}
	})
}

// Multi-step branches: the Go replacement for the Python sequence branch —
// a branch is a closure that calls several steps in order.

func parSeqFirstFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("seq_first")
	return "f", nil
}

func parSeqSecondFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("seq_second")
	return "s", nil
}

var (
	parSeqFirst  = es.Step("par.seq_first", parSeqFirstFn)
	parSeqSecond = es.Step("par.seq_second", parSeqSecondFn)
)

func parSeqBranchBody(ctx context.Context, a struct{}) ([]string, error) {
	var a1, b1, c2 string
	err := es.Parallel(ctx).Run(
		func(c context.Context) error {
			var e error
			if a1, e = parSeqFirst.Call(c, struct{}{}); e != nil {
				return e
			}
			if b1, e = parSeqSecond.Call(c, struct{}{}); e != nil {
				return e
			}
			return nil
		},
		func(c context.Context) error {
			var e error
			c2, e = parBranchTwo.Call(c, struct{}{})
			return e
		},
	)
	if err != nil {
		return nil, err
	}
	return []string{a1, b1, c2}, nil
}

var parSeqBranch = es.Workflow("par.seq_branch", parSeqBranchBody)

func TestMultiStepBranchRunsInOrder(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := parSeqBranch.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		wf := runToCompletion(t, db, id)
		var result []string
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(result, []string{"f", "s", "2"}) {
			t.Fatalf("result %v, want [f s 2]", result)
		}
		assertMap(t, stepNames(t, db, id), map[string]string{
			"1.0.1": "par.seq_first",
			"1.0.2": "par.seq_second",
			"1.1.1": "par.branch_two",
		})
		got := calls.listCopy()
		i1, i2 := -1, -1
		for i, m := range got {
			if m == "seq_first" {
				i1 = i
			}
			if m == "seq_second" {
				i2 = i
			}
		}
		if i1 == -1 || i2 == -1 || i1 > i2 {
			t.Fatalf("seq_first must run before seq_second in its branch: %v", got)
		}
	})
}

func TestMultiStepBranchRecoversAfterCrash(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := parSeqBranch.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "1.0.2")
		expectCrash(t, db, id)

		// seq_first was recorded; seq_second's side effect ran but its
		// record did not land.
		if calls.count("seq_first") != 1 {
			t.Fatalf("seq_first ran %d times, want 1", calls.count("seq_first"))
		}
		if calls.count("seq_second") != 1 {
			t.Fatalf("seq_second ran %d times, want 1", calls.count("seq_second"))
		}
		ids := stepIDs(t, db, id)
		foundFirst, foundSecond := false, false
		for _, sid := range ids {
			if sid == "1.0.1" {
				foundFirst = true
			}
			if sid == "1.0.2" {
				foundSecond = true
			}
		}
		if !foundFirst || foundSecond {
			t.Fatalf("recorded steps %v, want 1.0.1 only", ids)
		}

		reClaim(t, db, id)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}
		var result []string
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(result, []string{"f", "s", "2"}) {
			t.Fatalf("result %v, want [f s 2]", result)
		}
		// seq_first served from the store; seq_second re-ran.
		if calls.count("seq_first") != 1 {
			t.Fatalf("seq_first re-ran: %v", calls.listCopy())
		}
		if calls.count("seq_second") != 2 {
			t.Fatalf("seq_second ran %d times, want 2", calls.count("seq_second"))
		}
		if n := len(stepsFor(t, db, id)); n != 3 {
			t.Fatalf("got %d step rows, want 3", n)
		}
	})
}

func parSeqFailsBody(ctx context.Context, a struct{}) (string, error) {
	err := es.Parallel(ctx).Run(
		func(c context.Context) error {
			if _, e := parOk.Call(c, struct{}{}); e != nil {
				return e
			}
			_, e := parBoom.Call(c, struct{}{})
			return e
		},
		func(c context.Context) error {
			_, e := parBranchOne.Call(c, struct{}{})
			return e
		},
	)
	if err != nil {
		return "", err
	}
	return "unreached", nil
}

var parSeqFails = es.Workflow("par.seq_fails", parSeqFailsBody)

func TestMultiStepBranchFailureRecordsPriorSteps(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := parSeqFails.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusFailed {
			t.Fatalf("status %s, want failed", wf.Status)
		}
		enc := runError(t, wf)
		if enc.Message != "branch blew up" {
			t.Fatalf("error message %q, want %q", enc.Message, "branch blew up")
		}
		assertMap(t, stepStatuses(t, db, id), map[string]string{
			"1.0.1": store.StepDone,
			"1.0.2": store.StepFailed,
			"1.1.1": store.StepDone,
		})
	})
}

func TestParallelRequiresABranch(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Parallel.Run with no branches must panic")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "requires at least one branch") {
			t.Fatalf("panic %v, want the requires-a-branch message", r)
		}
	}()
	es.Parallel(context.Background()).Run()
}

func TestParallelOutsideWorkflow(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		var got string
		err := es.Parallel(context.Background()).Run(
			func(c context.Context) error {
				var e error
				got, e = parBranchOne.Call(c, struct{}{})
				return e
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if got != "1" {
			t.Fatalf("result %q, want %q", got, "1")
		}
		assertCalls(t, "branch_one")
		var wfs, steps int64
		if err := db.Table("everystep_workflow").Count(&wfs).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Table("everystep_step").Count(&steps).Error; err != nil {
			t.Fatal(err)
		}
		if wfs != 0 || steps != 0 {
			t.Fatalf("plain parallel persisted: %d workflows, %d steps", wfs, steps)
		}
	})
}
