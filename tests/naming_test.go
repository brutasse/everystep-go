package tests

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"gorm.io/gorm"

	es "github.com/brutasse/everystep-go/everystep"
	"github.com/brutasse/everystep-go/runner"
	"github.com/brutasse/everystep-go/serde"
	"github.com/brutasse/everystep-go/store"
)

func nmSOneFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("s_one")
	return "1", nil
}

func nmSTwoFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("s_two")
	return "2", nil
}

func nmSThreeFn(ctx context.Context, a string) (string, error) {
	calls.add("s_three")
	return a + "!", nil
}

func nmSIntFn(ctx context.Context, a int) (int, error) {
	calls.add("s_int")
	return a, nil
}

var (
	nmSOne   = es.Step("nm.s_one", nmSOneFn)
	nmSTwo   = es.Step("nm.s_two", nmSTwoFn)
	nmSThree = es.Step("nm.s_three", nmSThreeFn)
	nmSInt   = es.Step("nm.s_int", nmSIntFn)
)

func nmNamedChainBody(ctx context.Context, a struct{}) (string, error) {
	x, err := nmSOne.Call(ctx, struct{}{}, es.Named("one"))
	if err != nil {
		return "", err
	}
	y, err := nmSTwo.Call(ctx, struct{}{})
	if err != nil {
		return "", err
	}
	return nmSThree.Call(ctx, x+y)
}

var nmNamedChain = es.Workflow("nm.named_chain", nmNamedChainBody)

func TestNamedStepIDWithNumericSiblings(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := nmNamedChain.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		wf := runToCompletion(t, db, id)
		var result string
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result != "12!" {
			t.Fatalf("result %q, want %q", result, "12!")
		}
		assertMap(t, stepNames(t, db, id), map[string]string{
			"one": "nm.s_one",
			"2":   "nm.s_two",
			"3":   "nm.s_three",
		})
	})
}

func nmUnnamedChainBody(ctx context.Context, a struct{}) (string, error) {
	x, err := nmSOne.Call(ctx, struct{}{})
	if err != nil {
		return "", err
	}
	y, err := nmSTwo.Call(ctx, struct{}{})
	if err != nil {
		return "", err
	}
	return nmSThree.Call(ctx, x+y)
}

var nmUnnamedChain = es.Workflow("nm.unnamed_chain", nmUnnamedChainBody)

func nmHalfNamedChainBody(ctx context.Context, a struct{}) (string, error) {
	x, err := nmSOne.Call(ctx, struct{}{})
	if err != nil {
		return "", err
	}
	y, err := nmSTwo.Call(ctx, struct{}{}, es.Named("mid"))
	if err != nil {
		return "", err
	}
	return nmSThree.Call(ctx, x+y)
}

var nmHalfNamedChain = es.Workflow("nm.half_named_chain", nmHalfNamedChainBody)

func TestNamingStepDoesNotShiftOtherIDs(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id1, err := nmUnnamedChain.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, id1)
		assertStepIDs(t, stepIDs(t, db, id1), "1", "2", "3")

		id2, err := nmHalfNamedChain.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, id2)
		assertStepIDs(t, stepIDs(t, db, id2), "1", "mid", "3")
	})
}

func nmTwoNamedBody(ctx context.Context, a struct{}) (string, error) {
	x, err := nmSOne.Call(ctx, struct{}{}, es.Named("first"))
	if err != nil {
		return "", err
	}
	y, err := nmSTwo.Call(ctx, struct{}{}, es.Named("second"))
	if err != nil {
		return "", err
	}
	return x + y, nil
}

var nmTwoNamed = es.Workflow("nm.two_named", nmTwoNamedBody)

func nmTwoNamedPlusBody(ctx context.Context, a struct{}) (string, error) {
	x, err := nmSOne.Call(ctx, struct{}{}, es.Named("first"))
	if err != nil {
		return "", err
	}
	y, err := nmSThree.Call(ctx, "x", es.Named("inserted"))
	if err != nil {
		return "", err
	}
	z, err := nmSTwo.Call(ctx, struct{}{}, es.Named("second"))
	if err != nil {
		return "", err
	}
	return x + y + z, nil
}

var nmTwoNamedPlus = es.Workflow("nm.two_named_plus", nmTwoNamedPlusBody)

func TestNamedStepsStableUnderInsertion(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		idA, err := nmTwoNamed.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, idA)
		assertStepIDs(t, stepIDs(t, db, idA), "first", "second")

		idB, err := nmTwoNamedPlus.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, idB)
		assertStepIDs(t, stepIDs(t, db, idB), "first", "inserted", "second")
	})
}

func nmNamedForkBody(ctx context.Context, a struct{}) (string, error) {
	if _, err := nmSOne.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	var av, bv string
	err := es.Parallel(ctx, es.ForkID("fanout")).Run(
		func(c context.Context) error {
			var e error
			av, e = nmSTwo.Call(c, struct{}{}, es.Named("left"))
			return e
		},
		func(c context.Context) error {
			var e error
			bv, e = nmSThree.Call(c, "z", es.Named("right"))
			return e
		},
	)
	if err != nil {
		return "", err
	}
	return av + bv, nil
}

var nmNamedFork = es.Workflow("nm.named_fork", nmNamedForkBody)

func TestNamedForkAndBranchSteps(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := nmNamedFork.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		wf := runToCompletion(t, db, id)
		var result string
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result != "2z!" {
			t.Fatalf("result %q, want %q", result, "2z!")
		}
		assertMap(t, stepNames(t, db, id), map[string]string{
			"1":              "nm.s_one",
			"fanout.0.left":  "nm.s_two",
			"fanout.1.right": "nm.s_three",
		})
	})
}

func nmDuplicateNameBody(ctx context.Context, a struct{}) (string, error) {
	if _, err := nmSOne.Call(ctx, struct{}{}, es.Named("x")); err != nil {
		return "", err
	}
	if _, err := nmSTwo.Call(ctx, struct{}{}, es.Named("x")); err != nil {
		return "", err
	}
	return "unreached", nil
}

var nmDuplicateName = es.Workflow("nm.duplicate_name", nmDuplicateNameBody)

func TestDuplicateEverystepIDInScopeFailsLoudly(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := nmDuplicateName.Schedule(db, struct{}{})
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
		if enc.Code != "everystep.error" {
			t.Fatalf("error code %q, want everystep.error", enc.Code)
		}
		if !strings.Contains(enc.Message, "already used in this scope") {
			t.Fatalf("error message %q, want the already-used message", enc.Message)
		}
		// The second call was rejected before its side effect ran.
		assertCalls(t, "s_one")
	})
}

func nmSameNameInBranchesBody(ctx context.Context, a struct{}) (string, error) {
	var av, bv string
	err := es.Parallel(ctx).Run(
		func(c context.Context) error {
			var e error
			av, e = nmSOne.Call(c, struct{}{}, es.Named("x"))
			return e
		},
		func(c context.Context) error {
			var e error
			bv, e = nmSTwo.Call(c, struct{}{}, es.Named("x"))
			return e
		},
	)
	if err != nil {
		return "", err
	}
	return av + bv, nil
}

var nmSameNameInBranches = es.Workflow("nm.same_name_in_branches", nmSameNameInBranchesBody)

func TestSameEverystepIDOKInDifferentBranches(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := nmSameNameInBranches.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		wf := runToCompletion(t, db, id)
		var result string
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result != "12" {
			t.Fatalf("result %q, want %q", result, "12")
		}
		assertStepIDs(t, stepIDs(t, db, id), "1.0.x", "1.1.x")
	})
}

func nmForkNameCollidesBody(ctx context.Context, a struct{}) (string, error) {
	if _, err := nmSOne.Call(ctx, struct{}{}, es.Named("fanout")); err != nil {
		return "", err
	}
	err := es.Parallel(ctx, es.ForkID("fanout")).Run(
		func(c context.Context) error {
			_, e := nmSTwo.Call(c, struct{}{})
			return e
		},
	)
	if err != nil {
		return "", err
	}
	return "unreached", nil
}

var nmForkNameCollides = es.Workflow("nm.fork_name_collides", nmForkNameCollidesBody)

func TestForkNameCollidingWithStepNameFailsLoudly(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := nmForkNameCollides.Schedule(db, struct{}{})
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
		if enc.Code != "everystep.error" {
			t.Fatalf("error code %q, want everystep.error", enc.Code)
		}
		if !strings.Contains(enc.Message, "already used in this scope") {
			t.Fatalf("error message %q, want the already-used message", enc.Message)
		}
		assertCalls(t, "s_one")
	})
}

type nmIDArgs struct {
	ID string `json:"id"`
}

func nmNamedByArgBody(ctx context.Context, a nmIDArgs) (string, error) {
	return nmSOne.Call(ctx, struct{}{}, es.Named(a.ID))
}

var nmNamedByArg = es.Workflow("nm.named_by_arg", nmNamedByArgBody)

// TestInvalidEverystepIDFailsLoudly covers the invalid-name cases of the
// Python parameterized test; the non-string case (42) has no Go analog.
func TestInvalidEverystepIDFailsLoudly(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		for _, bad := range []string{"", "a.b", "123", "a b"} {
			resetCalls(t)
			id, err := nmNamedByArg.Schedule(db, nmIDArgs{ID: bad})
			if err != nil {
				t.Fatal(err)
			}
			claimNext(t, db)
			if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
				t.Fatal(err)
			}
			wf := getRun(t, db, id)
			if wf.Status != store.StatusFailed {
				t.Fatalf("id %q: status %s, want failed", bad, wf.Status)
			}
			enc := runError(t, wf)
			if enc.Code != "everystep.error" {
				t.Fatalf("id %q: error code %q, want everystep.error", bad, enc.Code)
			}
			if !strings.HasPrefix(enc.Message, "everystep_id") {
				t.Fatalf("id %q: error message %q, want an everystep_id message", bad, enc.Message)
			}
			assertCalls(t)
			if n := len(stepsFor(t, db, id)); n != 0 {
				t.Fatalf("id %q: got %d step rows, want 0", bad, n)
			}
		}
	})
}

func nmNamedFlakyBody(ctx context.Context, a struct{}) (string, error) {
	x, err := nmSOne.Call(ctx, struct{}{}, es.Named("first"))
	if err != nil {
		return "", err
	}
	y, err := nmSTwo.Call(ctx, struct{}{}, es.Named("second"))
	if err != nil {
		return "", err
	}
	z, err := nmSThree.Call(ctx, "k")
	if err != nil {
		return "", err
	}
	return x + y + z, nil
}

var nmNamedFlaky = es.Workflow("nm.named_flaky", nmNamedFlakyBody)

func TestNamedStepCrashAndResume(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := nmNamedFlaky.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "second")
		expectCrash(t, db, id)

		// "first" was recorded; "second"'s side effect ran but its record
		// did not land.
		assertCalls(t, "s_one", "s_two")
		assertStepIDs(t, stepIDs(t, db, id), "first")

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
		if result != "12k!" {
			t.Fatalf("result %q, want %q", result, "12k!")
		}
		assertCalls(t, "s_one", "s_two", "s_two", "s_three")
		if n := len(stepsFor(t, db, id)); n != 3 {
			t.Fatalf("got %d step rows, want 3", n)
		}
	})
}

func nmSourceFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("source")
	return "S", nil
}

func nmReadNamedFn(ctx context.Context, a string) (string, error) {
	calls.add("read_named")
	var s string
	if err := es.Result(ctx, a, &s); err != nil {
		return "", err
	}
	return a + "=" + s, nil
}

var (
	nmSource    = es.Step("nm.source", nmSourceFn)
	nmReadNamed = es.Step("nm.read_named", nmReadNamedFn)
)

func nmReadsPreviousBody(ctx context.Context, a struct{}) (string, error) {
	if _, err := nmSource.Call(ctx, struct{}{}, es.Named("source")); err != nil {
		return "", err
	}
	return nmReadNamed.Call(ctx, "source")
}

var nmReadsPrevious = es.Workflow("nm.reads_previous", nmReadsPreviousBody)

func TestStepReadsPreviousResultFresh(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := nmReadsPrevious.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		wf := runToCompletion(t, db, id)
		var result string
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result != "source=S" {
			t.Fatalf("result %q, want %q", result, "source=S")
		}
		assertCalls(t, "source", "read_named")
		assertStepIDs(t, stepIDs(t, db, id), "source", "2")
	})
}

func TestStepReadsPreviousResultOnReplay(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := nmReadsPrevious.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "2")
		expectCrash(t, db, id)

		// read_named's side effect ran but its record did not land.
		assertCalls(t, "source", "read_named")
		assertStepIDs(t, stepIDs(t, db, id), "source")

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
		if result != "source=S" {
			t.Fatalf("result %q, want %q", result, "source=S")
		}
		// "source" served from the store (not re-run); read_named re-ran
		// and re-read the stored result.
		assertCalls(t, "source", "read_named", "read_named")
	})
}

var nmSlotFn = "a"

func nmNamedSlotBody(ctx context.Context, a struct{}) (string, error) {
	var err error
	if nmSlotFn == "a" {
		_, err = nmSOne.Call(ctx, struct{}{}, es.Named("slot"))
	} else {
		_, err = nmSTwo.Call(ctx, struct{}{}, es.Named("slot"))
	}
	if err != nil {
		return "", err
	}
	if _, err = nmSThree.Call(ctx, "k"); err != nil {
		return "", err
	}
	return "done", nil
}

var nmNamedSlot = es.Workflow("nm.named_slot", nmNamedSlotBody)

func TestCodeChangeAtNamedSlotDetected(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := nmNamedSlot.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "2")
		expectCrash(t, db, id)

		assertStepIDs(t, stepIDs(t, db, id), "slot")

		nmSlotFn = "b"
		defer func() { nmSlotFn = "a" }()
		reClaim(t, db, id)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusFailed {
			t.Fatalf("status %s, want failed", wf.Status)
		}
		enc := runError(t, wf)
		if enc.Code != "everystep.workflow_code_error" {
			t.Fatalf("error code %q, want everystep.workflow_code_error", enc.Code)
		}
		if !strings.Contains(enc.Message, "nm.s_one") || !strings.Contains(enc.Message, "nm.s_two") {
			t.Fatalf("error message %q must name both steps", enc.Message)
		}
	})
}

func nmLoopedBody(ctx context.Context, a struct{}) (int, error) {
	total := 0
	for i := 0; i < 2; i++ {
		v, err := nmSInt.Call(ctx, i, es.Named(fmt.Sprintf("iter-%d", i)))
		if err != nil {
			return 0, err
		}
		total += v
	}
	return total, nil
}

var nmLooped = es.Workflow("nm.looped", nmLoopedBody)

func TestDynamicNamesInALoop(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := nmLooped.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		wf := runToCompletion(t, db, id)
		var result int
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result != 1 {
			t.Fatalf("result %d, want 1", result)
		}
		assertStepIDs(t, stepIDs(t, db, id), "iter-0", "iter-1")
	})
}

func TestEverystepIDIgnoredOutsideWorkflow(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		x, err := nmSOne.Call(context.Background(), struct{}{}, es.Named("x"))
		if err != nil {
			t.Fatal(err)
		}
		if x != "1" {
			t.Fatalf("result %q, want %q", x, "1")
		}
		y, err := nmSThree.Call(context.Background(), "v", es.Named("y"))
		if err != nil {
			t.Fatal(err)
		}
		if y != "v!" {
			t.Fatalf("result %q, want %q", y, "v!")
		}
		assertCalls(t, "s_one", "s_three")
		var wfs, steps int64
		if err := db.Table("everystep_workflow").Count(&wfs).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Table("everystep_step").Count(&steps).Error; err != nil {
			t.Fatal(err)
		}
		if wfs != 0 || steps != 0 {
			t.Fatalf("direct calls persisted: %d workflows, %d steps", wfs, steps)
		}
	})
}
