package tests

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	es "github.com/brutasse/everystep-go/everystep"
	"github.com/brutasse/everystep-go/runner"
	"github.com/brutasse/everystep-go/serde"
	"github.com/brutasse/everystep-go/store"
)

func rpOneFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("one")
	return "1", nil
}

func rpTwoFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("two")
	return "2", nil
}

func rpThreeFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("three")
	return "3", nil
}

var (
	rpOne   = es.Step("rp.one", rpOneFn)
	rpTwo   = es.Step("rp.two", rpTwoFn)
	rpThree = es.Step("rp.three", rpThreeFn)
)

func rpFlakyBody(ctx context.Context, a struct{}) (string, error) {
	x, err := rpOne.Call(ctx, struct{}{})
	if err != nil {
		return "", err
	}
	y, err := rpTwo.Call(ctx, struct{}{})
	if err != nil {
		return "", err
	}
	z, err := rpThree.Call(ctx, struct{}{})
	if err != nil {
		return "", err
	}
	return x + y + z, nil
}

var rpFlaky = es.Workflow("rp.flaky", rpFlakyBody)

func TestCrashBeforeRecordAndResume(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := rpFlaky.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "2")
		expectCrash(t, db, id)

		assertCalls(t, "one", "two")
		assertStepIDs(t, stepIDs(t, db, id), "1")
		wf := getRun(t, db, id)
		if wf.Status != store.StatusRunning {
			t.Fatalf("status %s, want running", wf.Status)
		}

		reClaim(t, db, id)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf = getRun(t, db, id)
		if wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}
		var result string
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result != "123" {
			t.Fatalf("result %q, want %q", result, "123")
		}
		assertCalls(t, "one", "two", "two", "three")
		if n := len(stepsFor(t, db, id)); n != 3 {
			t.Fatalf("got %d step rows, want 3", n)
		}
	})
}

// ipConflictError is a replayable error the guarded workflow matches on
// with errors.As, exactly as the Python suite's IpConflict.
type ipConflictError struct{ msg string }

func (e *ipConflictError) Error() string { return e.msg }
func (e *ipConflictError) Code() string  { return "test.ip_conflict" }
func (e *ipConflictError) Payload() any  { return nil }

func init() {
	es.RegisterError("test.ip_conflict", func(msg string, _ json.RawMessage) error {
		return &ipConflictError{msg: msg}
	})
}

func rpAcquireFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("acquire")
	return "", &ipConflictError{msg: "address in use"}
}

func rpReleaseFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("release")
	return "", nil
}

var (
	rpAcquire = es.Step("rp.acquire", rpAcquireFn)
	rpRelease = es.Step("rp.release", rpReleaseFn)
)

func rpGuardedBody(ctx context.Context, a struct{}) (string, error) {
	_, err := rpAcquire.Call(ctx, struct{}{})
	if err != nil {
		var conflict *ipConflictError
		if errors.As(err, &conflict) {
			if _, rerr := rpRelease.Call(ctx, struct{}{}); rerr != nil {
				return "", rerr
			}
		}
		return "", err
	}
	return "ok", nil
}

var rpGuarded = es.Workflow("rp.guarded", rpGuardedBody)

func TestFailedStepReraisesAndCleanupReplays(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := rpGuarded.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "2")
		expectCrash(t, db, id)

		assertCalls(t, "acquire", "release")
		assertStepIDs(t, stepIDs(t, db, id), "1")

		reClaim(t, db, id)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusFailed {
			t.Fatalf("status %s, want failed", wf.Status)
		}
		enc := runError(t, wf)
		if enc.Code != "test.ip_conflict" {
			t.Fatalf("error code %q, want test.ip_conflict", enc.Code)
		}
		if enc.Message != "address in use" {
			t.Fatalf("error message %q, want %q", enc.Message, "address in use")
		}
		assertCalls(t, "acquire", "release", "release")
		assertMap(t, stepStatuses(t, db, id), map[string]string{
			"1": store.StepFailed,
			"2": store.StepDone,
		})
		steps := stepsFor(t, db, id)
		var stored *store.Step
		for i := range steps {
			if steps[i].StepID == "1" {
				stored = &steps[i]
			}
		}
		if stored == nil {
			t.Fatal("no step row for id 1")
		}
		var senc serde.EncodedError
		if err := json.Unmarshal(stored.Error, &senc); err != nil {
			t.Fatal(err)
		}
		if senc.Code != "test.ip_conflict" {
			t.Fatalf("stored step error code %q, want test.ip_conflict", senc.Code)
		}
		if senc.Message != "address in use" {
			t.Fatalf("stored step error message %q, want %q", senc.Message, "address in use")
		}
	})
}

var rpPick = "a"

func rpBeginFn(ctx context.Context, a struct{}) (string, error) {
	return "started", nil
}

func rpStepAFn(ctx context.Context, a struct{}) (string, error) {
	return "a", nil
}

func rpStepBFn(ctx context.Context, a struct{}) (string, error) {
	return "b", nil
}

func rpFinishFn(ctx context.Context, a string) (string, error) {
	return "finished:" + a, nil
}

var (
	rpBegin  = es.Step("rp.begin", rpBeginFn)
	rpStepA  = es.Step("rp.step_a", rpStepAFn)
	rpStepB  = es.Step("rp.step_b", rpStepBFn)
	rpFinish = es.Step("rp.finish", rpFinishFn)
)

func rpBranchyBody(ctx context.Context, a struct{}) (string, error) {
	if _, err := rpBegin.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	var x string
	var err error
	if rpPick == "a" {
		x, err = rpStepA.Call(ctx, struct{}{})
	} else {
		x, err = rpStepB.Call(ctx, struct{}{})
	}
	if err != nil {
		return "", err
	}
	return rpFinish.Call(ctx, x)
}

var rpBranchy = es.Workflow("rp.branchy", rpBranchyBody)

func TestCodeChangeDetectedOnReplay(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := rpBranchy.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "3")
		expectCrash(t, db, id)

		assertStepIDs(t, stepIDs(t, db, id), "1", "2")

		rpPick = "b"
		defer func() { rpPick = "a" }()
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
		if !strings.Contains(enc.Message, "rp.step_a") || !strings.Contains(enc.Message, "rp.step_b") {
			t.Fatalf("error message %q must name both step_a and step_b", enc.Message)
		}
	})
}

type rpLevel int

const rpLevelHigh rpLevel = 2

type rpCarrierArgs struct {
	Uid   uuid.UUID `json:"uid"`
	When  time.Time `json:"when"`
	Level rpLevel   `json:"level"`
}

func rpPackFn(ctx context.Context, a rpCarrierArgs) (rpCarrierArgs, error) {
	return a, nil
}

func rpUnpackFn(ctx context.Context, a rpCarrierArgs) (rpCarrierArgs, error) {
	return a, nil
}

var (
	rpPack   = es.Step("rp.pack", rpPackFn)
	rpUnpack = es.Step("rp.unpack", rpUnpackFn)
)

func rpCarrierBody(ctx context.Context, a rpCarrierArgs) (rpCarrierArgs, error) {
	p, err := rpPack.Call(ctx, a)
	if err != nil {
		return rpCarrierArgs{}, err
	}
	return rpUnpack.Call(ctx, p)
}

var rpCarrier = es.Workflow("rp.carrier", rpCarrierBody)

func TestTypedValuesRoundtripThroughStore(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		uid := uuid.New()
		when := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
		id, err := rpCarrier.Schedule(db, rpCarrierArgs{Uid: uid, When: when, Level: rpLevelHigh})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "2")
		expectCrash(t, db, id)

		reClaim(t, db, id)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}
		var result rpCarrierArgs
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result.Uid != uid {
			t.Fatalf("result uid %s, want %s", result.Uid, uid)
		}
		if !result.When.Equal(when) {
			t.Fatalf("result when %s, want %s", result.When, when)
		}
		if result.Level != rpLevelHigh {
			t.Fatalf("result level %d, want %d", result.Level, rpLevelHigh)
		}
		steps := stepsFor(t, db, id)
		if steps[0].StepID != "1" {
			t.Fatalf("first step id %q, want 1", steps[0].StepID)
		}
		var pargs rpCarrierArgs
		if err := serde.Unmarshal(steps[0].Args, &pargs); err != nil {
			t.Fatal(err)
		}
		if pargs.Uid != uid {
			t.Fatalf("stored pack args uid %s, want %s", pargs.Uid, uid)
		}
		if pargs.Level != rpLevelHigh {
			t.Fatalf("stored pack args level %d, want %d", pargs.Level, rpLevelHigh)
		}
	})
}
