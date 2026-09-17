package tests

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"gorm.io/gorm"

	es "github.com/brutasse/everystep-go/everystep"
	esc "github.com/brutasse/everystep-go/internal/ctx"
	"github.com/brutasse/everystep-go/runner"
	"github.com/brutasse/everystep-go/store"
)

func nsChargeFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("charge")
	return "charged", nil
}

func nsConfirmFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("confirm")
	return "confirmed", nil
}

func nsRefundFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("refund")
	return "", nil
}

func nsPlainEffectFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("plain_effect")
	return "v", nil
}

var (
	nsCharge      = es.Step("ns.charge", nsChargeFn, es.UnsafeToRepeat())
	nsConfirm     = es.Step("ns.confirm", nsConfirmFn)
	nsRefund      = es.Step("ns.refund", nsRefundFn)
	nsPlainEffect = es.Step("ns.plain_effect", nsPlainEffectFn)
)

func nsPayingBody(ctx context.Context, a struct{}) (string, error) {
	if _, err := nsCharge.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	return nsConfirm.Call(ctx, struct{}{})
}

var nsPaying = es.Workflow("ns.paying", nsPayingBody)

func nsGuardedPayBody(ctx context.Context, a struct{}) (string, error) {
	_, err := nsCharge.Call(ctx, struct{}{})
	if err != nil {
		var sf *es.StepFailure
		if errors.As(err, &sf) {
			if _, rerr := nsRefund.Call(ctx, struct{}{}); rerr != nil {
				return "", rerr
			}
		}
		return "", err
	}
	return "ok", nil
}

var nsGuardedPay = es.Workflow("ns.guarded_pay", nsGuardedPayBody)

func nsPlainFlowBody(ctx context.Context, a struct{}) (string, error) {
	return nsPlainEffect.Call(ctx, struct{}{})
}

var nsPlainFlow = es.Workflow("ns.plain_flow", nsPlainFlowBody)

func nsParallelPayBody(ctx context.Context, a struct{}) (string, error) {
	var av, bv string
	err := es.Parallel(ctx).Run(
		func(c context.Context) error {
			var e error
			av, e = nsCharge.Call(c, struct{}{})
			return e
		},
		func(c context.Context) error {
			var e error
			bv, e = nsConfirm.Call(c, struct{}{})
			return e
		},
	)
	if err != nil {
		return "", err
	}
	return av + bv, nil
}

var nsParallelPay = es.Workflow("ns.parallel_pay", nsParallelPayBody)

// crashInMarkedStep claims nothing: the test already claimed; it crashes the
// run in the given step's effect window.
func crashInMarkedStep(t *testing.T, db *gorm.DB, id uuid.UUID, stepID string) {
	t.Helper()
	crashAt(t, stepID)
	expectCrash(t, db, id)
}

// block re-claims the crashed run; it must end blocked without re-running
// the effect.
func block(t *testing.T, db *gorm.DB, id uuid.UUID) *store.Workflow {
	t.Helper()
	reClaim(t, db, id)
	if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
		t.Fatal(err)
	}
	wf := getRun(t, db, id)
	if wf.Status != store.StatusBlocked {
		t.Fatalf("status %s, want blocked", wf.Status)
	}
	enc := runError(t, wf)
	if enc.Code != "everystep.effect_uncertain" {
		t.Fatalf("error code %q, want everystep.effect_uncertain", enc.Code)
	}
	return wf
}

func TestMarkedStepCompletesAndReplays(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := nsPaying.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashInMarkedStep(t, db, id, "2")

		// Step 1 (marked) recorded done; step 2's effect ran unrecorded.
		assertCalls(t, "charge", "confirm")
		rows := stepsFor(t, db, id)
		if len(rows) != 1 || rows[0].StepID != "1" {
			t.Fatalf("step rows %v, want exactly step 1", stepIDs(t, db, id))
		}
		if rows[0].Status != store.StepDone {
			t.Fatalf("step 1 status %s, want done", rows[0].Status)
		}
		if string(rows[0].Result) != `"charged"` {
			t.Fatalf("step 1 result %s, want \"charged\"", rows[0].Result)
		}

		reClaim(t, db, id)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}
		if string(wf.Result) != `"confirmed"` {
			t.Fatalf("result %s, want \"confirmed\"", wf.Result)
		}
		// The marked step was served from the store; only the unrecorded
		// unmarked step ran again.
		assertCalls(t, "charge", "confirm", "confirm")
	})
}

func TestCrashInEffectWindowBlocksTheRun(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := nsPaying.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashInMarkedStep(t, db, id, "1")

		assertCalls(t, "charge")
		rows := stepsFor(t, db, id)
		if len(rows) != 1 || rows[0].StepID != "1" {
			t.Fatalf("step rows %v, want exactly step 1", stepIDs(t, db, id))
		}
		if rows[0].Status != store.StepStarted {
			t.Fatalf("step 1 status %s, want started", rows[0].Status)
		}
		if wf := getRun(t, db, id); wf.Status != store.StatusRunning {
			t.Fatalf("status %s, want running", wf.Status)
		}

		block(t, db, id)
		// The effect is not performed a second time.
		assertCalls(t, "charge")
		// A blocked run sits in the dead-letter state: no runner claims it.
		claimed, err := store.ClaimScheduled(context.Background(), db, 1, "other-worker")
		if err != nil {
			t.Fatal(err)
		}
		if len(claimed) != 0 {
			t.Fatalf("blocked run was claimed: %v", claimedIDs(claimed))
		}
	})
}

func TestResolveWithResult(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := nsPaying.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashInMarkedStep(t, db, id, "1")
		block(t, db, id)

		if err := es.ResolveStep(context.Background(), db, id, "1", es.WithResult("charged")); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusScheduled {
			t.Fatalf("status %s, want scheduled", wf.Status)
		}
		if wf.ClaimedBy != nil {
			t.Fatalf("claimed_by %v, want nil", *wf.ClaimedBy)
		}
		if len(wf.Error) != 0 {
			t.Fatalf("error %s, want nil", wf.Error)
		}
		if wf.CompletedAt != nil {
			t.Fatal("completed_at, want nil")
		}
		rows := stepsFor(t, db, id)
		if len(rows) != 1 || rows[0].StepID != "1" {
			t.Fatalf("step rows %v, want exactly step 1", stepIDs(t, db, id))
		}
		if rows[0].Status != store.StepDone {
			t.Fatalf("step 1 status %s, want done", rows[0].Status)
		}
		if string(rows[0].Result) != `"charged"` {
			t.Fatalf("step 1 result %s, want \"charged\"", rows[0].Result)
		}

		claimNext(t, db)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf = getRun(t, db, id)
		if wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}
		if string(wf.Result) != `"confirmed"` {
			t.Fatalf("result %s, want \"confirmed\"", wf.Result)
		}
		// The effect was performed exactly once in total.
		assertCalls(t, "charge", "confirm")
	})
}

func TestResolveWithDiscardRerunsTheStep(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := nsPaying.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashInMarkedStep(t, db, id, "1")
		block(t, db, id)

		if err := es.ResolveStep(context.Background(), db, id, "1", es.Discard()); err != nil {
			t.Fatal(err)
		}
		if n := len(stepsFor(t, db, id)); n != 0 {
			t.Fatalf("step rows %v, want none after discard", stepIDs(t, db, id))
		}

		claimNext(t, db)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}
		assertCalls(t, "charge", "charge", "confirm")
	})
}

func TestResolveWithErrorReraisesAndCleansUp(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := nsGuardedPay.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashInMarkedStep(t, db, id, "1")
		block(t, db, id)

		if err := es.ResolveStep(context.Background(), db, id, "1", es.WithError("charge declined")); err != nil {
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
		if enc.Code != "everystep.step_failure" {
			t.Fatalf("error code %q, want everystep.step_failure", enc.Code)
		}
		if enc.Message != "charge declined" {
			t.Fatalf("error message %q, want %q", enc.Message, "charge declined")
		}
		assertCalls(t, "charge", "refund")
	})
}

func TestResolveRejectsInvalidRequests(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		ctx := context.Background()
		// A completed run cannot be resolved.
		id, err := nsPaying.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		if err := runner.Execute(ctx, db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		if wf := getRun(t, db, id); wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}
		if err := es.ResolveStep(ctx, db, id, "1", es.Discard()); err == nil {
			t.Fatal("resolving a completed run must fail")
		}
		// An unknown run id cannot be resolved.
		missing := uuid.New()
		if err := es.ResolveStep(ctx, db, missing, "1", es.Discard()); err == nil {
			t.Fatal("resolving an unknown run must fail")
		}
		// A blocked run: unknown step and unserializable result are
		// rejected.
		id2, err := nsPaying.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashInMarkedStep(t, db, id2, "1")
		block(t, db, id2)
		if err := es.ResolveStep(ctx, db, id2, "99", es.Discard()); err == nil {
			t.Fatal("resolving an unrecorded step must fail")
		}
		if err := es.ResolveStep(ctx, db, id2, "1", es.WithResult(func() {})); err == nil {
			t.Fatal("resolving with an unstorable result must fail")
		}
	})
}

func TestConcurrentClaimOfTheEffectRaisesUncertain(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		// Another claimant holds the effect: its started row exists but is
		// not in this context's outcome snapshot (seeded before the race),
		// so the claim insert must fail instead of running the effect again.
		id, err := nsPaying.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		if err := store.CreateStep(context.Background(), db, &store.Step{
			Workflow: store.UUID{UUID: id},
			StepID:   "1",
			Name:     "ns.charge",
			Status:   store.StepStarted,
		}); err != nil {
			t.Fatal(err)
		}
		rs := esc.NewRun(id, "ns.paying", db, esc.NewOutcomeStore(), &atomic.Bool{})
		ctx := esc.With(context.Background(), rs)
		_, err = runner.RunStep(ctx, "ns.charge", true, struct{}{}, nil, nsChargeFn)
		var u *es.EffectUncertain
		if !errors.As(err, &u) {
			t.Fatalf("want EffectUncertain, got %v", err)
		}
		assertCalls(t)
	})
}

func TestStartedRowForUnmarkedStepReexecutes(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		// A started row left by a previous attempt of a step that no longer
		// marks itself unsafe to repeat: the current code wins and
		// re-executes.
		id, err := nsPlainFlow.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		if err := store.CreateStep(context.Background(), db, &store.Step{
			Workflow: store.UUID{UUID: id},
			StepID:   "1",
			Name:     "ns.plain_effect",
			Status:   store.StepStarted,
		}); err != nil {
			t.Fatal(err)
		}
		reClaim(t, db, id)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}
		if string(wf.Result) != `"v"` {
			t.Fatalf("result %s, want \"v\"", wf.Result)
		}
		assertCalls(t, "plain_effect")
		rows := stepsFor(t, db, id)
		if len(rows) != 1 || rows[0].StepID != "1" {
			t.Fatalf("step rows %v, want exactly step 1", stepIDs(t, db, id))
		}
		if rows[0].Status != store.StepDone {
			t.Fatalf("step 1 status %s, want done", rows[0].Status)
		}
		if string(rows[0].Result) != `"v"` {
			t.Fatalf("step 1 result %s, want \"v\"", rows[0].Result)
		}
	})
}

func TestUncertainStepInParallelBlocksWithoutGroup(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := nsParallelPay.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashInMarkedStep(t, db, id, "1.0.1")
		block(t, db, id)
		// Each branch's effect ran once; the uncertain step was not
		// re-run.
		got := calls.listCopy()
		sort.Strings(got)
		if len(got) != 2 || got[0] != "charge" || got[1] != "confirm" {
			t.Fatalf("calls %v, want [charge confirm]", got)
		}
	})
}
