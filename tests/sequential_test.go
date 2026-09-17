package tests

import (
	"context"
	"reflect"
	"testing"

	"gorm.io/gorm"

	es "github.com/brutasse/everystep-go/everystep"
	"github.com/brutasse/everystep-go/serde"
	"github.com/brutasse/everystep-go/store"
)

type seqChainArgs struct {
	N int `json:"n"`
}

func seqAlphaFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("alpha")
	return "A", nil
}

func seqBetaFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("beta")
	return "B", nil
}

func seqGammaFn(ctx context.Context, a string) (string, error) {
	calls.add("gamma")
	return a + "!", nil
}

var (
	seqAlpha = es.Step("seq.alpha", seqAlphaFn)
	seqBeta  = es.Step("seq.beta", seqBetaFn)
	seqGamma = es.Step("seq.gamma", seqGammaFn)
)

func seqChainBody(ctx context.Context, a seqChainArgs) (string, error) {
	x, err := seqAlpha.Call(ctx, struct{}{})
	if err != nil {
		return "", err
	}
	y, err := seqBeta.Call(ctx, struct{}{})
	if err != nil {
		return "", err
	}
	return seqGamma.Call(ctx, x+y)
}

var seqChain = es.Workflow("seq.chain", seqChainBody)

func TestSequentialRunToCompletion(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := seqChain.Schedule(db, seqChainArgs{N: 7})
		if err != nil {
			t.Fatal(err)
		}
		wf := runToCompletion(t, db, id)
		if wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}
		var result string
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result != "AB!" {
			t.Fatalf("result %q, want %q", result, "AB!")
		}
		var args struct {
			N int `json:"n"`
		}
		if err := serde.Unmarshal(wf.Args, &args); err != nil {
			t.Fatal(err)
		}
		if args.N != 7 {
			t.Fatalf("args %s, want n=7", wf.Args)
		}
		steps := stepsFor(t, db, id)
		if len(steps) != 3 {
			t.Fatalf("got %d step rows, want 3", len(steps))
		}
		if got := []string{steps[0].StepID, steps[1].StepID, steps[2].StepID}; !reflect.DeepEqual(got, []string{"1", "2", "3"}) {
			t.Fatalf("step ids %v, want [1 2 3]", got)
		}
		if got := []string{steps[0].Name, steps[1].Name, steps[2].Name}; !reflect.DeepEqual(got, []string{"seq.alpha", "seq.beta", "seq.gamma"}) {
			t.Fatalf("step names %v, want [seq.alpha seq.beta seq.gamma]", got)
		}
		for _, s := range steps {
			if s.Status != store.StepDone {
				t.Fatalf("step %s status %s, want done", s.StepID, s.Status)
			}
		}
		if string(steps[2].Args) != `"AB"` {
			t.Fatalf("gamma args %s, want \"AB\"", steps[2].Args)
		}
		if string(steps[2].Result) != `"AB!"` {
			t.Fatalf("gamma result %s, want \"AB!\"", steps[2].Result)
		}
		assertCalls(t, "alpha", "beta", "gamma")
	})
}

func TestScheduleRollsBackWithCallerTransaction(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		tx := db.Begin()
		if err := tx.Error; err != nil {
			t.Fatal(err)
		}
		if _, err := seqChain.Schedule(tx, seqChainArgs{N: 1}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback().Error; err != nil {
			t.Fatal(err)
		}
		var n int64
		if err := db.Table("everystep_workflow").Count(&n).Error; err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("got %d workflow rows after rollback, want 0", n)
		}
	})
}

func TestScheduleCommitsWithCallerTransaction(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		tx := db.Begin()
		if err := tx.Error; err != nil {
			t.Fatal(err)
		}
		if _, err := seqChain.Schedule(tx, seqChainArgs{N: 1}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit().Error; err != nil {
			t.Fatal(err)
		}
		var n int64
		if err := db.Table("everystep_workflow").Count(&n).Error; err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("got %d workflow rows after commit, want 1", n)
		}
	})
}

func TestWorkflowIDIsTimeBasedUUID7(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := seqChain.Schedule(db, seqChainArgs{N: 1})
		if err != nil {
			t.Fatal(err)
		}
		if id.Version() != 7 {
			t.Fatalf("workflow id version %d, want 7", id.Version())
		}
	})
}

func TestIdempotencyKeyReturnsExistingWorkflow(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		first, err := seqChain.Schedule(db, seqChainArgs{N: 1}, es.Idempotency("order-42"))
		if err != nil {
			t.Fatal(err)
		}
		second, err := seqChain.Schedule(db, seqChainArgs{N: 2}, es.Idempotency("order-42"))
		if err != nil {
			t.Fatal(err)
		}
		if first != second {
			t.Fatalf("idempotent schedules returned different runs: %s vs %s", first, second)
		}
		var n int64
		if err := db.Table("everystep_workflow").Count(&n).Error; err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("got %d workflow rows, want 1", n)
		}
		wf := getRun(t, db, first)
		var args struct {
			N int `json:"n"`
		}
		if err := serde.Unmarshal(wf.Args, &args); err != nil {
			t.Fatal(err)
		}
		if args.N != 1 {
			t.Fatalf("first args should win: got %s, want n=1", wf.Args)
		}
	})
}

func TestDirectCallBypassesPersistence(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		got, err := seqChainBody(context.Background(), seqChainArgs{N: 1})
		if err != nil {
			t.Fatal(err)
		}
		if got != "AB!" {
			t.Fatalf("direct call result %q, want %q", got, "AB!")
		}
		a, err := seqAlpha.Call(context.Background(), struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		if a != "A" {
			t.Fatalf("direct step result %q, want %q", a, "A")
		}
		assertCalls(t, "alpha", "beta", "gamma", "alpha")
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
