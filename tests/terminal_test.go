package tests

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"gorm.io/gorm"

	es "github.com/brutasse/everystep-go/everystep"
	"github.com/brutasse/everystep-go/runner"
	"github.com/brutasse/everystep-go/store"
)

type tmUploadArgs struct {
	Node string `json:"node"`
}

func tmUploadFn(ctx context.Context, a tmUploadArgs) (string, error) {
	calls.add("upload:" + a.Node)
	return "", &es.Terminal{Reason: "node-full", Data: map[string]any{"node_id": a.Node}}
}

func tmAfterFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("after")
	return "done", nil
}

var (
	tmUpload = es.Step("tm.upload", tmUploadFn)
	tmAfter  = es.Step("tm.after", tmAfterFn)
)

func tmUploadFlowBody(ctx context.Context, a tmUploadArgs) (string, error) {
	if _, err := tmUpload.Call(ctx, a); err != nil {
		return "", err
	}
	if _, err := tmAfter.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	return "ok", nil
}

var tmUploadFlow = es.Workflow("tm.upload_flow", tmUploadFlowBody)

func TestTerminalStopsWorkflow(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetCalls(t)
		id, err := tmUploadFlow.Schedule(db, tmUploadArgs{Node: "a"})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusStopped {
			t.Fatalf("status %s, want stopped", wf.Status)
		}
		if wf.CompletedAt == nil {
			t.Fatal("stopped run must have a completed_at")
		}
		// The in-flight step ran and was recorded; the later step never
		// started.
		assertCalls(t, "upload:a")
		steps := stepsFor(t, db, id)
		if len(steps) != 1 {
			t.Fatalf("got %d step rows, want 1", len(steps))
		}
		if steps[0].StepID != "1" {
			t.Fatalf("step id %q, want 1", steps[0].StepID)
		}
		if steps[0].Status != store.StepFailed {
			t.Fatalf("step status %s, want failed", steps[0].Status)
		}
		// The caller reads the reason and payload back from the run's
		// stored error.
		var term *es.Terminal
		if !errors.As(runErrorDecoded(t, wf), &term) {
			t.Fatalf("stored error is not a Terminal: %v", runErrorDecoded(t, wf))
		}
		if term.Reason != "node-full" {
			t.Fatalf("terminal reason %q, want node-full", term.Reason)
		}
		var payload map[string]any
		if err := json.Unmarshal(runError(t, wf).Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["node_id"] != "a" {
			t.Fatalf("terminal payload %v, want node_id a", payload)
		}
	})
}

func tmOKBranchFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("ok_branch")
	return "ok", nil
}

func tmTerminalBranchFn(ctx context.Context, a struct{}) (string, error) {
	calls.add("terminal_branch")
	return "", &es.Terminal{Reason: "stop", Data: map[string]any{"x": 1}}
}

var (
	tmOKBranch       = es.Step("tm.ok_branch", tmOKBranchFn)
	tmTerminalBranch = es.Step("tm.terminal_branch", tmTerminalBranchFn)
)

func tmParallelTerminalFlowBody(ctx context.Context, a struct{}) ([]string, error) {
	var av, bv string
	err := es.Parallel(ctx).Run(
		func(c context.Context) error {
			var e error
			av, e = tmOKBranch.Call(c, struct{}{})
			return e
		},
		func(c context.Context) error {
			var e error
			bv, e = tmTerminalBranch.Call(c, struct{}{})
			return e
		},
	)
	if err != nil {
		return nil, err
	}
	return []string{av, bv}, nil
}

var tmParallelTerminalFlow = es.Workflow("tm.parallel_terminal_flow", tmParallelTerminalFlowBody)

func TestTerminalInParallelStopsWorkflow(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := tmParallelTerminalFlow.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		if err := runner.Execute(context.Background(), db, id, &atomic.Bool{}); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusStopped {
			t.Fatalf("status %s, want stopped", wf.Status)
		}
		var term *es.Terminal
		if !errors.As(runErrorDecoded(t, wf), &term) {
			t.Fatalf("stored error is not a Terminal: %v", runErrorDecoded(t, wf))
		}
		if term.Reason != "stop" {
			t.Fatalf("terminal reason %q, want stop", term.Reason)
		}
		var payload map[string]any
		if err := json.Unmarshal(runError(t, wf).Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["x"] != float64(1) {
			t.Fatalf("terminal payload %v, want x 1", payload)
		}
	})
}
