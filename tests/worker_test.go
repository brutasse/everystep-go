package tests

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	es "github.com/brutasse/everystep-go/everystep"
	"github.com/brutasse/everystep-go/serde"
	"github.com/brutasse/everystep-go/store"
	"github.com/brutasse/everystep-go/worker"
)

var (
	wkStepSleep      time.Duration
	wkSleeperStarted atomic.Bool
)

func resetWkSleeper(t *testing.T, sleep time.Duration) {
	t.Helper()
	wkSleeperStarted.Store(false)
	wkStepSleep = sleep
	t.Cleanup(func() {
		wkStepSleep = 0
		wkSleeperStarted.Store(false)
	})
}

func wkNoopFn(ctx context.Context, a struct{}) (string, error) {
	return "", nil
}

func wkQuickFn(ctx context.Context, a struct{}) (string, error) {
	return "q", nil
}

func wkSleeperFn(ctx context.Context, a struct{}) (string, error) {
	wkSleeperStarted.Store(true)
	if d := wkStepSleep; d > 0 {
		time.Sleep(d)
	}
	return "s", nil
}

func wkFinishFn(ctx context.Context, a struct{}) (string, error) {
	return "done", nil
}

var (
	wkNoop    = es.Step("wk.noop", wkNoopFn)
	wkQuick   = es.Step("wk.quick", wkQuickFn)
	wkSleeper = es.Step("wk.sleeper", wkSleeperFn)
	wkFinish  = es.Step("wk.finish", wkFinishFn)
)

func wkSimpleBody(ctx context.Context, a struct{}) (string, error) {
	if _, err := wkNoop.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	return "done", nil
}

var wkSimple = es.Workflow("wk.simple", wkSimpleBody)

func wkOnlySleeperBody(ctx context.Context, a struct{}) (string, error) {
	return wkSleeper.Call(ctx, struct{}{})
}

var wkOnlySleeper = es.Workflow("wk.only_sleeper", wkOnlySleeperBody)

func wkSlowFlowBody(ctx context.Context, a struct{}) (string, error) {
	if _, err := wkQuick.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	if _, err := wkSleeper.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	return wkFinish.Call(ctx, struct{}{})
}

var wkSlowFlow = es.Workflow("wk.slow_flow", wkSlowFlowBody)

// workerHandle runs a worker in a goroutine; cleanup cancels it and waits
// for it to stop.
type workerHandle struct {
	cancel  context.CancelFunc
	stopped chan struct{}
}

func startWorker(t *testing.T, w *worker.Worker) *workerHandle {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h := &workerHandle{cancel: cancel, stopped: make(chan struct{})}
	go func() {
		defer close(h.stopped)
		if err := w.Run(ctx); err != nil {
			t.Errorf("worker run: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-h.stopped:
		case <-time.After(20 * time.Second):
			t.Fatal("worker did not stop within 20s")
		}
	})
	return h
}

// waitFor polls cond until it holds or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestWorkerCatchupResumesOwnWorkflows(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := wkSimple.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimed, err := store.ClaimScheduled(context.Background(), db, 1, "wk-catchup")
		if err != nil {
			t.Fatal(err)
		}
		if len(claimed) != 1 || claimed[0].ID.UUID != id {
			t.Fatalf("claimed %v, want [%s]", claimedIDs(claimed), id)
		}
		// A fresh process with the same name picks the run up at startup.
		w := worker.New(worker.Opts{DB: db, Pool: 1, Poll: 50 * time.Millisecond, Name: "wk-catchup"})
		h := startWorker(t, w)
		waitFor(t, 30*time.Second, "catchup completion", func() bool {
			return getRun(t, db, id).Status == store.StatusCompleted
		})
		h.cancel()
		<-h.stopped
		wf := getRun(t, db, id)
		var result string
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result != "done" {
			t.Fatalf("result %q, want %q", result, "done")
		}
	})
}

func TestWorkerLoopCompletesDueWorkflows(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		for i := 0; i < 2; i++ {
			if _, err := wkSimple.Schedule(db, struct{}{}); err != nil {
				t.Fatal(err)
			}
		}
		w := worker.New(worker.Opts{DB: db, Pool: 2, Poll: 50 * time.Millisecond, Name: "wk-loop"})
		h := startWorker(t, w)
		waitFor(t, 30*time.Second, "two completions", func() bool {
			var n int64
			if err := db.Table("everystep_workflow").Where("status = ?", store.StatusCompleted).Count(&n).Error; err != nil {
				t.Fatal(err)
			}
			return n == 2
		})
		h.cancel()
		<-h.stopped
		var n int64
		if err := db.Table("everystep_workflow").Where("status = ?", store.StatusCompleted).Count(&n).Error; err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("got %d completed runs, want 2", n)
		}
	})
}

func TestDrainCompletesInflightWithinDeadline(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetWkSleeper(t, time.Second)
		id, err := wkOnlySleeper.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		pending, err := wkOnlySleeper.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		w := worker.New(worker.Opts{DB: db, Pool: 1, Poll: 50 * time.Millisecond, Name: "wk-drain", Drain: 5 * time.Second})
		h := startWorker(t, w)
		waitFor(t, 30*time.Second, "sleeper to start", func() bool {
			return getRun(t, db, id).Status == store.StatusRunning && wkSleeperStarted.Load()
		})
		h.cancel()
		<-h.stopped

		wf := getRun(t, db, id)
		if wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}
		var result string
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result != "s" {
			t.Fatalf("result %q, want %q", result, "s")
		}
		assertStepIDs(t, stepIDs(t, db, id), "1")
		// Nothing is claimed after stop: the second workflow stays
		// scheduled.
		if p := getRun(t, db, pending); p.Status != store.StatusScheduled {
			t.Fatalf("pending status %s, want scheduled", p.Status)
		}
	})
}

func TestDrainRequeuesAtStepBoundaryAndNewNameCompletes(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetWkSleeper(t, time.Second)
		id, err := wkSlowFlow.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		w1 := worker.New(worker.Opts{DB: db, Pool: 1, Poll: 50 * time.Millisecond, Name: "wk-drain-r", Drain: 5 * time.Second})
		h1 := startWorker(t, w1)
		waitFor(t, 30*time.Second, "sleeper to start", func() bool {
			return getRun(t, db, id).Status == store.StatusRunning && wkSleeperStarted.Load()
		})
		h1.cancel()
		<-h1.stopped

		// Stop landed mid step 2: step 2 finishes and is stored, step 3
		// never starts, and the run is requeued for any runner.
		wf := getRun(t, db, id)
		if wf.Status != store.StatusScheduled {
			t.Fatalf("status %s, want scheduled", wf.Status)
		}
		if wf.ClaimedBy != nil {
			t.Fatalf("claimed_by %v, want nil", *wf.ClaimedBy)
		}
		assertStepIDs(t, stepIDs(t, db, id), "1", "2")

		w2 := worker.New(worker.Opts{DB: db, Pool: 1, Poll: 50 * time.Millisecond, Name: "wk-drain-r2"})
		h2 := startWorker(t, w2)
		waitFor(t, 30*time.Second, "completion under the new name", func() bool {
			return getRun(t, db, id).Status == store.StatusCompleted
		})
		h2.cancel()
		<-h2.stopped

		wf = getRun(t, db, id)
		var result string
		if err := serde.Unmarshal(wf.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result != "done" {
			t.Fatalf("result %q, want %q", result, "done")
		}
		assertStepIDs(t, stepIDs(t, db, id), "1", "2", "3")
	})
}

func TestDrainDeadlineRequeuesLeftovers(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetWkSleeper(t, 2*time.Second)
		id, err := wkSlowFlow.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		w := worker.New(worker.Opts{DB: db, Pool: 1, Poll: 50 * time.Millisecond, Name: "wk-drain-t", Drain: 300 * time.Millisecond})
		h := startWorker(t, w)
		waitFor(t, 30*time.Second, "sleeper to start", func() bool {
			return getRun(t, db, id).Status == store.StatusRunning && wkSleeperStarted.Load()
		})
		h.cancel()
		<-h.stopped

		// The in-flight run outlived the drain deadline: it was requeued,
		// so nothing is left claimed by the exiting worker.
		wf := getRun(t, db, id)
		if wf.Status != store.StatusScheduled {
			t.Fatalf("status %s, want scheduled", wf.Status)
		}
		if wf.ClaimedBy != nil {
			t.Fatalf("claimed_by %v, want nil", *wf.ClaimedBy)
		}
		ids := stepIDs(t, db, id)
		if len(ids) != 1 || ids[0] != "1" {
			t.Fatalf("recorded steps %v, want [1]: the sleeper was still in flight", ids)
		}
	})
}

// E2E: a real worker subprocess against the test database.

func scheduleRaw(t *testing.T, db *gorm.DB, name string) uuid.UUID {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	wf := &store.Workflow{
		ID:        store.UUID{UUID: id},
		Name:      name,
		Args:      []byte("{}"),
		Status:    store.StatusScheduled,
		CreatedAt: time.Now().UTC(),
	}
	if err := db.Create(wf).Error; err != nil {
		t.Fatal(err)
	}
	return id
}

func spawnWorkerProc(t *testing.T, b Backend, name string, drainSec, sleepSec int) *exec.Cmd {
	t.Helper()
	driver := "postgres"
	if b.Name == "mariadb" {
		driver = "mysql"
	}
	cmd := exec.Command(workerBin)
	cmd.Env = append(os.Environ(),
		"EVERYSTEP_E2E_DRIVER="+driver,
		"EVERYSTEP_E2E_DSN="+b.DSN,
		"EVERYSTEP_E2E_NAME="+name,
		fmt.Sprintf("EVERYSTEP_E2E_DRAIN=%d", drainSec),
		fmt.Sprintf("EVERYSTEP_E2E_STEP_SLEEP=%d", sleepSec),
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_, _ = cmd.Process.Wait()
	})
	return cmd
}

func waitForStep(t *testing.T, db *gorm.DB, id uuid.UUID, stepID string) {
	t.Helper()
	waitFor(t, 30*time.Second, "step "+stepID+" to be recorded", func() bool {
		for _, s := range stepIDs(t, db, id) {
			if s == stepID {
				return true
			}
		}
		return false
	})
}

func TestE2ESigtermRequeuesAndNewNameCompletes(t *testing.T) {
	for _, b := range backends {
		t.Run(b.Name, func(t *testing.T) {
			truncateTables(t, b.DB)
			id := scheduleRaw(t, b.DB, "e2e.slow_flow")
			name := "e2e-rollout-w"

			proc := spawnWorkerProc(t, b, name, 2, 30)
			waitForStep(t, b.DB, id, "1")
			time.Sleep(500 * time.Millisecond) // let the sleeper get into its long sleep

			t0 := time.Now()
			if err := proc.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			if err := proc.Wait(); err != nil {
				t.Fatalf("worker exited with %v", err)
			}
			if elapsed := time.Since(t0); elapsed > 8*time.Second {
				t.Fatalf("worker took %s to exit after SIGTERM (drain is 2s)", elapsed)
			}

			wf := getRun(t, b.DB, id)
			if wf.Status != store.StatusScheduled {
				t.Fatalf("status %s, want scheduled", wf.Status)
			}
			if wf.ClaimedBy != nil {
				t.Fatalf("claimed_by %v, want nil", *wf.ClaimedBy)
			}
			// The in-flight step had not finished before the exit: only
			// step 1 stored.
			assertStepIDs(t, stepIDs(t, b.DB, id), "1")

			spawnWorkerProc(t, b, name+"2", 2, 0)
			waitFor(t, 30*time.Second, "handoff completion", func() bool {
				return getRun(t, b.DB, id).Status == store.StatusCompleted
			})
			wf = getRun(t, b.DB, id)
			var result string
			if err := serde.Unmarshal(wf.Result, &result); err != nil {
				t.Fatal(err)
			}
			if result != "done" {
				t.Fatalf("result %q, want %q", result, "done")
			}
			// Step 1 replayed from the store; steps 2 and 3 executed.
			assertStepIDs(t, stepIDs(t, b.DB, id), "1", "2", "3")
		})
	}
}

func TestE2ESigkillParksAndSameNameResumes(t *testing.T) {
	for _, b := range backends {
		t.Run(b.Name, func(t *testing.T) {
			truncateTables(t, b.DB)
			id := scheduleRaw(t, b.DB, "e2e.slow_flow")
			name := "e2e-rollout-k"

			proc := spawnWorkerProc(t, b, name, 2, 30)
			waitForStep(t, b.DB, id, "1")
			time.Sleep(500 * time.Millisecond) // let the sleeper get into its long sleep

			if err := proc.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = proc.Wait()

			// A killed process cannot requeue its runs: they stay parked.
			wf := getRun(t, b.DB, id)
			if wf.Status != store.StatusRunning {
				t.Fatalf("status %s, want running", wf.Status)
			}
			if wf.ClaimedBy == nil || *wf.ClaimedBy != name {
				t.Fatalf("claimed_by %v, want %s", wf.ClaimedBy, name)
			}
			assertStepIDs(t, stepIDs(t, b.DB, id), "1")

			spawnWorkerProc(t, b, name, 2, 0)
			waitFor(t, 30*time.Second, "same-name resume completion", func() bool {
				return getRun(t, b.DB, id).Status == store.StatusCompleted
			})
			wf = getRun(t, b.DB, id)
			var result string
			if err := serde.Unmarshal(wf.Result, &result); err != nil {
				t.Fatal(err)
			}
			if result != "done" {
				t.Fatalf("result %q, want %q", result, "done")
			}
			// Step 1 replayed from the store; steps 2 and 3 executed.
			assertStepIDs(t, stepIDs(t, b.DB, id), "1", "2", "3")
		})
	}
}
