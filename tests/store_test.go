package tests

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/brutasse/everystep-go/store"
)

func scheduleRow(t *testing.T, db *gorm.DB, name string, createdAt time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	wf := &store.Workflow{
		ID:        store.UUID{UUID: id},
		Name:      name,
		Args:      []byte("[]"),
		Status:    store.StatusScheduled,
		CreatedAt: createdAt.UTC(),
	}
	if err := db.Create(wf).Error; err != nil {
		t.Fatalf("create run: %v", err)
	}
	return id
}

func TestClaimOrdersByCreatedAtAndLimits(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		base := time.Now().UTC().Add(-time.Hour)
		var ids [3]uuid.UUID
		for i := range ids {
			// Insert out of created_at order: the claim must order by
			// created_at, not by insertion order.
			ids[(2-i)%3] = scheduleRow(t, db, "claim-order", base.Add(time.Duration(i)*time.Second))
		}
		// ids[2] is oldest, ids[0] newest.

		claimed, err := store.ClaimScheduled(context.Background(), db, 2, "w1")
		if err != nil {
			t.Fatal(err)
		}
		if len(claimed) != 2 {
			t.Fatalf("claimed %d, want 2", len(claimed))
		}
		if claimed[0].ID.UUID != ids[2] || claimed[1].ID.UUID != ids[1] {
			t.Fatalf("claimed %v, want [%s %s] (oldest first)", claimedIDs(claimed), ids[2], ids[1])
		}
		for i := range claimed {
			if claimed[i].Status != store.StatusRunning || claimed[i].ClaimedBy == nil || *claimed[i].ClaimedBy != "w1" {
				t.Fatalf("run %s: status=%q claimed_by=%v", claimed[i].ID, claimed[i].Status, claimed[i].ClaimedBy)
			}
		}
		// The newest run is still scheduled and unclaimed.
		rest := getRun(t, db, ids[0])
		if rest.Status != store.StatusScheduled || rest.ClaimedBy != nil {
			t.Fatalf("rest: status=%q claimed_by=%v", rest.Status, rest.ClaimedBy)
		}
	})
}

func TestClaimIsExclusiveAcrossWorkers(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		for i := 0; i < 6; i++ {
			scheduleRow(t, db, "claim-exclusive", time.Now().UTC().Add(time.Duration(i)*time.Millisecond))
		}
		var mu sync.Mutex
		var claimed [][]store.Workflow
		barrier := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < 3; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-barrier
				got, err := store.ClaimScheduled(context.Background(), db, 3, "worker-claim"+strconv.Itoa(w))
				if err != nil {
					t.Errorf("worker %d: %v", w, err)
					return
				}
				mu.Lock()
				claimed = append(claimed, got)
				mu.Unlock()
			}(w)
		}
		close(barrier)
		wg.Wait()

		seen := map[string]bool{}
		total := 0
		for _, batch := range claimed {
			for _, w := range batch {
				total++
				if seen[w.ID.UUID.String()] {
					t.Fatalf("run %s claimed twice", w.ID)
				}
				seen[w.ID.UUID.String()] = true
			}
		}
		if total != 6 {
			t.Fatalf("claimed %d total, want 6", total)
		}
	})
}

func TestClaimExcludesOtherStatuses(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		scheduled := scheduleRow(t, db, "claim-status", time.Now().UTC())
		other := scheduleRow(t, db, "claim-status", time.Now().UTC().Add(time.Second))
		if err := db.Model(&store.Workflow{}).Where("id = ?", store.UUID{UUID: other}).
			Update("status", store.StatusRunning).Error; err != nil {
			t.Fatal(err)
		}
		claimed, err := store.ClaimScheduled(context.Background(), db, 10, "w1")
		if err != nil {
			t.Fatal(err)
		}
		if len(claimed) != 1 || claimed[0].ID.UUID != scheduled {
			t.Fatalf("claimed %v, want [%s]", claimedIDs(claimed), scheduled)
		}
	})
}

func TestOtherRunnerCannotClaimOrReclaim(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		scheduleRow(t, db, "claim-other", time.Now().UTC())
		if _, err := store.ClaimScheduled(context.Background(), db, 1, "w1"); err != nil {
			t.Fatal(err)
		}
		// w1's running workflow is invisible to w2: neither as new work nor
		// as a restart of w2's own in-flight work.
		got, err := store.ClaimScheduled(context.Background(), db, 1, "w2")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("w2 claimed %v, want none", claimedIDs(got))
		}
		own, err := store.ResumeOwn(context.Background(), db, 1, "w2")
		if err != nil {
			t.Fatal(err)
		}
		if len(own) != 0 {
			t.Fatalf("w2 resumed %v, want none", claimedIDs(own))
		}
	})
}

func TestResumeOwnReturnsOwnRunningRuns(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		own := scheduleRow(t, db, "claim-resume", time.Now().UTC())
		other := scheduleRow(t, db, "claim-resume", time.Now().UTC().Add(time.Second))
		if _, err := store.ClaimScheduled(context.Background(), db, 1, "w1"); err != nil {
			t.Fatal(err)
		}
		if err := db.Model(&store.Workflow{}).Where("id = ?", store.UUID{UUID: other}).
			Updates(map[string]any{"status": store.StatusRunning, "claimed_by": "w2"}).Error; err != nil {
			t.Fatal(err)
		}
		claimed, err := store.ResumeOwn(context.Background(), db, 1, "w1")
		if err != nil {
			t.Fatal(err)
		}
		if len(claimed) != 1 || claimed[0].ID.UUID != own {
			t.Fatalf("resumed %v, want [%s]", claimedIDs(claimed), own)
		}
	})
}

func TestSetTerminalGuardedOnRunning(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		wf := scheduleRow(t, db, "terminal-guard", time.Now().UTC())
		now := time.Now().UTC()
		// Not running yet: the guarded update must not land.
		updated, err := store.SetTerminal(context.Background(), db, store.UUID{UUID: wf}, store.StatusCompleted, []byte(`"r"`), nil, now)
		if err != nil {
			t.Fatal(err)
		}
		if updated {
			t.Fatal("terminal update on a scheduled run: want no update")
		}
		if _, err := store.ClaimScheduled(context.Background(), db, 1, "w1"); err != nil {
			t.Fatal(err)
		}
		updated, err = store.SetTerminal(context.Background(), db, store.UUID{UUID: wf}, store.StatusCompleted, []byte(`"r"`), nil, now)
		if err != nil {
			t.Fatal(err)
		}
		if !updated {
			t.Fatal("terminal update on a running run: want an update")
		}
		// A second terminal write must not overwrite the first.
		updated, err = store.SetTerminal(context.Background(), db, store.UUID{UUID: wf}, store.StatusFailed, nil, []byte(`{"message":"x"}`), now)
		if err != nil {
			t.Fatal(err)
		}
		if updated {
			t.Fatal("terminal update on a completed run: want no update")
		}
		rest := getRun(t, db, wf)
		if rest.Status != store.StatusCompleted || len(rest.Result) == 0 || rest.Error != nil || rest.CompletedAt == nil {
			t.Fatalf("run: status=%q result=%v error=%v completed_at=%v", rest.Status, rest.Result, rest.Error, rest.CompletedAt)
		}
	})
}

func TestMarkFailedClaimedGuarded(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		wf := scheduleRow(t, db, "fail-guard", time.Now().UTC())
		if _, err := store.ClaimScheduled(context.Background(), db, 1, "w1"); err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC()
		// Another runner's failure must not land on w1's live run.
		updated, err := store.MarkFailedClaimed(context.Background(), db, store.UUID{UUID: wf}, "w2", []byte(`{"message":"x"}`), now)
		if err != nil {
			t.Fatal(err)
		}
		if updated {
			t.Fatal("failure by another claimer: want no update")
		}
		updated, err = store.MarkFailedClaimed(context.Background(), db, store.UUID{UUID: wf}, "w1", []byte(`{"message":"x"}`), now)
		if err != nil {
			t.Fatal(err)
		}
		if !updated {
			t.Fatal("failure by the claimer: want an update")
		}
		rest := getRun(t, db, wf)
		if rest.Status != store.StatusFailed || rest.ClaimedBy == nil {
			t.Fatalf("run: status=%q claimed_by=%v", rest.Status, rest.ClaimedBy)
		}
		// A late failure after a requeue must not land: a run requeued at
		// the drain deadline may belong to another runner by the time it
		// arrives.
		late := scheduleRow(t, db, "fail-guard", time.Now().UTC())
		if _, err := store.ClaimScheduled(context.Background(), db, 1, "w1"); err != nil {
			t.Fatal(err)
		}
		if ok, err := store.Requeue(context.Background(), db, store.UUID{UUID: late}, "w1"); err != nil || !ok {
			t.Fatalf("requeue: ok=%v err=%v", ok, err)
		}
		updated, err = store.MarkFailedClaimed(context.Background(), db, store.UUID{UUID: late}, "w1", []byte(`{"message":"x"}`), now)
		if err != nil {
			t.Fatal(err)
		}
		if updated {
			t.Fatal("late failure after requeue: want no update")
		}
		lateRun := getRun(t, db, late)
		if lateRun.Status != store.StatusScheduled || lateRun.ClaimedBy != nil {
			t.Fatalf("late run: status=%q claimed_by=%v", lateRun.Status, lateRun.ClaimedBy)
		}
	})
}

func TestRequeueGuarded(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		wf := scheduleRow(t, db, "requeue-guard", time.Now().UTC())
		updated, err := store.Requeue(context.Background(), db, store.UUID{UUID: wf}, "w1")
		if err != nil {
			t.Fatal(err)
		}
		if updated {
			t.Fatal("requeue of an unclaimed run: want no update")
		}
		if _, err := store.ClaimScheduled(context.Background(), db, 1, "w1"); err != nil {
			t.Fatal(err)
		}
		updated, err = store.Requeue(context.Background(), db, store.UUID{UUID: wf}, "w1")
		if err != nil {
			t.Fatal(err)
		}
		if !updated {
			t.Fatal("requeue of the claimer's run: want an update")
		}
		rest := getRun(t, db, wf)
		if rest.Status != store.StatusScheduled || rest.ClaimedBy != nil {
			t.Fatalf("run: status=%q claimed_by=%v", rest.Status, rest.ClaimedBy)
		}
	})
}

func TestDuplicateStepUniqueKey(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		wf := scheduleRow(t, db, "dup-step", time.Now().UTC())
		step := &store.Step{
			Workflow: store.UUID{UUID: wf},
			StepID:   "1",
			Name:     "dup-step.s",
			Status:   store.StepDone,
		}
		if err := store.CreateStep(context.Background(), db, step); err != nil {
			t.Fatal(err)
		}
		err := store.CreateStep(context.Background(), db, &store.Step{
			Workflow: store.UUID{UUID: wf},
			StepID:   "1",
			Name:     "dup-step.s",
			Status:   store.StepStarted,
		})
		if !errors.Is(err, gorm.ErrDuplicatedKey) {
			t.Fatalf("duplicate (workflow, step_id): err = %v, want ErrDuplicatedKey", err)
		}
		// A different step id on the same run is fine.
		if err := store.CreateStep(context.Background(), db, &store.Step{
			Workflow: store.UUID{UUID: wf},
			StepID:   "2",
			Name:     "dup-step.s",
			Status:   store.StepDone,
		}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestDuplicateWorkflowIdempotencyKey(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		key := "order-42"
		first := &store.Workflow{
			ID:             store.UUID{UUID: uuid.New()},
			Name:           "dup-wf",
			Args:           []byte("[]"),
			IdempotencyKey: &key,
			Status:         store.StatusScheduled,
			CreatedAt:      time.Now().UTC(),
		}
		if err := db.Create(first).Error; err != nil {
			t.Fatal(err)
		}
		second := &store.Workflow{
			ID:             store.UUID{UUID: uuid.New()},
			Name:           "dup-wf",
			Args:           []byte("[]"),
			IdempotencyKey: &key,
			Status:         store.StatusScheduled,
			CreatedAt:      time.Now().UTC(),
		}
		err := db.Create(second).Error
		if !errors.Is(err, gorm.ErrDuplicatedKey) {
			t.Fatalf("duplicate (name, idempotency_key): err = %v, want ErrDuplicatedKey", err)
		}
		// NULL keys never collide: two runs without a key are distinct.
		a := &store.Workflow{ID: store.UUID{UUID: uuid.New()}, Name: "dup-wf", Args: []byte("[]"), Status: store.StatusScheduled, CreatedAt: time.Now().UTC()}
		b := &store.Workflow{ID: store.UUID{UUID: uuid.New()}, Name: "dup-wf", Args: []byte("[]"), Status: store.StatusScheduled, CreatedAt: time.Now().UTC()}
		if err := db.Create(a).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(b).Error; err != nil {
			t.Fatalf("runs without an idempotency key must not collide: %v", err)
		}
	})
}

func TestStepFKCascade(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		wf := scheduleRow(t, db, "fk-cascade", time.Now().UTC())
		if err := store.CreateStep(context.Background(), db, &store.Step{
			Workflow: store.UUID{UUID: wf},
			StepID:   "1",
			Name:     "fk-cascade.s",
			Status:   store.StepDone,
		}); err != nil {
			t.Fatal(err)
		}
		if err := db.Delete(&store.Workflow{}, "id = ?", store.UUID{UUID: wf}).Error; err != nil {
			t.Fatal(err)
		}
		var n int64
		if err := db.Model(&store.Step{}).Where("workflow = ?", store.UUID{UUID: wf}).Count(&n).Error; err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("steps after run delete: %d, want 0 (ON DELETE CASCADE)", n)
		}
	})
}

func TestListStepsOrdersByID(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		wf := scheduleRow(t, db, "list-steps", time.Now().UTC())
		for _, id := range []string{"3", "1", "2"} {
			if err := store.CreateStep(context.Background(), db, &store.Step{
				Workflow: store.UUID{UUID: wf},
				StepID:   id,
				Name:     "list-steps.s",
				Status:   store.StepDone,
			}); err != nil {
				t.Fatal(err)
			}
		}
		steps, err := store.ListSteps(context.Background(), db, store.UUID{UUID: wf})
		if err != nil {
			t.Fatal(err)
		}
		// Order by the row id: insertion order, the claim-time snapshot
		// order the runner replays.
		if got := []string{steps[0].StepID, steps[1].StepID, steps[2].StepID}; len(steps) != 3 || got[0] != "3" || got[1] != "1" || got[2] != "2" {
			t.Fatalf("step ids %v, want [3 1 2] (order by id)", got)
		}
	})
}

func TestGetWorkflowNotFound(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		_, err := store.GetWorkflow(context.Background(), db, store.UUID{UUID: uuid.New()})
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			t.Fatalf("err = %v, want ErrRecordNotFound", err)
		}
	})
}
