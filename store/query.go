package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	errs "github.com/brutasse/everystep-go/internal/errors"
)

// lockClause returns the row-locking clause for claim selects: SKIP LOCKED
// on PostgreSQL so concurrent workers claim disjoint batches without
// blocking each other; plain FOR UPDATE elsewhere (MariaDB) where claims
// stay exclusive but serialize while a batch is being locked.
func lockClause(db *gorm.DB) clause.Locking {
	lock := clause.Locking{Strength: "UPDATE"}
	if db.Dialector.Name() == "postgres" {
		lock.Options = "SKIP LOCKED"
	}
	return lock
}

// GetWorkflow loads a run by id. It returns gorm.ErrRecordNotFound when
// absent.
func GetWorkflow(ctx context.Context, db *gorm.DB, id UUID) (*Workflow, error) {
	var wf Workflow
	if err := db.WithContext(ctx).First(&wf, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &wf, nil
}

// ListSteps loads all recorded steps of a run.
func ListSteps(ctx context.Context, db *gorm.DB, workflowID UUID) ([]Step, error) {
	var steps []Step
	err := db.WithContext(ctx).Where("workflow = ?", workflowID).Order("id").Find(&steps).Error
	return steps, err
}

// CreateStep inserts a step row. A unique (workflow, step_id) violation
// surfaces as gorm.ErrDuplicatedKey.
func CreateStep(ctx context.Context, db *gorm.DB, s *Step) error {
	return db.WithContext(ctx).Create(s).Error
}

// FinishStep closes out the existing row for (workflow, step_id) in place:
// either this attempt claimed the effect before running it, or a previous
// attempt left a started row behind and the step no longer marks itself
// unsafe to repeat.
func FinishStep(ctx context.Context, db *gorm.DB, workflowID UUID, stepID, status string, result, stepError []byte) error {
	return db.WithContext(ctx).Model(&Step{}).
		Where("workflow = ? AND step_id = ?", workflowID, stepID).
		Updates(map[string]any{"status": status, "result": result, "error": stepError}).Error
}

// ClaimScheduled claims up to limit scheduled workflows for name in one
// transaction: lock the batch, mark it running, and return the claimed rows.
func ClaimScheduled(ctx context.Context, db *gorm.DB, limit int, name string) ([]Workflow, error) {
	var wfs []Workflow
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ids []UUID
		if err := tx.Model(&Workflow{}).
			Where("status = ?", StatusScheduled).
			Order("created_at").
			Limit(limit).
			Clauses(lockClause(db)).
			Pluck("id", &ids).Error; err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		if err := tx.Model(&Workflow{}).
			Where("id IN ?", ids).
			Updates(map[string]any{"status": StatusRunning, "claimed_by": name}).Error; err != nil {
			return err
		}
		return tx.Where("id IN ?", ids).Order("created_at").Find(&wfs).Error
	})
	return wfs, err
}

// ResumeOwn re-claims the runs a crashed process with this name left
// behind: running rows whose claimed_by is name. Called once at startup;
// after a clean shutdown it finds nothing, because in-flight runs are
// requeued at shutdown.
func ResumeOwn(ctx context.Context, db *gorm.DB, limit int, name string) ([]Workflow, error) {
	var wfs []Workflow
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Model(&Workflow{}).
			Where("status = ? AND claimed_by = ?", StatusRunning, name).
			Order("created_at").
			Limit(limit).
			Clauses(lockClause(db)).
			Find(&wfs).Error
	})
	return wfs, err
}

// SetTerminal writes a terminal status guarded on the run still being
// running, so a run already terminated by another path is not overwritten.
// It reports whether the row was updated.
func SetTerminal(ctx context.Context, db *gorm.DB, id UUID, status string, result, stepError []byte, now time.Time) (bool, error) {
	res := db.WithContext(ctx).Model(&Workflow{}).
		Where("id = ? AND status = ?", id, StatusRunning).
		Updates(map[string]any{"status": status, "result": result, "error": stepError, "completed_at": now})
	return res.RowsAffected > 0, res.Error
}

// MarkFailedClaimed fails a run guarded on status and claimer: a run
// requeued at the drain deadline may belong to another runner by the time
// a late failure lands, and must not fail that runner's live run.
func MarkFailedClaimed(ctx context.Context, db *gorm.DB, id UUID, name string, stepError []byte, now time.Time) (bool, error) {
	res := db.WithContext(ctx).Model(&Workflow{}).
		Where("id = ? AND status = ? AND claimed_by = ?", id, StatusRunning, name).
		Updates(map[string]any{"status": StatusFailed, "error": stepError, "completed_at": now})
	return res.RowsAffected > 0, res.Error
}

// Requeue puts a run back in the queue — scheduled and unclaimed, so any
// runner can claim it and resume it from the recorded steps. Guarded on
// the run still being claimed by name.
func Requeue(ctx context.Context, db *gorm.DB, id UUID, name string) (bool, error) {
	res := db.WithContext(ctx).Model(&Workflow{}).
		Where("id = ? AND status = ? AND claimed_by = ?", id, StatusRunning, name).
		Updates(map[string]any{"status": StatusScheduled, "claimed_by": nil})
	return res.RowsAffected > 0, res.Error
}

// ResolveMode selects the ResolveStep operation.
type ResolveMode int

const (
	// ResolveResult records the effect's result on the step.
	ResolveResult ResolveMode = iota
	// ResolveError records a failure on the step, so the workflow's
	// durable cleanup replays on the next pass.
	ResolveError
	// ResolveDiscard deletes the started row so the step re-runs.
	ResolveDiscard
)

// ResolveStep resolves a run blocked on an unsafe-to-repeat step: record
// the effect's result or failure, or discard the unrecorded attempt. The
// run returns to scheduled with its error and claim cleared. Only a
// blocked run's started step can be resolved.
func ResolveStep(ctx context.Context, db *gorm.DB, runID UUID, stepID string, mode ResolveMode, result, stepError []byte) error {
	var run Workflow
	if err := db.WithContext(ctx).First(&run, "id = ?", runID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return &errs.EverystepError{Msg: fmt.Sprintf("no workflow run with id %s", runID)}
		}
		return err
	}
	if run.Status != StatusBlocked {
		return &errs.EverystepError{Msg: fmt.Sprintf(
			"run %s is %q, not blocked; only blocked runs can be resolved", runID, run.Status)}
	}
	var step Step
	if err := db.WithContext(ctx).First(&step, "workflow = ? AND step_id = ?", runID, stepID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return &errs.EverystepError{Msg: fmt.Sprintf("run %s has no recorded step %q", runID, stepID)}
		}
		return err
	}
	if step.Status != StepStarted {
		return &errs.EverystepError{Msg: fmt.Sprintf(
			"step %q is %q, not started; only a started step can be resolved", stepID, step.Status)}
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		switch mode {
		case ResolveResult:
			if err := tx.Model(&Step{}).Where("id = ?", step.ID).
				Updates(map[string]any{"status": StepDone, "result": result, "error": nil}).Error; err != nil {
				return err
			}
		case ResolveError:
			if err := tx.Model(&Step{}).Where("id = ?", step.ID).
				Updates(map[string]any{"status": StepFailed, "result": nil, "error": stepError}).Error; err != nil {
				return err
			}
		case ResolveDiscard:
			if err := tx.Delete(&Step{}, "id = ?", step.ID).Error; err != nil {
				return err
			}
		}
		// The run is no longer waiting on a human: put it back in the
		// queue so any runner can claim it and resume from the recorded
		// steps.
		return tx.Model(&Workflow{}).Where("id = ?", runID).
			Updates(map[string]any{"status": StatusScheduled, "claimed_by": nil, "error": nil, "completed_at": nil}).Error
	})
}
