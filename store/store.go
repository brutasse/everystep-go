// Package store is the gorm data layer: the everystep tables, migration,
// and the claim/resolve queries.
//
// The dialect is the backend: claim locking (FOR UPDATE SKIP LOCKED on
// PostgreSQL, plain FOR UPDATE on MariaDB), column types (native uuid and
// JSONB vs CHAR(36) and JSON), and unique-violation translation all come
// from the gorm dialector. Open the database with
// gorm.Config{TranslateError: true} so a unique violation on either
// backend surfaces as gorm.ErrDuplicatedKey.
package store

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// Run and step statuses.
const (
	StatusScheduled = "scheduled"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusStopped   = "stopped"
	StatusBlocked   = "blocked"

	StepDone    = "done"
	StepFailed  = "failed"
	StepStarted = "started"
)

// UUID maps to a native uuid column on PostgreSQL and CHAR(36) on
// MariaDB. Values are stored as the 36-character dash form; uuid.NewV7
// generates them in the application.
type UUID struct{ uuid.UUID }

func (UUID) GormDataType() string { return "char(36)" }

func (UUID) GormDBDataType(db *gorm.DB, _ *schema.Field) string {
	if db != nil && db.Dialector.Name() == "postgres" {
		return "uuid"
	}
	return "char(36)"
}

func (u UUID) Value() (driver.Value, error) { return u.UUID.String(), nil }

func (u *UUID) Scan(v any) error {
	switch t := v.(type) {
	case []byte:
		parsed, err := uuid.Parse(string(t))
		if err != nil {
			return err
		}
		u.UUID = parsed
	case string:
		parsed, err := uuid.Parse(t)
		if err != nil {
			return err
		}
		u.UUID = parsed
	case nil:
		u.UUID = uuid.Nil
	default:
		return fmt.Errorf("store: cannot scan %T into UUID", v)
	}
	return nil
}

// Workflow is a row in everystep_workflow.
type Workflow struct {
	ID             UUID           `gorm:"column:id;primaryKey"`
	Name           string         `gorm:"column:name;size:300;not null;uniqueIndex:uq_workflow_name_key"`
	Args           datatypes.JSON `gorm:"column:args;not null"`
	IdempotencyKey *string        `gorm:"column:idempotency_key;size:255;uniqueIndex:uq_workflow_name_key"`
	Status         string         `gorm:"column:status;size:16;not null;index"`
	ClaimedBy      *string        `gorm:"column:claimed_by;size:255"`
	Result         datatypes.JSON `gorm:"column:result"`
	Error          datatypes.JSON `gorm:"column:error"`
	CreatedAt      time.Time      `gorm:"column:created_at;not null;index"`
	CompletedAt    *time.Time     `gorm:"column:completed_at"`
}

func (Workflow) TableName() string { return "everystep_workflow" }

// Step is a row in everystep_step.
type Step struct {
	ID       int64          `gorm:"column:id;primaryKey;autoIncrement"`
	Workflow UUID           `gorm:"column:workflow;not null;uniqueIndex:uq_step_workflow_step_id"`
	StepID   string         `gorm:"column:step_id;size:300;not null;uniqueIndex:uq_step_workflow_step_id"`
	Name     string         `gorm:"column:name;size:300;not null"`
	Args     datatypes.JSON `gorm:"column:args"`
	Status   string         `gorm:"column:status;size:16;not null"`
	Result   datatypes.JSON `gorm:"column:result"`
	Error    datatypes.JSON `gorm:"column:error"`
}

func (Step) TableName() string { return "everystep_step" }

const stepFKName = "fk_everystep_step_workflow"

// Migrate creates the everystep tables. It is idempotent and per-dialect;
// the host owns any further schema tooling.
func Migrate(ctx context.Context, db *gorm.DB) error {
	if err := db.WithContext(ctx).AutoMigrate(&Workflow{}, &Step{}); err != nil {
		return err
	}
	return ensureStepFK(ctx, db)
}

func ensureStepFK(ctx context.Context, db *gorm.DB) error {
	exists, err := fkExists(ctx, db)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return db.WithContext(ctx).Exec(
		"ALTER TABLE everystep_step ADD CONSTRAINT " + stepFKName +
			" FOREIGN KEY (workflow) REFERENCES everystep_workflow(id) ON DELETE CASCADE").Error
}

func fkExists(ctx context.Context, db *gorm.DB) (bool, error) {
	var n int64
	var q string
	if db.Dialector.Name() == "postgres" {
		q = "SELECT COUNT(*) FROM pg_constraint WHERE conname = ?"
	} else {
		q = "SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE " +
			"WHERE CONSTRAINT_NAME = ? AND TABLE_NAME = 'everystep_step'"
	}
	if err := db.WithContext(ctx).Raw(q, stepFKName).Scan(&n).Error; err != nil {
		return false, err
	}
	return n > 0, nil
}
