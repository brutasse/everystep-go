package everystep

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	errs "github.com/brutasse/everystep-go/internal/errors"
	"github.com/brutasse/everystep-go/internal/registry"
	"github.com/brutasse/everystep-go/serde"
	"github.com/brutasse/everystep-go/store"
)

// workflow is the registered form of a durable workflow.
type workflow[R, A any] struct {
	name string
	body func(context.Context, A) (R, error)
}

// Workflow is a durable workflow body under a mandatory name: the identity
// under which runs are recorded and resolved. It is defined with the
// Workflow constructor and scheduled with its Schedule method.
func Workflow[R, A any](name string, body func(context.Context, A) (R, error)) workflow[R, A] {
	registry.RegisterWorkflow(name, uniformBody(body))
	return workflow[R, A]{name: name, body: body}
}

// uniformBody adapts the typed body to the registry's uniform shape:
// decode the stored args, run the body, return the result.
func uniformBody[R, A any](body func(context.Context, A) (R, error)) registry.Body {
	return func(ctx context.Context, args json.RawMessage) (any, error) {
		var a A
		if err := serde.Unmarshal(args, &a); err != nil {
			return nil, &errs.WorkflowCodeError{Msg: fmt.Sprintf(
				"workflow code changed: arguments no longer decode into %T: %v", a, err)}
		}
		return body(ctx, a)
	}
}

// ScheduleOpt configures a Schedule call.
type ScheduleOpt func(*scheduleConfig)

type scheduleConfig struct {
	idempotencyKey string
}

// Idempotency makes scheduling idempotent: a repeated or concurrent
// schedule with the same key returns the existing run instead of creating
// a new one (the first args win).
func Idempotency(key string) ScheduleOpt {
	return func(c *scheduleConfig) { c.idempotencyKey = key }
}

// Schedule inserts a scheduled run into the transaction tx. In gorm a
// transaction is a *gorm.DB (the return of Begin), so one parameter covers
// transactional scheduling (pass the tx) and plain scheduling (pass the
// db). The run row joins that transaction: it becomes claimable on commit
// and vanishes on rollback. The args are serialized up front, before
// anything is inserted.
func (w workflow[R, A]) Schedule(tx *gorm.DB, a A, opts ...ScheduleOpt) (uuid.UUID, error) {
	raw, err := serde.Marshal(a)
	if err != nil {
		return uuid.Nil, &errs.EverystepError{Msg: fmt.Sprintf("workflow arguments are not serializable: %v", err)}
	}
	c := &scheduleConfig{}
	for _, o := range opts {
		o(c)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, err
	}
	wf := &store.Workflow{
		ID:        store.UUID{UUID: id},
		Name:      w.name,
		Args:      raw,
		Status:    store.StatusScheduled,
		CreatedAt: time.Now().UTC(),
	}
	if c.idempotencyKey != "" {
		key := c.idempotencyKey
		wf.IdempotencyKey = &key
	}
	if err := tx.Create(wf).Error; err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			var existing store.Workflow
			if err := tx.Where("name = ? AND idempotency_key = ?", w.name, c.idempotencyKey).
				First(&existing).Error; err != nil {
				return uuid.Nil, err
			}
			return existing.ID.UUID, nil
		}
		return uuid.Nil, err
	}
	return id, nil
}
