// Package ctx carries the execution state of one workflow run on
// context.Context — the Go replacement for the Python thread-local.
//
// One RunState per scope (a workflow body, or one branch of one fork).
// Steps are identified by a dotpath like "3" or "3.1.2": unnamed steps take
// the next position in their scope; a named step takes that name as its
// segment, which keeps its id stable against edits elsewhere in the body.
// A named or unnamed step always consumes a position, so naming (or
// un-naming) a step never shifts the ids of the other steps in the scope.
//
// The outcomes store is shared by reference across branch states, exactly
// as the Python Context.branch shares the outcomes dict. Go maps are not
// safe for concurrent writes, so the store is mutex-guarded.
package ctx

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/google/uuid"
	"gorm.io/gorm"

	errs "github.com/brutasse/everystep-go/internal/errors"
	"github.com/brutasse/everystep-go/serde"
)

// MaxIDLength bounds the length of a step id.
const MaxIDLength = 300

// Outcome is a recorded step outcome: what a step id maps to in the
// outcomes store.
type Outcome struct {
	Name   string
	Status string // "done", "failed", or "started"
	Result json.RawMessage
	Error  *serde.EncodedError
}

type outcomeStore struct {
	mu sync.RWMutex
	m  map[string]Outcome
}

// NewOutcomeStore builds an empty outcomes store.
func NewOutcomeStore() *outcomeStore {
	return &outcomeStore{m: make(map[string]Outcome)}
}

func (s *outcomeStore) Get(id string) (Outcome, bool) {
	s.mu.RLock()
	o, ok := s.m[id]
	s.mu.RUnlock()
	return o, ok
}

func (s *outcomeStore) Set(id string, o Outcome) {
	s.mu.Lock()
	s.m[id] = o
	s.mu.Unlock()
}

// RunState is the execution state of one scope.
type RunState struct {
	workflowID uuid.UUID
	workflow   string
	db         *gorm.DB
	outcomes   *outcomeStore
	prefix     string
	counter    int
	used       map[string]bool
	persistent bool
	draining   *atomic.Bool
}

// NewRun builds the persistent state of a claimed run.
func NewRun(workflowID uuid.UUID, workflow string, db *gorm.DB, outcomes *outcomeStore, draining *atomic.Bool) *RunState {
	return &RunState{
		workflowID: workflowID,
		workflow:   workflow,
		db:         db,
		outcomes:   outcomes,
		persistent: true,
		draining:   draining,
		used:       make(map[string]bool),
	}
}

// NewTransient builds the non-persistent state used by Parallel outside a
// running workflow: branches execute plainly, nothing is recorded.
func NewTransient() *RunState {
	return &RunState{
		outcomes: NewOutcomeStore(),
		used:     make(map[string]bool),
	}
}

// WorkflowID returns the run's id (nil outside a real run).
func (rs *RunState) WorkflowID() uuid.UUID { return rs.workflowID }

// Workflow returns the run's workflow name.
func (rs *RunState) Workflow() string { return rs.workflow }

// DB returns the database handle the run persists to.
func (rs *RunState) DB() *gorm.DB { return rs.db }

// Outcomes returns the shared outcomes store.
func (rs *RunState) Outcomes() *outcomeStore { return rs.outcomes }

// Persistent reports whether steps are recorded (a claimed run) or executed
// plainly (outside a run).
func (rs *RunState) Persistent() bool { return rs.persistent }

// Draining returns the stop signal the runner sets on drain, or nil when
// draining is not in play.
func (rs *RunState) Draining() *atomic.Bool { return rs.draining }

// NextID assigns the next step id in this scope, consuming a position
// whether or not the step is named. A nil name takes the next position; a
// non-nil name is validated (an explicit empty name is an error, as in
// Python) and used as the segment.
func (rs *RunState) NextID(name *string) (string, error) {
	rs.counter++
	segment := strconv.Itoa(rs.counter)
	if name != nil {
		if err := validateName(*name); err != nil {
			return "", err
		}
		segment = *name
	}
	stepID := rs.prefix + segment
	if len(stepID) > MaxIDLength {
		shown := segment
		if name != nil {
			shown = *name
		}
		return "", &errs.EverystepError{Msg: fmt.Sprintf(
			"everystep_id %q is too long: step id would exceed %d characters", shown, MaxIDLength)}
	}
	if rs.used[stepID] {
		return "", &errs.EverystepError{Msg: fmt.Sprintf(
			"everystep_id %q is already used in this scope (step id %q); everystep_ids must be unique per body or branch",
			segment, stepID)}
	}
	rs.used[stepID] = true
	return stepID, nil
}

// Branch builds the state of one fork branch: its own scope (counter and
// used ids), the shared outcomes store.
func (rs *RunState) Branch(forkID string, index int) *RunState {
	return &RunState{
		workflowID: rs.workflowID,
		workflow:   rs.workflow,
		db:         rs.db,
		outcomes:   rs.outcomes,
		prefix:     fmt.Sprintf("%s.%d.", forkID, index),
		persistent: rs.persistent,
		draining:   rs.draining,
		used:       make(map[string]bool),
	}
}

func validateName(name string) error {
	if name == "" {
		return &errs.EverystepError{Msg: "everystep_id must be a non-empty string"}
	}
	if strings.ContainsRune(name, '.') {
		return &errs.EverystepError{Msg: fmt.Sprintf(
			"everystep_id %q must not contain dots (dots separate scopes)", name)}
	}
	for _, r := range name {
		if unicode.IsSpace(r) {
			return &errs.EverystepError{Msg: fmt.Sprintf(
				"everystep_id %q must not contain whitespace", name)}
		}
	}
	if strings.TrimFunc(name, func(r rune) bool { return unicode.IsDigit(r) }) == "" {
		return &errs.EverystepError{Msg: fmt.Sprintf(
			"everystep_id %q is purely numeric; names must not look like positions", name)}
	}
	return nil
}

type ctxKey struct{}

// With returns a context carrying the run state.
func With(parent context.Context, rs *RunState) context.Context {
	return context.WithValue(parent, ctxKey{}, rs)
}

// From returns the run state carried by ctx, or nil outside a run.
func From(ctx context.Context) *RunState {
	rs, _ := ctx.Value(ctxKey{}).(*RunState)
	return rs
}
