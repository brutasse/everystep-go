package tests

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"gorm.io/gorm"

	errs "github.com/brutasse/everystep-go/internal/errors"
	"github.com/brutasse/everystep-go/runner"
	"github.com/brutasse/everystep-go/serde"
	"github.com/brutasse/everystep-go/store"
)

const testWorker = "test-worker"

// eachBackend runs fn against every backend under test, on a clean table,
// and resets the engine's test hooks afterwards.
func eachBackend(t *testing.T, fn func(t *testing.T, db *gorm.DB)) {
	t.Helper()
	for _, b := range backends {
		t.Run(b.Name, func(t *testing.T) {
			truncateTables(t, b.DB)
			t.Cleanup(func() {
				runner.SetFault(nil)
				runner.SetFailureReporter(nil)
			})
			fn(t, b.DB)
		})
	}
}

func truncateTables(t *testing.T, db *gorm.DB) {
	t.Helper()
	if err := db.Exec("DELETE FROM everystep_step").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("DELETE FROM everystep_workflow").Error; err != nil {
		t.Fatal(err)
	}
}

// runToCompletion claims and executes the scheduled run, and returns the
// fresh run row.
func runToCompletion(t *testing.T, db *gorm.DB, runID uuid.UUID) *store.Workflow {
	t.Helper()
	claimed, err := store.ClaimScheduled(context.Background(), db, 1, testWorker)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID.UUID != runID {
		t.Fatalf("claimed %v, want [%s]", claimedIDs(claimed), runID)
	}
	if err := runner.Execute(context.Background(), db, runID, &atomic.Bool{}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	return getRun(t, db, runID)
}

// claimNext claims exactly one scheduled run and returns it.
func claimNext(t *testing.T, db *gorm.DB) *store.Workflow {
	t.Helper()
	claimed, err := store.ClaimScheduled(context.Background(), db, 1, testWorker)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected one claimable workflow, got %v", claimedIDs(claimed))
	}
	return &claimed[0]
}

// reClaim simulates the claiming runner restarting and re-claiming its
// in-flight run.
func reClaim(t *testing.T, db *gorm.DB, runID uuid.UUID) {
	t.Helper()
	run := getRun(t, db, runID)
	if run.ClaimedBy == nil {
		t.Fatalf("run %s is not claimed", runID)
	}
	claimed, err := store.ResumeOwn(context.Background(), db, 1, *run.ClaimedBy)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID.UUID != runID {
		t.Fatalf("expected to re-claim %s, got %v", runID, claimedIDs(claimed))
	}
}

// crashOn installs the fault hook that simulates a process death at the
// record point of stepID.
func crashOn(stepID string) {
	runner.SetFault(func(_ context.Context, id string) error {
		if id == stepID {
			return &errs.SimulatedCrash{}
		}
		return nil
	})
}

func getRun(t *testing.T, db *gorm.DB, id uuid.UUID) *store.Workflow {
	t.Helper()
	wf, err := store.GetWorkflow(context.Background(), db, store.UUID{UUID: id})
	if err != nil {
		t.Fatalf("get run %s: %v", id, err)
	}
	return wf
}

// stepIDs returns the run's recorded step ids in insertion order.
func stepIDs(t *testing.T, db *gorm.DB, id uuid.UUID) []string {
	t.Helper()
	steps, err := store.ListSteps(context.Background(), db, store.UUID{UUID: id})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(steps))
	for i, s := range steps {
		ids[i] = s.StepID
	}
	return ids
}

// assertStepIDs compares step id sets.
func assertStepIDs(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("step ids %v, want %v", got, want)
	}
	seen := map[string]bool{}
	for _, w := range want {
		seen[w] = true
	}
	for _, g := range got {
		if !seen[g] {
			t.Fatalf("step ids %v, want %v", got, want)
		}
	}
}

func claimedIDs(wfs []store.Workflow) []string {
	ids := make([]string, len(wfs))
	for i, w := range wfs {
		ids[i] = w.ID.UUID.String()
	}
	return ids
}

// The effect log: the Go stand-in for the Python suites' CALLS list.
// Parallel branches append from goroutines, so it is mutex-guarded.
type callsRecorder struct {
	mu   sync.Mutex
	list []string
}

var calls = &callsRecorder{}

func (r *callsRecorder) add(marker string) {
	r.mu.Lock()
	r.list = append(r.list, marker)
	r.mu.Unlock()
}

func (r *callsRecorder) listCopy() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.list))
	copy(out, r.list)
	return out
}

func (r *callsRecorder) count(marker string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, m := range r.list {
		if m == marker {
			n++
		}
	}
	return n
}

func (r *callsRecorder) reset() {
	r.mu.Lock()
	r.list = nil
	r.mu.Unlock()
}

// resetCalls clears the effect log at the start of a test and again at its
// end.
func resetCalls(t *testing.T) {
	t.Helper()
	calls.reset()
	t.Cleanup(calls.reset)
}

// assertCalls compares the effect log to want, order-sensitive.
func assertCalls(t *testing.T, want ...string) {
	t.Helper()
	got := calls.listCopy()
	if len(got) != len(want) {
		t.Fatalf("calls %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("calls %v, want %v", got, want)
		}
	}
}

// crashAt installs the fault hook at stepID; it is cleared when the test
// ends.
func crashAt(t *testing.T, stepID string) {
	t.Helper()
	crashOn(stepID)
	t.Cleanup(func() { runner.SetFault(nil) })
}

// runnerExecute executes a claimed run without a draining signal.
func runnerExecute(t *testing.T, db *gorm.DB, id uuid.UUID) error {
	t.Helper()
	return runner.Execute(context.Background(), db, id, &atomic.Bool{})
}

// expectCrash executes a claimed run and asserts it died with
// SimulatedCrash, then clears the fault hook.
func expectCrash(t *testing.T, db *gorm.DB, id uuid.UUID) {
	t.Helper()
	err := runnerExecute(t, db, id)
	var crash *errs.SimulatedCrash
	if !errors.As(err, &crash) {
		t.Fatalf("expected SimulatedCrash, got %v", err)
	}
	runner.SetFault(nil)
}

// stepsFor loads the run's step rows in insertion order.
func stepsFor(t *testing.T, db *gorm.DB, id uuid.UUID) []store.Step {
	t.Helper()
	steps, err := store.ListSteps(context.Background(), db, store.UUID{UUID: id})
	if err != nil {
		t.Fatal(err)
	}
	return steps
}

// stepStatuses maps step id to status for the run.
func stepStatuses(t *testing.T, db *gorm.DB, id uuid.UUID) map[string]string {
	t.Helper()
	steps := stepsFor(t, db, id)
	m := make(map[string]string, len(steps))
	for _, s := range steps {
		m[s.StepID] = s.Status
	}
	return m
}

// stepNames maps step id to recorded name for the run.
func stepNames(t *testing.T, db *gorm.DB, id uuid.UUID) map[string]string {
	t.Helper()
	steps := stepsFor(t, db, id)
	m := make(map[string]string, len(steps))
	for _, s := range steps {
		m[s.StepID] = s.Name
	}
	return m
}

// runError returns the run's stored error.
func runError(t *testing.T, wf *store.Workflow) serde.EncodedError {
	t.Helper()
	if len(wf.Error) == 0 {
		t.Fatalf("run %s has no stored error", wf.ID.UUID)
	}
	var enc serde.EncodedError
	if err := json.Unmarshal(wf.Error, &enc); err != nil {
		t.Fatal(err)
	}
	return enc
}

// runErrorDecoded rebuilds the run's stored error, exactly as a replay
// would.
func runErrorDecoded(t *testing.T, wf *store.Workflow) error {
	t.Helper()
	if len(wf.Error) == 0 {
		t.Fatalf("run %s has no stored error", wf.ID.UUID)
	}
	return serde.DecodeError(json.RawMessage(wf.Error))
}

// assertMap compares string maps.
func assertMap(t *testing.T, got, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("rows %v, want %v", got, want)
	}
	for k, w := range want {
		if g, ok := got[k]; !ok || g != w {
			t.Fatalf("rows %v, want %v", got, want)
		}
	}
}
