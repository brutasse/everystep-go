// Package worker is the claim loop: it claims scheduled workflows and
// executes them on a bounded pool of goroutines.
//
// A worker has a name that is recorded on the runs it claims. On startup it
// re-claims the runs a crashed process with the same name left behind
// (matched by name); after a clean shutdown there is nothing to catch up,
// because in-flight runs are requeued at shutdown. In steady state it only
// claims new scheduled workflows.
//
// On context cancellation the worker stops claiming and drains: the step in
// flight in each run finishes and is recorded, no new step starts, and at
// the next step boundary the run is requeued so any runner can resume it.
// Runs still in flight when the drain deadline expires are requeued and the
// worker returns; the host owns the process lifecycle (standalone hosts use
// signal.NotifyContext for SIGTERM/SIGINT).
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	errs "github.com/brutasse/everystep-go/internal/errors"
	"github.com/brutasse/everystep-go/metrics"
	"github.com/brutasse/everystep-go/runner"
	"github.com/brutasse/everystep-go/serde"
	"github.com/brutasse/everystep-go/store"
)

// Opts configures a Worker. Zero values take the defaults.
type Opts struct {
	DB *gorm.DB
	// Pool is the number of runs executed concurrently (default 4).
	Pool int
	// Poll is the claim interval (default 200ms).
	Poll time.Duration
	// Name is recorded on the runs this worker claims (default: the host
	// name). It must be unique among running workers and identical across
	// restarts of the same process, so catchup finds crash-leftover runs.
	Name string
	// Drain is the deadline for in-flight runs at shutdown (default 30s).
	Drain time.Duration
	// Logger is the worker's logger (default zap.L()).
	Logger *zap.Logger
	// MetricsPort serves /metrics on MetricsBind:MetricsPort (0: off).
	MetricsPort int
	// MetricsBind is the metrics bind address (default 0.0.0.0).
	MetricsBind string
}

type Worker struct {
	db       *gorm.DB
	pool     int
	poll     time.Duration
	name     string
	drain    time.Duration
	log      *zap.Logger
	bind     string
	port     int
	draining *atomic.Bool
	mu       sync.Mutex
	active   map[uuid.UUID]chan struct{}
	wg       sync.WaitGroup
}

// New builds a Worker from opts.
func New(opts Opts) *Worker {
	pool := opts.Pool
	if pool <= 0 {
		pool = 4
	}
	poll := opts.Poll
	if poll <= 0 {
		poll = 200 * time.Millisecond
	}
	name := opts.Name
	if name == "" {
		name = "everystep-worker"
		if h, err := os.Hostname(); err == nil {
			name = h
		}
	}
	drain := opts.Drain
	if drain <= 0 {
		drain = 30 * time.Second
	}
	lg := opts.Logger
	if lg == nil {
		lg = zap.L()
	}
	bind := opts.MetricsBind
	if bind == "" {
		bind = "0.0.0.0"
	}
	return &Worker{
		db:       opts.DB,
		pool:     pool,
		poll:     poll,
		name:     name,
		drain:    drain,
		log:      lg,
		bind:     bind,
		port:     opts.MetricsPort,
		draining: &atomic.Bool{},
		active:   map[uuid.UUID]chan struct{}{},
	}
}

// Name returns the runner name recorded on this worker's claims.
func (w *Worker) Name() string { return w.name }

// Run claims and executes workflows until ctx is cancelled, then drains and
// returns. It installs no signal handlers: a standalone host derives ctx
// with signal.NotifyContext.
func (w *Worker) Run(ctx context.Context) error {
	metrics.WorkerStarted(w.name, w.pool)
	stopMetrics := w.startMetrics()
	defer stopMetrics()

	if err := w.catchup(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer w.shutdown()

	for {
		w.reap()
		if ctx.Err() != nil {
			return nil
		}
		capacity := w.pool - w.inflight()
		if capacity > 0 {
			claimed, err := store.ClaimScheduled(ctx, w.db, capacity, w.name)
			if err != nil {
				// The stop signal can land mid-claim: that is a shutdown,
				// not a failure — the drain and requeue below still run.
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			for i := range claimed {
				w.launch(claimed[i])
			}
			metrics.RecordClaims(w.name, len(claimed))
		}
		metrics.SetInflight(w.name, w.inflight())
		select {
		case <-ctx.Done():
		case <-time.After(w.poll):
		}
	}
}

// catchup re-claims the runs a crashed process with this name left behind.
// It runs once at startup, before the poll loop, so it cannot re-select runs
// this process is already executing. After a clean shutdown it finds
// nothing: in-flight runs are requeued at shutdown.
func (w *Worker) catchup(ctx context.Context) error {
	wfs, err := store.ResumeOwn(ctx, w.db, w.pool, w.name)
	if err != nil {
		return err
	}
	for i := range wfs {
		w.launch(wfs[i])
	}
	metrics.RecordClaims(w.name, len(wfs))
	return nil
}

func (w *Worker) launch(wf store.Workflow) {
	done := make(chan struct{})
	w.mu.Lock()
	w.active[wf.ID.UUID] = done
	w.mu.Unlock()
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer close(done)
		w.execute(wf)
	}()
}

// reap drops finished runs from the active set.
func (w *Worker) reap() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for id, done := range w.active {
		select {
		case <-done:
			delete(w.active, id)
		default:
		}
	}
}

func (w *Worker) inflight() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.active)
}

// execute runs one claimed run to a terminal state (or a requeue on drain).
// It runs on context.Background: the worker's ctx is already cancelled by
// the time the drain window runs, and the draining flag carries the stop
// intent to the runner.
func (w *Worker) execute(wf store.Workflow) {
	started := time.Now()
	id := wf.ID.UUID
	err := runner.Execute(context.Background(), w.db, id, w.draining)
	if err == nil {
		return
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		w.log.Debug("workflow no longer exists", zap.Stringer("workflow", id))
		return
	}
	var orphan *errs.DrainOrphan
	if errors.As(err, &orphan) {
		// Drained at a step boundary with everything so far recorded: give
		// the run back to the queue for any runner to resume.
		w.requeue(id)
		return
	}
	var crash *errs.SimulatedCrash
	isCrash := errors.As(err, &crash)
	w.log.Error("workflow failed outside the runner", zap.Stringer("workflow", id), zap.Error(err))
	runner.ReportFailure(context.Background(), err, runner.FailureMeta{WorkflowID: id, Workflow: wf.Name})
	enc := serde.EncodeError(err)
	raw, _ := json.Marshal(enc)
	updated, uerr := store.MarkFailedClaimed(context.Background(), w.db, wf.ID, w.name, raw, time.Now().UTC())
	if uerr != nil {
		w.log.Error("could not mark workflow failed", zap.Stringer("workflow", id), zap.Error(uerr))
		return
	}
	if updated && !isCrash {
		metrics.RecordWorkflowTerminal(wf.Name, store.StatusFailed, time.Since(started).Seconds())
	}
}

// requeue puts a run back in the queue — scheduled and unclaimed — guarded
// on the run still being claimed by this worker.
func (w *Worker) requeue(id uuid.UUID) {
	updated, err := store.Requeue(context.Background(), w.db, store.UUID{UUID: id}, w.name)
	if err != nil {
		w.log.Error("could not requeue workflow", zap.Stringer("workflow", id), zap.Error(err))
		return
	}
	if !updated {
		w.log.Warn("could not requeue workflow: no longer claimed", zap.Stringer("workflow", id), zap.String("worker", w.name))
	}
}

// shutdown sets the drain flag, waits up to the drain deadline for the
// in-flight runs, and requeues whatever is still running.
func (w *Worker) shutdown() {
	w.draining.Store(true)
	leftovers := w.waitDrain()
	for _, id := range leftovers {
		w.requeue(id)
	}
	if len(leftovers) > 0 {
		metrics.RecordRequeues(w.name, len(leftovers))
		w.log.Warn("drain deadline expired; in-flight workflows requeued for the next available runner",
			zap.Duration("deadline", w.drain), zap.Int("workflows", len(leftovers)))
	}
	metrics.SetInflight(w.name, 0)
}

// waitDrain waits up to w.drain for the in-flight runs to finish and returns
// the ids still in flight when the deadline expires.
func (w *Worker) waitDrain() []uuid.UUID {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(w.drain):
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	leftovers := make([]uuid.UUID, 0, len(w.active))
	for id := range w.active {
		leftovers = append(leftovers, id)
	}
	return leftovers
}

func (w *Worker) startMetrics() func() {
	if w.port == 0 {
		return func() {}
	}
	addr := net.JoinHostPort(w.bind, strconv.Itoa(w.port))
	stop, err := metrics.StartHTTPServer(w.db, addr)
	if err != nil {
		w.log.Error("cannot start metrics server", zap.String("addr", addr), zap.Error(err))
		return func() {}
	}
	return stop
}
