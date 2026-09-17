// Package metrics registers everystep's Prometheus collectors in the
// default registry, so a host application's existing scrape endpoint serves
// them with no configuration. Names and labels are identical to the Python
// implementation, so existing dashboards and alert rules transfer. Labels
// are bounded to code-defined names (workflow and step names, runner
// name), never to per-run identifiers.
package metrics

import (
	"context"
	"database/sql"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gorm.io/gorm"
)

var (
	workflowRuns     *prometheus.CounterVec
	workflowDuration *prometheus.HistogramVec
	stepRuns         *prometheus.CounterVec
	stepDuration     *prometheus.HistogramVec
	workerPoolSize   *prometheus.GaugeVec
	workerInflight   *prometheus.GaugeVec
	workerClaims     *prometheus.CounterVec
	workerRequeues   *prometheus.CounterVec
	workerStartedAt  *prometheus.GaugeVec
	workflowsPending prometheus.Gauge
	workflowsRunning prometheus.Gauge
	workflowsBlocked prometheus.Gauge
	workflowsOldest  prometheus.Gauge
)

var collectors []prometheus.Collector

func define() {
	for _, c := range collectors {
		prometheus.Unregister(c)
	}
	workflowRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "everystep_workflow_runs",
		Help: "Workflow runs ended, by final status.",
	}, []string{"workflow", "status"})
	workflowDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "everystep_workflow_duration_seconds",
		Help: "Wall time from claim to terminal state, by final status.",
	}, []string{"workflow", "status"})
	stepRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "everystep_step_runs",
		Help: "Step executions, by outcome.",
	}, []string{"workflow", "step", "status"})
	stepDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "everystep_step_duration_seconds",
		Help: "Step execution time.",
	}, []string{"workflow", "step"})
	workerPoolSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "everystep_worker_pool_size",
		Help: "Worker pool size.",
	}, []string{"runner"})
	workerInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "everystep_worker_inflight",
		Help: "Workflows currently in flight in the worker.",
	}, []string{"runner"})
	workerClaims = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "everystep_worker_claims",
		Help: "Workflows claimed by the worker.",
	}, []string{"runner"})
	workerRequeues = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "everystep_worker_requeued",
		Help: "In-flight workflows requeued when the drain deadline expired.",
	}, []string{"runner"})
	workerStartedAt = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "everystep_worker_started_at_seconds",
		Help: "Unix time the worker started.",
	}, []string{"runner"})
	workflowsPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "everystep_workflows_pending",
		Help: "Workflows scheduled and waiting for a claim.",
	})
	workflowsRunning = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "everystep_workflows_running",
		Help: "Workflows currently running.",
	})
	workflowsBlocked = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "everystep_workflows_blocked",
		Help: "Workflows blocked awaiting a human decision on an uncertain step effect.",
	})
	workflowsOldest = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "everystep_workflows_oldest_pending_age_seconds",
		Help: "Age of the oldest scheduled workflow.",
	})
	collectors = []prometheus.Collector{
		workflowRuns, workflowDuration, stepRuns, stepDuration,
		workerPoolSize, workerInflight, workerClaims, workerRequeues, workerStartedAt,
		workflowsPending, workflowsRunning, workflowsBlocked, workflowsOldest,
	}
	for _, c := range collectors {
		prometheus.MustRegister(c)
	}
}

func init() { define() }

// ResetForTest re-creates all everystep collectors with fresh state.
func ResetForTest() { define() }

// RecordStep records one executed step's outcome and duration.
func RecordStep(workflow, step, status string, duration float64) {
	stepRuns.WithLabelValues(workflow, step, status).Inc()
	stepDuration.WithLabelValues(workflow, step).Observe(duration)
}

// RecordWorkflowTerminal records a run reaching a terminal status.
func RecordWorkflowTerminal(workflow, status string, duration float64) {
	workflowRuns.WithLabelValues(workflow, status).Inc()
	workflowDuration.WithLabelValues(workflow, status).Observe(duration)
}

// WorkerStarted records the worker's pool size and start time.
func WorkerStarted(name string, poolSize int) {
	workerPoolSize.WithLabelValues(name).Set(float64(poolSize))
	workerStartedAt.WithLabelValues(name).Set(float64(time.Now().Unix()))
}

// SetInflight sets the worker's in-flight count.
func SetInflight(name string, count int) {
	workerInflight.WithLabelValues(name).Set(float64(count))
}

// RecordClaims counts workflows claimed by the worker.
func RecordClaims(name string, count int) {
	if count > 0 {
		workerClaims.WithLabelValues(name).Add(float64(count))
	}
}

// RecordRequeues counts in-flight workflows requeued at the drain deadline.
func RecordRequeues(name string, count int) {
	if count > 0 {
		workerRequeues.WithLabelValues(name).Add(float64(count))
	}
}

// UpdateQueueGauges refreshes the queue-depth gauges from the database.
func UpdateQueueGauges(ctx context.Context, db *gorm.DB) error {
	count := func(status string) (int64, error) {
		var n int64
		err := db.WithContext(ctx).
			Raw("SELECT COUNT(*) FROM everystep_workflow WHERE status = ?", status).
			Scan(&n).Error
		return n, err
	}
	scheduled, err := count("scheduled")
	if err != nil {
		return err
	}
	workflowsPending.Set(float64(scheduled))
	var oldest sql.NullTime
	if err := db.WithContext(ctx).
		Raw("SELECT MIN(created_at) FROM everystep_workflow WHERE status = 'scheduled'").
		Scan(&oldest).Error; err != nil {
		return err
	}
	if oldest.Valid {
		workflowsOldest.Set(time.Since(oldest.Time).Seconds())
	} else {
		workflowsOldest.Set(0)
	}
	running, err := count("running")
	if err != nil {
		return err
	}
	workflowsRunning.Set(float64(running))
	blocked, err := count("blocked")
	if err != nil {
		return err
	}
	workflowsBlocked.Set(float64(blocked))
	return nil
}

// StartHTTPServer serves the metrics on addr in a background goroutine,
// refreshing the queue gauges on each scrape. It returns a stop function.
func StartHTTPServer(db *gorm.DB, addr string) (func(), error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if err := UpdateQueueGauges(r.Context(), db); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		promhttp.Handler().ServeHTTP(w, r)
	})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	return func() { _ = srv.Shutdown(context.Background()) }, nil
}
