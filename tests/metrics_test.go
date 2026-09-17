package tests

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"gorm.io/gorm"

	es "github.com/brutasse/everystep-go/everystep"
	"github.com/brutasse/everystep-go/metrics"
	"github.com/brutasse/everystep-go/store"
	"github.com/brutasse/everystep-go/worker"
)

// metricValue reads one sample of a counter or gauge, or the sum of a
// histogram, from the default registry.
func metricValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			match := true
			for _, lp := range m.GetLabel() {
				if want, ok := labels[lp.GetName()]; ok && want != lp.GetValue() {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				return m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				return m.GetGauge().GetValue()
			case dto.MetricType_HISTOGRAM:
				return m.GetHistogram().GetSampleSum()
			}
		}
	}
	t.Fatalf("metric %s %v has no sample", name, labels)
	return 0
}

func resetMetrics(t *testing.T) {
	t.Helper()
	metrics.ResetForTest()
	t.Cleanup(metrics.ResetForTest)
}

func mbOKStepFn(ctx context.Context, a struct{}) (string, error) {
	return "ok", nil
}

// esBoomError is an ordinary, unregistered error: only its message
// survives a replay.
type esBoomError string

func (e esBoomError) Error() string { return string(e) }

func mbBadStepFn(ctx context.Context, a struct{}) (string, error) {
	return "", esBoomError("boom")
}

func mbStopStepFn(ctx context.Context, a struct{}) (string, error) {
	return "", &es.Terminal{Reason: "known-reason", Data: map[string]any{"k": "v"}}
}

func mbMarkedStepFn(ctx context.Context, a struct{}) (string, error) {
	return "m", nil
}

var (
	mbOKStep     = es.Step("mb.ok_step", mbOKStepFn)
	mbBadStep    = es.Step("mb.bad_step", mbBadStepFn)
	mbStopStep   = es.Step("mb.stop_step", mbStopStepFn)
	mbMarkedStep = es.Step("mb.marked_step", mbMarkedStepFn, es.UnsafeToRepeat())
)

func mbGoodBody(ctx context.Context, a struct{}) (string, error) {
	return mbOKStep.Call(ctx, struct{}{})
}

var mbGood = es.Workflow("mb.good", mbGoodBody)

func mbBadBody(ctx context.Context, a struct{}) (string, error) {
	return mbBadStep.Call(ctx, struct{}{})
}

var mbBad = es.Workflow("mb.bad", mbBadBody)

func mbStoppedBody(ctx context.Context, a struct{}) (string, error) {
	return mbStopStep.Call(ctx, struct{}{})
}

var mbStopped = es.Workflow("mb.stopped_wf", mbStoppedBody)

func mbBlockedBody(ctx context.Context, a struct{}) (string, error) {
	return mbMarkedStep.Call(ctx, struct{}{})
}

var mbBlocked = es.Workflow("mb.blocked_flow", mbBlockedBody)

func mbTwoStepsBody(ctx context.Context, a struct{}) (string, error) {
	if _, err := mbOKStep.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	if _, err := mbOKStep.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	return "done", nil
}

var mbTwoSteps = es.Workflow("mb.two_steps", mbTwoStepsBody)

func mbSleeperBody(ctx context.Context, a struct{}) (string, error) {
	return wkSleeper.Call(ctx, struct{}{})
}

var mbSleeper = es.Workflow("mb.sleeper_flow", mbSleeperBody)

func TestCompletedWorkflowRecordsRunAndStep(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetMetrics(t)
		id, err := mbGood.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, id)
		if v := metricValue(t, "everystep_workflow_runs", map[string]string{"workflow": "mb.good", "status": "completed"}); v != 1 {
			t.Fatalf("workflow runs %v, want 1", v)
		}
		if v := metricValue(t, "everystep_workflow_duration_seconds", map[string]string{"workflow": "mb.good", "status": "completed"}); v <= 0 {
			t.Fatalf("workflow duration sum %v, want > 0", v)
		}
		if v := metricValue(t, "everystep_step_runs", map[string]string{"workflow": "mb.good", "step": "mb.ok_step", "status": "done"}); v != 1 {
			t.Fatalf("step runs %v, want 1", v)
		}
		if v := metricValue(t, "everystep_step_duration_seconds", map[string]string{"workflow": "mb.good", "step": "mb.ok_step"}); v <= 0 {
			t.Fatalf("step duration sum %v, want > 0", v)
		}
	})
}

func TestFailedWorkflowRecordsWorkflowAndStep(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetMetrics(t)
		id, err := mbBad.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, id)
		if v := metricValue(t, "everystep_workflow_runs", map[string]string{"workflow": "mb.bad", "status": "failed"}); v != 1 {
			t.Fatalf("workflow runs %v, want 1", v)
		}
		if v := metricValue(t, "everystep_step_runs", map[string]string{"workflow": "mb.bad", "step": "mb.bad_step", "status": "failed"}); v != 1 {
			t.Fatalf("step runs %v, want 1", v)
		}
	})
}

func TestStoppedWorkflowRecordsStoppedStatus(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetMetrics(t)
		id, err := mbStopped.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, id)
		if v := metricValue(t, "everystep_workflow_runs", map[string]string{"workflow": "mb.stopped_wf", "status": "stopped"}); v != 1 {
			t.Fatalf("workflow runs %v, want 1", v)
		}
	})
}

func TestBlockedWorkflowRecordsBlockedStatus(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetMetrics(t)
		id, err := mbBlocked.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "1")
		expectCrash(t, db, id)
		reClaim(t, db, id)
		if err := runnerExecute(t, db, id); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusBlocked {
			t.Fatalf("status %s, want blocked", wf.Status)
		}
		if v := metricValue(t, "everystep_workflow_runs", map[string]string{"workflow": "mb.blocked_flow", "status": "blocked"}); v != 1 {
			t.Fatalf("workflow runs %v, want 1", v)
		}
		if err := metrics.UpdateQueueGauges(context.Background(), db); err != nil {
			t.Fatal(err)
		}
		if v := metricValue(t, "everystep_workflows_blocked", nil); v != 1 {
			t.Fatalf("blocked gauge %v, want 1", v)
		}
	})
}

func TestReplayedStepIsNotRecounted(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetMetrics(t)
		id, err := mbTwoSteps.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "2")
		expectCrash(t, db, id)
		// Step 2 ran but the crash came before its record: only step 1
		// was counted.
		if v := metricValue(t, "everystep_step_runs", map[string]string{"workflow": "mb.two_steps", "step": "mb.ok_step", "status": "done"}); v != 1 {
			t.Fatalf("step runs after first pass %v, want 1", v)
		}
		if v := metricValueSafe(t, "everystep_workflow_runs", map[string]string{"workflow": "mb.two_steps", "status": "completed"}); v != 0 {
			t.Fatalf("completed runs %v, want 0", v)
		}

		reClaim(t, db, id)
		if err := runnerExecute(t, db, id); err != nil {
			t.Fatal(err)
		}
		wf := getRun(t, db, id)
		if wf.Status != store.StatusCompleted {
			t.Fatalf("status %s, want completed", wf.Status)
		}
		// On replay step 1 is served from the store (not re-executed) and
		// only step 2 runs again.
		if v := metricValue(t, "everystep_step_runs", map[string]string{"workflow": "mb.two_steps", "step": "mb.ok_step", "status": "done"}); v != 2 {
			t.Fatalf("step runs after replay %v, want 2", v)
		}
		if v := metricValue(t, "everystep_workflow_runs", map[string]string{"workflow": "mb.two_steps", "status": "completed"}); v != 1 {
			t.Fatalf("completed runs %v, want 1", v)
		}
	})
}

func TestWorkerRecordsPoolClaimsAndInflight(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetMetrics(t)
		resetWkSleeper(t, time.Second)
		if _, err := mbSleeper.Schedule(db, struct{}{}); err != nil {
			t.Fatal(err)
		}
		w := worker.New(worker.Opts{DB: db, Pool: 2, Poll: 50 * time.Millisecond, Name: "mb-w", Drain: 5 * time.Second})
		h := startWorker(t, w)
		waitFor(t, 30*time.Second, "inflight gauge to reach 1", func() bool {
			return metricValueSafe(t, "everystep_worker_inflight", map[string]string{"runner": "mb-w"}) == 1 && wkSleeperStarted.Load()
		})
		h.cancel()
		<-h.stopped
		if v := metricValue(t, "everystep_worker_pool_size", map[string]string{"runner": "mb-w"}); v != 2 {
			t.Fatalf("pool size %v, want 2", v)
		}
		if v := metricValue(t, "everystep_worker_claims", map[string]string{"runner": "mb-w"}); v != 1 {
			t.Fatalf("claims %v, want 1", v)
		}
		if v := metricValue(t, "everystep_worker_inflight", map[string]string{"runner": "mb-w"}); v != 0 {
			t.Fatalf("inflight after stop %v, want 0", v)
		}
		if v := metricValue(t, "everystep_worker_started_at_seconds", map[string]string{"runner": "mb-w"}); v <= 0 {
			t.Fatalf("started at %v, want > 0", v)
		}
		if v := metricValue(t, "everystep_workflow_runs", map[string]string{"workflow": "mb.sleeper_flow", "status": "completed"}); v != 1 {
			t.Fatalf("completed runs %v, want 1", v)
		}
	})
}

func TestQueueGauges(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetMetrics(t)
		ctx := context.Background()
		for i := 0; i < 3; i++ {
			if _, err := mbGood.Schedule(db, struct{}{}); err != nil {
				t.Fatal(err)
			}
		}
		if err := metrics.UpdateQueueGauges(ctx, db); err != nil {
			t.Fatal(err)
		}
		if v := metricValue(t, "everystep_workflows_pending", nil); v != 3 {
			t.Fatalf("pending %v, want 3", v)
		}
		if v := metricValue(t, "everystep_workflows_running", nil); v != 0 {
			t.Fatalf("running %v, want 0", v)
		}
		if v := metricValue(t, "everystep_workflows_oldest_pending_age_seconds", nil); v < 0 || v >= 5 {
			t.Fatalf("oldest pending age %v, want in [0, 5)", v)
		}

		claimed := claimNext(t, db)
		if err := runnerExecute(t, db, claimed.ID.UUID); err != nil {
			t.Fatal(err)
		}
		if err := metrics.UpdateQueueGauges(ctx, db); err != nil {
			t.Fatal(err)
		}
		if v := metricValue(t, "everystep_workflows_pending", nil); v != 2 {
			t.Fatalf("pending %v, want 2", v)
		}

		rest, err := store.ClaimScheduled(ctx, db, 2, testWorker)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rest {
			if err := runnerExecute(t, db, rest[i].ID.UUID); err != nil {
				t.Fatal(err)
			}
		}
		if err := metrics.UpdateQueueGauges(ctx, db); err != nil {
			t.Fatal(err)
		}
		if v := metricValue(t, "everystep_workflows_pending", nil); v != 0 {
			t.Fatalf("pending %v, want 0", v)
		}
		if v := metricValue(t, "everystep_workflows_running", nil); v != 0 {
			t.Fatalf("running %v, want 0", v)
		}
		if v := metricValue(t, "everystep_workflows_oldest_pending_age_seconds", nil); v != 0 {
			t.Fatalf("oldest pending age %v, want 0", v)
		}
	})
}

func TestWorkerServesMetricsEndpoint(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		resetMetrics(t)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()

		// One due run: the worker completes it, which also makes the
		// workflow runs counter observable (a never-observed counter
		// exports no TYPE line in the Go client).
		if _, err := mbGood.Schedule(db, struct{}{}); err != nil {
			t.Fatal(err)
		}
		w := worker.New(worker.Opts{
			DB:          db,
			Pool:        1,
			Poll:        50 * time.Millisecond,
			Name:        "mb-ep",
			Drain:       time.Second,
			MetricsPort: port,
			MetricsBind: "127.0.0.1",
		})
		h := startWorker(t, w)

		var body string
		waitFor(t, 15*time.Second, "metrics endpoint with the runs counter", func() bool {
			resp, err := http.Get("http://127.0.0.1:" + strconv.Itoa(port) + "/")
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			var buf bytes.Buffer
			if _, err := io.Copy(&buf, resp.Body); err != nil {
				return false
			}
			body = buf.String()
			return strings.Contains(body, `everystep_worker_pool_size{runner="mb-ep"}`) &&
				strings.Contains(body, "# TYPE everystep_workflow_runs counter")
		})
		h.cancel()
		<-h.stopped
		if _, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), time.Second); err == nil {
			t.Fatal("metrics endpoint is still serving after stop")
		}
	})
}

// metricValueSafe is metricValue without t.Fatal: an absent sample reads as
// 0, which is the true value of an unobserved counter or gauge.
func metricValueSafe(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return 0
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			match := true
			for _, lp := range m.GetLabel() {
				if want, ok := labels[lp.GetName()]; ok && want != lp.GetValue() {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				return m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				return m.GetGauge().GetValue()
			case dto.MetricType_HISTOGRAM:
				return m.GetHistogram().GetSampleSum()
			}
		}
	}
	return 0
}
