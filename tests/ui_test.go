// The UI and API tests: the Python suite's test_ui.py ported to Go, run
// against both backends. The handler is served from httptest servers; the
// pure DAG layout block is exercised under node exactly as in the Python
// suite.
package tests

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	es "github.com/brutasse/everystep-go/everystep"
	eshttp "github.com/brutasse/everystep-go/http"
	"github.com/brutasse/everystep-go/runner"
	"github.com/brutasse/everystep-go/store"
)

type uiChainArgs struct{ X string }

var (
	uiA     = es.Step("ui.a", func(ctx context.Context, a struct{}) (string, error) { return "a", nil })
	uiB     = es.Step("ui.b", func(ctx context.Context, a string) (string, error) { return "b" + a, nil })
	uiRisky = es.Step("ui.risky", func(ctx context.Context, a struct{}) (string, error) {
		return "r", nil
	}, es.UnsafeToRepeat())
	uiBoom = es.Step("ui.boom", func(ctx context.Context, a struct{}) (string, error) {
		return "", errors.New("kaput")
	})
)

var uiChain = es.Workflow("ui.chain", func(ctx context.Context, a uiChainArgs) (string, error) {
	x, err := uiA.Call(ctx, struct{}{})
	if err != nil {
		return "", err
	}
	y, err := uiB.Call(ctx, a.X)
	if err != nil {
		return "", err
	}
	return x + y, nil
})

var uiBlocked = es.Workflow("ui.blocked", func(ctx context.Context, a struct{}) (string, error) {
	if _, err := uiA.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	return uiRisky.Call(ctx, struct{}{})
})

var uiFailing = es.Workflow("ui.failing", func(ctx context.Context, a struct{}) (string, error) {
	if _, err := uiA.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	return uiBoom.Call(ctx, struct{}{})
})

var uiLoop = es.Workflow("ui.loop", func(ctx context.Context, a struct{}) (string, error) {
	var out string
	for i := 0; i < 2; i++ {
		s, err := uiB.Call(ctx, strconv.Itoa(i))
		if err != nil {
			return "", err
		}
		out += s
	}
	return out, nil
})

// uiServer serves the handler against the backend's database with a fast
// SSE poll interval, so the stream tests do not wait on the default.
func uiServer(t *testing.T, db *gorm.DB, opts eshttp.Opts) *httptest.Server {
	t.Helper()
	if opts.StreamPoll == 0 {
		opts.StreamPoll = 50 * time.Millisecond
	}
	srv := httptest.NewServer(eshttp.HandlerWithOpts(db, opts))
	t.Cleanup(srv.Close)
	return srv
}

func getJSON(t *testing.T, url string) map[string]any {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, res.StatusCode)
	}
	var data map[string]any
	if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
		t.Fatal(err)
	}
	return data
}

func fetchPage(t *testing.T, url string) string {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, res.StatusCode)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func runRow(t *testing.T, frame map[string]any, runID string) map[string]any {
	t.Helper()
	for _, r := range frame["runs"].([]any) {
		m := r.(map[string]any)
		if m["id"] == runID {
			return m
		}
	}
	t.Fatalf("run %s not in snapshot", runID)
	return nil
}

// readSSEFrame reads one server-sent event frame, skipping comment pings,
// and returns its parsed data payload.
func readSSEFrame(t *testing.T, br *bufio.Reader) map[string]any {
	t.Helper()
	var data []string
	for {
		line, err := br.ReadBytes('\n')
		if err != nil {
			t.Fatalf("read stream: %v", err)
		}
		line = bytes.TrimRight(line, "\r\n")
		if len(line) == 0 {
			break
		}
		if bytes.HasPrefix(line, []byte(":")) {
			continue
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			data = append(data, string(bytes.TrimSpace(line[len("data:"):])))
		}
	}
	if len(data) == 0 {
		t.Fatal("frame with no data")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.Join(data, "")), &payload); err != nil {
		t.Fatalf("frame data: %v: %s", err, strings.Join(data, ""))
	}
	return payload
}

func TestUIPage(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		srv := uiServer(t, db, eshttp.Opts{})
		res, err := http.Get(srv.URL + "/")
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", res.StatusCode)
		}
		if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Fatalf("content type = %q", ct)
		}
		html := fetchPage(t, srv.URL+"/")
		if strings.Contains(html, "__EVERYSTEP_BASE__") {
			t.Fatal("base placeholder left in page")
		}
		for _, want := range []string{`fetch("api/runs")`, `id="run-by-id"`} {
			if !strings.Contains(html, want) {
				t.Fatalf("page missing %q", want)
			}
		}
	})
}

func TestRunsList(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		doneID, err := uiChain.Schedule(db, uiChainArgs{X: "1"})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, doneID)
		runningID, err := uiChain.Schedule(db, uiChainArgs{X: "2"})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		scheduledID, err := uiChain.Schedule(db, uiChainArgs{X: "3"})
		if err != nil {
			t.Fatal(err)
		}

		srv := uiServer(t, db, eshttp.Opts{})
		data := getJSON(t, srv.URL+"/api/runs")
		byID := map[string]map[string]any{}
		for _, r := range data["runs"].([]any) {
			m := r.(map[string]any)
			byID[m["id"].(string)] = m
		}
		if got := byID[doneID.String()]["status"]; got != store.StatusCompleted {
			t.Fatalf("done status = %v", got)
		}
		if got := byID[doneID.String()]["steps"]; !maps.Equal(got.(map[string]any),
			map[string]any{"total": float64(2), "done": float64(2), "failed": float64(0)}) {
			t.Fatalf("done steps = %v", got)
		}
		if got := byID[scheduledID.String()]["status"]; got != store.StatusScheduled {
			t.Fatalf("scheduled status = %v", got)
		}
		if got := byID[scheduledID.String()]["steps"]; !maps.Equal(got.(map[string]any),
			map[string]any{"total": float64(0), "done": float64(0), "failed": float64(0)}) {
			t.Fatalf("scheduled steps = %v", got)
		}
		if got := byID[runningID.String()]["status"]; got != store.StatusRunning {
			t.Fatalf("running status = %v", got)
		}
		if got := byID[runningID.String()]["claimed_by"]; got != testWorker {
			t.Fatalf("running claimed_by = %v", got)
		}
		foundRunner := false
		for _, r := range data["runners"].([]any) {
			m := r.(map[string]any)
			if m["claimed_by"] == testWorker && m["inflight"].(float64) >= 1 {
				foundRunner = true
			}
		}
		if !foundRunner {
			t.Fatalf("no runner with in-flight work: %v", data["runners"])
		}
		for _, r := range data["runs"].([]any) {
			m := r.(map[string]any)
			if _, ok := m["created_at"].(string); !ok {
				t.Fatalf("created_at not a string: %v", m["created_at"])
			}
			if m["completed_at"] != nil {
				if _, ok := m["completed_at"].(string); !ok {
					t.Fatalf("completed_at not null or a string: %v", m["completed_at"])
				}
			}
		}
	})
}

func TestRunsListStatusFilter(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := uiChain.Schedule(db, uiChainArgs{X: "1"})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, id)
		if _, err := uiChain.Schedule(db, uiChainArgs{X: "2"}); err != nil {
			t.Fatal(err)
		}
		srv := uiServer(t, db, eshttp.Opts{})

		data := getJSON(t, srv.URL+"/api/runs?status=completed")
		runs := data["runs"].([]any)
		if len(runs) == 0 {
			t.Fatal("no completed runs")
		}
		for _, r := range runs {
			if got := r.(map[string]any)["status"]; got != store.StatusCompleted {
				t.Fatalf("status = %v", got)
			}
		}
		valid := map[string]bool{
			store.StatusScheduled: true, store.StatusRunning: true, store.StatusCompleted: true,
			store.StatusFailed: true, store.StatusStopped: true, store.StatusBlocked: true,
		}
		data = getJSON(t, srv.URL+"/api/runs?status=nonsense")
		for _, r := range data["runs"].([]any) {
			status := r.(map[string]any)["status"].(string)
			if !valid[status] {
				t.Fatalf("unknown status %q", status)
			}
		}
	})
}

func TestRunDetail(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := uiChain.Schedule(db, uiChainArgs{X: "1"})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, id)
		srv := uiServer(t, db, eshttp.Opts{})
		data := getJSON(t, srv.URL+"/api/run/"+id.String())

		run := data["run"].(map[string]any)
		if run["status"] != store.StatusCompleted {
			t.Fatalf("status = %v", run["status"])
		}
		// encoding/json keeps the single-letter field name uppercased.
		if !maps.Equal(run["args"].(map[string]any), map[string]any{"X": "1"}) {
			t.Fatalf("args = %v", run["args"])
		}
		if run["result"] != "ab1" {
			t.Fatalf("result = %v", run["result"])
		}

		g := data["graph"].(map[string]any)
		if g["supported"] != true {
			t.Fatalf("graph = %v", g)
		}
		var ids, statuses []string
		for _, n := range g["nodes"].([]any) {
			m := n.(map[string]any)
			ids = append(ids, m["id"].(string))
			statuses = append(statuses, m["status"].(string))
		}
		if !slices.Equal(ids, []string{"1", "2"}) {
			t.Fatalf("node ids = %v", ids)
		}
		if !slices.Equal(statuses, []string{"done", "done"}) {
			t.Fatalf("node statuses = %v", statuses)
		}
		if g["total"] != float64(2) || g["done"] != float64(2) {
			t.Fatalf("graph counts = %v", g)
		}
		if len(g["unmatched"].([]any)) != 0 {
			t.Fatalf("unmatched = %v", g["unmatched"])
		}

		steps := data["steps"].([]any)
		var stepIDs []string
		for _, s := range steps {
			stepIDs = append(stepIDs, s.(map[string]any)["step_id"].(string))
		}
		if !slices.Equal(stepIDs, []string{"1", "2"}) {
			t.Fatalf("step ids = %v", stepIDs)
		}
		if got := steps[0].(map[string]any)["result"]; got != "a" {
			t.Fatalf("step 1 result = %v", got)
		}
	})
}

func TestRunDetailFailedStep(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := uiFailing.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		runnerExecute(t, db, id)
		if got := getRun(t, db, id).Status; got != store.StatusFailed {
			t.Fatalf("run status = %s", got)
		}
		srv := uiServer(t, db, eshttp.Opts{})
		data := getJSON(t, srv.URL+"/api/run/"+id.String())

		if got := data["graph"].(map[string]any)["failed"]; got != float64(1) {
			t.Fatalf("graph failed = %v", got)
		}
		if got := data["run"].(map[string]any)["error"].(map[string]any)["message"]; got != "kaput" {
			t.Fatalf("run error = %v", got)
		}
		var failed *map[string]any
		for _, s := range data["steps"].([]any) {
			m := s.(map[string]any)
			if m["status"] == store.StepFailed {
				failed = &m
			}
		}
		if failed == nil {
			t.Fatalf("no failed step: %v", data["steps"])
		}
		if got := (*failed)["error"].(map[string]any)["message"]; got != "kaput" {
			t.Fatalf("step error = %v", got)
		}
	})
}

func TestRunDetailBlocked(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := uiBlocked.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "2")
		expectCrash(t, db, id)
		reClaim(t, db, id)
		runnerExecute(t, db, id)
		if got := getRun(t, db, id).Status; got != store.StatusBlocked {
			t.Fatalf("run status = %s", got)
		}

		srv := uiServer(t, db, eshttp.Opts{})
		data := getJSON(t, srv.URL+"/api/run/"+id.String())
		if got := data["run"].(map[string]any)["status"]; got != store.StatusBlocked {
			t.Fatalf("status = %v", got)
		}
		if got := data["run"].(map[string]any)["error"].(map[string]any)["code"]; got != "everystep.effect_uncertain" {
			t.Fatalf("error code = %v", got)
		}
		g := data["graph"].(map[string]any)
		if g["done"] != float64(1) || g["started"] != float64(1) {
			t.Fatalf("graph counts = %v", g)
		}
		var statuses []string
		for _, n := range g["nodes"].([]any) {
			statuses = append(statuses, n.(map[string]any)["status"].(string))
		}
		if !slices.Equal(statuses, []string{"done", "started"}) {
			t.Fatalf("node statuses = %v", statuses)
		}
		steps := data["steps"].([]any)
		if steps[0].(map[string]any)["status"] != store.StepDone {
			t.Fatalf("step 1 status = %v", steps[0].(map[string]any)["status"])
		}
		if steps[1].(map[string]any)["status"] != store.StepStarted {
			t.Fatalf("step 2 status = %v", steps[1].(map[string]any)["status"])
		}

		byStatus := getJSON(t, srv.URL+"/api/runs?status=blocked")
		if runRow(t, byStatus, id.String()) == nil {
			t.Fatal("blocked run not in the blocked snapshot")
		}
	})
}

func TestRunDetailMarkedStepInFlight(t *testing.T) {
	// A marked step executing is shown in flight, not uncertain: its
	// started pre-record is the in-flight marker while the run is running.
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := uiBlocked.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		crashAt(t, "2")
		expectCrash(t, db, id)

		srv := uiServer(t, db, eshttp.Opts{})
		data := getJSON(t, srv.URL+"/api/run/"+id.String())
		if got := data["run"].(map[string]any)["status"]; got != store.StatusRunning {
			t.Fatalf("status = %v", got)
		}
		g := data["graph"].(map[string]any)
		var statuses []string
		for _, n := range g["nodes"].([]any) {
			statuses = append(statuses, n.(map[string]any)["status"].(string))
		}
		if !slices.Equal(statuses, []string{"done", "in_flight"}) {
			t.Fatalf("node statuses = %v", statuses)
		}
		if g["in_flight"] != float64(1) || g["started"] != float64(0) {
			t.Fatalf("graph counts = %v", g)
		}
		if got := data["steps"].([]any)[1].(map[string]any)["status"]; got != store.StepStarted {
			t.Fatalf("step 2 status = %v", got)
		}
	})
}

func TestRunDetailLoop(t *testing.T) {
	// A body Python's parser rejects (a loop) renders from the recorded
	// dotpaths: the Go graph is always supported.
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := uiLoop.Schedule(db, struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, id)
		srv := uiServer(t, db, eshttp.Opts{})
		data := getJSON(t, srv.URL+"/api/run/"+id.String())
		g := data["graph"].(map[string]any)
		if g["supported"] != true {
			t.Fatalf("graph = %v", g)
		}
		var ids []string
		for _, n := range g["nodes"].([]any) {
			ids = append(ids, n.(map[string]any)["id"].(string))
		}
		if !slices.Equal(ids, []string{"1", "2"}) {
			t.Fatalf("node ids = %v", ids)
		}
		if got := data["run"].(map[string]any)["result"]; got != "b0b1" {
			t.Fatalf("result = %v", got)
		}
	})
}

func TestRunDetail404(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		srv := uiServer(t, db, eshttp.Opts{})
		for _, p := range []string{"/api/run/" + uuid.NewString(), "/api/run/not-a-uuid"} {
			res, err := http.Get(srv.URL + p)
			if err != nil {
				t.Fatal(err)
			}
			res.Body.Close()
			if res.StatusCode != http.StatusNotFound {
				t.Fatalf("%s: status = %d", p, res.StatusCode)
			}
		}
	})
}

func TestRunDetailBeyondListLimit(t *testing.T) {
	// A run older than the list window is absent from the runs snapshot
	// but still loads by id straight from the database.
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		ids := make([]uuid.UUID, 3)
		for i, x := range []string{"old", "mid", "new"} {
			id, err := uiChain.Schedule(db, uiChainArgs{X: x})
			if err != nil {
				t.Fatal(err)
			}
			ids[i] = id
			runToCompletion(t, db, id)
		}
		srv := uiServer(t, db, eshttp.Opts{RunsLimit: 2})
		data := getJSON(t, srv.URL+"/api/runs")
		for _, r := range data["runs"].([]any) {
			if r.(map[string]any)["id"] == ids[0].String() {
				t.Fatal("oldest run beyond the list window is listed")
			}
		}
		old := getJSON(t, srv.URL+"/api/run/"+ids[0].String())
		if got := old["run"].(map[string]any)["id"]; got != ids[0].String() {
			t.Fatalf("id = %v", got)
		}
		if got := old["run"].(map[string]any)["result"]; got != "abold" {
			t.Fatalf("result = %v", got)
		}
	})
}

func TestMetricsEndpoint(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		// Record one run first: a counter no one has observed exports no
		// TYPE line in the Go client.
		id, err := uiChain.Schedule(db, uiChainArgs{X: "1"})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, id)
		srv := uiServer(t, db, eshttp.Opts{})
		body := fetchPage(t, srv.URL+"/metrics")
		if !strings.Contains(body, "everystep_workflows_pending") {
			t.Fatal("metrics missing the queue gauges")
		}
		if !strings.Contains(body, "# TYPE everystep_workflow_runs counter") {
			t.Fatal("metrics missing the workflow runs counter")
		}
	})
}

func openStream(t *testing.T, url string) (*http.Response, *bufio.Reader) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		t.Fatalf("status = %d", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		res.Body.Close()
		t.Fatalf("content type = %q", ct)
	}
	if got := res.Header.Get("X-Accel-Buffering"); got != "no" {
		res.Body.Close()
		t.Fatalf("X-Accel-Buffering = %q", got)
	}
	t.Cleanup(func() { res.Body.Close() })
	return res, bufio.NewReader(res.Body)
}

func TestStreamSSE(t *testing.T) {
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := uiChain.Schedule(db, uiChainArgs{X: "1"})
		if err != nil {
			t.Fatal(err)
		}
		runToCompletion(t, db, id)
		srv := uiServer(t, db, eshttp.Opts{})
		_, br := openStream(t, srv.URL+"/api/stream")

		frame := readSSEFrame(t, br)
		if runRow(t, frame, id.String()) == nil {
			t.Fatal("existing run not in the first frame")
		}
		newID, err := uiChain.Schedule(db, uiChainArgs{X: "2"})
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			frame = readSSEFrame(t, br)
			for _, r := range frame["runs"].([]any) {
				if r.(map[string]any)["id"] == newID.String() {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatal("new run never appeared on the stream")
			}
		}
	})
}

func TestStreamShowsStepProgress(t *testing.T) {
	// A running workflow's step counts advance over the stream.
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		id, err := uiChain.Schedule(db, uiChainArgs{X: "1"})
		if err != nil {
			t.Fatal(err)
		}
		claimNext(t, db)
		srv := uiServer(t, db, eshttp.Opts{})
		_, br := openStream(t, srv.URL+"/api/stream")

		frame := readSSEFrame(t, br)
		row := runRow(t, frame, id.String())
		if row["status"] != store.StatusRunning {
			t.Fatalf("status = %v", row["status"])
		}
		if !maps.Equal(row["steps"].(map[string]any),
			map[string]any{"total": float64(0), "done": float64(0), "failed": float64(0)}) {
			t.Fatalf("steps = %v", row["steps"])
		}

		done := make(chan error, 1)
		go func() {
			done <- runner.Execute(context.Background(), db, id, &atomic.Bool{})
		}()
		var final map[string]any
		deadline := time.Now().Add(15 * time.Second)
		for {
			if time.Now().After(deadline) {
				t.Fatal("run never completed on the stream")
			}
			frame = readSSEFrame(t, br)
			row = runRow(t, frame, id.String())
			if row["status"] == store.StatusCompleted {
				final = row
				break
			}
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if !maps.Equal(final["steps"].(map[string]any),
			map[string]any{"total": float64(2), "done": float64(2), "failed": float64(0)}) {
			t.Fatalf("final steps = %v", final["steps"])
		}
	})
}

// nodeAvailable skips the node-driven tests when node is absent, as the
// Python suite does.
func nodeAvailable(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not available")
	}
}

// runNode executes js under node and returns its stdout.
func runNode(t *testing.T, js string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "block.js")
	if err := os.WriteFile(path, []byte(js), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "node", path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v: %s", err, out)
	}
	return string(out)
}

func scriptBlock(t *testing.T, html, pattern string) string {
	t.Helper()
	m := regexp.MustCompile(pattern).FindStringSubmatch(html)
	if m == nil {
		t.Fatalf("script block missing: %s", pattern)
	}
	return m[1]
}

func TestPageScriptBlocksParse(t *testing.T) {
	// Both script blocks are syntactically valid JS.
	nodeAvailable(t)
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		srv := uiServer(t, db, eshttp.Opts{})
		html := fetchPage(t, srv.URL+"/")
		for _, pattern := range []string{
			`(?s)<script id="everystep-dag">(.*?)</script>`,
			`(?s)<script>\n(.*?)</script>`,
		} {
			js := scriptBlock(t, html, pattern)
			path := filepath.Join(t.TempDir(), "block.js")
			if err := os.WriteFile(path, []byte(js), 0o644); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			out, err := exec.CommandContext(ctx, "node", "--check", path).CombinedOutput()
			cancel()
			if err != nil {
				t.Fatalf("node --check %s: %v: %s", pattern, err, out)
			}
		}
	})
}

func TestDagLayout(t *testing.T) {
	// The pure layout block positions nodes without overlap, wires
	// fan-out/fan-in edges, and flows strictly top-down.
	nodeAvailable(t)
	eachBackend(t, func(t *testing.T, db *gorm.DB) {
		srv := uiServer(t, db, eshttp.Opts{})
		html := fetchPage(t, srv.URL+"/")
		js := scriptBlock(t, html, `(?s)<script id="everystep-dag">(.*?)</script>`) + dagChecksJS
		var results map[string]layoutCounts
		if err := json.Unmarshal([]byte(runNode(t, js)), &results); err != nil {
			t.Fatal(err)
		}
		want := map[string]layoutCounts{
			"linear":    {Nodes: 3, Edges: 2},
			"fork":      {Nodes: 4, Edges: 4},
			"nested":    {Nodes: 7, Edges: 8},
			"forkFirst": {Nodes: 2, Edges: 4},
			"flat":      {Nodes: 6, Edges: 6},
		}
		if !maps.Equal(results, want) {
			t.Fatalf("layout = %v, want %v", results, want)
		}
	})
}

type layoutCounts struct {
	Nodes int `json:"nodes"`
	Edges int `json:"edges"`
}

const dagChecksJS = `
const results = {};

function check(name, items, wantNodes, wantEdges) {
  const layout = dagLayout(items);
  const ids = layout.nodes.map((n) => n.id).sort();
  if (JSON.stringify(ids) !== JSON.stringify([...wantNodes].sort()))
    throw new Error(name + ": nodes " + ids.join(",") + " != " + wantNodes.join(","));
  if (layout.edges.length !== wantEdges)
    throw new Error(name + ": " + layout.edges.length + " edges, want " + wantEdges);
  const minD = 2 * DAG.radius + 2;
  for (let i = 0; i < layout.nodes.length; i++) {
    for (let j = i + 1; j < layout.nodes.length; j++) {
      const a = layout.nodes[i], b = layout.nodes[j];
      if (Math.hypot(a.x - b.x, a.y - b.y) < minD)
        throw new Error(name + ": " + a.id + " overlaps " + b.id);
    }
  }
  const anchor = (p, side) => layout.nodes.find((n) =>
    Math.abs(n.x - p.x) < 0.5 &&
    Math.abs(n.y + (side === "bottom" ? DAG.radius : -DAG.radius) - p.y) < 0.5);
  const entryExit = [];
  if (items.length && items[0].kind === "fork") entryExit.push([layout.width / 2, -14]);
  if (items.length && items[items.length - 1].kind === "fork")
    entryExit.push([layout.width / 2, layout.height + 14]);
  const isEntryExit = (p) => entryExit.some(([x, y]) => Math.abs(x - p.x) < 0.5 && Math.abs(y - p.y) < 0.5);
  if (layout.anchors.length !== entryExit.length)
    throw new Error(name + ": " + layout.anchors.length + " anchors, want " + entryExit.length);
  for (const e of layout.edges) {
    if (!isEntryExit(e.from) && !anchor(e.from, "bottom"))
      throw new Error(name + ": edge from " + JSON.stringify(e.from) + " not at a node bottom or entry");
    if (!isEntryExit(e.to) && !anchor(e.to, "top"))
      throw new Error(name + ": edge to " + JSON.stringify(e.to) + " not at a node top or exit");
    if (e.from.y >= e.to.y)
      throw new Error(name + ": edge not top-down " + JSON.stringify(e.from) + " -> " + JSON.stringify(e.to));
  }
  for (const n of layout.nodes) {
    if (n.x < -0.5 || n.x > layout.width + 0.5 || n.y < -0.5 || n.y > layout.height + 0.5)
      throw new Error(name + ": node " + n.id + " out of bounds");
  }
  results[name] = { nodes: layout.nodes.length, edges: layout.edges.length };
}

const S = (id) => ({ kind: "step", id, func: id, status: "done" });
const F = (id, branches) => ({ kind: "fork", id, branches });

check("linear", [S("1"), S("2"), S("3")], ["1", "2", "3"], 2);
check("fork", [S("1"), F("fan", [[S("fan.0.1")], [S("fan.1.1")]]), S("3")],
  ["1", "3", "fan.0.1", "fan.1.1"], 4);
check("nested",
  [S("1"),
   F("fan", [
    [S("fan.0.1"), F("fan.0.2", [[S("fan.0.2.0.1")], [S("fan.0.2.1.1")]]), S("fan.0.3")],
    [S("fan.1.1")],
   ]),
   S("4")],
  ["1", "4", "fan.0.1", "fan.0.2.0.1", "fan.0.2.1.1", "fan.0.3", "fan.1.1"], 8);
// workflow that starts and ends in a fork gets entry/exit anchors
check("forkFirst", [F("fan", [[S("fan.0.1")], [S("fan.1.1")]])], ["fan.0.1", "fan.1.1"], 4);

// recorded dot-paths (flat mode) normalize to the same item shape:
// steps 1, 2, fork in slot 3 (branch 0: two steps, branch 1: one), step 4
check("flat", dagFromSteps([
  { step_id: "1", name: "a", status: "done" },
  { step_id: "2", name: "b", status: "done" },
  { step_id: "3.0.1", name: "c", status: "done" },
  { step_id: "3.0.2", name: "d", status: "done" },
  { step_id: "3.1.1", name: "e", status: "done" },
  { step_id: "4", name: "f", status: "done" },
]), ["1", "2", "3.0.1", "3.0.2", "3.1.1", "4"], 6);

console.log(JSON.stringify(results));
`
