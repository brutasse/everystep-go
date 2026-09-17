// Package eshttp exposes the everystep UI and API as plain http.Handlers
// the host mounts anywhere:
//
//	mux.Handle("/everystep/", eshttp.Handler(db))
//
// The UI is a self-contained static page; it addresses the API with URLs
// relative to itself, so the mount point can be anywhere.
package eshttp

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gorm.io/gorm"

	"github.com/brutasse/everystep-go/metrics"
	"github.com/brutasse/everystep-go/store"
)

//go:embed ui.html
var uiPage string

// errRunNotFound is the 404 for api/run/<id>.
var errRunNotFound = errors.New("run not found")

// Opts tunes the handler; zero values take the defaults.
type Opts struct {
	// RunsLimit bounds the runs in a snapshot (default 200).
	RunsLimit int
	// StreamPoll is the SSE snapshot interval (default 1s).
	StreamPoll time.Duration
}

type handler struct {
	db    *gorm.DB
	limit int
	poll  time.Duration
}

// Handler returns the everystep UI and API as an http.Handler.
func Handler(db *gorm.DB) http.Handler {
	return HandlerWithOpts(db, Opts{})
}

// HandlerWithOpts returns the everystep UI and API with tuned limits.
func HandlerWithOpts(db *gorm.DB, opts Opts) http.Handler {
	limit := opts.RunsLimit
	if limit <= 0 {
		limit = 200
	}
	poll := opts.StreamPoll
	if poll <= 0 {
		poll = time.Second
	}
	h := &handler{db: db, limit: limit, poll: poll}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", h.serveMetrics)
	mux.HandleFunc("/api/runs", h.serveRuns)
	mux.HandleFunc("/api/run/{id}", h.serveRun)
	mux.HandleFunc("/api/stream", h.serveStream)
	mux.HandleFunc("/", h.serveUI)
	return mux
}

// serveUI serves the self-contained UI page at the root of the mounted
// subtree.
func (h *handler) serveUI(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/") {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, uiPage)
}

// serveMetrics serves everystep's Prometheus metrics, refreshing the queue
// gauges from the database on each scrape.
func (h *handler) serveMetrics(w http.ResponseWriter, r *http.Request) {
	if err := metrics.UpdateQueueGauges(r.Context(), h.db); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	promhttp.Handler().ServeHTTP(w, r)
}

func (h *handler) serveRuns(w http.ResponseWriter, r *http.Request) {
	payload, err := h.runsPayload(r.Context(), r.URL.Query().Get("status"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func (h *handler) serveRun(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "run not found"})
		return
	}
	payload, err := h.runDetail(r.Context(), id)
	if err != nil {
		if errors.Is(err, errRunNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "run not found"})
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// isRunStatus reports whether s is a run status.
func isRunStatus(s string) bool {
	switch s {
	case store.StatusScheduled, store.StatusRunning, store.StatusCompleted,
		store.StatusFailed, store.StatusStopped, store.StatusBlocked:
		return true
	}
	return false
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func formatNullableTime(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := formatTime(*t)
	return &s
}
