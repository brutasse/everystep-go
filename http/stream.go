package eshttp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ssePingEvery is the number of unchanged polls between stream pings.
const ssePingEvery = 15

// serveStream is the runs snapshot as server-sent events: re-sent on every
// change, a comment ping every 15 unchanged polls.
func (h *handler) serveStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	ticker := time.NewTicker(h.poll)
	defer ticker.Stop()
	last := ""
	idle := 0
	for {
		payload, err := h.runsPayload(r.Context(), "")
		if err == nil {
			data, _ := json.Marshal(payload)
			if string(data) == last {
				idle++
				if idle >= ssePingEvery {
					if _, werr := fmt.Fprint(w, ": ping\n\n"); werr == nil {
						flusher.Flush()
						idle = 0
					}
				}
			} else if _, werr := fmt.Fprintf(w, "data: %s\n\n", data); werr == nil {
				flusher.Flush()
				last = string(data)
				idle = 0
			}
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}
