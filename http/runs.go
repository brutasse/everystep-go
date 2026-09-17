package eshttp

import (
	"context"

	"github.com/brutasse/everystep-go/store"
)

type runsPayload struct {
	Runs    []runEntry    `json:"runs"`
	Runners []runnerEntry `json:"runners"`
}

type runEntry struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Status      string     `json:"status"`
	ClaimedBy   *string    `json:"claimed_by"`
	CreatedAt   string     `json:"created_at"`
	CompletedAt *string    `json:"completed_at"`
	Steps       stepTotals `json:"steps"`
}

type stepTotals struct {
	Total  int `json:"total"`
	Done   int `json:"done"`
	Failed int `json:"failed"`
}

type runnerEntry struct {
	ClaimedBy string `json:"claimed_by"`
	Inflight  int    `json:"inflight"`
}

// runsPayload builds the runs snapshot: recent runs with their step counts,
// plus the runners with in-flight work. status filters the runs when it is
// a known run status.
func (h *handler) runsPayload(ctx context.Context, status string) (runsPayload, error) {
	q := h.db.WithContext(ctx).Model(&store.Workflow{})
	if status != "" && isRunStatus(status) {
		q = q.Where("status = ?", status)
	}
	var wfs []store.Workflow
	if err := q.Order("created_at DESC, id DESC").Limit(h.limit).Find(&wfs).Error; err != nil {
		return runsPayload{}, err
	}
	counts := map[string]stepTotals{}
	var rows []struct {
		Workflow string `gorm:"column:workflow"`
		Status   string `gorm:"column:status"`
		N        int    `gorm:"column:n"`
	}
	if err := h.db.WithContext(ctx).Raw(
		"SELECT workflow, status, COUNT(*) AS n FROM everystep_step GROUP BY workflow, status").
		Scan(&rows).Error; err != nil {
		return runsPayload{}, err
	}
	for _, r := range rows {
		c := counts[r.Workflow]
		switch r.Status {
		case store.StepDone:
			c.Done = r.N
		case store.StepFailed:
			c.Failed = r.N
		}
		counts[r.Workflow] = c
	}
	payload := runsPayload{Runs: make([]runEntry, 0, len(wfs))}
	for i := range wfs {
		wf := &wfs[i]
		c := counts[wf.ID.UUID.String()]
		payload.Runs = append(payload.Runs, runEntry{
			ID:          wf.ID.UUID.String(),
			Name:        wf.Name,
			Status:      wf.Status,
			ClaimedBy:   wf.ClaimedBy,
			CreatedAt:   formatTime(wf.CreatedAt),
			CompletedAt: formatNullableTime(wf.CompletedAt),
			// No static graph in Go: the total is the recorded steps.
			Steps: stepTotals{Total: c.Done + c.Failed, Done: c.Done, Failed: c.Failed},
		})
	}
	var runners []runnerEntry
	if err := h.db.WithContext(ctx).Raw(
		"SELECT claimed_by, COUNT(*) AS inflight FROM everystep_workflow "+
			"WHERE status = ? AND claimed_by IS NOT NULL "+
			"GROUP BY claimed_by ORDER BY inflight DESC, claimed_by", store.StatusRunning).
		Scan(&runners).Error; err != nil {
		return runsPayload{}, err
	}
	if runners == nil {
		runners = []runnerEntry{}
	}
	payload.Runners = runners
	return payload, nil
}
