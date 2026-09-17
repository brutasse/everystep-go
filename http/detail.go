package eshttp

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/brutasse/everystep-go/store"
)

type detailPayload struct {
	Run   runDetail   `json:"run"`
	Graph graphDetail `json:"graph"`
	Steps []stepEntry `json:"steps"`
}

type runDetail struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Status    string          `json:"status"`
	ClaimedBy *string         `json:"claimed_by"`
	Args      json.RawMessage `json:"args"`
	Result    json.RawMessage `json:"result"`
	// Error is the stored encoded error, verbatim: the {code, message,
	// payload} shape serde writes.
	Error       json.RawMessage `json:"error"`
	CreatedAt   string          `json:"created_at"`
	CompletedAt *string         `json:"completed_at"`
}

type stepEntry struct {
	StepID string          `json:"step_id"`
	Name   string          `json:"name"`
	Status string          `json:"status"`
	Args   json.RawMessage `json:"args"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

type graphDetail struct {
	Supported bool        `json:"supported"`
	Nodes     []graphNode `json:"nodes"`
	Total     int         `json:"total"`
	Done      int         `json:"done"`
	Failed    int         `json:"failed"`
	InFlight  int         `json:"in_flight"`
	Started   int         `json:"started"`
	Pending   int         `json:"pending"`
	Unmatched []string    `json:"unmatched"`
}

type graphNode struct {
	Kind     string        `json:"kind"`
	ID       string        `json:"id"`
	Func     string        `json:"func,omitempty"`
	Status   string        `json:"status,omitempty"`
	Branches [][]graphNode `json:"branches,omitempty"`
}

func (h *handler) runDetail(ctx context.Context, id uuid.UUID) (detailPayload, error) {
	wf, err := store.GetWorkflow(ctx, h.db, store.UUID{UUID: id})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return detailPayload{}, errRunNotFound
		}
		return detailPayload{}, err
	}
	steps, err := store.ListSteps(ctx, h.db, store.UUID{UUID: id})
	if err != nil {
		return detailPayload{}, err
	}
	payload := detailPayload{
		Run: runDetail{
			ID:          wf.ID.UUID.String(),
			Name:        wf.Name,
			Status:      wf.Status,
			ClaimedBy:   wf.ClaimedBy,
			Args:        json.RawMessage(wf.Args),
			Result:      json.RawMessage(wf.Result),
			Error:       json.RawMessage(wf.Error),
			CreatedAt:   formatTime(wf.CreatedAt),
			CompletedAt: formatNullableTime(wf.CompletedAt),
		},
		Graph: deriveGraph(steps, wf.Status == store.StatusRunning),
		Steps: make([]stepEntry, 0, len(steps)),
	}
	for i := range steps {
		s := &steps[i]
		payload.Steps = append(payload.Steps, stepEntry{
			StepID: s.StepID,
			Name:   s.Name,
			Status: s.Status,
			Args:   json.RawMessage(s.Args),
			Result: json.RawMessage(s.Result),
			Error:  json.RawMessage(s.Error),
		})
	}
	return payload, nil
}

// deriveGraph rebuilds the step graph from the recorded dotpaths: the
// dotpath already encodes the tree — a step at "2.0.1" is step 1 of branch
// 0 of fork 2 — so a run's recorded steps are the structure. Steps not yet
// executed do not appear, and the unmatched list is always empty.
func deriveGraph(steps []store.Step, running bool) graphDetail {
	root := newScope()
	for i := range steps {
		s := &steps[i]
		scope := root
		for _, part := range strings.Split(s.StepID, ".") {
			child, ok := scope.children[part]
			if !ok {
				child = newScope()
				scope.children[part] = child
			}
			scope = child
		}
		scope.step = s
	}
	g := graphDetail{Supported: true, Nodes: scopeItems(root, "", running), Unmatched: []string{}}
	var count func(items []graphNode)
	count = func(items []graphNode) {
		for _, n := range items {
			switch n.Kind {
			case "step":
				g.Total++
				switch n.Status {
				case "done":
					g.Done++
				case "failed":
					g.Failed++
				case "in_flight":
					g.InFlight++
				case "started":
					g.Started++
				case "pending":
					g.Pending++
				}
			default:
				for _, b := range n.Branches {
					count(b)
				}
			}
		}
	}
	count(g.Nodes)
	return g
}

type scope struct {
	children map[string]*scope
	step     *store.Step
}

func newScope() *scope { return &scope{children: map[string]*scope{}} }

// scopeItems turns one scope into nodes, in the same order as the page's
// dagFromSteps: numeric segments first in numeric order, then named
// segments lexicographically.
func scopeItems(sc *scope, prefix string, running bool) []graphNode {
	items := make([]graphNode, 0, len(sc.children))
	for _, key := range sortedSegments(sc.children) {
		child := sc.children[key]
		id := prefix + key
		if child.step != nil {
			status := child.step.Status
			if status == store.StepStarted {
				// A started row of an unsafe-to-repeat step reads as in
				// flight while the run is still running (the effect is
				// executing) and as uncertain once the run has blocked on
				// it.
				if running {
					status = "in_flight"
				} else {
					status = "started"
				}
			}
			items = append(items, graphNode{Kind: "step", ID: id, Func: child.step.Name, Status: status})
		} else {
			branches := make([][]graphNode, 0, len(child.children))
			for _, b := range sortedSegments(child.children) {
				branches = append(branches, scopeItems(child.children[b], id+"."+b+".", running))
			}
			items = append(items, graphNode{Kind: "fork", ID: id, Branches: branches})
		}
	}
	return items
}

func sortedSegments(children map[string]*scope) []string {
	keys := make([]string, 0, len(children))
	for k := range children {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		na, ea := number(a)
		nb, eb := number(b)
		switch {
		case !ea && !eb:
			return na < nb
		case !ea:
			return true
		case !eb:
			return false
		default:
			return a < b
		}
	})
	return keys
}

// number mirrors the page's Number(segment) test: a segment sorts numerically
// when it parses as a number, otherwise lexicographically.
func number(s string) (float64, bool) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}
