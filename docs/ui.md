# The UI and API

The UI is a self-contained `http.Handler` with no build step, no
dependencies, and no configuration surface beyond two options. Mount it
anywhere on your own mux; it serves its page and a small JSON API, and the
page talks to that API with relative paths.

## Mounting

```go
mux := http.NewServeMux()
mux.Handle("/everystep/", eshttp.Handler(db))
http.ListenAndServe(":8080", mux)
```

The handler uses relative URLs internally, so it works at the root or under
any prefix. Pass a `*gorm.DB`; it is read-only as far as the API is
concerned.

For non-defaults:

```go
eshttp.HandlerWithOpts(db, eshttp.Opts{
    RunsLimit:  500,  // default 200
    StreamPoll: 500 * time.Millisecond, // default 1s
})
```

## Endpoints

| endpoint | purpose |
| --- | --- |
| `GET /` | the single-page UI (HTML). |
| `GET /api/runs` | recent runs, most recent first, with per-run step counts. `?status=` filters to one status. |
| `GET /api/run/{id}` | one run: its record, the derived graph, and its recorded steps. 404 if absent. |
| `GET /api/stream` | Server-Sent Events. Resends the runs snapshot on change; falls back to a heartbeat. |
| `GET /metrics` | Prometheus metrics (see [observability](observability.md)). |

The runs list returns each run's id, workflow name, status, claimed worker,
and a `steps` count object (`done`, `failed`, `started`, `total`). The run
detail returns the run, a `graph` object, and the full `steps` list.

## What the page shows

Three panes, no login, no external assets:

- **Runs** — a table of recent runs with a status badge and a progress bar
  (`done / total`). Click a row to open its detail.
- **Runners** — workers seen in the database, with their in-flight count.
- **Detail** — the selected run: its args, result or error, and a DAG of
  its steps.

Errors render as stored: `code`, `message`, and `payload` are shown
verbatim. There is no client-side decoding — what the database holds is
what the page shows.

## The graph

The DAG is derived from the recorded steps' dotpaths, not from the workflow
definition. A step's id `a.b.c` places it under its parent; a fork is a
node with one child per branch. Status is drawn per node: done, failed, or
started (in flight).

Because the graph is built from what actually ran, it is a faithful record
of a specific run — including branches that were taken and steps that were
skipped by the body's control flow. It is not a prediction of what the
workflow *would* do; unexecuted steps are not shown.

Loops render as the recorded iterations. A step that ran twice appears twice
in the graph, because the dotpath recorded each attempt.

## Streaming

`/api/stream` is an SSE endpoint. The page opens it and applies snapshots
as they arrive; if the connection drops or the proxy buffers SSE, the page
falls back to polling `/api/runs` on a timer. `StreamPoll` sets the server's
snapshot recheck interval; `RunsLimit` caps the rows in every snapshot.

## What it is not

It is an operator view, not a control plane. It does not resolve blocked
runs, cancel runs, or edit records — those are database-level operations
(see [the model](model.md) for `es.ResolveStep`). The UI is read-only.
