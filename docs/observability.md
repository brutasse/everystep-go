# Observability

Three independent surfaces, none of them required:

- **Prometheus metrics**, registered in the default registry.
- **OpenTelemetry traces**, using the host's provider.
- **A failure reporter hook**, for alerting on unhandled failures.

Each is wired by the host. Everystep does not open ports, exporters, or
connections of its own (except an optional worker metrics port).

## Metrics

All everystep collectors register in the **default Prometheus registry** at
import time, so a host's existing `/metrics` scrape serves them with no
configuration. Names and labels match the Python implementation, so existing
dashboards and alert rules transfer.

Labels are bounded to code-defined names (workflow name, step name, runner
name) — never to per-run identifiers — so series stay finite.

| metric | type | labels | meaning |
| --- | --- | --- | --- |
| `everystep_workflow_runs` | counter | `workflow`, `status` | runs ended, by final status. |
| `everystep_workflow_duration_seconds` | histogram | `workflow`, `status` | wall time from claim to terminal state. |
| `everystep_step_runs` | counter | `workflow`, `step`, `status` | step executions, by outcome. |
| `everystep_step_duration_seconds` | histogram | `workflow`, `step` | step execution time. |
| `everystep_worker_pool_size` | gauge | `runner` | pool size, set at worker start. |
| `everystep_worker_inflight` | gauge | `runner` | runs currently in flight. |
| `everystep_worker_claims` | counter | `runner` | runs claimed. |
| `everystep_worker_requeued` | counter | `runner` | runs requeued when the drain deadline expired. |
| `everystep_worker_started_at_seconds` | gauge | `runner` | Unix time the worker started. |
| `everystep_workflows_pending` | gauge | — | scheduled, waiting for a claim. |
| `everystep_workflows_running` | gauge | — | currently running. |
| `everystep_workflows_blocked` | gauge | — | blocked, awaiting a human decision. |
| `everystep_workflows_oldest_pending_age_seconds` | gauge | — | age of the oldest scheduled run. |

The four queue gauges (`pending`, `running`, `blocked`, `oldest_pending_age`)
are refreshed from the database on each scrape — both the UI's `/metrics`
endpoint and the worker metrics port call `metrics.UpdateQueueGauges` before
rendering. Everything else is recorded in-process as runs and steps complete.

### Serving

- **Embedded in the host:** nothing to do — the host's scrape endpoint
  already has the collectors.
- **Worker's own port:** `worker.Opts.MetricsPort` serves `/metrics` on
  `MetricsBind:MetricsPort`, refreshing the queue gauges per scrape.
- **Programmatic:** `metrics.StartHTTPServer(db, addr)` returns a stop
  function for the same behavior.

The worker's per-runner metrics are recorded by that worker process; a fleet
of workers produces one labeled series set per runner.

## Traces

The host owns the `TracerProvider`. Everystep looks up a tracer named
`"everystep"` and uses it if present; with no provider installed, tracing is
a no-op and costs a branch per span.

Spans:

| span | when | carries |
| --- | --- | --- |
| workflow | one per run, from claim to terminal state | the run id |
| step | one per step that executes (not one per recorded replay) | the step dotpath |
| fork | one per `es.Parallel` | the fork id |

Span status is set explicitly: `traces.MarkOK(span)` on success,
`traces.MarkError(span, err)` on failure — a failed step marks its span
errored and the run ends, a completed run marks its span OK.

```go
// inside a step function, if you want sub-step spans:
ctx, span := traces.StepSpan(ctx, "attach-ip", "attach-ip", true)
defer func() { traces.MarkOK(span) }()
```

Typically the host does not call these directly; the runner opens the
workflow, step, and fork spans itself.

## Failure reporting

`es.SetFailureReporter` installs the hook called for every unhandled
workflow failure:

```go
es.SetFailureReporter(func(ctx context.Context, err error, meta es.FailureMeta) {
    // meta.Workflow — the workflow name
    // meta.WorkflowID — the run id
    // err — the error that ended the run
})
```

The hook runs in the worker's context, after the run is recorded `failed`.
It is for alerting — paging, chat, event bus — not for retry: the run is
already failed, and retrying is a scheduler decision (reschedule a new run).

Never reported: `*es.Terminal` (a stop, not a failure),
`*es.EffectUncertain` (a hold, the run is blocked not failed),
`*es.DrainOrphan` (a shutdown artifact). A run that ends in one of those does
not reach the hook.

### Sentry

There is no sentry adapter in the module. Point the hook at your
sentry-go client from host code:

```go
es.SetFailureReporter(func(ctx context.Context, err error, meta es.FailureMeta) {
    sentry.CaptureException(err,
        sentry.WithTag("workflow", meta.Workflow),
        sentry.WithTag("run", meta.WorkflowID.String()))
})
```
