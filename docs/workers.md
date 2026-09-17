# Workers

A worker claims scheduled runs from the shared database and executes them.
There is no dispatcher and no queue service: the database is the queue, and
any number of workers can run against it. A worker is a process, a goroutine,
or a `Run` call — nothing else.

## The claim loop

Each worker runs a poller. Every poll (default 200ms) it claims up to the
pool size worth of scheduled runs:

```sql
-- the claim, as run against the database
SELECT ... FROM everystep_workflow
WHERE status = 'scheduled'
ORDER BY created_at
FOR UPDATE SKIP LOCKED   -- FOR UPDATE on MariaDB
```

Each claimed run moves to `running` with `claimed_by` set to the worker's
name and gets its own goroutine from a pool (default 4). The run executes
body-by-body as in [the model](model.md): recorded steps are served from
SQL, the first unrecorded step runs.

The worker never blocks on work; an empty poll is a no-op.

## Configuration

`worker.New` takes an `Opts` struct; every field has a default.

| field | type | default | meaning |
| --- | --- | --- | --- |
| `DB` | `*gorm.DB` | — | the shared database. Required. |
| `Pool` | `int` | `4` | concurrent in-flight runs. |
| `Poll` | `time.Duration` | `200ms` | claim poll interval. |
| `Name` | `string` | the hostname | this worker's identity in the database. |
| `Drain` | `time.Duration` | `30s` | how long to wait for in-flight runs at shutdown. |
| `Logger` | `*zap.Logger` | `zap.L()` | where worker log lines go. |
| `MetricsPort` | `int` | `0` (off) | serve Prometheus metrics on this port. |
| `MetricsBind` | `string` | `0.0.0.0` | interface for the metrics port. |

`w.Name()` returns the effective name.

### The name

The name is how a worker owns its in-flight runs in the database
(`claimed_by`). Two rules:

- **Unique among running workers.** Two workers with the same name at the
  same time will interfere with each other's shutdown requeueing.
- **Stable across restarts of the same process.** Catchup (below) matches on
  it. A deploy that restarts one worker process keeps its name; a fleet of
  distinct processes uses distinct names.

The default is the hostname, which satisfies both rules for a one-process-per
host fleet.

## Catchup

At startup, before the first poll, the worker re-claims its own
`running` rows — the ones where `claimed_by` is its name and no other worker
took them. This covers a crash or kill -9 that left runs half-executed:
they come back to the owner as `scheduled` and are picked up like any other
work.

After a clean shutdown this normally finds nothing, because in-flight runs
are requeued before the worker exits (below).

## Shutdown and draining

`Run(ctx)` returns when `ctx` is cancelled. The sequence:

1. **Stop claiming.** The poller stops issuing claims.
2. **Drain.** The worker sets a draining flag. Each in-flight run: the step
   currently executing finishes and is recorded; no new step starts — the
   runner checks the flag at every step boundary and raises
   `*es.DrainOrphan` to stop the body cleanly.
3. **Requeue.** At the next step boundary the run is returned to
   `scheduled` and its claim released. Any worker can resume it; recorded
   steps mean it picks up exactly where it stopped.
4. **Wait.** The worker waits up to `Drain` (default 30s) for in-flight runs
   to reach a boundary.
5. **Requeue stragglers.** Anything still in flight at the deadline is
   requeued so it is not lost.

A run that reaches a step boundary is requeued *before* the deadline, so a
well-behaved workload drains fast. The deadline is a backstop for a step that
is itself long-running — the in-flight step is allowed to finish, then the
run is requeued.

A late failure of a run that was requeued at the deadline is guarded on
`claimed_by`, so it cannot mark the new owner's run as failed.

### Embedded in a server

The host owns the signal handling:

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
defer stop()

w := worker.New(worker.Opts{DB: db, Name: "provisioner"})
if err := w.Run(ctx); err != nil {
    log.Fatal(err)
}
```

`Run` blocks until `ctx` is done and the drain completes, then returns.
`nil` is a clean shutdown; a non-nil return is an operational error.

### Standalone worker process

```go
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	gormpg "gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/brutasse/everystep-go/store"
	"github.com/brutasse/everystep-go/worker"
)

func main() {
	db, err := gorm.Open(gormpg.Open("postgres://user:pass@localhost:5432/app?sslmode=disable"), &gorm.Config{
		TranslateError: true,
		Logger:         gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := store.Migrate(context.Background(), db); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	w := worker.New(worker.Opts{DB: db, Name: "provisioner", MetricsPort: 9090})
	log.Fatal(w.Run(ctx))
}
```

`store.Migrate` is idempotent — the host owns the schema, and the worker
re-running it at startup is a no-op once the tables exist.

### Metrics port

With `MetricsPort` set, the worker serves its own `/metrics` on
`MetricsBind:MetricsPort` (Prometheus text format, queue gauges refreshed per
scrape). See [observability](observability.md).
