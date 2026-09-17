# everystep-go

Durable workflow execution in Go. A workflow is an ordinary Go function;
every step it calls is recorded in SQL, so a run resumes where it left off
after any crash — and a step marked unsafe to repeat can never run its
effect twice.

Targets PostgreSQL and MariaDB. Go 1.25.

## Quickstart

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"

	gormpg "gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	es "github.com/brutasse/everystep-go/everystep"
	"github.com/brutasse/everystep-go/store"
	"github.com/brutasse/everystep-go/worker"
)

type CreateVMArgs struct {
	Name string
	Size string
}

// A step is a named function with typed args and result.
var createVM = es.Step("CreateVM", func(ctx context.Context, a CreateVMArgs) (string, error) {
	return cloud.CreateVM(ctx, a.Name, a.Size) // your side effect
}, es.UnsafeToRepeat()) // the effect must not run twice

var tagVM = es.Step("TagVM", func(ctx context.Context, vmID string) (string, error) {
	return cloud.TagVM(ctx, vmID, "ready")
})

// A workflow is a body function that calls steps in order.
func provisionBody(ctx context.Context, a CreateVMArgs) (string, error) {
	vmID, err := createVM.Call(ctx, a)
	if err != nil {
		return "", err
	}
	return tagVM.Call(ctx, vmID)
}

var provisionVM = es.Workflow("provision_vm", provisionBody)

func main() {
	// TranslateError must be on: idempotent scheduling matches on the
	// driver's duplicate-key error.
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

	// Schedule a run. Pass a tx from Begin() to make it transactional.
	runID, err := provisionVM.Schedule(db, CreateVMArgs{Name: "web-1", Size: "m"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("scheduled run", runID)

	// A worker claims and executes scheduled runs until the process stops.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	w := worker.New(worker.Opts{DB: db, Name: "provisioner"})
	if err := w.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

MariaDB: `gorm.io/driver/mysql` with a `mysql://` DSN; everything else is
identical.

## How it works

- The body re-runs from the top on every claim. Recorded steps are served
  from SQL; the first unrecorded step runs.
- Step identity is a dotpath derived from the body's structure
  (`2.0.1` = step 1 of branch 0 of fork 2), so it survives edits that do
  not rename.
- Steps run at-least-once by default. `es.UnsafeToRepeat()` marks a step
  whose effect must not run twice: the attempt is claimed in the database
  before the effect, and an unrecorded attempt blocks the run instead of
  re-running the effect.
- A blocked run waits for a human: `es.ResolveStep(ctx, db, runID, stepID,
  es.WithResult(v) | es.WithError(msg) | es.Discard())`.
- `*es.Terminal` stops a run for a known reason without being a failure
  (status `stopped`).
- A self-contained UI (runs, live stream, per-run DAG) ships as an
  `http.Handler`.

## Docs

Published at <https://brutasse.github.io/everystep-go/>.

## Status

Pre-1.0: the API can still move.

## Tests

`go test ./...` — the suite starts a throwaway `postgres:16` and `mariadb:11`
(testcontainers) and runs every test against both.
