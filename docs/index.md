# everystep

Durable workflow execution in Go. A workflow is an ordinary Go function;
every step it calls is recorded in SQL, so a run resumes where it left off
after any crash — and a step marked unsafe to repeat can never run its
effect twice.

Targets PostgreSQL and MariaDB. Go 1.25. Pre-1.0: the API can still move.

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

## The docs

- [The execution model](model.md) — replay, step identity, unsafe-to-repeat, blocked runs.
- [Workers](workers.md) — the claim loop, drain, catchup, standalone worker.
- [Errors](errors.md) — error replay, `Replayable`, built-in codes.
- [The UI and API](ui.md) — the UI/API handler and what the page shows.
- [Observability](observability.md) — metrics, traces, failure reporting.
