// Command rolloutworker is the subprocess entrypoint of the rollout E2E
// tests: a real everystep worker against the test database.
//
// Environment:
//
//	EVERYSTEP_E2E_DRIVER     "postgres" or "mysql"
//	EVERYSTEP_E2E_DSN        database DSN
//	EVERYSTEP_E2E_NAME       stable runner name
//	EVERYSTEP_E2E_DRAIN      drain deadline in seconds (default 2)
//	EVERYSTEP_E2E_STEP_SLEEP sleeper duration in seconds (default 0)
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	gormmysql "gorm.io/driver/mysql"
	gormpg "gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	es "github.com/brutasse/everystep-go/everystep"
	"github.com/brutasse/everystep-go/worker"
)

var e2eQuick = es.Step("e2e.quick", func(ctx context.Context, _ struct{}) (string, error) {
	return "q", nil
})

var e2eSleeper = es.Step("e2e.sleeper", func(ctx context.Context, _ struct{}) (string, error) {
	if n, _ := strconv.Atoi(os.Getenv("EVERYSTEP_E2E_STEP_SLEEP")); n > 0 {
		time.Sleep(time.Duration(n) * time.Second)
	}
	return "s", nil
})

var e2eFinish = es.Step("e2e.finish", func(ctx context.Context, _ struct{}) (string, error) {
	return "done", nil
})

var e2eSlowFlow = es.Workflow("e2e.slow_flow", func(ctx context.Context, _ struct{}) (string, error) {
	if _, err := e2eQuick.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	if _, err := e2eSleeper.Call(ctx, struct{}{}); err != nil {
		return "", err
	}
	return e2eFinish.Call(ctx, struct{}{})
})

func main() {
	driver := os.Getenv("EVERYSTEP_E2E_DRIVER")
	dsn := os.Getenv("EVERYSTEP_E2E_DSN")
	if driver == "" || dsn == "" {
		fmt.Fprintln(os.Stderr, "rolloutworker: EVERYSTEP_E2E_DRIVER and EVERYSTEP_E2E_DSN are required")
		os.Exit(2)
	}
	var dialector gorm.Dialector
	switch driver {
	case "postgres":
		dialector = gormpg.Open(dsn)
	case "mysql":
		dialector = gormmysql.Open(dsn)
	default:
		fmt.Fprintln(os.Stderr, "rolloutworker: unknown driver", driver)
		os.Exit(2)
	}
	db, err := gorm.Open(dialector, &gorm.Config{
		TranslateError: true,
		Logger:         gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "rolloutworker: cannot open database:", err)
		os.Exit(1)
	}
	drain := 2
	if s := os.Getenv("EVERYSTEP_E2E_DRAIN"); s != "" {
		if n, perr := strconv.Atoi(s); perr == nil {
			drain = n
		}
	}
	w := worker.New(worker.Opts{
		DB:    db,
		Poll:  100 * time.Millisecond,
		Name:  os.Getenv("EVERYSTEP_E2E_NAME"),
		Drain: time.Duration(drain) * time.Second,
	})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := w.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "rolloutworker:", err)
		os.Exit(1)
	}
}
