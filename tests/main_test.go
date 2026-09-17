// Package tests is the everystep test suite: the Python test matrix
// ported to Go, run against both PostgreSQL and MariaDB. TestMain starts a
// throwaway postgres:16 and a mariadb:11 container each, opens both through
// their gorm dialect, and every test runs against both backends (see
// eachBackend), which is what exercises the claim-SQL, locking-clause, and
// DDL divergence.
package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcppg "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	gormmysql "gorm.io/driver/mysql"
	gormpg "gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/brutasse/everystep-go/store"
)

// Backend is one database under test.
type Backend struct {
	Name string
	DSN  string
	DB   *gorm.DB
}

var (
	backends  []Backend
	stops     []func(context.Context)
	workerBin string
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	if err := startBackends(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "everystep tests: cannot start databases:", err)
		stopAll(ctx)
		os.Exit(1)
	}
	if err := buildWorkerBin(); err != nil {
		fmt.Fprintln(os.Stderr, "everystep tests: cannot build rolloutworker:", err)
		stopAll(ctx)
		os.Exit(1)
	}
	code := m.Run()
	stopAll(ctx)
	os.Exit(code)
}

func startBackends(ctx context.Context) error {
	pg, err := tcppg.Run(ctx, "postgres:16",
		tcppg.WithDatabase("everystep"),
		tcppg.WithUsername("everystep"),
		tcppg.WithPassword("everystep"))
	if err != nil {
		return fmt.Errorf("start postgres: %w", err)
	}
	port, err := pg.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return fmt.Errorf("postgres port: %w", err)
	}
	pgDSN := fmt.Sprintf("postgres://everystep:everystep@127.0.0.1:%s/everystep?sslmode=disable", port.Port())
	pgDB, err := openWithRetry(func() (*gorm.DB, error) {
		return gorm.Open(gormpg.Open(pgDSN), gormCfg())
	})
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	backends = append(backends, Backend{Name: "postgres", DSN: pgDSN, DB: pgDB})
	stops = append(stops, func(ctx context.Context) { _ = pg.Terminate(ctx) })
	if err := store.Migrate(ctx, pgDB); err != nil {
		return fmt.Errorf("migrate postgres: %w", err)
	}

	maria, err := testcontainers.Run(ctx, "mariadb:11",
		testcontainers.WithExposedPorts("3306/tcp"),
		testcontainers.WithEnv(map[string]string{
			"MARIADB_ROOT_PASSWORD": "everystep",
			"MARIADB_DATABASE":      "everystep",
		}),
		testcontainers.WithWaitStrategy(wait.ForLog("ready for connections")))
	if err != nil {
		return fmt.Errorf("start mariadb: %w", err)
	}
	port, err = maria.MappedPort(ctx, "3306/tcp")
	if err != nil {
		return fmt.Errorf("mariadb port: %w", err)
	}
	mariaDSN := fmt.Sprintf("root:everystep@tcp(127.0.0.1:%s)/everystep?parseTime=true", port.Port())
	mariaDB, err := openWithRetry(func() (*gorm.DB, error) {
		return gorm.Open(gormmysql.Open(mariaDSN), gormCfg())
	})
	if err != nil {
		return fmt.Errorf("open mariadb: %w", err)
	}
	backends = append(backends, Backend{Name: "mariadb", DSN: mariaDSN, DB: mariaDB})
	stops = append(stops, func(ctx context.Context) { _ = maria.Terminate(ctx) })
	if err := store.Migrate(ctx, mariaDB); err != nil {
		return fmt.Errorf("migrate mariadb: %w", err)
	}
	return nil
}

// openWithRetry opens the database, retrying until the server accepts
// connections: the container can report ready a beat before its entrypoint
// has finished bouncing the server into its final configuration.
func openWithRetry(open func() (*gorm.DB, error)) (*gorm.DB, error) {
	var lastErr error
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		db, err := open()
		if err == nil {
			var dbErr error
			var sqlDB *sql.DB
			sqlDB, dbErr = db.DB()
			if dbErr == nil && sqlDB.PingContext(context.Background()) == nil {
				return db, nil
			} else {
				lastErr = dbErr
			}
		} else {
			lastErr = err
		}
		time.Sleep(250 * time.Millisecond)
	}
	return nil, lastErr
}

func gormCfg() *gorm.Config {
	return &gorm.Config{
		// A unique violation on either backend surfaces as
		// gorm.ErrDuplicatedKey (23505 on PG, 1062 on MariaDB).
		TranslateError: true,
		Logger:         gormlogger.Default.LogMode(gormlogger.Silent),
	}
}

func stopAll(ctx context.Context) {
	for _, stop := range stops {
		stop(ctx)
	}
}

// buildWorkerBin compiles the rollout E2E entrypoint: a real worker
// subprocess the SIGTERM/SIGKILL tests drive.
func buildWorkerBin() error {
	dir, err := os.MkdirTemp("", "everystep-e2e")
	if err != nil {
		return err
	}
	workerBin = filepath.Join(dir, "rolloutworker")
	cmd := exec.Command("go", "build", "-o", workerBin, "github.com/brutasse/everystep-go/tests/rolloutworker")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v: %s", err, out)
	}
	return nil
}
