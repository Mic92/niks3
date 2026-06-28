package server_test

import (
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server"
)

// TestGCAdvisoryLockBlocksConcurrentRun checks GC fails fast when another
// instance already holds the advisory lock.
//
//nolint:paralleltest // goose globals in pg.Connect race when createTestService runs in parallel
func TestGCAdvisoryLockBlocksConcurrentRun(t *testing.T) {
	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	// Hold the lock on a dedicated connection to simulate another instance.
	conn, err := service.Pool.Acquire(ctx)
	ok(t, err)

	defer conn.Release()

	var acquired bool

	err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", server.GCAdvisoryLockKey).Scan(&acquired)
	ok(t, err)

	if !acquired {
		t.Fatal("expected to acquire GC advisory lock for the simulated peer")
	}

	defer func() {
		_, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", server.GCAdvisoryLockKey)
		ok(t, err)
	}()

	// GC must not proceed while the lock is held elsewhere.
	status := service.RunGCForTest(720*time.Hour, 6*time.Hour, false)

	if status.State != api.GCTaskStateFailed {
		t.Fatalf("expected GC to fail while advisory lock is held, got state %q", status.State)
	}

	if !strings.Contains(status.Error, "already running") {
		t.Fatalf("expected lock-held error, got %q", status.Error)
	}
}
