package server_test

import (
	"testing"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server"
)

// Another process holding the advisory lock (but no row yet visible) must
// still be reported as a conflict rather than starting a second GC.
func TestGCAdvisoryLockBlocksConcurrentRun(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	conn, err := service.Pool.Acquire(ctx)
	ok(t, err)

	defer conn.Release()

	var acquired bool

	ok(t, conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", server.GCAdvisoryLockKey).Scan(&acquired))

	if !acquired {
		t.Fatal("expected to acquire GC advisory lock for the simulated peer")
	}

	defer func() {
		_, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", server.GCAdvisoryLockKey)
		ok(t, err)
	}()

	_, err = service.Pool.Exec(ctx, `INSERT INTO gc_runs (state, params) VALUES ('running', '{"older_than":"1h","failed_uploads_older_than":"","force":false}')`)
	ok(t, err)

	res, err := service.GCTasks.Start(ctx, api.GCTaskParams{OlderThan: "2h"})
	ok(t, err)

	if res.IsNew || !res.Conflict {
		t.Fatalf("expected conflict while lock is held elsewhere, got %+v", res)
	}

	res, err = service.GCTasks.Start(ctx, api.GCTaskParams{OlderThan: "1h"})
	ok(t, err)

	if res.IsNew || res.Conflict {
		t.Fatalf("expected join for identical params, got %+v", res)
	}
}
