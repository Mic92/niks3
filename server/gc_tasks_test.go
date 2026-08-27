package server_test

import (
	"testing"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server"
)

func TestGCTaskStore(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	// Two stores on one database stand in for two replicas.
	a := server.NewGCTaskStore(service.Pool)
	b := server.NewGCTaskStore(service.Pool)

	if _, found, err := a.Get(ctx); err != nil || found {
		t.Fatalf("fresh store: found=%v err=%v", found, err)
	}

	params := api.GCTaskParams{OlderThan: "720h", FailedUploadsOlderThan: "6h"}

	first, err := a.Start(ctx, params)
	ok(t, err)

	if !first.IsNew || first.Conflict || first.Status.State != api.GCTaskStateRunning || first.Status.Params != params {
		t.Fatalf("first start: %+v", first)
	}

	same, err := b.Start(ctx, params)
	ok(t, err)

	if same.IsNew || same.Conflict {
		t.Fatalf("same params on other replica should join: %+v", same)
	}

	other, err := b.Start(ctx, api.GCTaskParams{OlderThan: "1h", FailedUploadsOlderThan: "1h", Force: true})
	ok(t, err)

	if !other.Conflict {
		t.Fatalf("different params should conflict: %+v", other)
	}

	first.Task.TestSetPhase(api.GCTaskPhaseCleanupOrphanObjects)
	first.Task.TestUpdateStats(api.GCStats{FailedUploadsDeleted: 3})

	st, found, err := b.Get(ctx)
	ok(t, err)

	if !found || st.Phase != api.GCTaskPhaseCleanupOrphanObjects || st.Stats.FailedUploadsDeleted != 3 {
		t.Fatalf("progress not visible from other replica: %+v", st)
	}

	first.Task.TestFail(api.GCStats{FailedUploadsDeleted: 3, OldClosuresDeleted: 1}, "S3 connection refused")

	st, _, err = b.Get(ctx)
	ok(t, err)

	if st.State != api.GCTaskStateFailed || st.Error != "S3 connection refused" || st.FinishedAt == nil || st.Stats.OldClosuresDeleted != 1 {
		t.Fatalf("failed state not recorded: %+v", st)
	}

	next, err := b.Start(ctx, api.GCTaskParams{OlderThan: "24h", FailedUploadsOlderThan: "6h"})
	ok(t, err)

	if !next.IsNew {
		t.Fatalf("expected new task after previous finished: %+v", next)
	}

	next.Task.TestSucceed(api.GCStats{})

	st, _, err = a.Get(ctx)
	ok(t, err)

	if st.State != api.GCTaskStateSucceeded {
		t.Fatalf("succeeded state not recorded: %+v", st)
	}
}

// A "running" row whose process died must not block GC forever.
func TestGCTaskStoreInterruptedRun(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	_, err := service.Pool.Exec(ctx, `INSERT INTO gc_runs (state, params) VALUES ('running', '{}')`)
	ok(t, err)

	res, err := service.GCTasks.Start(ctx, api.GCTaskParams{OlderThan: "1h"})
	ok(t, err)

	if !res.IsNew {
		t.Fatalf("stale running row blocked new GC: %+v", res)
	}

	defer res.Task.TestSucceed(api.GCStats{})

	var interrupted int

	ok(t, service.Pool.QueryRow(ctx, `SELECT count(*) FROM gc_runs WHERE error = 'interrupted'`).Scan(&interrupted))

	if interrupted != 1 {
		t.Fatalf("interrupted rows = %d", interrupted)
	}
}
