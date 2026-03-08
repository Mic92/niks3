package server_test

import (
	"testing"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server"
)

func TestGCTaskStore_StartNew(t *testing.T) {
	t.Parallel()

	store := server.NewGCTaskStore()
	params := api.GCTaskParams{
		OlderThan:              "720h",
		FailedUploadsOlderThan: "6h",
		Force:                  false,
	}

	result := store.Start(params)

	if !result.IsNew {
		t.Fatal("expected new task")
	}

	if result.Conflict {
		t.Fatal("expected no conflict")
	}

	if result.Status.State != api.GCTaskStateRunning {
		t.Errorf("expected running state, got %s", result.Status.State)
	}

	if result.Status.Params != params {
		t.Errorf("expected params %+v, got %+v", params, result.Status.Params)
	}
}

func TestGCTaskStore_DeduplicateSameParams(t *testing.T) {
	t.Parallel()

	store := server.NewGCTaskStore()
	params := api.GCTaskParams{
		OlderThan:              "720h",
		FailedUploadsOlderThan: "6h",
		Force:                  false,
	}

	first := store.Start(params)
	if !first.IsNew {
		t.Fatal("expected first start to be new")
	}

	second := store.Start(params)
	if second.IsNew {
		t.Fatal("expected second start to be deduplicated")
	}

	if second.Conflict {
		t.Fatal("expected no conflict for same params")
	}
}

func TestGCTaskStore_ConflictDifferentParams(t *testing.T) {
	t.Parallel()

	store := server.NewGCTaskStore()
	params1 := api.GCTaskParams{
		OlderThan:              "720h",
		FailedUploadsOlderThan: "6h",
		Force:                  false,
	}
	params2 := api.GCTaskParams{
		OlderThan:              "24h",
		FailedUploadsOlderThan: "1h",
		Force:                  true,
	}

	first := store.Start(params1)
	if !first.IsNew {
		t.Fatal("expected first start to be new")
	}

	second := store.Start(params2)
	if second.IsNew {
		t.Fatal("expected conflict, not new task")
	}

	if !second.Conflict {
		t.Fatal("expected conflict for different params")
	}
}

func TestGCTaskStore_GetEmpty(t *testing.T) {
	t.Parallel()

	store := server.NewGCTaskStore()

	_, ok := store.Get()
	if ok {
		t.Fatal("expected no task in fresh store")
	}
}

func TestGCTaskStore_GetReturnsLatest(t *testing.T) {
	t.Parallel()

	store := server.NewGCTaskStore()
	params := api.GCTaskParams{OlderThan: "1h", FailedUploadsOlderThan: "1h"}

	result := store.Start(params)

	status, ok := store.Get()
	if !ok {
		t.Fatal("expected to find task")
	}

	if status.State != api.GCTaskStateRunning {
		t.Errorf("expected running state, got %s", status.State)
	}

	result.Task.TestSucceed(api.GCStats{FailedUploadsDeleted: 5})

	status, _ = store.Get()
	if status.State != api.GCTaskStateSucceeded {
		t.Errorf("expected succeeded state, got %s", status.State)
	}

	if status.Stats.FailedUploadsDeleted != 5 {
		t.Errorf("expected FailedUploadsDeleted=5, got %d", status.Stats.FailedUploadsDeleted)
	}
}

func TestGCTaskStore_CompletedAllowsNewTask(t *testing.T) {
	t.Parallel()

	store := server.NewGCTaskStore()
	params := api.GCTaskParams{OlderThan: "1h", FailedUploadsOlderThan: "1h"}

	first := store.Start(params)
	first.Task.TestSucceed(api.GCStats{FailedUploadsDeleted: 5})

	params2 := api.GCTaskParams{OlderThan: "24h", FailedUploadsOlderThan: "6h"}
	second := store.Start(params2)

	if !second.IsNew {
		t.Fatal("expected new task after previous one completed")
	}
}

func TestGCTaskStore_PhaseUpdates(t *testing.T) {
	t.Parallel()

	store := server.NewGCTaskStore()
	params := api.GCTaskParams{OlderThan: "1h", FailedUploadsOlderThan: "1h"}
	result := store.Start(params)
	task := result.Task

	task.TestSetPhase(api.GCTaskPhaseCleanupPendingUploads)
	s, _ := store.Get()

	if s.Phase != api.GCTaskPhaseCleanupPendingUploads {
		t.Errorf("expected phase %s, got %s", api.GCTaskPhaseCleanupPendingUploads, s.Phase)
	}

	task.TestUpdateStats(api.GCStats{FailedUploadsDeleted: 3})
	s, _ = store.Get()

	if s.Stats.FailedUploadsDeleted != 3 {
		t.Errorf("expected FailedUploadsDeleted=3, got %d", s.Stats.FailedUploadsDeleted)
	}

	task.TestSetPhase(api.GCTaskPhaseCleanupOrphanObjects)
	s, _ = store.Get()

	if s.Phase != api.GCTaskPhaseCleanupOrphanObjects {
		t.Errorf("expected phase %s, got %s", api.GCTaskPhaseCleanupOrphanObjects, s.Phase)
	}
}

func TestGCTaskStore_Fail(t *testing.T) {
	t.Parallel()

	store := server.NewGCTaskStore()
	params := api.GCTaskParams{OlderThan: "1h", FailedUploadsOlderThan: "1h"}
	result := store.Start(params)

	partialStats := api.GCStats{FailedUploadsDeleted: 2, OldClosuresDeleted: 1}
	result.Task.TestFail(partialStats, "S3 connection refused")

	s, ok := store.Get()
	if !ok {
		t.Fatal("expected to find failed task")
	}

	if s.State != api.GCTaskStateFailed {
		t.Errorf("expected failed state, got %s", s.State)
	}

	if s.Error != "S3 connection refused" {
		t.Errorf("expected error message, got %q", s.Error)
	}

	if s.Stats.FailedUploadsDeleted != 2 || s.Stats.OldClosuresDeleted != 1 {
		t.Errorf("expected partial stats preserved, got %+v", s.Stats)
	}

	if s.FinishedAt == nil {
		t.Error("expected FinishedAt to be set")
	}
}
