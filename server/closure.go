package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/jackc/pgx/v5"
)

// GetClosureHandler handles the GET /closures/<key> endpoint.
func (s *Service) GetClosureHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received get closure request", "method", r.Method, "path", r.URL.Path)

	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)

		return
	}

	closure, err := getClosure(r.Context(), s.Pool, key)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "closure not found", http.StatusNotFound)

			return
		}

		http.Error(w, "failed to get closure objects: "+err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(closure)
	if err != nil {
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)

		return
	}
}

// CleanupClosuresOlder handles the DELETE /closures endpoint.
// It validates parameters, starts an asynchronous GC task, and returns
// 202 Accepted with the task status. Identical in-flight requests are
// deduplicated; conflicting ones receive 409 Conflict.
func (s *Service) CleanupClosuresOlder(w http.ResponseWriter, r *http.Request) {
	slog.Info("Starting cleanup of old closures", "method", r.Method, "path", r.URL.Path)

	olderThan := r.URL.Query().Get("older-than")
	if olderThan == "" {
		http.Error(w, "missing age", http.StatusBadRequest)

		return
	}

	age, err := time.ParseDuration(olderThan)
	if err != nil {
		http.Error(w, "failed to parse age: "+err.Error(), http.StatusBadRequest)

		return
	}

	if age < 0 {
		http.Error(w, "older-than must not be negative", http.StatusBadRequest)

		return
	}

	// Clean up pending closures first (failed/stale uploads)
	// Use separate timeout if provided, otherwise default to 6 hours
	// This is longer than presigned URL validity (5h) to avoid aborting active uploads
	failedUploadsOlderThan := r.URL.Query().Get("failed-uploads-older-than")
	if failedUploadsOlderThan == "" {
		// Fallback to old parameter name for backwards compatibility
		failedUploadsOlderThan = r.URL.Query().Get("pending-older-than")
	}

	if failedUploadsOlderThan == "" {
		failedUploadsOlderThan = "6h"
	}

	pendingAge, err := time.ParseDuration(failedUploadsOlderThan)
	if err != nil {
		http.Error(w, "failed to parse failed-uploads-older-than: "+err.Error(), http.StatusBadRequest)

		return
	}

	if pendingAge < 0 {
		http.Error(w, "failed-uploads-older-than must not be negative", http.StatusBadRequest)

		return
	}

	force := r.URL.Query().Get("force") == "true"

	params := api.GCTaskParams{
		OlderThan:              olderThan,
		FailedUploadsOlderThan: failedUploadsOlderThan,
		Force:                  force,
	}

	result := s.GCTasks.Start(params)

	if result.Conflict {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)

		_ = json.NewEncoder(w).Encode(api.GCConflictResponse{
			Error:      "a different garbage collection is already running",
			ActiveTask: result.Status,
		})

		return
	}

	if result.IsNew {
		go s.runGarbageCollection(result.Task, age, pendingAge, force)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(result.Status)
}

// runGarbageCollection executes the full GC sequence in a background goroutine,
// updating the task snapshot after each phase. It uses context.Background() so
// that client disconnects or reverse-proxy timeouts do not cancel the work.
func (s *Service) runGarbageCollection(task *gcTask, age, pendingAge time.Duration, force bool) {
	ctx := context.Background()
	stats := &api.GCStats{}

	task.setPhase(api.GCTaskPhaseCleanupPendingUploads)

	failedUploadsCount, err := s.cleanupPendingClosures(ctx, pendingAge)
	if err != nil {
		task.fail(*stats, "failed to cleanup pending closures: "+err.Error())

		return
	}

	stats.FailedUploadsDeleted = failedUploadsCount
	task.updateStats(*stats)

	task.setPhase(api.GCTaskPhaseCleanupOldClosures)

	oldClosuresCount, err := cleanupClosureOlderThan(ctx, s.Pool, age)
	if err != nil {
		task.fail(*stats, "failed to cleanup old closures: "+err.Error())

		return
	}

	stats.OldClosuresDeleted = oldClosuresCount
	task.updateStats(*stats)

	task.setPhase(api.GCTaskPhaseCleanupOrphanObjects)

	var gracePeriod int32
	if force {
		// Force mode: immediate deletion (grace period = 0)
		gracePeriod = 0

		slog.Warn("Force mode enabled - objects will be deleted immediately without grace period")
	} else {
		// Use same grace period for object cleanup as pending closure cleanup
		// This ensures no pending closure can resurrect an object being deleted
		gracePeriod = int32(pendingAge.Seconds())
	}

	objectStats, err := s.cleanupOrphanObjects(ctx, gracePeriod, func(live ObjectCleanupStats) {
		stats.ObjectsMarkedForDeletion = live.MarkedCount
		stats.ObjectsDeletedAfterGracePeriod = live.DeletedCount
		stats.ObjectsFailedToDelete = live.FailedCount
		task.updateStats(*stats)
	})

	// Always reconcile final object stats from the returned value,
	// which includes counts from the last batch flush that may not
	// have triggered an onProgress callback.
	if objectStats != nil {
		stats.ObjectsMarkedForDeletion = objectStats.MarkedCount
		stats.ObjectsDeletedAfterGracePeriod = objectStats.DeletedCount
		stats.ObjectsFailedToDelete = objectStats.FailedCount
	}

	if err != nil {
		task.fail(*stats, "failed to cleanup orphan objects: "+err.Error())

		return
	}

	slog.Info(
		"Garbage collection completed",
		"failed-uploads-deleted", stats.FailedUploadsDeleted,
		"old-closures-deleted", stats.OldClosuresDeleted,
		"objects-marked-for-deletion", stats.ObjectsMarkedForDeletion,
		"objects-deleted-after-grace-period", stats.ObjectsDeletedAfterGracePeriod,
		"objects-failed-to-delete", stats.ObjectsFailedToDelete,
	)

	task.setPhase(api.GCTaskPhaseVacuumTables)
	s.vacuumGCTables(ctx)

	task.succeed(*stats)
}

// GCStatusHandler handles GET /api/gc/status.
func (s *Service) GCStatusHandler(w http.ResponseWriter, r *http.Request) {
	status, ok := s.GCTasks.Get()
	if !ok {
		http.Error(w, "no garbage collection has run yet", http.StatusNotFound)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

// vacuumGCTables runs VACUUM ANALYZE on all tables modified during garbage collection.
// This reclaims space from deleted rows and updates query planner statistics.
// Failures are logged but don't cause the GC to fail.
func (s *Service) vacuumGCTables(ctx context.Context) {
	tables := []string{"pending_closures", "pending_objects", "multipart_uploads", "closures", "objects"}
	for _, table := range tables {
		if _, err := s.Pool.Exec(ctx, "VACUUM ANALYZE "+table); err != nil {
			// Log but don't fail - vacuum is nice to have but not critical
			slog.Warn("Failed to vacuum table", "table", table, "error", err)
		} else {
			slog.Info("Vacuumed table", "table", table)
		}
	}
}
