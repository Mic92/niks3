package api

import "time"

// GCStats contains statistics about a garbage collection run.
type GCStats struct {
	// FailedUploadsDeleted is the number of failed/incomplete uploads cleaned up
	FailedUploadsDeleted int `json:"failed_uploads_deleted"`

	// OldClosuresDeleted is the number of closures older than the threshold that were deleted
	OldClosuresDeleted int `json:"old_closures_deleted"`

	// ObjectsMarkedForDeletion is the number of unreachable objects marked as deleted
	ObjectsMarkedForDeletion int `json:"objects_marked_for_deletion"`

	// ObjectsDeletedAfterGracePeriod is the number of objects actually removed from S3 and database after the grace period
	ObjectsDeletedAfterGracePeriod int `json:"objects_deleted_after_grace_period"`

	// ObjectsFailedToDelete is the number of objects that couldn't be deleted from S3 and were marked active again
	ObjectsFailedToDelete int `json:"objects_failed_to_delete"`
}

type GCTaskState string

const (
	GCTaskStateRunning   GCTaskState = "running"
	GCTaskStateSucceeded GCTaskState = "succeeded"
	GCTaskStateFailed    GCTaskState = "failed"
)

type GCTaskPhase string

const (
	GCTaskPhaseCleanupPendingUploads GCTaskPhase = "cleanup_pending_uploads"
	GCTaskPhaseCleanupOldClosures    GCTaskPhase = "cleanup_old_closures"
	GCTaskPhaseCleanupOrphanObjects  GCTaskPhase = "cleanup_orphan_objects"
	GCTaskPhaseVacuumTables          GCTaskPhase = "vacuum_tables"
)

// GCTaskParams captures the normalized request parameters for a GC run.
// Struct equality is used by the task store to deduplicate identical requests.
type GCTaskParams struct {
	OlderThan              string `json:"older_than"`
	FailedUploadsOlderThan string `json:"failed_uploads_older_than"`
	Force                  bool   `json:"force"`
}

// GCTaskStatus is the full snapshot of a GC task, returned by both the
// start endpoint (202 Accepted) and the polling endpoint (200 OK).
type GCTaskStatus struct {
	State      GCTaskState  `json:"state"`
	Phase      GCTaskPhase  `json:"phase,omitempty"`
	Params     GCTaskParams `json:"params"`
	Stats      GCStats      `json:"stats"`
	Error      string       `json:"error,omitempty"`
	StartedAt  time.Time    `json:"started_at"`
	UpdatedAt  time.Time    `json:"updated_at"`
	FinishedAt *time.Time   `json:"finished_at,omitempty"`
}

// GCConflictResponse is returned with 409 Conflict when a different GC task
// is already active.
type GCConflictResponse struct {
	Error      string       `json:"error"`
	ActiveTask GCTaskStatus `json:"active_task"`
}
