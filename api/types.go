package api

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
