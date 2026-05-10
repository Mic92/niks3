package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Mic92/niks3/api"
)

const gcPollInterval = 2 * time.Second

// GCConflictError is returned when a different GC task is already running.
type GCConflictError struct {
	ActiveTask api.GCTaskStatus
	Message    string
}

func (e *GCConflictError) Error() string {
	return fmt.Sprintf("%s (phase: %s)", e.Message, e.ActiveTask.Phase)
}

// StartGarbageCollection initiates a GC task on the server.
// Returns the task status on 202 Accepted (new or deduplicated).
// Returns a *GCConflictError if a different GC is already active (409).
func (c *Client) StartGarbageCollection(ctx context.Context, olderThan string, failedUploadsOlderThan string, force bool) (*api.GCTaskStatus, error) {
	gcURL := c.baseURL.JoinPath("/api/closures")
	query := gcURL.Query()
	query.Set("older-than", olderThan)

	if failedUploadsOlderThan != "" {
		query.Set("failed-uploads-older-than", failedUploadsOlderThan)
	}

	if force {
		query.Set("force", "true")
	}

	gcURL.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, gcURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.DoServerRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer deferCloseBody(resp)

	if resp.StatusCode == http.StatusConflict {
		var conflict api.GCConflictResponse
		if err := json.NewDecoder(resp.Body).Decode(&conflict); err != nil {
			return nil, fmt.Errorf("garbage collection conflict (failed to parse response): %w", err)
		}

		return nil, &GCConflictError{
			ActiveTask: conflict.ActiveTask,
			Message:    conflict.Error,
		}
	}

	if err := checkResponse(resp, http.StatusAccepted); err != nil {
		return nil, fmt.Errorf("starting garbage collection: %w", err)
	}

	var status api.GCTaskStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	return &status, nil
}

// GetGCStatus retrieves the current GC task status from the server.
func (c *Client) GetGCStatus(ctx context.Context) (*api.GCTaskStatus, error) {
	taskURL := c.baseURL.JoinPath("/api/gc/status")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, taskURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.DoServerRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("getting gc status: %w", err)
	}

	var status api.GCTaskStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	return &status, nil
}

// RunGarbageCollection starts a GC task and polls until it completes.
// This is the high-level convenience method used by the CLI.
func (c *Client) RunGarbageCollection(ctx context.Context, olderThan string, failedUploadsOlderThan string, force bool) (*api.GCStats, error) {
	status, err := c.StartGarbageCollection(ctx, olderThan, failedUploadsOlderThan, force)
	if err != nil {
		return nil, err
	}

	slog.Info("Garbage collection started")

	lastPhase := status.Phase
	lastStats := status.Stats

	for status.State == api.GCTaskStateRunning {
		select {
		case <-ctx.Done():
			return nil, ctx.Err() //nolint:wrapcheck // ctx.Err() is the canonical sentinel for cancellation
		case <-time.After(gcPollInterval):
		}

		status, err = c.GetGCStatus(ctx)
		if err != nil {
			return nil, fmt.Errorf("polling gc status: %w", err)
		}

		if status.Phase != lastPhase || status.Stats != lastStats {
			slog.Info(
				"Garbage collection progress",
				"phase", status.Phase,
				"failed_uploads_deleted", status.Stats.FailedUploadsDeleted,
				"old_closures_deleted", status.Stats.OldClosuresDeleted,
				"objects_marked", status.Stats.ObjectsMarkedForDeletion,
				"objects_deleted", status.Stats.ObjectsDeletedAfterGracePeriod,
				"objects_failed", status.Stats.ObjectsFailedToDelete,
			)
			lastPhase = status.Phase
			lastStats = status.Stats
		}
	}

	if status.State == api.GCTaskStateFailed {
		return &status.Stats, fmt.Errorf("garbage collection failed: %s", status.Error)
	}

	return &status.Stats, nil
}
