package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// DefaultPendingClosureConflictRetry returns the default retry config used
// for HTTP 409 responses from POST /api/pending_closures. The server holds a
// per-NAR exclusive lock for multipartLockWindow (10 minutes) so a parallel
// closure targeting the same NAR sees 409 until the holder finishes or the
// lock ages out. These settings let us poll across that whole window with
// gentle pacing: ~30s steady-state delay after the first few attempts,
// totalling roughly 15 minutes of patience before giving up.
func DefaultPendingClosureConflictRetry() RetryConfig {
	return RetryConfig{
		MaxRetries:     32,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2.0,
		Jitter:         0.2,
	}
}

// createPendingClosureRequest is the request to create a pending closure.
type createPendingClosureRequest struct {
	Closure  string           `json:"closure"`
	Objects  []ObjectWithRefs `json:"objects"`
	VerifyS3 bool             `json:"verify_s3,omitempty"`
}

// PendingObject contains upload information for an object.
type PendingObject struct {
	Type          string               `json:"type"`                     // Object type (narinfo, listing, build_log, nar)
	PresignedURL  string               `json:"presigned_url,omitempty"`  // For small files
	MultipartInfo *MultipartUploadInfo `json:"multipart_info,omitempty"` // For large files
}

// CreatePendingClosureResponse is the response from creating a pending closure.
type CreatePendingClosureResponse struct {
	ID             string                   `json:"id"`
	StartedAt      string                   `json:"started_at"`
	PendingObjects map[string]PendingObject `json:"pending_objects"`
}

// CreatePendingClosure creates a pending closure and returns upload URLs.
//
// HTTP 409 from the server signals that another pending_closure is still
// uploading the same NAR (the multipart upload is treated as an exclusive
// lock; see server/pending_multipart.go for the rationale). We back off and
// retry with exponential delay so the caller does not have to know about
// the lock. Either the holder completes and GetExistingObjects short-circuits
// the NAR on the next attempt, or the lock ages out past multipartLockWindow
// and a fresh upload can open.
func (c *Client) CreatePendingClosure(ctx context.Context, closure string, objects []ObjectWithRefs, verifyS3 bool) (*CreatePendingClosureResponse, error) {
	reqURL := c.baseURL.JoinPath("api/pending_closures")

	reqBody := createPendingClosureRequest{
		Closure:  closure,
		Objects:  objects,
		VerifyS3: verifyS3,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	conflictRetry := c.ConflictRetry
	if conflictRetry.MaxRetries < 0 {
		conflictRetry.MaxRetries = 0
	}

	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), bytes.NewReader(jsonData))
		if err != nil {
			return nil, fmt.Errorf("creating request: %w", err)
		}

		req.Header.Set("Content-Type", "application/json")

		resp, err := c.DoServerRequest(ctx, req) //nolint:bodyclose // closed in decodePendingClosure on the success path, closeResponseBody on the 409 retry path
		if err != nil {
			return nil, fmt.Errorf("sending request: %w", err)
		}

		if resp.StatusCode == http.StatusConflict && attempt < conflictRetry.MaxRetries {
			backoff := conflictRetry.calculateBackoff(attempt)
			if ra := retryAfterDuration(resp); ra > backoff {
				backoff = ra
			}

			closeResponseBody(resp.Body)

			slog.Warn("Server reported duplicate multipart upload, retrying",
				"closure", closure,
				"attempt", attempt+1,
				"max_attempts", conflictRetry.MaxRetries+1,
				"backoff", backoff)

			if backoff > 0 {
				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("context canceled during 409 backoff: %w", ctx.Err())
				case <-time.After(backoff):
				}
			}

			continue
		}

		result, err := decodePendingClosure(resp)
		if err != nil {
			return nil, err
		}

		slog.Debug("Created pending closure", "id", result.ID, "pending_objects", len(result.PendingObjects))

		return result, nil
	}
}

func decodePendingClosure(resp *http.Response) (*CreatePendingClosureResponse, error) {
	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK, http.StatusCreated); err != nil {
		return nil, err
	}

	var result CreatePendingClosureResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &result, nil
}

// NarinfoMetadata contains metadata for a narinfo file to be signed by the server.
type NarinfoMetadata struct {
	StorePath   string   `json:"store_path"`
	URL         string   `json:"url"`         // e.g., "nar/xxxxx.nar.zst"
	Compression string   `json:"compression"` // e.g., "zstd"
	NarHash     string   `json:"nar_hash"`    // e.g., "sha256:xxxxx"
	NarSize     uint64   `json:"nar_size"`    // Uncompressed NAR size
	References  []string `json:"references"`  // Store paths (with /nix/store prefix)
	Deriver     *string  `json:"deriver,omitempty"`
	Signatures  []string `json:"signatures,omitempty"`
	CA          *string  `json:"ca,omitempty"`
}

// CompletePendingClosure marks a closure as complete after all objects have been uploaded.
// This should be called after narinfos have been signed and uploaded.
func (c *Client) CompletePendingClosure(ctx context.Context, closureID string) error {
	reqURL := c.baseURL.JoinPath("api/pending_closures", closureID, "complete")

	// Empty request body
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), http.NoBody)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.DoServerRequest(ctx, req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK, http.StatusNoContent); err != nil {
		return err
	}

	slog.Debug("Completed pending closure", "id", closureID)

	return nil
}

type signNarinfosRequest struct {
	Narinfos map[string]NarinfoMetadata `json:"narinfos"`
}

type signNarinfosResponse struct {
	Signatures map[string][]string `json:"signatures"`
}

// SignPendingClosure sends narinfo metadata to the server for signing and returns signatures.
func (c *Client) SignPendingClosure(ctx context.Context, closureID string, narinfos map[string]NarinfoMetadata) (map[string][]string, error) {
	reqURL := c.baseURL.JoinPath("api/pending_closures", closureID, "sign")

	reqBody := signNarinfosRequest{
		Narinfos: narinfos,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.DoServerRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK); err != nil {
		return nil, err
	}

	var result signNarinfosResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	slog.Debug("Signed narinfos", "id", closureID, "count", len(result.Signatures))

	return result.Signatures, nil
}
