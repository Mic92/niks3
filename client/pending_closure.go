package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
)

// createPendingClosureRequest is the request to create a pending closure.
type createPendingClosureRequest struct {
	Closure string           `json:"closure"`
	Objects []ObjectWithRefs `json:"objects"`
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
func (c *Client) CreatePendingClosure(ctx context.Context, closure string, objects []ObjectWithRefs) (*CreatePendingClosureResponse, error) {
	reqURL := c.baseURL.JoinPath("api/pending_closures")

	reqBody := createPendingClosureRequest{
		Closure: closure,
		Objects: objects,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.authToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK, http.StatusCreated); err != nil {
		return nil, err
	}

	var result CreatePendingClosureResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	slog.Debug("Created pending closure", "id", result.ID, "pending_objects", len(result.PendingObjects))

	return &result, nil
}

// NarinfoMetadata contains metadata for a narinfo file to be signed by the server.
type NarinfoMetadata struct {
	StorePath   string   `json:"store_path"`
	URL         string   `json:"url"`         // e.g., "nar/xxxxx.nar.zst"
	Compression string   `json:"compression"` // e.g., "zstd"
	NarHash     string   `json:"nar_hash"`    // e.g., "sha256:xxxxx"
	NarSize     uint64   `json:"nar_size"`    // Uncompressed NAR size
	FileHash    string   `json:"file_hash"`   // Hash of compressed file
	FileSize    uint64   `json:"file_size"`   // Size of compressed file
	References  []string `json:"references"`  // Store paths (with /nix/store prefix)
	Deriver     *string  `json:"deriver,omitempty"`
	Signatures  []string `json:"signatures,omitempty"`
	CA          *string  `json:"ca,omitempty"`
}

type commitPendingClosureRequest struct {
	Narinfos map[string]NarinfoMetadata `json:"narinfos"`
}

// CompletePendingClosure marks a closure as complete, sending narinfo metadata for server-side signing.
// The narinfos map can be empty when all objects already exist (deduplication).
func (c *Client) CompletePendingClosure(ctx context.Context, closureID string, narinfos map[string]NarinfoMetadata) error {
	reqURL := c.baseURL.JoinPath("api/pending_closures", closureID, "complete")

	reqBody := commitPendingClosureRequest{
		Narinfos: narinfos,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshaling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.authToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK, http.StatusNoContent); err != nil {
		return err
	}

	slog.Debug("Completed pending closure", "id", closureID, "narinfos", len(narinfos))

	return nil
}
