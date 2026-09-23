package client

import (
	"context"
	"log/slog"
	"net/http"
)

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
func (c *Client) CreatePendingClosure(ctx context.Context, closure string, objects []ObjectWithRefs, verifyS3 bool) (*CreatePendingClosureResponse, error) {
	reqURL := c.baseURL.JoinPath("api/pending_closures")

	reqBody := createPendingClosureRequest{
		Closure:  closure,
		Objects:  objects,
		VerifyS3: verifyS3,
	}

	var result CreatePendingClosureResponse
	if err := c.doJSONRequest(ctx, http.MethodPost, reqURL.String(), reqBody, &result, http.StatusOK, http.StatusCreated); err != nil {
		return nil, err
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
	References  []string `json:"references"`  // Store paths (with /nix/store prefix)
	Deriver     *string  `json:"deriver,omitempty"`
	Signatures  []string `json:"signatures,omitempty"`
	CA          *string  `json:"ca,omitempty"`
}

// CompletePendingClosure marks a closure or push (route) as complete after all objects have been uploaded.
// This should be called after narinfos have been signed and uploaded.
func (c *Client) CompletePendingClosure(ctx context.Context, route, closureID string) error {
	reqURL := c.baseURL.JoinPath("api", route, closureID, "complete")

	if err := c.doJSONRequest(ctx, http.MethodPost, reqURL.String(), nil, nil, http.StatusOK, http.StatusNoContent); err != nil {
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
func (c *Client) SignPendingClosure(ctx context.Context, route, closureID string, narinfos map[string]NarinfoMetadata) (map[string][]string, error) {
	reqURL := c.baseURL.JoinPath("api", route, closureID, "sign")

	reqBody := signNarinfosRequest{
		Narinfos: narinfos,
	}

	var result signNarinfosResponse
	if err := c.doJSONRequest(ctx, http.MethodPost, reqURL.String(), reqBody, &result, http.StatusOK); err != nil {
		return nil, err
	}

	slog.Debug("Signed narinfos", "id", closureID, "count", len(result.Signatures))

	return result.Signatures, nil
}

type createPushRequest struct {
	Roots    []string         `json:"roots"`
	Objects  []ObjectWithRefs `json:"objects"`
	VerifyS3 bool             `json:"verify_s3,omitempty"`
}

// CreatePush registers all roots in one request and returns the objects to
// upload. Every object is sent once, however many roots reach it.
func (c *Client) CreatePush(ctx context.Context, closures []ClosureInfo, verifyS3 bool) (*CreatePendingClosureResponse, error) {
	req := createPushRequest{VerifyS3: verifyS3}
	seen := make(map[string]bool)

	for _, closure := range closures {
		req.Roots = append(req.Roots, closure.NarinfoKey)

		for _, obj := range closure.Objects {
			if !seen[obj.Key] {
				seen[obj.Key] = true
				req.Objects = append(req.Objects, obj)
			}
		}
	}

	var result CreatePendingClosureResponse
	if err := c.doJSONRequest(ctx, http.MethodPost, c.baseURL.JoinPath("api/pushes").String(), req, &result, http.StatusOK); err != nil {
		return nil, err
	}

	slog.Debug("Created push", "id", result.ID, "roots", len(req.Roots), "pending_objects", len(result.PendingObjects))

	return &result, nil
}
