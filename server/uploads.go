package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Mic92/niks3/server/pg"
	"github.com/Mic92/niks3/server/signing"
	"github.com/minio/minio-go/v7"
)

type objectWithRefs struct {
	Key     string   `json:"key"`
	Type    string   `json:"type"`
	Refs    []string `json:"refs"`
	NarSize *uint64  `json:"nar_size,omitempty"` // For estimating multipart parts
}

type createPendingClosureRequest struct {
	Closure  *string          `json:"closure"`
	Objects  []objectWithRefs `json:"objects"`
	VerifyS3 bool             `json:"verify_s3,omitempty"`
}

// CreatePendingClosureHandler handles POST /pending_closures endpoint.
// Request body:
//
//	{
//	 "closure": "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo",
//	 "objects": [
//		 {"key": "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", "refs": ["nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.xz"]},
//		 {"key": "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.xz", "refs": []}
//	 ]
//	}
//
// Response body:
//
//	{
//	  "id": "1",
//	  "started_at": "2021-08-31T00:00:00Z",
//	  "pending_objects": {
//		  "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo": {"presigned_url": "https://yours3endpoint?authkey=..."},
//		  "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.xz": {"presigned_url": "https://yours3endpoint?authkey=..."}
//	   }
//	}
func (s *Service) CreatePendingClosureHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received uploads request", "method", r.Method, "url", r.URL)

	defer func() {
		if err := r.Body.Close(); err != nil {
			slog.Error("Failed to close request body", "error", err)
		}
	}()

	req := &createPendingClosureRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		http.Error(w, "failed to decode request: "+err.Error(), http.StatusBadRequest)

		return
	}

	if req.Closure == nil {
		http.Error(w, "missing closure key", http.StatusBadRequest)

		return
	}

	if !strings.HasSuffix(*req.Closure, ".narinfo") {
		http.Error(w, "closure key must end with .narinfo", http.StatusBadRequest)

		return
	}

	if len(req.Objects) == 0 {
		http.Error(w, "missing objects key", http.StatusBadRequest)

		return
	}

	objectsMap := make(map[string]objectWithRefs)
	for _, object := range req.Objects {
		objectsMap[object.Key] = object
	}

	upload, err := s.createPendingClosure(r.Context(), s.Pool, *req.Closure, objectsMap, req.VerifyS3)
	if err != nil {
		http.Error(w, "failed to start upload: "+err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(upload)
	if err != nil {
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)

		return
	}
}

type completedPart struct {
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
}

type completeMultipartRequest struct {
	ObjectKey string          `json:"object_key"`
	UploadID  string          `json:"upload_id"`
	Parts     []completedPart `json:"parts"`
}

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

// RequestMorePartsHandler handles POST /api/multipart/request-parts endpoint.
// Requests additional presigned URLs for an existing multipart upload.
func (s *Service) RequestMorePartsHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received request for more parts", "method", r.Method, "url", r.URL)

	defer func() {
		if err := r.Body.Close(); err != nil {
			slog.Error("Failed to close request body", "error", err)
		}
	}()

	req := &requestPartsRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		http.Error(w, "failed to decode request: "+err.Error(), http.StatusBadRequest)

		return
	}

	if req.ObjectKey == "" {
		http.Error(w, "missing object_key", http.StatusBadRequest)

		return
	}

	if req.UploadID == "" {
		http.Error(w, "missing upload_id", http.StatusBadRequest)

		return
	}

	if req.StartPartNumber <= 0 {
		http.Error(w, "start_part_number must be positive", http.StatusBadRequest)

		return
	}

	if req.NumParts <= 0 || req.NumParts > 1000 {
		http.Error(w, "num_parts must be between 1 and 1000", http.StatusBadRequest)

		return
	}

	// Validate that the end part number doesn't exceed S3's limit
	endPart := req.StartPartNumber + req.NumParts - 1
	if endPart > 10000 {
		http.Error(w, "requested parts exceed S3 maximum of 10000", http.StatusBadRequest)

		return
	}

	// Verify the upload exists and belongs to a valid pending closure
	queries := pg.New(s.Pool)

	upload, err := queries.GetMultipartUpload(r.Context(), pg.GetMultipartUploadParams{
		UploadID:  req.UploadID,
		ObjectKey: req.ObjectKey,
	})
	if err != nil {
		slog.Error("Failed to get multipart upload", "error", err, "upload_id", req.UploadID, "object_key", req.ObjectKey)
		http.Error(w, "multipart upload not found", http.StatusNotFound)

		return
	}

	slog.Info("Generating additional parts",
		"upload_id", req.UploadID,
		"object_key", req.ObjectKey,
		"start_part_number", req.StartPartNumber,
		"num_parts", req.NumParts,
		"pending_closure_id", upload.PendingClosureID)

	// Generate presigned URLs for the requested parts
	partURLs, err := s.generatePartURLs(r.Context(), req.ObjectKey, req.UploadID, req.StartPartNumber, req.NumParts)
	if err != nil {
		slog.Error("Failed to generate part URLs", "error", err)
		http.Error(w, fmt.Sprintf("failed to generate part URLs: %v", err), http.StatusInternalServerError)

		return
	}

	// Return the part URLs
	resp := requestPartsResponse{
		PartURLs:        partURLs,
		StartPartNumber: req.StartPartNumber,
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("Failed to encode response", "error", err)
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)

		return
	}

	slog.Info("Generated additional parts", "upload_id", req.UploadID, "parts", len(partURLs))
}

// CompleteMultipartUploadHandler completes a multipart upload with the list of ETags.
// It handles POST /api/multipart/complete endpoint.
func (s *Service) CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received complete multipart upload request", "method", r.Method, "url", r.URL)

	defer func() {
		if err := r.Body.Close(); err != nil {
			slog.Error("Failed to close request body", "error", err)
		}
	}()

	req := &completeMultipartRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		http.Error(w, "failed to decode request: "+err.Error(), http.StatusBadRequest)

		return
	}

	if req.ObjectKey == "" {
		http.Error(w, "missing object_key", http.StatusBadRequest)

		return
	}

	if req.UploadID == "" {
		http.Error(w, "missing upload_id", http.StatusBadRequest)

		return
	}

	if len(req.Parts) == 0 {
		http.Error(w, "missing parts", http.StatusBadRequest)

		return
	}

	// Convert to Minio format
	completeParts := make([]minio.CompletePart, len(req.Parts))
	for i, part := range req.Parts {
		completeParts[i] = minio.CompletePart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		}
	}

	// Sort parts by PartNumber (S3 expects ascending order)
	sort.Slice(completeParts, func(i, j int) bool {
		return completeParts[i].PartNumber < completeParts[j].PartNumber
	})

	// Create Core client for multipart operations
	coreClient := minio.Core{Client: s.MinioClient}

	// Complete multipart upload
	_, err := coreClient.CompleteMultipartUpload(r.Context(), s.Bucket, req.ObjectKey, req.UploadID, completeParts, minio.PutObjectOptions{})
	if err != nil {
		slog.Error("Failed to complete multipart upload", "error", err, "object_key", req.ObjectKey, "upload_id", req.UploadID)
		http.Error(w, fmt.Sprintf("failed to complete multipart upload: %v", err), http.StatusInternalServerError)

		return
	}

	// Delete the multipart upload tracking row
	queries := pg.New(s.Pool)
	if err := queries.DeleteMultipartUpload(r.Context(), req.UploadID); err != nil {
		// Log the error but don't fail the request - the upload already succeeded in S3
		slog.Error("Failed to delete multipart upload tracking row", "error", err, "upload_id", req.UploadID)
	}

	slog.Info("Completed multipart upload", "object_key", req.ObjectKey, "upload_id", req.UploadID, "parts", len(req.Parts))

	w.WriteHeader(http.StatusNoContent)
}

type signNarinfosRequest struct {
	Narinfos map[string]NarinfoMetadata `json:"narinfos"`
}

type signNarinfosResponse struct {
	Signatures map[string][]string `json:"signatures"`
}

// SignNarinfosHandler handles POST /api/pending_closures/{id}/sign endpoint.
// Signs narinfo metadata and returns signatures for client to upload.
// Request body: JSON with narinfos metadata
// Response body: JSON with signatures map.
func (s *Service) SignNarinfosHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received sign narinfos request", "method", r.Method, "url", r.URL)

	defer func() {
		if err := r.Body.Close(); err != nil {
			slog.Error("Failed to close request body", "error", err)
		}
	}()

	pendingClosureValue := r.PathValue("id")
	if pendingClosureValue == "" {
		http.Error(w, "missing id", http.StatusBadRequest)

		return
	}

	parsedUploadID, err := strconv.ParseInt(pendingClosureValue, 10, 32)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid id: %v", err), http.StatusBadRequest)

		return
	}

	// Parse request body with narinfo metadata
	req := &signNarinfosRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		http.Error(w, "failed to decode request: "+err.Error(), http.StatusBadRequest)

		return
	}

	// Get list of valid pending objects for this closure to validate against
	queries := pg.New(s.Pool)

	validObjectKeys, err := queries.GetPendingObjectKeys(r.Context(), parsedUploadID)
	if err != nil {
		slog.Error("Failed to get pending objects", "id", parsedUploadID, "error", err)
		http.Error(w, "failed to get pending objects", http.StatusInternalServerError)

		return
	}

	// Create a set of valid object keys for O(1) lookup
	validKeys := make(map[string]bool, len(validObjectKeys))
	for _, key := range validObjectKeys {
		validKeys[key] = true
	}

	// Sign each narinfo and collect signatures
	signaturesMap := make(map[string][]string, len(req.Narinfos))

	for objectKey, meta := range req.Narinfos {
		// Validate objectKey belongs to this pending closure
		if !validKeys[objectKey] {
			slog.Error("Invalid narinfo key: not part of pending closure", "object_key", objectKey, "closure_id", parsedUploadID)
			http.Error(w, "invalid narinfo key: not part of this pending closure", http.StatusForbidden)

			return
		}

		// Sign narinfo if signing keys are configured
		// Initialize as empty slice to ensure JSON serializes as [] not null
		signatures := []string{}

		if len(s.SigningKeys) > 0 {
			narInfo := &signing.NarInfo{
				StorePath:  meta.StorePath,
				NarHash:    meta.NarHash,
				NarSize:    meta.NarSize,
				References: meta.References,
			}

			signatures, err = signing.SignNarinfo(s.SigningKeys, narInfo)
			if err != nil {
				slog.Error("Failed to sign narinfo", "object_key", objectKey, "error", err)
				http.Error(w, fmt.Sprintf("failed to sign narinfo: %v", err), http.StatusInternalServerError)

				return
			}

			slog.Debug("Signed narinfo", "object_key", objectKey, "signatures", len(signatures))
		}

		signaturesMap[objectKey] = signatures
	}

	slog.Info("Signed narinfos", "id", parsedUploadID, "count", len(signaturesMap))

	// Return signatures
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(signNarinfosResponse{Signatures: signaturesMap}); err != nil {
		slog.Error("Failed to encode response", "error", err)
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)

		return
	}
}

// CommitPendingClosureHandler handles POST /api/pending_closures/{id}/complete endpoint.
// Commits the pending closure to the database after all objects have been uploaded.
// Request body: empty (all uploads should be complete before calling this)
// Response body: -.
func (s *Service) CommitPendingClosureHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received complete upload request", "method", r.Method, "url", r.URL)

	defer func() {
		if err := r.Body.Close(); err != nil {
			slog.Error("Failed to close request body", "error", err)
		}
	}()

	pendingClosureValue := r.PathValue("id")
	if pendingClosureValue == "" {
		http.Error(w, "missing id", http.StatusBadRequest)

		return
	}

	parsedUploadID, err := strconv.ParseInt(pendingClosureValue, 10, 32)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid id: %v", err), http.StatusBadRequest)

		return
	}

	// Commit the pending closure (all objects including narinfos should already be uploaded)
	if err = commitPendingClosure(r.Context(), s.Pool, parsedUploadID); err != nil {
		if errors.Is(err, errPendingClosureNotFound) {
			http.Error(w, "pending closure not found", http.StatusNotFound)

			return
		}

		slog.Error("Failed to complete upload", "id", parsedUploadID, "error", err)

		http.Error(w, fmt.Sprintf("failed to complete upload: %v", err), http.StatusInternalServerError)

		return
	}

	slog.Info("Completed upload", "id", parsedUploadID)

	w.WriteHeader(http.StatusNoContent)
}

// CleanupPendingClosuresHandler handles DELETE /api/pending_closures?older-than=1h endpoint.
// Request body: -
// Response body: -.
func (s *Service) CleanupPendingClosuresHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received cleanup request", "method", r.Method, "url", r.URL)

	olderThanParam := r.URL.Query().Get("older-than")
	if olderThanParam == "" {
		olderThanParam = "1h"
	}

	olderThan, err := time.ParseDuration(olderThanParam)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid duration: %v", err), http.StatusBadRequest)

		return
	}

	if _, err := s.cleanupPendingClosures(r.Context(), olderThan); err != nil {
		http.Error(w, fmt.Sprintf("cleanup failed: %v", err), http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
