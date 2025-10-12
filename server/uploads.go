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
	"github.com/minio/minio-go/v7"
)

type objectWithRefs struct {
	Key     string   `json:"key"`
	Type    string   `json:"type"`
	Refs    []string `json:"refs"`
	NarSize *uint64  `json:"nar_size,omitempty"` // For estimating multipart parts
}

type createPendingClosureRequest struct {
	Closure *string          `json:"closure"`
	Objects []objectWithRefs `json:"objects"`
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

	upload, err := s.createPendingClosure(r.Context(), s.Pool, *req.Closure, objectsMap)
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

// CommitPendingClosureHandler handles POST /api/pending_closures/{id}/complete endpoint.
// Request body: -
// Response body: -.
func (s *Service) CommitPendingClosureHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received complete upload request", "method", r.Method, "url", r.URL)

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

	if err := cleanupPendingClosures(r.Context(), s.Pool, s.MinioClient, s.Bucket, olderThan); err != nil {
		http.Error(w, fmt.Sprintf("cleanup failed: %v", err), http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
