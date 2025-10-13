package server

import (
	"bytes"
	"context"
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
	"github.com/klauspost/compress/zstd"
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

// generateNarinfoContent creates the narinfo file content from metadata.
// If signatures are provided, they will be included in the output.
func generateNarinfoContent(meta *NarinfoMetadata, signatures []string) string {
	var sb strings.Builder

	// StorePath
	fmt.Fprintf(&sb, "StorePath: %s\n", meta.StorePath)

	// URL to the NAR file
	fmt.Fprintf(&sb, "URL: %s\n", meta.URL)

	// Compression
	fmt.Fprintf(&sb, "Compression: %s\n", meta.Compression)

	// NAR hash and size (uncompressed)
	fmt.Fprintf(&sb, "NarHash: %s\n", meta.NarHash)
	fmt.Fprintf(&sb, "NarSize: %d\n", meta.NarSize)

	// FileHash and FileSize for compressed file
	fmt.Fprintf(&sb, "FileHash: %s\n", meta.FileHash)
	fmt.Fprintf(&sb, "FileSize: %d\n", meta.FileSize)

	// References (must have space after colon, even if empty)
	fmt.Fprint(&sb, "References:")

	// Sort references for deterministic output
	sort.Strings(meta.References)

	for _, ref := range meta.References {
		// Remove /nix/store/ prefix
		refName := strings.TrimPrefix(ref, "/nix/store/")
		fmt.Fprintf(&sb, " %s", refName)
	}

	// Always add a space after "References:" even if empty
	if len(meta.References) == 0 {
		fmt.Fprint(&sb, " ")
	}

	fmt.Fprint(&sb, "\n")

	// Deriver (optional)
	if meta.Deriver != nil {
		deriverName := strings.TrimPrefix(*meta.Deriver, "/nix/store/")
		fmt.Fprintf(&sb, "Deriver: %s\n", deriverName)
	}

	// Signatures (passed as parameter from signing process)
	if len(signatures) > 0 {
		// Sort signatures for deterministic output
		sortedSigs := make([]string, len(signatures))
		copy(sortedSigs, signatures)
		sort.Strings(sortedSigs)

		for _, sig := range sortedSigs {
			fmt.Fprintf(&sb, "Sig: %s\n", sig)
		}
	}

	// CA (optional)
	if meta.CA != nil {
		fmt.Fprintf(&sb, "CA: %s\n", *meta.CA)
	}

	return sb.String()
}

// processNarinfo generates, signs, compresses, and uploads a narinfo file to S3.
func (s *Service) processNarinfo(ctx context.Context, objectKey string, meta *NarinfoMetadata) error {
	// Sign narinfo if signing keys are configured
	var signatures []string

	if len(s.SigningKeys) > 0 {
		signatures, err := signing.SignNarinfo(
			s.SigningKeys,
			meta.StorePath,
			meta.NarHash,
			meta.NarSize,
			meta.References,
		)
		if err != nil {
			return fmt.Errorf("failed to sign narinfo: %w", err)
		}

		slog.Info("Signed narinfo", "object_key", objectKey, "signatures", len(signatures))
	}

	narinfoContent := generateNarinfoContent(meta, signatures)

	var compressedBuf bytes.Buffer

	zstdWriter, err := zstd.NewWriter(&compressedBuf)
	if err != nil {
		return fmt.Errorf("failed to create zstd writer: %w", err)
	}

	if _, err := zstdWriter.Write([]byte(narinfoContent)); err != nil {
		return fmt.Errorf("failed to compress narinfo: %w", err)
	}

	if err := zstdWriter.Close(); err != nil {
		return fmt.Errorf("failed to close zstd writer: %w", err)
	}

	compressedData := compressedBuf.Bytes()

	// Upload to S3 with Content-Encoding: zstd header
	_, err = s.MinioClient.PutObject(ctx, s.Bucket, objectKey,
		bytes.NewReader(compressedData),
		int64(len(compressedData)),
		minio.PutObjectOptions{
			ContentType:     "text/x-nix-narinfo",
			ContentEncoding: "zstd",
		})
	if err != nil {
		return fmt.Errorf("failed to upload narinfo to S3: %w", err)
	}

	slog.Info("Uploaded narinfo", "object_key", objectKey, "size", len(compressedData))

	return nil
}

// CommitPendingClosureHandler handles POST /api/pending_closures/{id}/complete endpoint.
// Request body: JSON with narinfos metadata
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

	// Parse request body with narinfo metadata
	req := &commitPendingClosureRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		http.Error(w, "failed to decode request: "+err.Error(), http.StatusBadRequest)

		return
	}

	// Validate that narinfos map is non-empty
	if len(req.Narinfos) == 0 {
		http.Error(w, "no narinfo metadata provided", http.StatusBadRequest)

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

	// Validate and process each narinfo
	for objectKey, meta := range req.Narinfos {
		// Validate objectKey belongs to this pending closure
		if !validKeys[objectKey] {
			slog.Error("Invalid narinfo key: not part of pending closure", "object_key", objectKey, "closure_id", parsedUploadID)
			http.Error(w, "invalid narinfo key: not part of this pending closure", http.StatusForbidden)

			return
		}

		// Process narinfo (validated to be part of this closure)
		if err := s.processNarinfo(r.Context(), objectKey, &meta); err != nil {
			slog.Error("Failed to process narinfo", "object_key", objectKey, "error", err)
			http.Error(w, fmt.Sprintf("failed to process narinfo: %v", err), http.StatusInternalServerError)

			return
		}
	}

	// Commit the pending closure
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
