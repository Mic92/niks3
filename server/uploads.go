package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

type CreatePendingClosureRequest struct {
	Closure *string  `json:"closure"`
	Objects []string `json:"objects"`
}

// POST /pending_closures
// Request body:
//
//	{
//	 "closure": "26xbg1ndr7hbcncrlf9nhx5is2b25d13",
//	 "objects": [
//		 "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo",
//		 "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.xz"
//	 ]
//	}
//
// Response body:
//
//	{
//	  "id": 1,
//	  "started_at": "2021-08-31T00:00:00Z"
//	  "pending_objects": {
//		  "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo": "https://yours3endpoint?authkey=...",
//	   }
//	}
func (s *Service) CreatePendingClosureHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received uploads request", "method", r.Method, "url", r.URL)

	defer func() {
		if err := r.Body.Close(); err != nil {
			slog.Error("Failed to close request body", "error", err)
		}
	}()

	req := &CreatePendingClosureRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		http.Error(w, "failed to decode request: "+err.Error(), http.StatusBadRequest)

		return
	}

	if req.Closure == nil {
		http.Error(w, "missing closure key", http.StatusBadRequest)

		return
	}

	if len(req.Objects) == 0 {
		http.Error(w, "missing objects key", http.StatusBadRequest)

		return
	}

	storePathSet := make(map[string]bool)

	for _, object := range req.Objects {
		storePathSet[object] = true
	}

	upload, err := s.createPendingClosure(r.Context(), s.Pool, *req.Closure, storePathSet)
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

// POST /pending_closures/{key}/commit
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
		}

		slog.Error("Failed to complete upload", "id", parsedUploadID, "error", err)

		http.Error(w, fmt.Sprintf("failed to complete upload: %v", err), http.StatusInternalServerError)

		return
	}

	slog.Info("Completed upload", "id", parsedUploadID)

	w.WriteHeader(http.StatusNoContent)
}

// DELETE /pending_closures?duration=1h
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

	if err := cleanupPendingClosures(r.Context(), s.Pool, olderThan); err != nil {
		http.Error(w, fmt.Sprintf("failed to cleanup pending closures: %v", err), http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
