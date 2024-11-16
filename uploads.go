package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5"
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
func (s *Server) createPendingClosureHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received uploads request", "method", r.Method, "url", r.URL)
	defer r.Body.Close()

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

	upload, err := createPendingClosure(r.Context(), s.pool, *req.Closure, storePathSet)
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

	w.WriteHeader(http.StatusOK)
}

// POST /pending_closures/{key}/commit
// Request body: -
// Response body: -.
func (s *Server) commitPendingClosureHandler(w http.ResponseWriter, r *http.Request) {
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

	if err = commitPendingClosure(r.Context(), s.pool, parsedUploadID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "upload not found", http.StatusNotFound)

			return
		}

		http.Error(w, fmt.Sprintf("failed to complete upload: %v", err), http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
