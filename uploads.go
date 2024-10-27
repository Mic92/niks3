package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/Mic92/niks3/pg"
)

type UploadsRequest struct {
	Closure *string  `json:"closure"`
	Objects []string `json:"objects"`
}

// startUploadHandler
// POST /uploads
// Request body:
//
//	{
//	 "closure": "26xbg1ndr7hbcncrlf9nhx5is2b25d13", "objects": ["26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.xz"]
//	}
//
// Response body:
//
//	{
//	  "id": 1,
//	  "started_at": "2021-08-31T00:00:00Z"
//	  "objects": {
//		  "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo": "https://yours3endpoint?authkey=...",
//	   }
//	}
func (s *Server) startUploadHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received uploads request", "method", r.Method, "url", r.URL)
	defer r.Body.Close()

	req := &UploadsRequest{}
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
	upload, err := StartUpload(r.Context(), s.pool, *req.Closure, storePathSet)
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

// POST /uploads/{upload_id}/complete
// Request body: -
// Response body: -
func (s *Server) completeUploadHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received complete upload request", "method", r.Method, "url", r.URL)

	uploadID := r.URL.Query().Get("upload_id")
	if uploadID == "" {
		http.Error(w, "missing upload_id", http.StatusBadRequest)
		return
	}

	parsedUploadID, err := strconv.ParseInt(uploadID, 10, 64)
	if err != nil {
		http.Error(w, "invalid upload_id", http.StatusBadRequest)
		return
	}

	queries := pg.New(s.pool)
	if err = queries.DeleteUpload(r.Context(), parsedUploadID); err != nil {
		http.Error(w, "failed to complete upload: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
