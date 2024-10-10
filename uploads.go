package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
)

type UploadsRequest struct {
	ClosureNarHash string   `json:"closure_nar_hash"`
	StorePaths     []string `json:"store_paths"`
}
// POST /uploads
// Request body:
// {
//  "closure_nar_hash": "3dyw8dzj9ab4m8hv5dpyx7zii8d0w6fi", "store_paths": ["3dyw8dzj9ab4m8hv5dpyx7zii8d0w6fi", "3dyw8dzj9ab4m8hv5dpyx7zii8d0w6fi"]
// }
// Response body:
// {
//  "id": 1,
//  "started_at": "2021-08-31T00:00:00Z"
// }
func (s *Server) startUploadHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received uploads request", "method", r.Method, "url", r.URL)
	req := &UploadsRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		http.Error(w, "failed to decode request: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	upload, err := s.db.StartUpload(req.ClosureNarHash, req.StorePaths)
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
	if err = s.db.CompleteUpload(parsedUploadID); err != nil {
		http.Error(w, "failed to complete upload: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
