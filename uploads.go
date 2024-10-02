package main

import (
	"log/slog"
	"encoding/json"
	"net/http"
)

type UploadsRequest struct {
	StorePaths []string `json:"store_paths"`
}

// POST /uploads
// Request body: {"store_paths": ["3dyw8dzj9ab4m8hv5dpyx7zii8d0w6fi", "3dyw8dzj9ab4m8hv5dpyx7zii8d0w6fi"]}
func (s *Server) uploadsHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received uploads request", "method", r.Method, "url", r.URL)
	req := &UploadsRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		http.Error(w, "failed to decode request: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	upload, err := s.db.StartUpload(req.StorePaths)
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
