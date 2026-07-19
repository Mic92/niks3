package server

import (
	"log/slog"
	"net/http"
)

type skippedUploadsRequest struct {
	Paths    uint64 `json:"paths"`
	NarBytes uint64 `json:"nar_bytes"`
}

// SkippedUploadsHandler handles POST /api/uploads/skipped. Clients report
// store paths they skipped due to the max NAR size limit, so operators can
// see how much data never reaches the cache via /metrics.
func (s *Service) SkippedUploadsHandler(w http.ResponseWriter, r *http.Request) {
	defer closeRequestBody(r)

	req := &skippedUploadsRequest{}
	if !decodeJSONBody(w, r, maxAPIRequestBody, req) {
		return
	}

	s.Metrics.recordSkippedUploads(req.Paths, req.NarBytes)
	slog.Info("Client skipped oversized paths", "paths", req.Paths, "nar_bytes", req.NarBytes)

	w.WriteHeader(http.StatusNoContent)
}
