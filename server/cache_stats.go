package server

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server/pg"
)

// CacheStatsHandler serves public cache inventory stats for the landing page.
// CORS-open so the page can fetch it when served cross-origin from S3 or a CDN.
func (s *Service) CacheStatsHandler(w http.ResponseWriter, r *http.Request) {
	stats, err := pg.New(s.Pool).GetObjectStats(r.Context())
	if err != nil {
		slog.Error("failed to read cache stats", "error", err)
		http.Error(w, "failed to read cache stats", http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	_ = json.NewEncoder(w).Encode(api.CacheStats{
		Objects:      stats.ObjectCount,
		LogicalBytes: stats.TotalBytes,
	})
}
