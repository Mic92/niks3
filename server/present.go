package server

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server/pg"
)

// PresentHandler answers which narinfo keys are cached as closure roots and
// refreshes their age, so a client can skip pushing them.
func (s *Service) PresentHandler(w http.ResponseWriter, r *http.Request) {
	defer closeRequestBody(r)

	var req api.PresentRequest
	if !decodeJSONBody(w, r, MaxClosureRequestBody, &req) {
		return
	}

	q := pg.New(s.Pool)

	present, err := q.GetPresentClosures(r.Context(), req.Keys)
	if err != nil {
		slog.Error("present", "error", err)
		http.Error(w, "present: "+err.Error(), http.StatusInternalServerError)

		return
	}

	if len(present) > 0 {
		_ = q.TouchClosures(r.Context(), present)
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(api.PresentResponse{Present: present}); err != nil {
		slog.Error("present: encode", "error", err)
	}
}
