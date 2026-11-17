package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server/pg"
)

// ClosuresExistHandler answers which narinfo keys were pushed as closures.
// A client may skip pushing those. Hits are touched.
func (s *Service) ClosuresExistHandler(w http.ResponseWriter, r *http.Request) {
	// Check and refresh in one statement, so a closure that GC is deleting
	// is not reported.
	if keys, ok := s.existing(w, r, pg.New(s.Pool).TouchPresentClosures); ok {
		writeExisting(w, api.ExistsResponse{Exists: keys})
	}
}

// ObjectsExistHandler answers which narinfo keys can be fetched. That
// includes the dependencies of a pushed closure, which are deleted with it.
func (s *Service) ObjectsExistHandler(w http.ResponseWriter, r *http.Request) {
	if keys, ok := s.existing(w, r, pg.New(s.Pool).GetPresentObjects); ok {
		writeExisting(w, api.ExistsResponse{Exists: keys})
	}
}

// PresentHandler serves /api/objects/present, the former name of
// /api/closures/exists.
func (s *Service) PresentHandler(w http.ResponseWriter, r *http.Request) {
	if keys, ok := s.existing(w, r, pg.New(s.Pool).TouchPresentClosures); ok {
		writeExisting(w, api.PresentResponse{Present: keys})
	}
}

func (s *Service) existing(
	w http.ResponseWriter, r *http.Request, query func(context.Context, []string) ([]string, error),
) ([]string, bool) {
	defer closeRequestBody(r)

	var req api.ExistsRequest
	if !decodeJSONBody(w, r, MaxClosureRequestBody, &req) {
		return nil, false
	}

	keys, err := query(r.Context(), req.Keys)
	if err != nil {
		slog.Error("exists", "error", err)
		http.Error(w, "exists: "+err.Error(), http.StatusInternalServerError)

		return nil, false
	}

	if keys == nil {
		keys = []string{}
	}

	return keys, true
}

func writeExisting(w http.ResponseWriter, answer any) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(answer); err != nil {
		slog.Error("exists: encode", "error", err)
	}
}
