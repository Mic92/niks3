package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

// getClosureObjects handles the GET /closures/<key> endpoint.
func (s *Server) getClosureHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received get closure request", "method", r.Method, "url", r.URL)

	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)

		return
	}

	closure, err := getClosure(r.Context(), s.pool, key)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "closure not found", http.StatusNotFound)

			return
		}

		http.Error(w, "failed to get closure objects: "+err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(closure)
	if err != nil {
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// cleanupClosuresOlders handles the DELETE /closures endpoint.
func (s *Server) cleanupClosuresOlder(w http.ResponseWriter, r *http.Request) {
	slog.Info("Starting cleanup of old closures", "method", r.Method, "url", r.URL)

	olderThan := r.URL.Query().Get("older-than")
	if olderThan == "" {
		http.Error(w, "missing age", http.StatusBadRequest)

		return
	}

	age, err := time.ParseDuration(olderThan)
	if err != nil {
		http.Error(w, "failed to parse age: "+err.Error(), http.StatusBadRequest)

		return
	}

	if err = cleanupClosureOlderThan(r.Context(), s.pool, age); err != nil {
		http.Error(w, "failed to cleanup old closures: "+err.Error(), http.StatusInternalServerError)

		return
	}

	if err = s.cleanupOrphanObjects(r.Context(), s.pool); err != nil {
		http.Error(w, "failed to cleanup orphan objects: "+err.Error(), http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
