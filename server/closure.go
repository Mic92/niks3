package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

// GetClosureHandler handles the GET /closures/<key> endpoint.
func (s *Service) GetClosureHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Received get closure request", "method", r.Method, "url", r.URL)

	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)

		return
	}

	closure, err := getClosure(r.Context(), s.Pool, key)
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
}

// CleanupClosuresOlder handles the DELETE /closures endpoint.
func (s *Service) CleanupClosuresOlder(w http.ResponseWriter, r *http.Request) {
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

	if age < 0 {
		http.Error(w, "older-than must not be negative", http.StatusBadRequest)

		return
	}

	if err = cleanupClosureOlderThan(r.Context(), s.Pool, age); err != nil {
		http.Error(w, "failed to cleanup old closures: "+err.Error(), http.StatusInternalServerError)

		return
	}

	// Check if force mode is enabled
	force := r.URL.Query().Get("force") == "true"

	var gracePeriod int32
	if force {
		// Force mode: immediate deletion (grace period = 0)
		gracePeriod = 0

		slog.Warn("Force mode enabled - objects will be deleted immediately without grace period")
	} else {
		// Use same grace period for object cleanup as pending closure cleanup
		// This ensures no pending closure can resurrect an object being deleted
		gracePeriod = int32(age.Seconds())
	}

	if err = s.cleanupOrphanObjects(r.Context(), s.Pool, gracePeriod); err != nil {
		http.Error(w, "failed to cleanup orphan objects: "+err.Error(), http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
