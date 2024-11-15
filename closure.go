package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
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

	err = cleanupClosureOlderThan(r.Context(), s.pool, age)
	if err != nil {
		slog.Error("Failed to cleanup old closures", "error", err)
	}

	w.WriteHeader(http.StatusNoContent)
}
