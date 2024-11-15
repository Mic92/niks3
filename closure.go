package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
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
