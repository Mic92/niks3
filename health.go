package main

import (
	"log/slog"
	"net/http"
)

func (s *Server) healthCheckHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)

	_, err := w.Write([]byte("OK"))
	if err != nil {
		slog.Warn("Could not write health check response", "error", err)
	}
}
