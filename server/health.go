package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

const readinessTimeout = 2 * time.Second

// HealthCheckHandler is the liveness probe and deliberately has no
// dependencies, so a Postgres outage does not restart every replica.
func (s *Service) HealthCheckHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)

	_, err := w.Write([]byte("OK"))
	if err != nil {
		slog.Warn("Could not write health check response", "error", err)
	}
}

// ReadinessHandler fails while Postgres is unreachable so load balancers stop
// routing to this instance.
func (s *Service) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
	defer cancel()

	if err := s.Pool.Ping(ctx); err != nil {
		slog.Warn("readiness check failed", "error", err)
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)

		return
	}

	s.HealthCheckHandler(w, r)
}
