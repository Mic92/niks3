package server

import (
	"net/http"
)

// RootRedirectHandler redirects requests to the root path to the public cache URL.
func (s *Service) RootRedirectHandler(w http.ResponseWriter, r *http.Request) {
	// Only handle requests to the root path
	if r.URL.Path != "/" {
		http.NotFound(w, r)

		return
	}

	// If no cache URL is configured, return a simple message
	if s.CacheURL == "" {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("niks3 binary cache server\n"))

		return
	}

	// Redirect to the public cache URL
	http.Redirect(w, r, s.CacheURL, http.StatusMovedPermanently)
}
