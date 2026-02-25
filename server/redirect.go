package server

import (
	"net/http"
)

// RootRedirectHandler redirects requests to the root path.
// When the read proxy is enabled and a cache URL is configured (meaning
// an index.html landing page was uploaded to S3), it redirects to
// /index.html so the proxy can serve it directly — avoiding a redirect
// loop when the cache URL points back to this server.
// Otherwise it redirects to the external public cache URL.
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

	// When the read proxy serves objects from S3 directly, redirect to
	// the landing page instead of the external cache URL to avoid loops.
	if s.EnableReadProxy {
		http.Redirect(w, r, "/index.html", http.StatusMovedPermanently)

		return
	}

	// Redirect to the public cache URL
	http.Redirect(w, r, s.CacheURL, http.StatusMovedPermanently)
}
