package server

import (
	"log/slog"
	"net/http"

	"github.com/Mic92/niks3/server/oidc"
)

// ReadAuthMiddleware gates the read proxy behind mTLS when
// MTLSBoundSubjectsRead is non-empty. Reads are otherwise public: Nix
// substituters present no credentials and the cache contents are
// integrity-signed.
func (s *Service) ReadAuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	if len(s.MTLSBoundSubjectsRead) == 0 {
		return next
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if !s.mtlsCheck(r, s.MTLSBoundSubjectsRead) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)

			return
		}

		next.ServeHTTP(w, r)
	}
}

// mtlsAuthenticated reports whether the request carries proxy headers
// proving a verified mTLS client cert that is allowed by MTLSBoundSubjects.
// Used for the write API.
func (s *Service) mtlsAuthenticated(r *http.Request) bool {
	return s.mtlsCheck(r, s.MTLSBoundSubjects)
}

// mtlsCheck verifies the proxy headers and, if boundSubjects is non-empty,
// matches the cert subject DN against them.
func (s *Service) mtlsCheck(r *http.Request, boundSubjects []string) bool {
	if s.MTLSProxyHeader == "" {
		return false
	}

	if r.Header.Get(s.MTLSProxyHeader) != "SUCCESS" {
		return false
	}

	if len(boundSubjects) == 0 {
		slog.Debug("mTLS auth: any verified cert accepted")

		return true
	}

	subject := r.Header.Get(s.MTLSSubjectHeader)
	if subject == "" {
		slog.Warn("mTLS auth: bound subjects configured but subject header missing or empty",
			"header", s.MTLSSubjectHeader)

		return false
	}

	for _, pattern := range boundSubjects {
		if oidc.GlobMatch(pattern, subject) {
			slog.Debug("mTLS auth: subject matched", "subject", subject, "pattern", pattern)

			return true
		}
	}

	slog.Warn("mTLS auth: subject not in bound subjects", "subject", subject)

	return false
}
