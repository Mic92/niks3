package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/Mic92/niks3/server/oidc"
)

// serverTLSConfig builds a tls.Config for ListenAndServeTLS. If clientCA
// is set, the server requests and verifies client certs against it (native
// mTLS).
func serverTLSConfig(clientCA string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	if clientCA == "" {
		return cfg, nil
	}

	pem, err := os.ReadFile(clientCA)
	if err != nil {
		return nil, fmt.Errorf("reading client CA: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("parsing client CA %q: no PEM certificates found", clientCA)
	}

	cfg.ClientCAs = pool
	// VerifyClientCertIfGiven: anonymous TLS is allowed so bearer-token
	// auth and public reads still work; AuthMiddleware/ReadAuthMiddleware
	// decide what an unauthenticated request may do.
	cfg.ClientAuth = tls.VerifyClientCertIfGiven

	return cfg, nil
}

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

// mtlsCheck verifies the client cert (native or proxied) and, if
// boundSubjects is non-empty, matches the subject DN against them.
func (s *Service) mtlsCheck(r *http.Request, boundSubjects []string) bool {
	subject, ok := s.mtlsSubject(r)
	if !ok {
		return false
	}

	if len(boundSubjects) == 0 {
		slog.Debug("mTLS auth: any verified cert accepted")

		return true
	}

	if subject == "" {
		slog.Warn("mTLS auth: bound subjects configured but subject DN unavailable")

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

// mtlsSubject returns the verified client cert's subject DN, and whether a
// cert was verified at all. Native mTLS reads r.TLS directly; otherwise
// the configured proxy headers are trusted.
func (s *Service) mtlsSubject(r *http.Request) (string, bool) {
	if s.NativeMTLS {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			return "", false
		}

		return r.TLS.PeerCertificates[0].Subject.String(), true
	}

	if s.MTLSProxyHeader == "" || r.Header.Get(s.MTLSProxyHeader) != "SUCCESS" {
		return "", false
	}

	return r.Header.Get(s.MTLSSubjectHeader), true
}
