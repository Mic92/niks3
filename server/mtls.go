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
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		// ListenAndServeTLS only auto-enables HTTP/2 when TLSConfig is nil.
		NextProtos: []string{"h2", "http/1.1"},
	}

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
	// auth and public reads still work. RequireScope
	// decides what an unauthenticated request may do.
	cfg.ClientAuth = tls.VerifyClientCertIfGiven

	return cfg, nil
}

func subjectMatches(subject string, patterns []string) bool {
	if subject == "" && len(patterns) > 0 {
		slog.Warn("mTLS auth: bound subjects configured but subject DN unavailable")

		return false
	}

	for _, pattern := range patterns {
		if oidc.GlobMatch(pattern, subject) {
			return true
		}
	}

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
