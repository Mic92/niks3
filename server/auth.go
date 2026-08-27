package server

import (
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/Mic92/niks3/server/oidc"
)

var allScopes = []oidc.Scope{oidc.ScopeRead, oidc.ScopeWrite, oidc.ScopeAdmin}

// readGated reports whether the read proxy requires authentication. Reads are
// public unless the operator configured a read rule somewhere, since Nix
// substituters present no credentials and contents are signed.
func (s *Service) readGated() bool {
	return len(s.MTLSBoundSubjectsRead) > 0 || (s.OIDCValidator != nil && s.OIDCValidator.GrantsScope(oidc.ScopeRead))
}

// certScopes trusts any verified cert fully when no subject lists are set.
// Otherwise a cert gets only what its subject matches.
func (s *Service) certScopes(r *http.Request) []oidc.Scope {
	subject, ok := s.mtlsSubject(r)
	if !ok {
		return nil
	}

	if len(s.MTLSBoundSubjects) == 0 && len(s.MTLSBoundSubjectsRead) == 0 {
		return allScopes
	}

	if subjectMatches(subject, s.MTLSBoundSubjects) {
		return allScopes
	}

	if subjectMatches(subject, s.MTLSBoundSubjectsRead) {
		return []oidc.Scope{oidc.ScopeRead}
	}

	slog.Warn("mTLS auth: subject not in bound subjects", "subject", subject)

	return nil
}

// bearerScopes returns what the Authorization header grants.
func (s *Service) bearerScopes(r *http.Request) []oidc.Scope {
	token, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !found || token == "" {
		return nil
	}

	if s.APIToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.APIToken)) == 1 {
		return allScopes
	}

	if s.OIDCValidator == nil {
		s.logAuthFailure(token, nil)

		return nil
	}

	claims, err := s.OIDCValidator.ValidateToken(r.Context(), token)
	if err != nil {
		var vErr *oidc.ValidationError

		errors.As(err, &vErr)
		s.logAuthFailure(token, vErr)

		return nil
	}

	slog.Info("OIDC auth successful", "provider", claims.Provider, "scopes", claims.Scopes)
	slog.Debug("OIDC auth details", "subject", claims.Subject)

	// Anyone who may upload or administer may also read.
	return append(slices.Clone(claims.Scopes), oidc.ScopeRead)
}

// requestScopes unions all presented credentials. ok is false if none were valid.
func (s *Service) requestScopes(r *http.Request) ([]oidc.Scope, bool) {
	scopes := append(s.certScopes(r), s.bearerScopes(r)...)

	return scopes, len(scopes) > 0
}

// RequireScope wraps next so it only runs for principals holding scope.
func (s *Service) RequireScope(scope oidc.Scope, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if scope == oidc.ScopeRead && !s.readGated() {
			next.ServeHTTP(w, r)

			return
		}

		scopes, ok := s.requestScopes(r)
		if !ok {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)

			return
		}

		if !slices.Contains(scopes, scope) {
			http.Error(w, "Forbidden: token lacks scope "+string(scope), http.StatusForbidden)

			return
		}

		next.ServeHTTP(w, r)
	}
}
