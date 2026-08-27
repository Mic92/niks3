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

// requestScopes authenticates r and returns the granted scopes. ok is false
// when no valid credentials were presented at all.
func (s *Service) requestScopes(r *http.Request) (scopes []oidc.Scope, ok bool) {
	if s.mtlsCheck(r, s.MTLSBoundSubjects) {
		return allScopes, true
	}

	if len(s.MTLSBoundSubjectsRead) > 0 && s.mtlsCheck(r, s.MTLSBoundSubjectsRead) {
		return []oidc.Scope{oidc.ScopeRead}, true
	}

	token, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !found || token == "" {
		return nil, false
	}

	if s.APIToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.APIToken)) == 1 {
		return allScopes, true
	}

	if s.OIDCValidator == nil {
		s.logAuthFailure(token, nil)

		return nil, false
	}

	claims, err := s.OIDCValidator.ValidateToken(r.Context(), token)
	if err != nil {
		var vErr *oidc.ValidationError

		errors.As(err, &vErr)
		s.logAuthFailure(token, vErr)

		return nil, false
	}

	slog.Info("OIDC auth successful", "provider", claims.Provider, "scopes", claims.Scopes)
	slog.Debug("OIDC auth details", "subject", claims.Subject)

	scopes = claims.Scopes
	// Anyone who may upload or administer may also read.
	if !claims.Has(oidc.ScopeRead) {
		scopes = append(slices.Clone(scopes), oidc.ScopeRead)
	}

	return scopes, true
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
