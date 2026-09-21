package server

import (
	"context"
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

// principal is the result of authenticating a request.
type principal struct {
	scopes []oidc.Scope
	// pins are the reserved pin patterns this principal may write.
	pins []string
}

type principalKey struct{}

var adminPrincipal = principal{scopes: allScopes}

// authenticate returns the principal for r. ok is false when no valid
// credentials were presented at all.
func (s *Service) authenticate(r *http.Request) (principal, bool) {
	if s.mtlsCheck(r, s.MTLSBoundSubjects) {
		return adminPrincipal, true
	}

	if len(s.MTLSBoundSubjectsRead) > 0 && s.mtlsCheck(r, s.MTLSBoundSubjectsRead) {
		return principal{scopes: []oidc.Scope{oidc.ScopeRead}}, true
	}

	token, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !found || token == "" {
		return principal{}, false
	}

	if s.APIToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.APIToken)) == 1 {
		return adminPrincipal, true
	}

	if s.OIDCValidator == nil {
		s.logAuthFailure(token, nil)

		return principal{}, false
	}

	claims, err := s.OIDCValidator.ValidateToken(r.Context(), token)
	if err != nil {
		var vErr *oidc.ValidationError

		errors.As(err, &vErr)
		s.logAuthFailure(token, vErr)

		return principal{}, false
	}

	slog.Debug("OIDC auth successful", "provider", claims.Provider, "subject", claims.Subject, "scopes", claims.Scopes)

	scopes := claims.Scopes
	// Anyone who may upload or administer may also read.
	if !claims.Has(oidc.ScopeRead) {
		scopes = append(slices.Clone(scopes), oidc.ScopeRead)
	}

	return principal{scopes: scopes, pins: claims.Pins}, true
}

// RequireScope wraps next so it only runs for principals holding scope.
func (s *Service) RequireScope(scope oidc.Scope, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if scope == oidc.ScopeRead && !s.readGated() {
			next.ServeHTTP(w, r)

			return
		}

		p, ok := s.authenticate(r)
		if !ok {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)

			return
		}

		if !slices.Contains(p.scopes, scope) {
			http.Error(w, "Forbidden: token lacks scope "+string(scope), http.StatusForbidden)

			return
		}

		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	}
}

// mayWritePin reports whether the request may create or move pin name.
// Reserved names need a matching rule or admin; RequireScope already
// checked write for everything else.
func (s *Service) mayWritePin(r *http.Request, name string) bool {
	if s.OIDCValidator == nil || !s.OIDCValidator.ReservesPin(name) {
		return true
	}

	p, ok := r.Context().Value(principalKey{}).(principal)
	if !ok {
		return false
	}

	return slices.Contains(p.scopes, oidc.ScopeAdmin) || oidc.GlobMatchAny(p.pins, name)
}
