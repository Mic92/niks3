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

// allScopes is the full grant given to mTLS clients and the static API token.
func allScopes() []oidc.Scope {
	return []oidc.Scope{oidc.ScopeRead, oidc.ScopeWrite, oidc.ScopeAdmin}
}

// readGated reports whether the read proxy requires authentication. Reads are
// public unless the operator configured a read rule somewhere, since Nix
// substituters present no credentials and contents are signed.
func (s *Service) readGated() bool {
	return len(s.MTLSBoundSubjectsRead) > 0 || len(s.MTLSPinReadRules) > 0 || (s.OIDCValidator != nil && s.OIDCValidator.GrantsScope(oidc.ScopeRead))
}

// principal is the result of authenticating a request.
type principal struct {
	scopes []oidc.Scope
	// writePins are the reserved pin patterns this principal may write.
	writePins []string
	// readPins are the glob patterns this principal may read (pin-scoped reads).
	readPins []string
}

type principalKey struct{}

// adminPrincipal is the principal for mTLS clients and the static API token.
func adminPrincipal() principal {
	return principal{scopes: allScopes()}
}

// pinReadPatterns returns the pin patterns that the mTLS client can read.
func (s *Service) pinReadPatterns(r *http.Request) []string {
	subject, ok := s.mtlsSubject(r)
	if !ok || subject == "" {
		return nil
	}

	cn, cnFound := commonName(subject)
	if !cnFound || !pinNameRegex.MatchString(cn) {
		return nil
	}

	var readPins []string
	for _, rule := range s.MTLSPinReadRules {
		if oidc.GlobMatch(rule.Subject, subject) {
			pin := strings.ReplaceAll(rule.Pin, "{cn}", cn)
			readPins = append(readPins, pin)
		}
	}

	return readPins
}

// authenticate returns the principal for r. The bool is false when no valid
// credentials were presented at all.
func (s *Service) authenticate(r *http.Request) (principal, bool) {
	if s.mtlsCheck(r, s.MTLSBoundSubjects) {
		return adminPrincipal(), true
	}

	if len(s.MTLSBoundSubjectsRead) > 0 && s.mtlsCheck(r, s.MTLSBoundSubjectsRead) {
		return principal{scopes: []oidc.Scope{oidc.ScopeRead}}, true
	}

	// Check for pin-scoped reads
	if len(s.MTLSPinReadRules) > 0 {
		if readPins := s.pinReadPatterns(r); len(readPins) > 0 {
			return principal{readPins: readPins}, true
		}
	}

	token := requestToken(r)
	if token == "" {
		return principal{}, false
	}

	if s.APIToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.APIToken)) == 1 {
		return adminPrincipal(), true
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

	return principal{scopes: scopes, writePins: claims.Pins}, true
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

// RequireRead wraps the read proxy handler. It admits the principals that
// RequireScope(oidc.ScopeRead) admits, and also pin-scoped principals. A
// pin-scoped principal has no read scope, so it gets no other read route.
// The wrapped handler must apply the pin check to such a principal.
func (s *Service) RequireRead(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.readGated() {
			next.ServeHTTP(w, r)

			return
		}

		p, ok := s.authenticate(r)
		if !ok {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)

			return
		}

		if len(p.readPins) == 0 && !slices.Contains(p.scopes, oidc.ScopeRead) {
			http.Error(w, "Forbidden: token lacks scope "+string(oidc.ScopeRead), http.StatusForbidden)

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

	return slices.Contains(p.scopes, oidc.ScopeAdmin) || oidc.GlobMatchAny(p.writePins, name)
}

// requestToken returns the bearer token, or the password of HTTP Basic auth.
// Nix sends credentials from its netrc file only as Basic auth, so this is
// how a plain Nix substituter authenticates to a gated read proxy. It
// ignores the user name.
func requestToken(r *http.Request) string {
	if token, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); found {
		return token
	}

	if _, password, ok := r.BasicAuth(); ok {
		return password
	}

	return ""
}
