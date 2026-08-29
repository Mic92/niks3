package oidc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"

	gooidc "github.com/coreos/go-oidc/v3/oidc"
)

// ErrTokenValidationFailed is returned when token validation fails.
// Use errors.As to get the detailed *ValidationError.
var ErrTokenValidationFailed = errors.New("token validation failed")

// ValidationError contains detailed information about why token validation failed.
type ValidationError struct {
	// Reason is a human-readable description of why validation failed
	Reason string
	// Provider is the provider name that was tried (if any)
	Provider string
	// Claims contains the token claims (if successfully parsed)
	Claims map[string]any
	// TriedProviders lists all providers that were attempted
	TriedProviders []string
}

func (e *ValidationError) Error() string {
	if e.Provider != "" {
		return fmt.Sprintf("OIDC validation failed for provider %q: %s", e.Provider, e.Reason)
	}

	return "OIDC validation failed: " + e.Reason
}

// Unwrap allows errors.Is to work with ErrTokenValidationFailed.
func (e *ValidationError) Unwrap() error {
	return ErrTokenValidationFailed
}

// Validator validates OIDC tokens against configured providers.
type Validator struct {
	config    *Config
	verifiers map[string]*providerVerifier // issuer -> verifier
}

type providerVerifier struct {
	provider *gooidc.Provider
	verifier *gooidc.IDTokenVerifier
	config   *ProviderConfig
}

// ValidatedClaims contains the validated claims from a token.
type ValidatedClaims struct {
	// Subject is the 'sub' claim
	Subject string
	// Issuer is the 'iss' claim
	Issuer string
	// Provider is the provider name from config
	Provider string
	// Scopes is the union of scopes of all matching rules.
	Scopes []Scope
	// RawClaims contains all claims for logging/debugging
	RawClaims map[string]any
}

// bearerFileTransport re-reads the token per request to follow rotation.
type bearerFileTransport struct {
	path string
	base http.RoundTripper
}

func (t *bearerFileTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := os.ReadFile(t.path)
	if err != nil {
		return nil, fmt.Errorf("reading bearer token file: %w", err)
	}

	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))

	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, fmt.Errorf("oidc discovery request: %w", err)
	}

	return resp, nil
}

func httpClientFor(p *ProviderConfig) (*http.Client, error) {
	if p.CAFile == "" && p.BearerTokenFile == "" {
		return http.DefaultClient, nil
	}

	transport := &http.Transport{Proxy: http.ProxyFromEnvironment}

	if p.CAFile != "" {
		pem, err := os.ReadFile(p.CAFile)
		if err != nil {
			return nil, fmt.Errorf("reading ca_file: %w", err)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file %s contains no certificates", p.CAFile)
		}

		transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}

	var rt http.RoundTripper = transport
	if p.BearerTokenFile != "" {
		rt = &bearerFileTransport{path: p.BearerTokenFile, base: transport}
	}

	return &http.Client{Transport: rt}, nil
}

// asymmetricAlgs is accepted when discovery is skipped. The JWKS key type
// still pins the actual algorithm.
var asymmetricAlgs = []string{ //nolint:gochecknoglobals
	gooidc.RS256, gooidc.RS384, gooidc.RS512,
	gooidc.ES256, gooidc.ES384, gooidc.ES512,
	gooidc.PS256, gooidc.PS384, gooidc.PS512,
	gooidc.EdDSA,
}

// NewValidator creates a new OIDC validator from config.
func NewValidator(ctx context.Context, cfg *Config) (*Validator, error) {
	v := &Validator{
		config:    cfg,
		verifiers: make(map[string]*providerVerifier, len(cfg.Providers)),
	}

	// Initialize each provider
	for name, providerCfg := range cfg.Providers {
		slog.Debug("Initializing OIDC provider", "name", name, "issuer", providerCfg.Issuer)

		client, err := httpClientFor(providerCfg)
		if err != nil {
			return nil, fmt.Errorf("provider %q: %w", name, err)
		}

		// go-oidc reuses this context's client for later JWKS fetches.
		clientCtx := gooidc.ClientContext(ctx, client)

		var provider *gooidc.Provider
		if providerCfg.JWKSURL != "" {
			provider = (&gooidc.ProviderConfig{
				IssuerURL:  providerCfg.Issuer,
				JWKSURL:    providerCfg.JWKSURL,
				Algorithms: asymmetricAlgs,
			}).NewProvider(clientCtx)
		} else if provider, err = gooidc.NewProvider(clientCtx, providerCfg.Issuer); err != nil {
			return nil, fmt.Errorf("failed to initialize OIDC provider for %s: %w", providerCfg.Issuer, err)
		}

		// Create the verifier with our expected audience
		verifier := provider.Verifier(&gooidc.Config{
			ClientID: providerCfg.Audience,
		})

		v.verifiers[providerCfg.Issuer] = &providerVerifier{
			provider: provider,
			verifier: verifier,
			config:   providerCfg,
		}

		slog.Info("OIDC provider initialized", "name", name, "issuer", providerCfg.Issuer)
	}

	return v, nil
}

// GrantsScope reports whether any configured rule can grant s.
func (v *Validator) GrantsScope(s Scope) bool {
	for _, p := range v.config.Providers {
		for _, r := range p.effectiveRules() {
			if slices.Contains(r.Scopes, s) {
				return true
			}
		}
	}

	return false
}

// AudienceForIssuer returns the configured audience for the given issuer URL,
// and whether a provider is configured for that issuer.
func (v *Validator) AudienceForIssuer(issuer string) (string, bool) {
	for _, p := range v.config.Providers {
		if p.Issuer == issuer {
			return p.Audience, true
		}
	}

	return "", false
}

// ValidateToken validates a JWT token and returns the validated claims.
// On failure, returns a *ValidationError with detailed information about why validation failed.
func (v *Validator) ValidateToken(ctx context.Context, tokenString string) (*ValidatedClaims, error) {
	triedProviders := make([]string, 0, len(v.verifiers))

	// Try each provider's verifier
	// go-oidc will check issuer claim matches the provider
	for issuer, pv := range v.verifiers {
		triedProviders = append(triedProviders, pv.config.Name())

		idToken, err := pv.verifier.Verify(ctx, tokenString)
		if err != nil {
			slog.Debug("OIDC token verification failed", "provider", pv.config.Name(), "error", err)

			continue
		}

		// Token verified! Extract claims
		var claims map[string]any
		if err := idToken.Claims(&claims); err != nil {
			slog.Debug("Failed to extract claims", "provider", pv.config.Name(), "error", err)

			return nil, &ValidationError{
				Reason:         fmt.Sprintf("failed to extract claims: %v", err),
				Provider:       pv.config.Name(),
				Claims:         nil,
				TriedProviders: triedProviders,
			}
		}

		scopes, err := matchRules(claims, pv.config.effectiveRules())
		if err != nil {
			slog.Debug("No rule matched", "provider", pv.config.Name(), "error", err)

			return nil, &ValidationError{
				Reason:         err.Error(),
				Provider:       pv.config.Name(),
				Claims:         claims,
				TriedProviders: triedProviders,
			}
		}

		return &ValidatedClaims{
			Subject:   idToken.Subject,
			Issuer:    issuer,
			Provider:  pv.config.Name(),
			Scopes:    scopes,
			RawClaims: claims,
		}, nil
	}

	// No provider matched - return detailed error
	return nil, &ValidationError{
		Reason:         "no provider could verify the token (signature or issuer mismatch)",
		TriedProviders: triedProviders,
	}
}
