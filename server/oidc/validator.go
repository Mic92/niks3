package oidc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

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
	return fmt.Sprintf("OIDC validation failed: %s", e.Reason)
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
	// RawClaims contains all claims for logging/debugging
	RawClaims map[string]any
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

		// Create the OIDC provider (handles discovery and JWKS)
		provider, err := gooidc.NewProvider(ctx, providerCfg.Issuer)
		if err != nil {
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

		slog.Info("OIDC provider initialized", "name", name)
	}

	return v, nil
}

// ValidateToken validates a JWT token and returns the validated claims.
// On failure, returns a *ValidationError with detailed information about why validation failed.
func (v *Validator) ValidateToken(ctx context.Context, tokenString string) (*ValidatedClaims, error) {
	var triedProviders []string

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

		// Validate bound claims (our custom authorization logic)
		if err := validateBoundClaims(claims, pv.config.BoundClaims); err != nil {
			slog.Debug("Bound claims validation failed", "provider", pv.config.Name(), "error", err)
			return nil, &ValidationError{
				Reason:         fmt.Sprintf("bound claims validation failed: %v", err),
				Provider:       pv.config.Name(),
				Claims:         claims,
				TriedProviders: triedProviders,
			}
		}

		// Validate bound subject
		if err := validateBoundSubject(claims, pv.config.BoundSubject); err != nil {
			slog.Debug("Bound subject validation failed", "provider", pv.config.Name(), "error", err)
			return nil, &ValidationError{
				Reason:         fmt.Sprintf("bound subject validation failed: %v", err),
				Provider:       pv.config.Name(),
				Claims:         claims,
				TriedProviders: triedProviders,
			}
		}

		return &ValidatedClaims{
			Subject:   idToken.Subject,
			Issuer:    issuer,
			Provider:  pv.config.Name(),
			RawClaims: claims,
		}, nil
	}

	// No provider matched - return detailed error
	return nil, &ValidationError{
		Reason:         "no provider could verify the token (signature or issuer mismatch)",
		TriedProviders: triedProviders,
	}
}
