// Package oidc provides OIDC authentication for niks3.
package oidc

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
)

// Config holds the OIDC configuration with multiple providers.
type Config struct {
	Providers map[string]*ProviderConfig `json:"providers"`

	// AllowInsecure permits HTTP issuers instead of requiring HTTPS.
	// This should ONLY be used for testing purposes.
	AllowInsecure bool `json:"allow_insecure,omitempty"`
}

// ProviderConfig configures a single OIDC provider.
type ProviderConfig struct {
	// Issuer is the OIDC issuer URL (required).
	// Used to construct discovery URL: {issuer}/.well-known/openid-configuration
	// Example: "https://token.actions.githubusercontent.com"
	Issuer string `json:"issuer"`

	// Audience is the expected audience claim (required).
	// Should be your service URL or a unique identifier.
	// Example: "https://cache.example.com"
	Audience string `json:"audience"`

	// BoundClaims specifies claims that must match for authorization (optional).
	// All specified claims must match (AND logic).
	// Values support glob patterns.
	// Example: {"repository_owner": ["myorg"], "ref": ["refs/heads/main", "refs/tags/*"]}
	BoundClaims map[string][]string `json:"bound_claims,omitempty"`

	// BoundSubject specifies subject patterns that must match (optional).
	// If set, the 'sub' claim must match one of these patterns.
	// Example: ["repo:myorg/*:*"]
	BoundSubject []string `json:"bound_subject,omitempty"`

	// name is set from the map key during config loading
	name string
}

// Name returns the provider name.
func (p *ProviderConfig) Name() string {
	return p.name
}

// LoadConfig loads OIDC configuration from a JSON file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	// Set provider names from map keys
	for name, provider := range cfg.Providers {
		provider.name = name
	}

	return &cfg, nil
}

func (c *Config) validate() error {
	if len(c.Providers) == 0 {
		return fmt.Errorf("no providers configured")
	}

	issuers := make(map[string]string) // issuer -> provider name
	for name, provider := range c.Providers {
		if provider.Issuer == "" {
			return fmt.Errorf("provider %q: missing issuer", name)
		}

		// Validate issuer URL format and require HTTPS (unless AllowInsecure)
		issuerURL, err := url.Parse(provider.Issuer)
		if err != nil {
			return fmt.Errorf("provider %q: invalid issuer URL %q: %w", name, provider.Issuer, err)
		}
		if issuerURL.Scheme == "" || issuerURL.Host == "" {
			return fmt.Errorf("provider %q: issuer URL %q must be absolute with scheme and host", name, provider.Issuer)
		}
		if issuerURL.Scheme != "https" && !c.AllowInsecure {
			return fmt.Errorf("provider %q: issuer URL %q must use HTTPS (scheme is %q)", name, provider.Issuer, issuerURL.Scheme)
		}

		if provider.Audience == "" {
			return fmt.Errorf("provider %q: missing audience", name)
		}

		// Check for duplicate issuers
		if existing, ok := issuers[provider.Issuer]; ok {
			return fmt.Errorf("provider %q: duplicate issuer (already used by %q)",
				name, existing)
		}
		issuers[provider.Issuer] = name
	}

	return nil
}
