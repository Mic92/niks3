// Package oidc provides OIDC authentication for niks3.
package oidc

import (
	"encoding/json"
	"errors"
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

// Scope is a permission class granted to an authenticated principal.
type Scope string

const (
	// ScopeRead allows fetching from the read proxy when it is gated.
	ScopeRead Scope = "read"
	// ScopeWrite allows uploading store paths.
	ScopeWrite Scope = "write"
	// ScopeAdmin allows GC, pin management and closure deletion.
	ScopeAdmin Scope = "admin"
)

func (s Scope) valid() bool {
	return s == ScopeRead || s == ScopeWrite || s == ScopeAdmin
}

// Rule grants Scopes to tokens matching BoundClaims and BoundSubject.
type Rule struct {
	BoundClaims  map[string][]string `json:"bound_claims,omitempty"`
	BoundSubject []string            `json:"bound_subject,omitempty"`
	Scopes       []Scope             `json:"scopes"`
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

	// Scopes granted when the top-level BoundClaims/BoundSubject match.
	// Defaults to [write]. Mutually exclusive with Rules.
	Scopes []Scope `json:"scopes,omitempty"`

	// Rules grant scopes per matching rule (union). When set, the
	// top-level BoundClaims/BoundSubject/Scopes must be empty.
	Rules []Rule `json:"rules,omitempty"`

	// CAFile and BearerTokenFile are used when fetching discovery/JWKS,
	// e.g. from a Kubernetes API server (private CA, authenticated).
	CAFile          string `json:"ca_file,omitempty"`
	BearerTokenFile string `json:"bearer_token_file,omitempty"`

	// name is set from the map key during config loading
	name string
}

// Name returns the provider name.
func (p *ProviderConfig) Name() string {
	return p.name
}

// effectiveRules folds the legacy top-level bound_* fields into a rule list.
func (p *ProviderConfig) effectiveRules() []Rule {
	if len(p.Rules) > 0 {
		return p.Rules
	}

	scopes := p.Scopes
	if len(scopes) == 0 {
		scopes = []Scope{ScopeWrite}
	}

	return []Rule{{BoundClaims: p.BoundClaims, BoundSubject: p.BoundSubject, Scopes: scopes}}
}

func validateScopes(scopes []Scope) error {
	for _, s := range scopes {
		if !s.valid() {
			return fmt.Errorf("unknown scope %q (want read, write or admin)", s)
		}
	}

	return nil
}

func (p *ProviderConfig) validateRules() error {
	if len(p.Rules) == 0 {
		return validateScopes(p.Scopes)
	}

	if len(p.BoundClaims) > 0 || len(p.BoundSubject) > 0 || len(p.Scopes) > 0 {
		return errors.New("rules cannot be combined with top-level bound_claims/bound_subject/scopes")
	}

	for i, r := range p.Rules {
		if len(r.Scopes) == 0 {
			return fmt.Errorf("rules[%d]: scopes must not be empty", i)
		}

		if err := validateScopes(r.Scopes); err != nil {
			return fmt.Errorf("rules[%d]: %w", i, err)
		}
	}

	return nil
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
		return errors.New("no providers configured")
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

		if err := provider.validateRules(); err != nil {
			return fmt.Errorf("provider %q: %w", name, err)
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
