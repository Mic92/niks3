// Package oidclogin implements opt-in OIDC device login. Only verified ID
// tokens are used as niks3 bearer credentials; OAuth access tokens are not.
package oidclogin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

const expiryMargin = 30 * time.Second

// Config describes a public OIDC client. Issuer must exactly match discovery.
type Config struct {
	Issuer   string   `json:"issuer"`
	ClientID string   `json:"client_id"`
	Scopes   []string `json:"scopes,omitempty"`
}

// Credential is secret state. It must only be persisted in a secure store.
// Access tokens are deliberately discarded: their expiry is unrelated to the
// lifetime of the ID token used by niks3.
type Credential struct {
	IDToken        string    `json:"id_token"`
	RefreshToken   string    `json:"refresh_token,omitempty"`
	Subject        string    `json:"subject"`
	ExpiresAt      time.Time `json:"expires_at"`
	RefreshAt      time.Time `json:"refresh_at"`
	MissingIDToken bool      `json:"missing_id_token,omitempty"`
}

// Flow holds discovery and verification state for a public client.
type Flow struct {
	issuer   string
	config   oauth2.Config
	verifier *oidc.IDTokenVerifier
	http     *http.Client
	now      func() time.Time
}

// secureURL rejects credential-bearing URLs and insecure endpoints. Tests may
// explicitly use a loopback HTTP issuer; this never permits a downgrade from
// an HTTPS issuer to HTTP.
func secureURL(raw string, loopback bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" {
		return errors.New("invalid OIDC URL")
	}
	if u.Scheme == "https" {
		return nil
	}
	if loopback && u.Scheme == "http" && net.ParseIP(u.Hostname()).IsLoopback() {
		return nil
	}

	return errors.New("OIDC requires HTTPS (HTTP is only allowed for loopback tests)")
}

// New discovers the provider and validates every endpoint used to transmit
// credentials. HTTP errors and provider response bodies are never exposed.
func New(ctx context.Context, cfg Config) (*Flow, error) {
	if cfg.ClientID == "" {
		return nil, errors.New("OIDC client ID is required")
	}
	if err := secureURL(cfg.Issuer, true); err != nil {
		return nil, err
	}
	loopback := strings.HasPrefix(cfg.Issuer, "http://")
	h := &http.Client{
		Transport: http.DefaultTransport, Timeout: 30 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errors.New("OIDC endpoint redirects are not allowed")
		},
	}
	ctx = oidc.ClientContext(ctx, h)
	p, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, safeError(ctx, "OIDC discovery failed")
	}
	var metadata struct {
		JWKS string `json:"jwks_uri"`
	}
	if err := p.Claims(&metadata); err != nil {
		return nil, errors.New("invalid OIDC discovery metadata")
	}
	ep := p.Endpoint()
	for _, raw := range []string{ep.TokenURL, ep.DeviceAuthURL, metadata.JWKS} {
		if raw == "" {
			return nil, errors.New("OIDC provider must advertise token, device authorization and JWKS endpoints")
		}
		if err := secureURL(raw, loopback); err != nil {
			return nil, err
		}
	}
	scopes := slices.Clone(cfg.Scopes)
	if len(scopes) == 0 {
		scopes = []string{oidc.ScopeOpenID, "profile", "offline_access"}
	}
	if !slices.Contains(scopes, oidc.ScopeOpenID) {
		return nil, errors.New("OIDC scopes must include openid")
	}
	// Public device clients authenticate in the form body without a secret.
	ep.AuthStyle = oauth2.AuthStyleInParams

	return &Flow{
		issuer: cfg.Issuer,
		config: oauth2.Config{ClientID: cfg.ClientID, Endpoint: ep, Scopes: scopes},
		// Cached ID tokens may be expired when refreshing, but must still have
		// valid signatures and identities. Expiry is enforced below using the
		// signed exp claim, with a safety margin before any token is returned.
		verifier: p.Verifier(&oidc.Config{ClientID: cfg.ClientID, SkipExpiryCheck: true}), http: h, now: time.Now,
	}, nil
}

// oauth2's device authorization request does not attach ctx in the pinned
// library version. Bind it at the transport too, so cancellation covers that
// initial request as well as polling and refresh.
type contextTransport func(*http.Request) (*http.Response, error)

func (t contextTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t(req)
}

func safeError(ctx context.Context, message string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", message, err)
	}

	return errors.New(message)
}

// Login is the only interactive operation. Instructions go to the supplied
// writer (normally stderr); device_code and token responses never do.
func (f *Flow) Login(ctx context.Context, instructions io.Writer) (*Credential, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	ctx = context.WithValue(ctx, oauth2.HTTPClient, f.http)
	device, err := f.deviceAuth(ctx)
	if err != nil {
		return nil, safeError(ctx, "OIDC device authorization failed")
	}
	if device.DeviceCode == "" || device.UserCode == "" || device.Expiry.IsZero() || !device.Expiry.After(f.now()) || device.Interval < 0 || device.Interval > 900 {
		return nil, errors.New("invalid OIDC device authorization response")
	}
	if err := secureURL(device.VerificationURI, strings.HasPrefix(f.config.Endpoint.DeviceAuthURL, "http://")); err != nil {
		return nil, err
	}
	if _, err := fmt.Fprintf(instructions, "Open %s and enter code %q\n", device.VerificationURI, device.UserCode); err != nil {
		return nil, errors.New("cannot display OIDC device instructions")
	}
	token, err := f.config.DeviceAccessToken(ctx, device)
	if err != nil {
		return nil, safeError(ctx, "OIDC device login failed or expired; run login again")
	}

	return f.credential(ctx, token, "")
}

// Token validates cached state and refreshes when the ID token needs renewal.
// A returned credential must be saved even when err is non-nil: a successful
// refresh grant may already have rotated the refresh token. Missing ID tokens
// allow use of the old token until its expiry margin, without polling on every
// request. They require a new explicit login after that.
func (f *Flow) Token(ctx context.Context, current *Credential) (*Credential, error) {
	verified, err := f.verify(ctx, current.IDToken, current.Subject)
	if err != nil || !verified.Expiry.Equal(current.ExpiresAt) || verified.Subject != current.Subject {
		return nil, errors.New("stored OIDC ID token verification failed; run login again")
	}
	if f.usable(current) && (f.now().Before(current.RefreshAt) || current.MissingIDToken) {
		return current, nil
	}
	if current.MissingIDToken || current.RefreshToken == "" {
		return nil, errors.New("OIDC credential cannot be renewed; run niks3 auth login again")
	}
	// Force a refresh independently of access-token expiry. oauth2.TokenSource
	// caches by access-token lifetime, which cannot govern our ID-token source.
	ctx = context.WithValue(ctx, oauth2.HTTPClient, f.http)
	token, err := f.config.TokenSource(ctx, &oauth2.Token{RefreshToken: current.RefreshToken}).Token()
	if err != nil {
		return nil, safeError(ctx, "OIDC refresh failed; run niks3 auth login again")
	}
	next := *current
	next.RefreshToken = token.RefreshToken
	raw, _ := token.Extra("id_token").(string)
	if raw == "" {
		next.MissingIDToken = true
		if f.usable(&next) {
			return &next, nil
		}

		return &next, errors.New("OIDC refresh omitted an ID token and the previous token expired; run niks3 auth login again")
	}
	renewed, err := f.credential(ctx, token, current.Subject)
	if err != nil {
		next.IDToken = ""

		return &next, err
	}

	return renewed, nil
}

// Ephemeral returns a lazy, concurrency-safe token function. No state is saved
// to disk, and login is never restarted automatically after the first attempt.
func Ephemeral(cfg Config, instructions io.Writer) func(context.Context) (string, error) {
	var mu sync.Mutex
	var flow *Flow
	var credential *Credential
	var loginErr error
	attempted := false

	return func(ctx context.Context) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if !attempted {
			attempted = true
			flow, loginErr = New(ctx, cfg)
			if loginErr == nil {
				credential, loginErr = flow.Login(ctx, instructions)
			}
		}
		if loginErr != nil {
			return "", loginErr
		}
		next, err := flow.Token(ctx, credential)
		if next != nil {
			credential = next
		}
		if err != nil {
			return "", err
		}

		return credential.IDToken, nil
	}
}

func (f *Flow) deviceAuth(ctx context.Context) (*oauth2.DeviceAuthResponse, error) {
	// This context lives until DeviceAuth has consumed the response body.
	// Binding a parent context in a reusable transport would discard the
	// HTTP client's per-request deadline, particularly over HTTP/2.
	ctx, cancel := context.WithTimeout(ctx, f.http.Timeout)
	defer cancel()
	h := *f.http
	h.Transport = contextTransport(func(req *http.Request) (*http.Response, error) {
		resp, err := f.http.Transport.RoundTrip(req.WithContext(ctx))
		if err != nil {
			return nil, fmt.Errorf("OIDC HTTP request: %w", err)
		}

		return resp, nil
	})

	device, err := f.config.DeviceAuth(context.WithValue(ctx, oauth2.HTTPClient, &h))
	if err != nil {
		return nil, fmt.Errorf("authorizing OIDC device: %w", err)
	}

	return device, nil
}

func (f *Flow) credential(ctx context.Context, token *oauth2.Token, subject string) (*Credential, error) {
	raw, _ := token.Extra("id_token").(string)
	if raw == "" {
		return nil, errors.New("OIDC response has no ID token; run login again")
	}
	verified, err := f.verify(ctx, raw, subject)
	if err != nil {
		return nil, err
	}
	lifetime := verified.Expiry.Sub(f.now())
	if lifetime <= expiryMargin {
		return nil, errors.New("OIDC ID token expires too soon; run login again")
	}

	return &Credential{
		IDToken: raw, RefreshToken: token.RefreshToken, Subject: verified.Subject,
		ExpiresAt: verified.Expiry, RefreshAt: f.now().Add(lifetime * 3 / 4),
	}, nil
}

func (f *Flow) verify(ctx context.Context, raw, subject string) (*oidc.IDToken, error) {
	verified, err := f.verifier.Verify(oidc.ClientContext(ctx, f.http), raw)
	if err != nil {
		return nil, errors.New("OIDC ID token verification failed; run login again")
	}
	if verified.Issuer != f.issuer {
		return nil, errors.New("OIDC ID token issuer mismatch")
	}
	if verified.Subject == "" || (subject != "" && verified.Subject != subject) {
		return nil, errors.New("OIDC ID token subject mismatch; run login again")
	}
	var claims struct {
		AuthorizedParty string   `json:"azp"`
		NotBefore       *float64 `json:"nbf"`
	}
	if err := verified.Claims(&claims); err != nil {
		return nil, errors.New("invalid OIDC ID token claims")
	}
	// SkipExpiryCheck also disables go-oidc's nbf check. Enforce signed
	// temporal claims here, including when validating an expired cached JWT
	// solely to renew its refresh grant.
	latest := f.now().Add(expiryMargin)
	if verified.IssuedAt.IsZero() || verified.IssuedAt.After(latest) || !verified.Expiry.After(verified.IssuedAt) {
		return nil, errors.New("invalid OIDC ID token issue time")
	}
	if claims.NotBefore != nil && *claims.NotBefore > float64(latest.Unix()) {
		return nil, errors.New("OIDC ID token is not valid yet")
	}
	if (len(verified.Audience) > 1 && claims.AuthorizedParty == "") || (claims.AuthorizedParty != "" && claims.AuthorizedParty != f.config.ClientID) {
		return nil, errors.New("OIDC ID token authorized party mismatch")
	}

	return verified, nil
}

func (f *Flow) usable(c *Credential) bool {
	return c.IDToken != "" && f.now().Add(expiryMargin).Before(c.ExpiresAt)
}
