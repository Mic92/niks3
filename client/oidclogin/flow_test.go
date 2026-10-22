package oidclogin //nolint:testpackage // exercises internal validation, clock, locking and wire protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/server/oidc/oidcmock"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/oauth2"
)

type testIssuer struct {
	server    *httptest.Server
	key       *oidcmock.Keypair
	response  atomic.Value
	polls     atomic.Int64
	refreshes atomic.Int64
}

type issuerResponse struct {
	claims           jwt.MapClaims
	omitID           bool
	omitRefresh      bool
	oauthError       string
	pending          bool
	slow             bool
	deviceInvalid    bool
	blockDevice      bool
	badDiscovery     bool
	noDevice         bool
	insecureEndpoint bool
	redirect         bool
}

func newTestIssuer(t *testing.T, response issuerResponse) *testIssuer {
	t.Helper()
	key, err := oidcmock.NewKeypair()
	if err != nil {
		t.Fatal(err)
	}
	m := &testIssuer{key: key}
	m.response.Store(response)
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spec, ok := m.response.Load().(issuerResponse)
		if !ok {
			t.Error("invalid issuer fixture")

			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			issuer := m.server.URL
			if spec.badDiscovery {
				issuer += "/wrong"
			}
			device := m.server.URL + "/device"
			if spec.noDevice {
				device = ""
			}
			if spec.insecureEndpoint {
				device = "http://example.com/device"
			}
			if spec.redirect {
				http.Redirect(w, r, m.server.URL+"/other", http.StatusTemporaryRedirect)

				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"issuer": issuer, "token_endpoint": m.server.URL + "/token", "device_authorization_endpoint": device, "jwks_uri": m.server.URL + "/keys", "id_token_signing_alg_values_supported": []string{"RS256"}})
		case "/keys":
			data, keyErr := key.JWKS()
			if keyErr != nil {
				t.Error(keyErr)
				w.WriteHeader(http.StatusInternalServerError)

				return
			}
			_, _ = w.Write(data)
		case "/device":
			_, _ = io.Copy(io.Discard, r.Body)
			if spec.blockDevice {
				<-r.Context().Done()

				return
			}
			interval := 1
			if spec.deviceInvalid {
				interval = -1
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"device_code": "secret-device", "user_code": "ABCD", "verification_uri": "https://login.example.com/verify", "expires_in": 600, "interval": interval})
		case "/token":
			if err := r.ParseForm(); err != nil {
				t.Error(err)

				return
			}
			if r.Form.Get("client_id") != "cli" || r.Form.Get("client_secret") != "" {
				t.Error("expected public client without secret")
			}
			grant := r.Form.Get("grant_type")
			n := m.polls.Add(1)
			if grant == "refresh_token" {
				m.refreshes.Add(1)
			}
			oauthErr := spec.oauthError
			if spec.pending && n == 1 {
				oauthErr = "authorization_pending"
			}
			if spec.slow && n == 2 {
				oauthErr = "slow_down"
			}
			if oauthErr != "" {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": oauthErr, "error_description": "secret-device secret-refresh"})

				return
			}
			result := map[string]any{"access_token": "opaque-access", "token_type": "Bearer", "expires_in": 7200}
			if !spec.omitRefresh {
				result["refresh_token"] = "secret-rotated"
			}
			if !spec.omitID {
				claims := jwt.MapClaims{"iss": m.server.URL, "aud": "cli", "sub": "alice", "iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix()}
				maps.Copy(claims, spec.claims)
				signed, signErr := key.SignJWT(claims)
				if signErr != nil {
					t.Error(signErr)

					return
				}
				result["id_token"] = signed
			}
			_ = json.NewEncoder(w).Encode(result)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(m.server.Close)

	return m
}

func (m *testIssuer) config() Config { return Config{Issuer: m.server.URL, ClientID: "cli"} }
func testFlow(t *testing.T, m *testIssuer) *Flow {
	t.Helper()
	f, err := New(t.Context(), m.config())
	if err != nil {
		t.Fatal(err)
	}

	return f
}

func initialCredential(t *testing.T, f *Flow, m *testIssuer) *Credential {
	t.Helper()
	signed, err := m.key.SignJWT(jwt.MapClaims{"iss": m.server.URL, "aud": "cli", "sub": "alice", "iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix()})
	if err != nil {
		t.Fatal(err)
	}
	c, err := f.credential(t.Context(), (&oauth2.Token{AccessToken: "opaque-access", RefreshToken: "secret-refresh"}).WithExtra(map[string]any{"id_token": signed}), "")
	if err != nil {
		t.Fatal(err)
	}

	return c
}

func TestDeviceLogin(t *testing.T) {
	t.Parallel()
	m := newTestIssuer(t, issuerResponse{pending: true, slow: true})
	f := testFlow(t, m)
	var instructions bytes.Buffer
	c, err := f.Login(t.Context(), &instructions)
	if err != nil {
		t.Fatal(err)
	}
	if c.Subject != "alice" || c.IDToken == "opaque-access" || m.polls.Load() != 3 {
		t.Fatal("device polling did not produce a verified ID token")
	}
	if !strings.Contains(instructions.String(), "ABCD") || strings.Contains(instructions.String(), "secret-") {
		t.Fatal("device instructions missing code or leaking a secret")
	}
}

func TestDeviceFailures(t *testing.T) {
	t.Parallel()
	for name, spec := range map[string]issuerResponse{
		"denied": {oauthError: "access_denied"}, "expired": {oauthError: "expired_token"},
		"no-id": {omitID: true}, "wrong-aud": {claims: jwt.MapClaims{"aud": "other"}},
		"wrong-issuer":     {claims: jwt.MapClaims{"iss": "https://wrong.example.com"}},
		"expired-id":       {claims: jwt.MapClaims{"exp": time.Now().Add(-time.Minute).Unix()}},
		"future-nbf":       {claims: jwt.MapClaims{"nbf": time.Now().Add(time.Hour).Unix()}},
		"future-iat":       {claims: jwt.MapClaims{"iat": time.Now().Add(time.Hour).Unix()}},
		"wrong-azp":        {claims: jwt.MapClaims{"azp": "other"}},
		"multi-aud-no-azp": {claims: jwt.MapClaims{"aud": []string{"cli", "other"}}},
		"invalid-interval": {deviceInvalid: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := newTestIssuer(t, spec)
			_, err := testFlow(t, m).Login(t.Context(), io.Discard)
			if err == nil {
				t.Fatal("invalid login accepted")
			}
			if strings.Contains(err.Error(), "secret-") || strings.Contains(err.Error(), "opaque-access") {
				t.Fatal("provider error leaked secrets")
			}
		})
	}
}

func TestDiscoveryFailures(t *testing.T) {
	t.Parallel()
	for name, spec := range map[string]issuerResponse{"issuer": {badDiscovery: true}, "no-device": {noDevice: true}, "http-endpoint": {insecureEndpoint: true}, "redirect": {redirect: true}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := newTestIssuer(t, spec)
			if _, err := New(t.Context(), m.config()); err == nil {
				t.Fatal("invalid discovery accepted")
			}
		})
	}
	if _, err := New(t.Context(), Config{Issuer: "http://example.com", ClientID: "cli"}); err == nil {
		t.Fatal("insecure issuer accepted")
	}
}

func TestDeviceCancellation(t *testing.T) {
	t.Parallel()
	for _, block := range []bool{false, true} {
		t.Run(map[bool]string{false: "polling", true: "initial-request"}[block], func(t *testing.T) {
			t.Parallel()
			m := newTestIssuer(t, issuerResponse{blockDevice: block})
			f := testFlow(t, m)
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			started := time.Now()
			_, err := f.Login(ctx, io.Discard)
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 3*time.Second {
				t.Fatalf("login cancellation failed: %v", err)
			}
		})
	}
}

func TestRefreshContracts(t *testing.T) {
	t.Parallel()
	for _, omit := range []bool{false, true} {
		t.Run(map[bool]string{false: "new-id", true: "no-id"}[omit], func(t *testing.T) {
			t.Parallel()
			m := newTestIssuer(t, issuerResponse{omitID: omit})
			f := testFlow(t, m)
			c := initialCredential(t, f, m)
			c.RefreshAt = time.Now().Add(-time.Minute)
			next, err := f.Token(t.Context(), c)
			if err != nil {
				t.Fatal(err)
			}
			if next.RefreshToken != "secret-rotated" || m.refreshes.Load() != 1 {
				t.Fatal("refresh rotation lost")
			}
			if omit && (next.IDToken != c.IDToken || !next.ExpiresAt.Equal(c.ExpiresAt)) {
				t.Fatal("ID-less refresh changed token or lifetime")
			}
			// An access token with two hours' lifetime must not govern renewal.
			next.RefreshAt = time.Now().Add(-time.Minute)
			if _, err := f.Token(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			want := int64(2)
			if omit {
				want = 1
			}
			if m.refreshes.Load() != want {
				t.Fatal("incorrect refresh scheduling")
			}
			if omit {
				f.now = func() time.Time { return next.ExpiresAt.Add(-expiryMargin) }
				if _, err := f.Token(t.Context(), next); err == nil {
					t.Fatal("expired ID-less refresh credential accepted")
				}
			}
		})
	}
}

func TestTokenWithoutRefreshToken(t *testing.T) {
	t.Parallel()
	m := newTestIssuer(t, issuerResponse{})
	f := testFlow(t, m)
	c := initialCredential(t, f, m)
	c.RefreshToken = ""
	for _, tc := range []struct {
		name string
		now  time.Time
		want bool
	}{
		{name: "before-refresh", now: c.RefreshAt.Add(-time.Second), want: true},
		{name: "at-refresh", now: c.RefreshAt, want: true},
		{name: "before-expiry-margin", now: c.ExpiresAt.Add(-expiryMargin - time.Second), want: true},
		{name: "at-expiry-margin", now: c.ExpiresAt.Add(-expiryMargin)},
		{name: "expired", now: c.ExpiresAt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			flow := testFlow(t, m)
			flow.now = func() time.Time { return tc.now }
			next, err := flow.Token(t.Context(), c)
			if tc.want {
				if err != nil || next != c {
					t.Fatalf("usable cached token was not returned: %v", err)
				}
			} else if err == nil || next != nil {
				t.Fatal("unusable token accepted without a refresh token")
			}
			if m.polls.Load() != 0 {
				t.Fatal("token endpoint called without a refresh token")
			}
		})
	}
}

func TestRefreshRejectsIdentityChangeAndKeepsRotation(t *testing.T) {
	t.Parallel()
	m := newTestIssuer(t, issuerResponse{claims: jwt.MapClaims{"sub": "bob"}})
	f := testFlow(t, m)
	c := initialCredential(t, f, m)
	c.RefreshAt = time.Time{}
	next, err := f.Token(t.Context(), c)
	if err == nil || next == nil || next.IDToken != "" || next.RefreshToken != "secret-rotated" {
		t.Fatal("invalid refreshed identity or rotation handling")
	}
}

func TestRefreshRetainsOmittedRefreshToken(t *testing.T) {
	t.Parallel()
	m := newTestIssuer(t, issuerResponse{omitRefresh: true})
	f := testFlow(t, m)
	c := initialCredential(t, f, m)
	c.RefreshAt = time.Time{}
	next, err := f.Token(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if next.RefreshToken != "secret-refresh" {
		t.Fatal("omitted refresh token was not preserved")
	}
}

func TestExpiredTokenRefresh(t *testing.T) {
	t.Parallel()
	for _, omit := range []bool{false, true} {
		t.Run(map[bool]string{false: "new-id", true: "no-id"}[omit], func(t *testing.T) {
			t.Parallel()
			m := newTestIssuer(t, issuerResponse{omitID: omit})
			f := testFlow(t, m)
			c := initialCredential(t, f, m)
			f.now = func() time.Time { return c.ExpiresAt.Add(-expiryMargin) }
			// Issue a fresh token long enough to remain usable at the simulated time.
			m.response.Store(issuerResponse{omitID: omit, claims: jwt.MapClaims{"exp": c.ExpiresAt.Add(time.Hour).Unix()}})
			next, err := f.Token(t.Context(), c)
			if omit {
				if err == nil || next == nil || next.RefreshToken != "secret-rotated" {
					t.Fatal("expired ID token reused or rotation lost")
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFractionalTemporalClaims(t *testing.T) {
	t.Parallel()
	m := newTestIssuer(t, issuerResponse{})
	f := testFlow(t, m)
	raw, err := m.key.SignJWT(jwt.MapClaims{"iss": m.server.URL, "aud": "cli", "sub": "alice", "iat": float64(time.Now().Unix()) - 0.5, "nbf": float64(time.Now().Unix()) - 1.5, "exp": time.Now().Add(time.Hour).Unix()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.credential(t.Context(), (&oauth2.Token{AccessToken: "opaque"}).WithExtra(map[string]any{"id_token": raw}), "")
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeviceHTTPRequestTimeout(t *testing.T) {
	t.Parallel()
	m := newTestIssuer(t, issuerResponse{blockDevice: true})
	f := testFlow(t, m)
	f.http.Timeout = 100 * time.Millisecond
	started := time.Now()
	if _, err := f.Login(t.Context(), io.Discard); err == nil || time.Since(started) > 3*time.Second {
		t.Fatal("initial device request ignored HTTP timeout")
	}
}
