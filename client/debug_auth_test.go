package client_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Mic92/niks3/client"
)

func TestDebugHTTPRedactsCredentials(t *testing.T) { //nolint:paralleltest // replaces the process-wide logger to inspect secret redaction
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-bearer" {
			t.Error("redaction changed actual authorization")
		}
		w.Header().Set("Set-Cookie", "secret-response-cookie")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	c := client.NewTestClientWithToken(&http.Client{}, client.DefaultRetryConfig(), client.StaticToken("secret-bearer"))
	c.SetDebugHTTP(true)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Cookie", "secret-request-cookie")
	resp, err := c.DoServerRequest(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	for _, secret := range []string{"secret-bearer", "secret-response-cookie", "secret-request-cookie"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatal("HTTP debug logging exposed credentials")
		}
	}
	if !strings.Contains(logs.String(), "[REDACTED]") {
		t.Fatal("debug request not logged")
	}
}

func TestAuthenticatedRedirectStaysOnServer(t *testing.T) {
	t.Parallel()
	var received atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	c, err := client.NewClient(t.Context(), origin.URL, "secret-bearer")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.ListPins(t.Context())
	if err == nil || received.Load() {
		t.Fatal("authenticated redirect escaped the configured server")
	}
}
