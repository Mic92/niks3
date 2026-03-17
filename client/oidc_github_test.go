package client_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Mic92/niks3/client"
)

func TestGitHubOIDCTokenSource_Token(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer request-token" {
			http.Error(w, "bad auth", http.StatusUnauthorized)

			return
		}

		if r.URL.Query().Get("audience") != "https://cache.example.com" {
			http.Error(w, "bad audience", http.StatusBadRequest)

			return
		}

		// The real endpoint URL already has query params; make sure we didn't
		// clobber them.
		if r.URL.Query().Get("api-version") != "2.0" {
			http.Error(w, "lost existing query param", http.StatusBadRequest)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"value":"eyJfake.jwt.token"}`)
	}))
	defer srv.Close()

	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "request-token")
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_URL", srv.URL+"?api-version=2.0")

	src := &client.GitHubOIDCTokenSource{
		Audience:   "https://cache.example.com",
		HTTPClient: srv.Client(),
	}

	tok, err := src.Token(t.Context())
	if err != nil {
		t.Fatalf("Token() error = %v", err)
	}

	if tok != "eyJfake.jwt.token" {
		t.Errorf("Token() = %q, want %q", tok, "eyJfake.jwt.token")
	}
}

func TestGitHubOIDCTokenSource_NotInGitHubActions(t *testing.T) {
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "")
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_URL", "")

	src := &client.GitHubOIDCTokenSource{Audience: "x"}

	_, err := src.Token(t.Context())
	if !errors.Is(err, client.ErrNotInGitHubActions) {
		t.Errorf("Token() error = %v, want ErrNotInGitHubActions", err)
	}
}

func TestGitHubOIDCTokenSource_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "request-token")
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_URL", srv.URL)

	src := &client.GitHubOIDCTokenSource{
		Audience:   "x",
		HTTPClient: srv.Client(),
	}

	_, err := src.Token(t.Context())
	if err == nil {
		t.Fatal("Token() expected error on 500, got nil")
	}
}
