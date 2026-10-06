package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTransit(t *testing.T, handler http.HandlerFunc) *transit {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("s3cr3t\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	return &transit{client: server.Client(), signURL: server.URL + "/v1/transit/sign/niks3", tokenFile: tokenFile}
}

func TestSign(t *testing.T) {
	t.Parallel()

	signer := newTransit(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/transit/sign/niks3" || r.Header.Get("X-Vault-Token") != "s3cr3t" {
			http.Error(w, `{"errors":["permission denied"]}`, http.StatusForbidden)

			return
		}

		var body struct {
			Input string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Input != base64.StdEncoding.EncodeToString([]byte("fp")) {
			http.Error(w, `{"errors":["bad input"]}`, http.StatusBadRequest)

			return
		}

		_, _ = w.Write([]byte(`{"data":{"signature":"vault:v1:` + base64.StdEncoding.EncodeToString([]byte("sig")) + `"}}`))
	})

	signature, err := signer.sign(t.Context(), []byte("fp"))
	if err != nil || string(signature) != "sig" {
		t.Fatalf("sign = %q, %v", signature, err)
	}
}

func TestSignErrors(t *testing.T) {
	t.Parallel()

	for name, handler := range map[string]http.HandlerFunc{
		"http error": func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, `{"errors":["sealed"]}`, http.StatusServiceUnavailable)
		},
		"invalid json": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("nope")) },
		"no prefix":    func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"data":{"signature":"c2ln"}}`)) },
		"invalid base64": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"data":{"signature":"vault:v1:!"}}`))
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if _, err := newTransit(t, handler).sign(t.Context(), []byte("fp")); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestServe(t *testing.T) {
	t.Parallel()

	in := strings.NewReader(`{"id":1,"fingerprints":{"a.narinfo":"YQ=="}}
{"id":2,"fingerprints":{"a.narinfo":"YQ==","bad.narinfo":"Yg=="}}
{"id":3,"fingerprints":{}}
`)

	var out bytes.Buffer

	err := serve(t.Context(), in, &out, "key-1", func(_ context.Context, fingerprint []byte) ([]byte, error) {
		if string(fingerprint) == "b" {
			return nil, errors.New("denied")
		}

		return fingerprint, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	want := `{"id":1,"signatures":{"a.narinfo":"key-1:YQ=="}}
{"id":2,"error":"narinfo \"bad.narinfo\": denied"}
{"id":3,"signatures":{}}
`
	if out.String() != want {
		t.Fatalf("got:\n%s\nwant:\n%s", out.String(), want)
	}
}

func TestServeRejectsGarbage(t *testing.T) {
	t.Parallel()

	if err := serve(t.Context(), strings.NewReader("not json\n"), &bytes.Buffer{}, "k", nil); err == nil {
		t.Fatal("expected an error")
	}
}
