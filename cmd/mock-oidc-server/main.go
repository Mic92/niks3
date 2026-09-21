// mock-oidc-server runs the oidcmock provider for NixOS integration tests.
// Besides discovery and JWKS it exposes a separate /issue endpoint that mints
// signed tokens with arbitrary claims.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Mic92/niks3/server/oidc/oidcmock"
	"github.com/golang-jwt/jwt/v5"
)

const (
	defaultExpirySeconds = 3600
	shutdownTimeout      = 5 * time.Second
	readHeaderTimeout    = 5 * time.Second
)

func main() {
	if err := run(); err != nil {
		slog.Error("mock-oidc-server failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", "127.0.0.1:8080", "address to listen on (OIDC discovery and JWKS)")
	issueAddr := flag.String("issue-addr", "127.0.0.1:8081", "address for the token issuance endpoint")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	m, err := oidcmock.NewServer(nil)
	if err != nil {
		return fmt.Errorf("create mock OIDC server: %w", err)
	}

	// Binding to a fixed address keeps the issuer predictable for the test.
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *addr, err)
	}

	m.Start(ln)

	issueMux := http.NewServeMux()
	issueMux.HandleFunc("GET /issue", func(w http.ResponseWriter, r *http.Request) {
		issueToken(w, r, m)
	})

	issueServer := &http.Server{Addr: *issueAddr, Handler: issueMux, ReadHeaderTimeout: readHeaderTimeout}

	serveErr := make(chan error, 1)

	go func() {
		serveErr <- issueServer.ListenAndServe()
	}()

	_, _ = fmt.Fprintf(os.Stdout, "Mock OIDC Server running\n")
	_, _ = fmt.Fprintf(os.Stdout, "  OIDC Address: %s\n", ln.Addr())
	_, _ = fmt.Fprintf(os.Stdout, "  Issue Address: %s\n", *issueAddr)
	_, _ = fmt.Fprintf(os.Stdout, "  Issuer: %s\n", m.Issuer())
	_, _ = fmt.Fprintf(os.Stdout, "  JWKS: %s\n", m.JWKSEndpoint())
	_, _ = fmt.Fprintf(os.Stdout, "  Discovery: %s\n", m.DiscoveryEndpoint())
	_, _ = fmt.Fprintf(os.Stdout, "  Issue tokens: http://%s/issue?sub=...\n", *issueAddr)

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("issue server: %w", err)
		}
	}

	slog.Info("Shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := issueServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown issue server: %w", err)
	}

	if err := m.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown OIDC server: %w", err)
	}

	return nil
}

// issueToken generates and returns a JWT with custom claims.
// Query parameters:
//   - sub: subject claim (required)
//   - aud: audience claim (optional, defaults to the server's ClientID)
//   - exp: expiration in seconds from now (optional, defaults to 3600)
//   - claims: JSON object with additional claims (optional)
//   - repository_owner, repository, ref, ref_type, actor, workflow: set as
//     individual claims when present
func issueToken(w http.ResponseWriter, r *http.Request, m *oidcmock.Server) {
	query := r.URL.Query()

	sub := query.Get("sub")
	if sub == "" {
		http.Error(w, "missing required 'sub' parameter", http.StatusBadRequest)

		return
	}

	aud := query.Get("aud")
	if aud == "" {
		aud = m.ClientID
	}

	expSeconds := int64(defaultExpirySeconds)

	if expStr := query.Get("exp"); expStr != "" {
		parsed, err := strconv.ParseInt(expStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid 'exp' parameter: must be integer seconds", http.StatusBadRequest)

			return
		}

		expSeconds = parsed
	}

	now := time.Now()
	claims := jwt.MapClaims{
		"iss": m.Issuer(),
		"sub": sub,
		"aud": aud,
		"iat": now.Unix(),
		"exp": now.Add(time.Duration(expSeconds) * time.Second).Unix(),
	}

	if claimsJSON := query.Get("claims"); claimsJSON != "" {
		var extra map[string]any
		if err := json.Unmarshal([]byte(claimsJSON), &extra); err != nil {
			http.Error(w, "invalid 'claims' parameter: "+err.Error(), http.StatusBadRequest)

			return
		}

		maps.Copy(claims, extra)
	}

	for _, key := range []string{"repository_owner", "repository", "ref", "ref_type", "actor", "workflow"} {
		if val := query.Get(key); val != "" {
			claims[key] = val
		}
	}

	token, err := m.Keypair.SignJWT(claims)
	if err != nil {
		http.Error(w, "failed to sign token: "+err.Error(), http.StatusInternalServerError)

		return
	}

	if strings.Contains(r.Header.Get("Accept"), "application/json") {
		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(map[string]string{"token": token}); err != nil {
			slog.Error("failed to encode token response", "error", err)
		}

		return
	}

	// Plain text is easier to use with curl.
	w.Header().Set("Content-Type", "text/plain")
	_, _ = fmt.Fprint(w, token)
}
