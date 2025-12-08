// mock-oidc-server is a wrapper around mockoidc for NixOS integration tests.
// It starts a mock OIDC server and adds a /issue endpoint for generating test tokens.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/oauth2-proxy/mockoidc"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "address to listen on (OIDC)")
	issueAddr := flag.String("issue-addr", "127.0.0.1:8081", "address for token issuance endpoint")
	flag.Parse()

	// Create mock OIDC server
	m, err := mockoidc.NewServer(nil)
	if err != nil {
		log.Fatalf("Failed to create mock OIDC server: %v", err)
	}

	// Start listening for OIDC - binding to a specific address ensures the issuer is predictable
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", *addr, err)
	}

	// Get actual address (useful when port is 0)
	actualAddr := ln.Addr().String()

	// Start the mock OIDC server
	if err := m.Start(ln, nil); err != nil {
		log.Fatalf("Failed to start mock OIDC server: %v", err)
	}

	// Start a separate HTTP server for the /issue endpoint
	issueMux := http.NewServeMux()
	issueMux.HandleFunc("/issue", func(w http.ResponseWriter, r *http.Request) {
		issueToken(w, r, m)
	})
	issueServer := &http.Server{Addr: *issueAddr, Handler: issueMux}
	go func() {
		if err := issueServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Issue server error: %v", err)
		}
	}()

	// Print server info
	fmt.Printf("Mock OIDC Server running\n")
	fmt.Printf("  OIDC Address: %s\n", actualAddr)
	fmt.Printf("  Issue Address: %s\n", *issueAddr)
	fmt.Printf("  Issuer: %s\n", m.Issuer())
	fmt.Printf("  JWKS: %s\n", m.JWKSEndpoint())
	fmt.Printf("  Discovery: %s\n", m.DiscoveryEndpoint())
	fmt.Printf("  Issue tokens: http://%s/issue?sub=...\n", *issueAddr)

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	if err := m.Shutdown(); err != nil {
		log.Printf("Error during OIDC shutdown: %v", err)
	}
	if err := issueServer.Shutdown(nil); err != nil {
		log.Printf("Error during issue server shutdown: %v", err)
	}
}

// issueToken generates and returns a JWT token with custom claims.
// Query parameters:
//   - sub: subject claim (required)
//   - aud: audience claim (optional, defaults to server's ClientID)
//   - exp: expiration in seconds from now (optional, defaults to 3600)
//   - claims: JSON object with additional claims (optional)
func issueToken(w http.ResponseWriter, r *http.Request, m *mockoidc.MockOIDC) {
	sub := r.URL.Query().Get("sub")
	if sub == "" {
		http.Error(w, "missing required 'sub' parameter", http.StatusBadRequest)
		return
	}

	aud := r.URL.Query().Get("aud")
	if aud == "" {
		aud = m.Config().ClientID
	}

	expSeconds := int64(3600)
	if expStr := r.URL.Query().Get("exp"); expStr != "" {
		if _, err := fmt.Sscanf(expStr, "%d", &expSeconds); err != nil {
			http.Error(w, "invalid 'exp' parameter: must be integer seconds", http.StatusBadRequest)
			return
		}
	}

	// Base claims
	now := m.Now()
	claims := jwt.MapClaims{
		"iss": m.Issuer(),
		"sub": sub,
		"aud": aud,
		"iat": now.Unix(),
		"exp": now.Add(time.Duration(expSeconds) * time.Second).Unix(),
	}

	// Parse additional claims from query parameter
	if claimsJSON := r.URL.Query().Get("claims"); claimsJSON != "" {
		var extraClaims map[string]any
		if err := json.Unmarshal([]byte(claimsJSON), &extraClaims); err != nil {
			http.Error(w, fmt.Sprintf("invalid 'claims' parameter: %v", err), http.StatusBadRequest)
			return
		}
		for k, v := range extraClaims {
			claims[k] = v
		}
	}

	// Parse repository_owner and other common claims from individual query params
	for _, key := range []string{"repository_owner", "repository", "ref", "ref_type", "actor", "workflow"} {
		if val := r.URL.Query().Get(key); val != "" {
			claims[key] = val
		}
	}

	// Sign the token
	token, err := m.Keypair.SignJWT(claims)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to sign token: %v", err), http.StatusInternalServerError)
		return
	}

	// Return token as plain text (easier to use with curl)
	w.Header().Set("Content-Type", "text/plain")
	if strings.Contains(r.Header.Get("Accept"), "application/json") {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"token": token})
	} else {
		fmt.Fprint(w, token)
	}
}
