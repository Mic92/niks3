// Package oidcmock is a minimal OIDC provider for tests. It publishes a
// discovery document and a JWKS and signs tokens with the matching key. It
// deliberately implements no login, authorization or token endpoints: the
// niks3 validator only ever reads discovery and JWKS.
package oidcmock

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	// issuerPath is appended to the listen address to form the issuer URL, so
	// the discovery document does not live at the server root.
	issuerPath        = "/oidc"
	discoveryPath     = "/.well-known/openid-configuration"
	jwksPath          = "/.well-known/jwks.json"
	keyBits           = 2048
	clientIDBytes     = 12
	readHeaderTimeout = 5 * time.Second
)

// Keypair signs tokens and publishes the matching public key.
type Keypair struct {
	key *rsa.PrivateKey
	kid string
}

// NewKeypair generates a fresh RSA keypair.
func NewKeypair() (*Keypair, error) {
	key, err := rsa.GenerateKey(rand.Reader, keyBits)
	if err != nil {
		return nil, fmt.Errorf("generate RSA key: %w", err)
	}

	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("marshal public key: %w", err)
	}

	sum := sha256.Sum256(der)

	return &Keypair{key: key, kid: base64.RawURLEncoding.EncodeToString(sum[:])}, nil
}

type jwk struct {
	Kty string `json:"kty"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

type jwkSet struct {
	Keys []jwk `json:"keys"`
}

// JWKS returns the JSON Web Key Set containing the public key.
func (k *Keypair) JWKS() ([]byte, error) {
	pub := &k.key.PublicKey
	set := jwkSet{Keys: []jwk{{
		Kty: "RSA",
		Use: "sig",
		Alg: "RS256",
		Kid: k.kid,
		N:   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		E:   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
	}}}

	data, err := json.Marshal(set)
	if err != nil {
		return nil, fmt.Errorf("marshal JWKS: %w", err)
	}

	return data, nil
}

// SignJWT signs claims with RS256 and sets the kid header.
func (k *Keypair) SignJWT(claims jwt.Claims) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = k.kid

	signed, err := token.SignedString(k.key)
	if err != nil {
		return "", fmt.Errorf("sign JWT: %w", err)
	}

	return signed, nil
}

// Server serves discovery and JWKS for a single issuer.
type Server struct {
	Keypair *Keypair
	// ClientID is the audience tokens are issued for unless a caller picks
	// another one.
	ClientID string

	addr string
	srv  *http.Server
}

// NewServer creates a server with a random ClientID. A nil kp means a fresh
// keypair is generated.
func NewServer(kp *Keypair) (*Server, error) {
	if kp == nil {
		var err error

		kp, err = NewKeypair()
		if err != nil {
			return nil, err
		}
	}

	raw := make([]byte, clientIDBytes)
	if _, err := rand.Read(raw); err != nil {
		return nil, fmt.Errorf("generate client id: %w", err)
	}

	return &Server{Keypair: kp, ClientID: "client-" + hex.EncodeToString(raw)}, nil
}

// Run starts a server with a fresh keypair on a random loopback port. ctx
// only bounds the listen call; the server runs until Shutdown.
func Run(ctx context.Context) (*Server, error) {
	s, err := NewServer(nil)
	if err != nil {
		return nil, err
	}

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	s.Start(ln)

	return s, nil
}

// Start serves on ln in the background until Shutdown is called.
func (s *Server) Start(ln net.Listener) {
	s.addr = ln.Addr().String()
	s.srv = &http.Server{Handler: s.Handler(), ReadHeaderTimeout: readHeaderTimeout}

	go func() {
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("mock OIDC server stopped", "error", err)
		}
	}()
}

// Shutdown stops a server started with Start or Run.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}

	if err := s.srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown mock OIDC server: %w", err)
	}

	return nil
}

// Issuer is the value of the iss claim and the base of the discovery URL.
func (s *Server) Issuer() string {
	return "http://" + s.addr + issuerPath
}

// DiscoveryEndpoint is the URL of the OpenID configuration document.
func (s *Server) DiscoveryEndpoint() string {
	return s.Issuer() + discoveryPath
}

// JWKSEndpoint is the URL of the JSON Web Key Set.
func (s *Server) JWKSEndpoint() string {
	return s.Issuer() + jwksPath
}

// Handler serves the discovery document and the JWKS under issuerPath.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+issuerPath+discoveryPath, s.serveDiscovery)
	mux.HandleFunc("GET "+issuerPath+jwksPath, s.serveJWKS)

	return mux
}

type discoveryDocument struct {
	Issuer                           string   `json:"issuer"`
	JWKSURI                          string   `json:"jwks_uri"`
	ResponseTypesSupported           []string `json:"response_types_supported"`
	SubjectTypesSupported            []string `json:"subject_types_supported"`
	IDTokenSigningAlgValuesSupported []string `json:"id_token_signing_alg_values_supported"`
}

func (s *Server) serveDiscovery(w http.ResponseWriter, _ *http.Request) {
	doc := discoveryDocument{
		Issuer:                           s.Issuer(),
		JWKSURI:                          s.JWKSEndpoint(),
		ResponseTypesSupported:           []string{"id_token"},
		SubjectTypesSupported:            []string{"public"},
		IDTokenSigningAlgValuesSupported: []string{"RS256"},
	}

	data, err := json.Marshal(doc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

func (s *Server) serveJWKS(w http.ResponseWriter, _ *http.Request) {
	data, err := s.Keypair.JWKS()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/jwk-set+json")
	_, _ = w.Write(data)
}
