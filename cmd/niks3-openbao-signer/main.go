// niks3-openbao-signer is an example --sign-program that signs narinfo
// fingerprints with an Ed25519 key in OpenBao's (or Vault's) transit engine.
// The private key never leaves OpenBao.
//
// It reads one JSON request per line from stdin and writes one JSON response
// per line to stdout:
//
//	{"id":1,"fingerprints":{"object-key":"base64-fingerprint"}}
//	{"id":1,"signatures":{"object-key":"name:base64-signature"}}
//	{"id":1,"error":"message"}
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type request struct {
	ID           uint64            `json:"id"`
	Fingerprints map[string][]byte `json:"fingerprints"`
}

type response struct {
	ID         uint64            `json:"id"`
	Signatures map[string]string `json:"signatures,omitzero"`
	Error      string            `json:"error,omitempty"`
}

type transit struct {
	client    *http.Client
	signURL   string
	tokenFile string
}

func main() {
	if err := run(); err != nil {
		slog.Error("niks3-openbao-signer failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	// niks3 starts the program without arguments, so deployments without a
	// wrapper script configure it through the environment.
	addr := flag.String("addr", os.Getenv("NIKS3_OPENBAO_ADDR"), "OpenBao address ($NIKS3_OPENBAO_ADDR)")
	tokenFile := flag.String("token-file", os.Getenv("NIKS3_OPENBAO_TOKEN_FILE"),
		"file containing the OpenBao token, read on every request ($NIKS3_OPENBAO_TOKEN_FILE)")
	key := flag.String("key", os.Getenv("NIKS3_OPENBAO_KEY"), "transit key name ($NIKS3_OPENBAO_KEY)")
	name := flag.String("signature-name", os.Getenv("NIKS3_OPENBAO_SIGNATURE_NAME"),
		"key name in signatures, must match the public key given to niks3 ($NIKS3_OPENBAO_SIGNATURE_NAME)")
	flag.Parse()

	if *addr == "" || *tokenFile == "" || *key == "" || *name == "" {
		return errors.New("address, token file, key and signature name are required, see -help")
	}

	t := &transit{
		client:    &http.Client{Timeout: 30 * time.Second},
		signURL:   strings.TrimRight(*addr, "/") + "/v1/transit/sign/" + url.PathEscape(*key),
		tokenFile: *tokenFile,
	}

	if err := serve(context.Background(), os.Stdin, os.Stdout, *name, t.sign); err != nil {
		return fmt.Errorf("serving signing requests: %w", err)
	}

	return nil
}

// sign returns the raw signature of fingerprint.
func (t *transit) sign(ctx context.Context, fingerprint []byte) ([]byte, error) {
	token, err := os.ReadFile(t.tokenFile)
	if err != nil {
		return nil, fmt.Errorf("reading token: %w", err)
	}

	body, err := json.Marshal(map[string]string{"input": base64.StdEncoding.EncodeToString(fingerprint)})
	if err != nil {
		return nil, fmt.Errorf("encoding request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.signURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("X-Vault-Token", strings.TrimSpace(string(token)))

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("calling OpenBao: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	// Error replies carry {"errors": [...]} in the same envelope.
	var reply struct {
		Errors []string `json:"errors"`
		Data   struct {
			Signature string `json:"signature"`
		} `json:"data"`
	}
	decodeErr := json.NewDecoder(resp.Body).Decode(&reply)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OpenBao returned %s: %s", resp.Status, strings.Join(reply.Errors, ", "))
	}

	if decodeErr != nil {
		return nil, fmt.Errorf("decoding OpenBao reply: %w", decodeErr)
	}

	// Transit signatures look like "vault:v1:<base64>".
	parts := strings.SplitN(reply.Data.Signature, ":", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("unexpected OpenBao signature format %q", reply.Data.Signature)
	}

	signature, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("decoding OpenBao signature: %w", err)
	}

	return signature, nil
}

// serve answers requests until stdin closes. A signing error fails only the
// request it belongs to.
func serve(ctx context.Context, in io.Reader, out io.Writer, keyName string, sign func(ctx context.Context, fingerprint []byte) ([]byte, error)) error {
	dec := json.NewDecoder(in)
	enc := json.NewEncoder(out)

	for {
		var req request
		if err := dec.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return fmt.Errorf("reading request: %w", err)
		}

		resp := response{ID: req.ID, Signatures: make(map[string]string, len(req.Fingerprints))}

		for objectKey, fingerprint := range req.Fingerprints {
			signature, err := sign(ctx, fingerprint)
			if err != nil {
				resp = response{ID: req.ID, Error: fmt.Sprintf("narinfo %q: %v", objectKey, err)}

				break
			}

			resp.Signatures[objectKey] = keyName + ":" + base64.StdEncoding.EncodeToString(signature)
		}

		if err := enc.Encode(resp); err != nil {
			return fmt.Errorf("writing response: %w", err)
		}
	}
}
