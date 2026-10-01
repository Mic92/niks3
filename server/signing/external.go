package signing

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

// ExternalSigner signs narinfo batches with an external program.
// The program takes no arguments, inherits the server's environment, and runs
// with the server's permissions.
// It reads a JSON request from stdin and writes a JSON response to stdout:
//
//	{"fingerprints": {"object-key": "base64-fingerprint"}}
//	{"signatures": {"object-key": "name:base64-signature"}}
//
// Programs must ignore unknown top-level request fields. Unknown top-level
// response fields are ignored. Diagnostics go to stderr.
type ExternalSigner struct {
	program   string
	publicKey string
	verifyKey ed25519.PublicKey
}

type signRequest struct {
	Fingerprints map[string][]byte `json:"fingerprints"`
}

type signResponse struct {
	Signatures map[string]string `json:"signatures"`
}

// NewExternalSigner configures an executable.
func NewExternalSigner(program, publicKey string) (*ExternalSigner, error) {
	name, encoded, found := strings.Cut(strings.TrimSpace(publicKey), ":")
	if !found || name == "" {
		return nil, errors.New("signing public key must have the format name:base64-public-key")
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("invalid signing public key: %w", err)
	}
	if len(decoded) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid Ed25519 public key length: expected %d, got %d", ed25519.PublicKeySize, len(decoded))
	}

	path, err := exec.LookPath(program)
	if err != nil {
		return nil, fmt.Errorf("finding signing program: %w", err)
	}

	return &ExternalSigner{program: path, publicKey: name + ":" + encoded, verifyKey: decoded}, nil
}

// PublicKey returns the configured public key.
func (s *ExternalSigner) PublicKey() (string, error) {
	return s.publicKey, nil
}

// Sign sends base64 fingerprints and verifies the returned signatures.
func (s *ExternalSigner) Sign(ctx context.Context, infos map[string]*NarInfo) (map[string]string, error) {
	fingerprints := make(map[string][]byte, len(infos))
	for objectKey, info := range infos {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("signing canceled: %w", err)
		}
		fingerprint, err := GenerateFingerprint(info)
		if err != nil {
			return nil, fmt.Errorf("narinfo %q: %w", objectKey, err)
		}
		fingerprints[objectKey] = fingerprint
	}

	input, err := json.Marshal(signRequest{Fingerprints: fingerprints})
	if err != nil {
		return nil, fmt.Errorf("encoding signing input: %w", err)
	}

	//nolint:gosec // program is configured by the server operator
	cmd := exec.CommandContext(ctx, s.program)
	cmd.Stdin = bytes.NewReader(input)
	cmd.Stderr = os.Stderr
	// Use a separate process group so cancellation also kills child processes.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		// A negative PID targets the process group.
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		if err != nil {
			return fmt.Errorf("stopping signing process group: %w", err)
		}

		return nil
	}

	output, err := cmd.Output()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("signing canceled: %w", ctxErr)
		}

		return nil, fmt.Errorf("running signing program: %w", err)
	}

	var response signResponse
	if err := json.Unmarshal(output, &response); err != nil {
		return nil, fmt.Errorf("decoding signing program output: %w", err)
	}
	if response.Signatures == nil {
		return nil, errors.New("signing program must return a signatures object")
	}
	keyName, _, _ := strings.Cut(s.publicKey, ":")
	for objectKey, signature := range response.Signatures {
		name, encoded, found := strings.Cut(signature, ":")
		if !found || name != keyName {
			return nil, fmt.Errorf("narinfo %q: signature must use key name %q", objectKey, keyName)
		}
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("narinfo %q: invalid signature encoding: %w", objectKey, err)
		}
		if len(decoded) != ed25519.SignatureSize {
			return nil, fmt.Errorf("narinfo %q: invalid Ed25519 signature length: expected %d, got %d", objectKey, ed25519.SignatureSize, len(decoded))
		}
		fingerprint, ok := fingerprints[objectKey]
		if !ok || !ed25519.Verify(s.verifyKey, fingerprint, decoded) {
			return nil, fmt.Errorf("narinfo %q: signature verification failed", objectKey)
		}
	}

	return response.Signatures, nil
}
