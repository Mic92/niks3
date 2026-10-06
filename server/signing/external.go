package signing

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
)

// ExternalSigner signs narinfo batches with a long-running external program.
//
// The program takes no arguments, inherits the server's environment, and runs
// with the server's permissions. It is started on first use and talks
// newline-delimited JSON over stdin/stdout, one request in flight at a time:
//
//	-> {"id":1,"fingerprints":{"object-key":"base64-fingerprint"}}
//	<- {"id":1,"signatures":{"object-key":"name:base64-signature"}}
//	<- {"id":1,"error":"message"}          (request failed, program keeps running)
//
// The response must echo the request id and sign exactly the requested keys.
// Programs must ignore unknown request fields; unknown response fields are
// ignored. Diagnostics go to stderr. When the program exits, crashes, or
// misbehaves, it is killed (with its process group) and restarted on demand.
type ExternalSigner struct {
	program   string
	publicKey string
	keyName   string
	verifyKey ed25519.PublicKey

	// slot is a mutex that waiters can abandon on context cancellation.
	// It guards the fields below.
	slot   chan struct{}
	proc   *signerProcess
	closed bool
	nextID uint64
}

type signRequest struct {
	ID           uint64            `json:"id"`
	Fingerprints map[string][]byte `json:"fingerprints"`
}

type signResponse struct {
	ID         uint64            `json:"id"`
	Signatures map[string]string `json:"signatures"`
	Error      string            `json:"error"`
}

// requestError is a failure reported by a healthy program; no restart needed.
type requestError string

func (e requestError) Error() string { return "signing program: " + string(e) }

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

	return &ExternalSigner{
		program:   path,
		publicKey: name + ":" + encoded,
		keyName:   name,
		verifyKey: decoded,
		slot:      make(chan struct{}, 1),
	}, nil
}

// PublicKey returns the configured public key.
func (s *ExternalSigner) PublicKey() (string, error) {
	return s.publicKey, nil
}

// Close stops the signing program. Later Sign calls fail.
func (s *ExternalSigner) Close() {
	s.slot <- struct{}{}
	defer func() { <-s.slot }()

	s.closed = true
	s.drop()
}

// Sign sends base64 fingerprints and verifies the returned signatures.
func (s *ExternalSigner) Sign(ctx context.Context, infos map[string]*NarInfo) (map[string]string, error) {
	fingerprints := make(map[string][]byte, len(infos))
	for objectKey, info := range infos {
		fingerprint, err := GenerateFingerprint(info)
		if err != nil {
			return nil, fmt.Errorf("narinfo %q: %w", objectKey, err)
		}
		fingerprints[objectKey] = fingerprint
	}
	if len(fingerprints) == 0 {
		return map[string]string{}, nil
	}

	select {
	case s.slot <- struct{}{}:
		defer func() { <-s.slot }()
	case <-ctx.Done():
		return nil, fmt.Errorf("signing canceled: %w", ctx.Err())
	}

	if s.closed {
		return nil, errors.New("external signer is closed")
	}

	// A process that died while idle only shows up when we write to it,
	// so retry once on a fresh process if a reused one fails.
	for attempt := 0; ; attempt++ {
		reused := s.proc != nil
		if !reused {
			proc, err := startSignerProcess(context.WithoutCancel(ctx), s.program)
			if err != nil {
				return nil, err
			}
			s.proc = proc
		}

		signatures, err := s.exchange(ctx, fingerprints)
		if err == nil {
			return signatures, nil
		}
		if _, ok := errors.AsType[requestError](err); ok {
			return nil, err
		}

		s.drop()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("signing canceled: %w", ctxErr)
		}
		if !reused || attempt > 0 {
			return nil, err
		}
	}
}

// drop kills the current process so the next request starts a fresh one.
func (s *ExternalSigner) drop() {
	if s.proc != nil {
		s.proc.kill()
		s.proc = nil
	}
}

func (s *ExternalSigner) exchange(ctx context.Context, fingerprints map[string][]byte) (map[string]string, error) {
	proc := s.proc
	s.nextID++
	id := s.nextID

	// Blocking pipe I/O has no deadline; killing the process unblocks it.
	stop := context.AfterFunc(ctx, proc.kill)
	defer stop()

	if err := proc.enc.Encode(signRequest{ID: id, Fingerprints: fingerprints}); err != nil {
		return nil, fmt.Errorf("sending request to signing program: %w", err)
	}

	var response signResponse
	if err := proc.dec.Decode(&response); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}

		return nil, fmt.Errorf("reading response from signing program: %w", err)
	}
	if response.ID != id {
		return nil, fmt.Errorf("signing program answered request %d, want %d", response.ID, id)
	}
	if response.Error != "" {
		return nil, requestError(response.Error)
	}

	return s.verify(fingerprints, response.Signatures)
}

func (s *ExternalSigner) verify(fingerprints map[string][]byte, signatures map[string]string) (map[string]string, error) {
	if signatures == nil {
		return nil, errors.New("signing program must return a signatures object")
	}
	if len(signatures) != len(fingerprints) {
		return nil, fmt.Errorf("signing program returned %d signatures, want %d", len(signatures), len(fingerprints))
	}
	for objectKey, signature := range signatures {
		name, encoded, found := strings.Cut(signature, ":")
		if !found || name != s.keyName {
			return nil, fmt.Errorf("narinfo %q: signature must use key name %q", objectKey, s.keyName)
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

	return signatures, nil
}

// signerProcess is one running instance of the signing program.
type signerProcess struct {
	cmd   *exec.Cmd
	stdin io.Closer
	enc   *json.Encoder
	dec   *json.Decoder
	once  sync.Once
}

func startSignerProcess(ctx context.Context, program string) (*signerProcess, error) {
	// The process outlives the request that started it; kill() ends it.
	cmd := exec.CommandContext(ctx, program)
	cmd.Stderr = os.Stderr
	// Separate process group so killing it also takes down child processes.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("creating signing program stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("creating signing program stdout: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting signing program: %w", err)
	}

	return &signerProcess{cmd: cmd, stdin: stdin, enc: json.NewEncoder(stdin), dec: json.NewDecoder(stdout)}, nil
}

// kill terminates the process group and reaps the process. Idempotent.
func (p *signerProcess) kill() {
	p.once.Do(func() {
		_ = p.stdin.Close()
		// A negative PID targets the process group.
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		go func() { _ = p.cmd.Wait() }()
	})
}
