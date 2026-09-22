package server_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	testRustfsServer *rustfsServer //nolint:gochecknoglobals
	testBucketCount  atomic.Int32  //nolint:gochecknoglobals
	// rustfs rejects concurrent CreateBucket calls with 503 SlowDown, which
	// minio-go retries with backoff long enough to blow the test deadline.
	testBucketMu sync.Mutex //nolint:gochecknoglobals
)

type rustfsServer struct {
	cmd     *exec.Cmd
	tempDir string
	secret  string
	port    uint16
}

func randToken(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed to read random bytes: %w", err)
	}

	return hex.EncodeToString(bytes), nil
}

func randPort(ctx context.Context) (uint16, error) {
	lc := net.ListenConfig{}

	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("failed to listen: %w", err)
	}

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = ln.Close()

		return 0, errors.New("listener did not return *net.TCPAddr")
	}

	port := uint16(addr.Port) //nolint:gosec
	_ = ln.Close()

	return port, nil
}

func (s *rustfsServer) Client(tb testing.TB) *minio.Client {
	tb.Helper()

	endpoint := fmt.Sprintf("localhost:%d", s.port)

	return s.ClientWithEndpoint(tb, endpoint)
}

// ClientWithEndpoint creates a minio client pointing to a custom endpoint.
// This is useful for testing with proxies.
func (s *rustfsServer) ClientWithEndpoint(tb testing.TB, endpoint string) *minio.Client {
	tb.Helper()

	// minio-go client works with any S3-compatible storage including RustFS
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  s.Creds(),
		Secure: false,
	})
	ok(tb, err)

	return minioClient
}

func (s *rustfsServer) Creds() *credentials.Credentials {
	return credentials.NewStaticV4("rustfsadmin", s.secret, "")
}

func terminateProcess(cmd *exec.Cmd) {
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		slog.Error("failed to get pgid", "error", err)

		return
	}

	// The escalation runs on its own goroutine: it must not share err with
	// this one, and it is stopped once Wait returns so it does not fire on a
	// process group that is already gone.
	killTimer := time.AfterFunc(10*time.Second, func() {
		if killErr := syscall.Kill(-pgid, syscall.SIGKILL); killErr != nil {
			slog.Error("failed to kill process group", "error", killErr)

			return
		}

		slog.Info("killed process group", "pgid", pgid)
	})
	defer killTimer.Stop()

	if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil {
		slog.Error("failed to terminate process group", "error", err)
	}

	if err := cmd.Wait(); err != nil {
		slog.Error("failed to wait for process", "error", err)
	}
}

func (s *rustfsServer) Cleanup() {
	defer func() {
		if err := os.RemoveAll(s.tempDir); err != nil {
			slog.Warn("Failed to remove rustfs temp directory", "error", err)
		}
	}()

	terminateProcess(s.cmd)
}

func startRustfsServer(ctx context.Context) (*rustfsServer, error) {
	tempDir, err := os.MkdirTemp("", "rustfs")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	defer func() {
		if err != nil {
			if removeErr := os.RemoveAll(tempDir); removeErr != nil {
				slog.Warn("Failed to remove temp directory during startup cleanup", "error", removeErr)
			}
		}
	}()

	port, err := randPort(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to find free port: %w", err)
	}

	// random hex string
	secret, err := randToken(20)
	if err != nil {
		return nil, fmt.Errorf("failed to generate access key: %w", err)
	}

	dataDir := filepath.Join(tempDir, "data")
	if err = os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	consolePort, err := randPort(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to find free console port: %w", err)
	}

	//nolint:gosec
	rustfsProc := exec.CommandContext(ctx, "rustfs",
		"--address", fmt.Sprintf("127.0.0.1:%d", port),
		"--console-address", fmt.Sprintf("127.0.0.1:%d", consolePort),
		"--access-key", "rustfsadmin",
		"--secret-key", secret,
		dataDir)
	rustfsProc.Stdout = os.Stdout
	rustfsProc.Stderr = os.Stderr
	rustfsProc.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}

	env := os.Environ()
	env = append(env, "AWS_ACCESS_KEY_ID=rustfsadmin")
	env = append(env, "AWS_SECRET_ACCESS_KEY="+secret)
	rustfsProc.Env = env

	if err = rustfsProc.Start(); err != nil {
		return nil, fmt.Errorf("failed to start rustfs: %w", err)
	}

	// The port opens before storage is usable ("waiting for storage_quorum").
	readyURL := fmt.Sprintf("http://localhost:%d/health/ready", port)

	for {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, readyURL, nil)
		if reqErr != nil {
			return nil, fmt.Errorf("rustfs readiness request: %w", reqErr)
		}

		resp, respErr := http.DefaultClient.Do(req)
		if respErr == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timeout waiting for rustfs to become ready: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}

	server := &rustfsServer{
		cmd:     rustfsProc,
		tempDir: tempDir,
		secret:  secret,
		port:    port,
	}

	defer func() {
		if err != nil {
			server.Cleanup()
		}
	}()

	return server, nil
}

func TestService_Rustfstest(t *testing.T) {
	t.Parallel()

	server := createTestService(t)
	defer server.Close()

	_, err := server.MinioClient.BucketExists(t.Context(), server.Bucket)
	ok(t, err)
}
