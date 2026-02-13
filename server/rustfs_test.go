package server_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
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
		Creds:  credentials.NewStaticV4("rustfsadmin", s.secret, ""),
		Secure: false,
	})
	ok(tb, err)

	return minioClient
}

func terminateProcess(cmd *exec.Cmd) {
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		slog.Error("failed to get pgid", "error", err)

		return
	}

	time.AfterFunc(10*time.Second, func() {
		err = syscall.Kill(-pgid, syscall.SIGKILL)
		if err != nil {
			slog.Error("failed to kill rustfs", "error", err)

			return
		}

		slog.Info("killed rustfs")
	})

	err = syscall.Kill(-pgid, syscall.SIGTERM)
	if err != nil {
		slog.Error("failed to kill rustfs", "error", err)
	}

	err = cmd.Wait()
	if err != nil {
		slog.Error("failed to wait for rustfs", "error", err)

		return
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

	//nolint:gosec
	rustfsProc := exec.CommandContext(ctx, "rustfs",
		"--address", fmt.Sprintf("127.0.0.1:%d", port),
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

	// wait for server to start
	dialer := net.Dialer{}

	for range 200 {
		// Check if context has been cancelled/timed out
		if ctx.Err() != nil {
			return nil, fmt.Errorf("timeout waiting for rustfs server to start: %w", ctx.Err())
		}

		var conn net.Conn

		conn, err = dialer.DialContext(ctx, "tcp", fmt.Sprintf("localhost:%d", port))
		if err == nil {
			_ = conn.Close()

			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to rustfs server: %w", err)
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
