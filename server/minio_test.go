package server_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	testMinioServer *minioServer //nolint:gochecknoglobals
	testBucketCount atomic.Int32 //nolint:gochecknoglobals
)

type minioServer struct {
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

func randPort() (uint16, error) {
	lc := net.ListenConfig{}

	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("failed to listen: %w", err)
	}

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = ln.Close()
		return 0, fmt.Errorf("listener did not return *net.TCPAddr")
	}
	port := uint16(addr.Port) //nolint:gosec
	_ = ln.Close()

	return port, nil
}

func (s *minioServer) Client(tb testing.TB) *minio.Client {
	tb.Helper()

	endpoint := fmt.Sprintf("localhost:%d", s.port)
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", s.secret, ""),
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
		err = syscall.Kill(pgid, syscall.SIGKILL)
		if err != nil {
			slog.Error("failed to kill minio", "error", err)

			return
		}

		slog.Info("killed minio")
	})

	err = syscall.Kill(pgid, syscall.SIGTERM)
	if err != nil {
		slog.Error("failed to kill minio", "error", err)
	}

	err = cmd.Wait()
	if err != nil {
		slog.Error("failed to wait for minio", "error", err)

		return
	}
}

func (s *minioServer) Cleanup() {
	defer func() {
		if err := os.RemoveAll(s.tempDir); err != nil {
			slog.Warn("Failed to remove minio temp directory", "error", err)
		}
	}()

	terminateProcess(s.cmd)
}

func startMinioServer() (*minioServer, error) {
	tempDir, err := os.MkdirTemp("", "minio")
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

	port, err := randPort()
	if err != nil {
		return nil, fmt.Errorf("failed to find free port: %w", err)
	}

	//nolint:gosec
	minioProc := exec.CommandContext(context.Background(), "minio", "server", "--address", fmt.Sprintf(":%d", port), filepath.Join(tempDir, "data"))
	minioProc.Stdout = os.Stdout
	minioProc.Stderr = os.Stderr
	minioProc.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}

	// random hex string
	secret, err := randToken(20)
	if err != nil {
		return nil, fmt.Errorf("failed to generate access key: %w", err)
	}

	env := os.Environ()
	env = append(env, "MINIO_ROOT_USER=minioadmin")
	env = append(env, "MINIO_ROOT_PASSWORD="+secret)
	env = append(env, "AWS_ACCESS_KEY_ID=minioadmin")
	env = append(env, "AWS_SECRET_ACCESS_KEY="+secret)
	minioProc.Env = env

	if err = minioProc.Start(); err != nil {
		return nil, fmt.Errorf("failed to start minio: %w", err)
	}

	// wait for server to start
	dialer := net.Dialer{}
	for range 200 {
		var conn net.Conn

		conn, err = dialer.DialContext(context.Background(), "tcp", fmt.Sprintf("localhost:%d", port))
		if err == nil {
			_ = conn.Close()

			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to minio server: %w", err)
	}

	server := &minioServer{
		cmd:     minioProc,
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

// TODO: remove this test once we use minio in actual code.
func TestService_Miniotest(t *testing.T) {
	t.Parallel()

	server := createTestService(t)
	defer server.Close()

	_, err := server.MinioClient.BucketExists(t.Context(), server.Bucket)
	ok(t, err)
}
