package server

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
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return 0, fmt.Errorf("failed to listen: %w", err)
	}

	ln.Close()
	time.Sleep(1 * time.Second)

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("failed to get port: %w", err)
	}

	return (uint16)(addr.Port), nil //nolint:gosec
}

func (s *minioServer) Client(t *testing.T) *minio.Client {
	t.Helper()

	endpoint := fmt.Sprintf("localhost:%d", s.port)
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", s.secret, ""),
		Secure: false,
	})
	ok(t, err)

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
	defer os.RemoveAll(s.tempDir)

	terminateProcess(s.cmd)
}

func startMinioServer() (*minioServer, error) {
	tempDir, err := os.MkdirTemp("", "minio")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	defer func() {
		if err != nil {
			os.RemoveAll(tempDir)
		}
	}()

	port, err := randPort()
	if err != nil {
		return nil, fmt.Errorf("failed to find free port: %w", err)
	}

	//nolint:gosec
	minioProc := exec.Command("minio", "server", "--address", fmt.Sprintf(":%d", port), filepath.Join(tempDir, "data"))
	minioProc.Stdout = os.Stdout
	minioProc.Stderr = os.Stderr
	minioProc.SysProcAttr = &syscall.SysProcAttr{}
	minioProc.SysProcAttr.Setsid = true

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
		return nil, fmt.Errorf("failed to start postgres: %w", err)
	}

	// wait for server to start
	for range 200 {
		var conn net.Conn
		conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", port))

		if err == nil {
			conn.Close()

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
func TestServer_Miniotest(t *testing.T) {
	t.Parallel()

	server := createTestServer(t)
	defer server.Close()
	_, err := server.minioClient.BucketExists(context.Background(), server.bucketName)
	ok(t, err)
}
