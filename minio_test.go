package main

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
	testMinioServer *minioServer
	testBucketCount atomic.Int32
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
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func randPort() (uint16, error) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	ln.Close()
	time.Sleep(1 * time.Second)
	return (uint16)(ln.Addr().(*net.TCPAddr).Port), nil
}

func (s *minioServer) Client(t *testing.T) *minio.Client {
	endpoint := fmt.Sprintf("localhost:%d", s.port)
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", s.secret, ""),
		Secure: false,
	})
	ok(t, err)
	return minioClient
}

func (s *minioServer) Cleanup() {
	err := syscall.Kill(s.cmd.Process.Pid, syscall.SIGKILL)
	if err != nil {
		slog.Error("failed to kill postgres", "error", err)
	}
	err = s.cmd.Wait()
	if err != nil {
		slog.Error("failed to wait for postgres", "error", err)
	}

	os.RemoveAll(s.tempDir)
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

	minioProc := exec.Command("minio", "server", "--address", fmt.Sprintf(":%d", port), filepath.Join(tempDir, "data"))
	minioProc.Stdout = os.Stdout
	minioProc.Stderr = os.Stderr

	// random hex string
	secret, err := randToken(20)
	if err != nil {
		return nil, fmt.Errorf("failed to generate access key: %w", err)
	}

	env := os.Environ()
	env = append(env, "MINIO_ROOT_USER=minioadmin")
	env = append(env, fmt.Sprintf("MINIO_ROOT_PASSWORD=%s", secret))
	env = append(env, "AWS_ACCESS_KEY_ID=minioadmin")
	env = append(env, fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", secret))
	minioProc.Env = env

	if err = minioProc.Start(); err != nil {
		return nil, fmt.Errorf("failed to start postgres: %w", err)
	}

	// wait for server to start
	for i := 0; i < 200; i++ {
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

// TODO: remove this test once we use minio in actual code
func TestServer_Miniotest(t *testing.T) {
	server := createTestServer(t)
	defer server.Close()
	_, err := server.minioClient.BucketExists(context.Background(), server.bucketName)
	ok(t, err)
}
