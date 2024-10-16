package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	testPostgresServer *postgresServer
	testDbCount        atomic.Int32
)

type postgresServer struct {
	cmd     *exec.Cmd
	tempDir string
}

func (s *postgresServer) Cleanup() {
	err := syscall.Kill(s.cmd.Process.Pid, syscall.SIGTERM)
	if err != nil {
		slog.Error("failed to kill postgres", "error", err)
	}
	err = s.cmd.Wait()
	if err != nil {
		slog.Error("failed to wait for postgres", "error", err)
	}

	os.RemoveAll(s.tempDir)
}

func startPostgresServer() (*postgresServer, error) {
	tempDir, err := os.MkdirTemp("", "postgres")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() {
		if err != nil {
			os.RemoveAll(tempDir)
		}
	}()
	configFile := filepath.Join(tempDir, "postgresql.conf")
	// only listen on a unix socket
	configContent := `
unix_socket_directories = '` + tempDir + `'
	`
	if err = os.WriteFile(configFile, []byte(configContent), 0o644); err != nil {
		return nil, fmt.Errorf("failed to write config file: %w", err)
	}
	// initialize the database
	dbPath := filepath.Join(tempDir, "data")
	initdb := exec.Command("initdb", "-D", dbPath, "-U", "postgres")
	initdb.Stdout = os.Stdout
	initdb.Stderr = os.Stderr
	if err = initdb.Run(); err != nil {
		return nil, fmt.Errorf("failed to run initdb: %w", err)
	}

	postgresProc := exec.Command("postgres", "-D", dbPath, "-k", tempDir, "-c", "listen_addresses=")
	postgresProc.Stdout = os.Stdout
	postgresProc.Stderr = os.Stderr
	if err = postgresProc.Start(); err != nil {
		return nil, fmt.Errorf("failed to start postgres: %w", err)
	}
	server := &postgresServer{
		cmd:     postgresProc,
		tempDir: tempDir,
	}
	defer func() {
		if err != nil {
			server.Cleanup()
		}
	}()

	for i := 0; i < 30; i++ {
		waitForPostgres := exec.Command("pg_isready", "-h", tempDir, "-U", "postgres")
		waitForPostgres.Stdout = os.Stdout
		waitForPostgres.Stderr = os.Stderr
		err = waitForPostgres.Run()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to wait for postgres: %w", err)
	}

	return server, nil
}
