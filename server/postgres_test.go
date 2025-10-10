package server_test

import (
	"context"
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
	testPostgresServer *postgresServer //nolint:gochecknoglobals
	testDBCount        atomic.Int32    //nolint:gochecknoglobals
)

const (
	debugPostgres = false
)

type postgresServer struct {
	cmd     *exec.Cmd
	tempDir string
}

func (s *postgresServer) Cleanup() {
	defer func() {
		if err := os.RemoveAll(s.tempDir); err != nil {
			slog.Warn("Failed to remove postgres temp directory", "error", err)
		}
	}()

	terminateProcess(s.cmd)
}

func startPostgresServer() (*postgresServer, error) {
	tempDir, err := os.MkdirTemp("", "postgres")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	defer func() {
		if err != nil {
			err = os.RemoveAll(tempDir)
			if err != nil {
				slog.Warn("Failed to remove temp dir", "error", err)
			}
		}
	}()
	// initialize the database
	dbPath := filepath.Join(tempDir, "data")
	initdb := exec.CommandContext(context.Background(), "initdb", "-D", dbPath, "-U", "postgres")
	initdb.Stdout = os.Stdout
	initdb.Stderr = os.Stderr

	if err = initdb.Run(); err != nil {
		return nil, fmt.Errorf("failed to run initdb: %w", err)
	}

	args := []string{"-D", dbPath, "-k", tempDir, "-c", "listen_addresses="}

	if debugPostgres {
		args = append(args, "-c", "log_statement=all", "-c", "log_min_duration_statement=0")
	}

	postgresProc := exec.CommandContext(context.Background(), "postgres", args...)
	postgresProc.Stdout = os.Stdout
	postgresProc.Stderr = os.Stderr
	postgresProc.SysProcAttr = &syscall.SysProcAttr{}
	postgresProc.SysProcAttr.Setsid = true

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

	for range 30 {
		waitForPostgres := exec.CommandContext(context.Background(), "pg_isready", "-h", tempDir, "-U", "postgres")
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
