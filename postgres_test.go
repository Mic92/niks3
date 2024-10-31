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
	testPostgresServer *postgresServer //nolint:gochecknoglobals
	testDBCount        atomic.Int32    //nolint:gochecknoglobals
)

type postgresServer struct {
	cmd     *exec.Cmd
	tempDir string
}

func (s *postgresServer) Cleanup() {
	defer os.RemoveAll(s.tempDir)

	pgid, err := syscall.Getpgid(s.cmd.Process.Pid)
	if err != nil {
		slog.Error("failed to get pgid", "error", err)

		return
	}

	err = syscall.Kill(pgid, syscall.SIGKILL)
	if err != nil {
		slog.Error("failed to kill postgres", "error", err)

		return
	}

	err = s.cmd.Wait()
	if err != nil {
		slog.Error("failed to wait for postgres", "error", err)
	}
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
	// initialize the database
	dbPath := filepath.Join(tempDir, "data")
	initdb := exec.Command("initdb", "-D", dbPath, "-U", "postgres")
	initdb.Stdout = os.Stdout
	initdb.Stderr = os.Stderr

	if err = initdb.Run(); err != nil {
		return nil, fmt.Errorf("failed to run initdb: %w", err)
	}

	postgresProc := exec.Command("postgres", "-D", dbPath, "-k", tempDir,
		"-c", "listen_addresses=",
		"-c", "log_statement=all",
		"-c", "log_min_duration_statement=0")
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
