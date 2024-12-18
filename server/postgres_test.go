package server_test

import (
	"fmt"
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
	defer os.RemoveAll(s.tempDir)

	terminateProcess(s.cmd)
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

	args := []string{"-D", dbPath, "-k", tempDir, "-c", "listen_addresses="}

	if debugPostgres {
		args = append(args, "-c", "log_statement=all", "-c", "log_min_duration_statement=0")
	}

	postgresProc := exec.Command("postgres", args...)
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
