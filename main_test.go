package main

import (
	"log/slog"
	"os"
	"testing"
)

func innerTestMain(m *testing.M) int {
	var err error

	// unload environment variables from the devenv
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("PGDATABASE")
	os.Unsetenv("PGUSER")
	os.Unsetenv("PGHOST")

	testPostgresServer, err = startPostgresServer()
	defer testPostgresServer.Cleanup()

	if err != nil {
		slog.Error("failed to start postgres", "error", err)

		return 1
	}

	testMinioServer, err = startMinioServer()
	defer testMinioServer.Cleanup()

	if err != nil {
		slog.Error("failed to start minio", "error", err)

		return 1
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	// inner main is required to be able to defer cleanup
	os.Exit(innerTestMain(m))
}
