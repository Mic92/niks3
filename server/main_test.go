package server_test

import (
	"log/slog"
	"os"
	"testing"
)

func innerTestMain(m *testing.M) int {
	var err error

	// unload environment variables from the devenv
	_ = os.Unsetenv("DATABASE_URL")
	_ = os.Unsetenv("PGDATABASE")
	_ = os.Unsetenv("PGUSER")
	_ = os.Unsetenv("PGHOST")

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
