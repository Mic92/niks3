package server_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
)

func innerTestMain(m *testing.M) int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var err error

	// unload environment variables from the devenv
	_ = os.Unsetenv("DATABASE_URL")
	_ = os.Unsetenv("PGDATABASE")
	_ = os.Unsetenv("PGUSER")
	_ = os.Unsetenv("PGHOST")

	testPostgresServer, err = startPostgresServer(ctx)

	if err != nil {
		slog.Error("failed to start postgres", "error", err)

		return 1
	}
	defer testPostgresServer.Cleanup()

	testMinioServer, err = startMinioServer(ctx)

	if err != nil {
		slog.Error("failed to start minio", "error", err)

		return 1
	}
	defer testMinioServer.Cleanup()

	return m.Run()
}

func TestMain(m *testing.M) {
	// inner main is required to be able to defer cleanup
	os.Exit(innerTestMain(m))
}
