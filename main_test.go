package main

import (
	"log/slog"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	var err error
	testPostgresServer, err = startPostgresServer()
	defer testPostgresServer.Cleanup()
	if err != nil {
		slog.Error("failed to start postgres", "error", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}
