package server_test

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server"
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

	testRustfsServer, err = startRustfsServer(ctx)
	if err != nil {
		slog.Error("failed to start rustfs", "error", err)

		return 1
	}

	defer testRustfsServer.Cleanup()

	return m.Run()
}

func TestMain(m *testing.M) {
	// inner main is required to be able to defer cleanup
	os.Exit(innerTestMain(m))
}

func TestValidatePinReadNeedsWriteSubjects(t *testing.T) {
	t.Parallel()

	// Pin-read rules require MTLSBoundSubjects.
	opts := &server.Options{
		MTLSPinReadRules: []server.PinReadRule{
			{Subject: "CN=*", Pin: "host-{cn}"},
		},
	}
	if err := server.ValidateTLSOptions(opts); err == nil {
		t.Error("expected error, got nil")
	}

	// With MTLSBoundSubjects, no error.
	opts.MTLSBoundSubjects = []string{"CN=*"}
	if err := server.ValidateTLSOptions(opts); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

//nolint:paralleltest // uses t.Setenv which modifies process environment
func TestParseArgsReadPinFlag(t *testing.T) {
	// Clear NIKS3_* environment variables that could interfere with the test
	for _, env := range os.Environ() {
		if strings.HasPrefix(env, "NIKS3_") {
			key, _, _ := strings.Cut(env, "=")
			t.Setenv(key, "")
			_ = os.Unsetenv(key)
		}
	}

	// Test that --mtls-read-pin flag is correctly parsed through production parseArgsFrom.
	// This is a regression test for the bug where the flag was silently ignored
	// because registerPinReadFlags modified a copy of the slice instead of
	// modifying the caller's variable.
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	args := []string{
		"--mtls-read-pin", "CN=*=host-{cn}",
		"--mtls-bound-subject", "CN=*",
		"--db", "postgresql://localhost/niks3",
		"--s3-endpoint", "http://localhost:9000",
		"--s3-bucket", "test",
		"--s3-access-key", "x",
		"--s3-secret-key", "y",
		"--api-token", "test-token-12345678901234567890abcdef",
	}

	opts, err := server.ParseArgsFrom(fs, args)
	if err != nil {
		t.Fatalf("ParseArgsFrom failed: %v", err)
	}

	if opts == nil {
		t.Fatalf("got nil options")
	}

	if len(opts.MTLSPinReadRules) != 1 {
		t.Fatalf("expected 1 pin-read rule, got %d", len(opts.MTLSPinReadRules))
	}

	if opts.MTLSPinReadRules[0].Subject != "CN=*" {
		t.Errorf("expected subject 'CN=*', got '%s'", opts.MTLSPinReadRules[0].Subject)
	}

	if opts.MTLSPinReadRules[0].Pin != "host-{cn}" {
		t.Errorf("expected pin 'host-{cn}', got '%s'", opts.MTLSPinReadRules[0].Pin)
	}
}
