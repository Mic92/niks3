package server_test

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// keepRaisedFileLimit hands the open-file limit Go raised for this binary on
// to the processes it starts. Go gives children the soft limit it started
// with, 256 on macOS, unless the program sets the limit itself; rustfs runs
// out of descriptors at 256 when the tests run in parallel.
func keepRaisedFileLimit() {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		slog.Warn("failed to read the open-file limit", "error", err)

		return
	}

	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		slog.Warn("failed to set the open-file limit", "error", err)
	}
}

func innerTestMain(m *testing.M) int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var err error

	// unload environment variables from the devenv
	_ = os.Unsetenv("DATABASE_URL")
	_ = os.Unsetenv("PGDATABASE")
	_ = os.Unsetenv("PGUSER")
	_ = os.Unsetenv("PGHOST")

	keepRaisedFileLimit()

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

// TestChildrenInheritRaisedFileLimit checks that postgres and rustfs get the
// open-file limit Go raised for the test binary, not the soft limit it
// started with: 256 on macOS, which rustfs runs out of under parallel tests.
func TestChildrenInheritRaisedFileLimit(t *testing.T) {
	t.Parallel()

	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		t.Fatalf("getrlimit: %v", err)
	}

	out, err := exec.CommandContext(t.Context(), "sh", "-c", "ulimit -n").Output()
	if err != nil {
		t.Fatalf("sh -c 'ulimit -n': %v", err)
	}

	got := strings.TrimSpace(string(out))
	if want := strconv.FormatUint(lim.Cur, 10); got != want && got != "unlimited" {
		t.Errorf("child open-file limit = %s, want %s", got, want)
	}
}
