package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Mic92/niks3/cmdutil"
)

// Push-mode names, stored in $GITHUB_STATE and switched on in stop.
const (
	modeDaemon    = "daemon"
	modeStorescan = "storescan"
	modeNone      = "none"
)

// State keys written by setup via $GITHUB_STATE, read by stop via $STATE_*.
const (
	stateKeyMode      = "mode"
	stateKeySocket    = "socket"
	stateKeyDaemonPID = "daemon_pid"
	stateKeyDaemonLog = "daemon_log"
	stateKeySnapshot  = "snapshot"
	stateKeyServerURL = "server_url"
	stateKeyAudience  = "audience"
)

func printCIHelp() {
	fmt.Fprintln(os.Stderr, "Usage: niks3 ci <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "\nGitHub Actions integration. These subcommands are driven by the")
	fmt.Fprintln(os.Stderr, "Mic92/niks3 action and are not meant for interactive use.")
	fmt.Fprintln(os.Stderr, "\nSubcommands:")
	fmt.Fprintln(os.Stderr, "  setup    Fetch cache config, write nix.conf, start daemon (action main)")
	fmt.Fprintln(os.Stderr, "  stop     Drain daemon or run storescan diff (action post)")
	fmt.Fprintln(os.Stderr, "  daemon   Unix-socket upload daemon (forked by setup)")
	fmt.Fprintln(os.Stderr, "  push     Post-build-hook client (writes OUT_PATHS to daemon socket)")
	fmt.Fprintln(os.Stderr, "\nShared flags:")
	fmt.Fprintln(os.Stderr, "  --work-dir string   Working directory (default: $RUNNER_TEMP/niks3)")
}

// defaultWorkDir resolves the default working directory for CI subcommands.
// $RUNNER_TEMP is job-scoped and cleaned up by the runner between jobs.
func defaultWorkDir() string {
	if rt := os.Getenv("RUNNER_TEMP"); rt != "" {
		return filepath.Join(rt, "niks3")
	}

	return filepath.Join(os.TempDir(), "niks3")
}

// runCI dispatches `niks3 ci <sub>`. Each subcommand gets its own FlagSet;
// there's no shared flag parsing beyond --work-dir.
func runCI(args []string) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		printCIHelp()

		return nil
	}

	sub, rest := args[0], args[1:]

	switch sub {
	case "setup":
		fs := flag.NewFlagSet("ci setup", flag.ContinueOnError)
		workDir := fs.String("work-dir", defaultWorkDir(), "working directory")
		debug := fs.Bool("debug", false, "enable debug logging")

		if err := fs.Parse(rest); err != nil {
			return fmt.Errorf("parsing flags: %w", err)
		}

		cmdutil.SetupLogger(*debug)

		return runCISetup(*workDir)

	case "stop":
		fs := flag.NewFlagSet("ci stop", flag.ContinueOnError)
		timeout := fs.Duration("timeout", 600*time.Second, "drain timeout")
		debug := fs.Bool("debug", false, "enable debug logging")

		if err := fs.Parse(rest); err != nil {
			return fmt.Errorf("parsing flags: %w", err)
		}

		cmdutil.SetupLogger(*debug)

		return runCIStop(*timeout)

	case "daemon":
		fs := flag.NewFlagSet("ci daemon", flag.ContinueOnError)
		socket := fs.String("socket", "", "unix socket path (required)")
		serverURL := fs.String("server-url", "", "niks3 server URL (required)")
		audience := fs.String("audience", "", "OIDC audience (required)")
		debug := fs.Bool("debug", false, "enable debug logging")

		if err := fs.Parse(rest); err != nil {
			return fmt.Errorf("parsing flags: %w", err)
		}

		if *socket == "" || *serverURL == "" || *audience == "" {
			return errors.New("ci daemon requires --socket, --server-url, --audience")
		}

		cmdutil.SetupLogger(*debug)

		return runCIDaemon(*socket, daemonConfig{
			serverURL: *serverURL,
			audience:  *audience,
			debug:     *debug,
		})

	case "push":
		fs := flag.NewFlagSet("ci push", flag.ContinueOnError)
		socket := fs.String("socket", "", "unix socket path (required)")

		if err := fs.Parse(rest); err != nil {
			return fmt.Errorf("parsing flags: %w", err)
		}

		if *socket == "" {
			// Still exit 0 — this is the hook path. A missing flag means
			// the shim is broken; failing here would wedge the build.
			fmt.Fprintln(os.Stderr, "niks3 ci push: --socket required")

			return nil
		}

		runCIPush(*socket)

		return nil

	default:
		printCIHelp()

		return fmt.Errorf("unknown ci subcommand: %s", sub)
	}
}
