package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Mic92/niks3/client"
)

// setupLogger configures the global slog logger with the specified level.
func setupLogger(debug bool) {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}

	handler := slog.NewTextHandler(os.Stderr, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)
}

func main() {
	if err := run(); err != nil {
		slog.Error("Fatal error", "error", err)
		os.Exit(1)
	}
}

// defaultAuthTokenPath returns the default XDG-compliant path for the auth token.
func defaultAuthTokenPath() string {
	configDir := os.Getenv("XDG_CONFIG_HOME")
	if configDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		configDir = filepath.Join(home, ".config")
	}
	return filepath.Join(configDir, "niks3", "auth-token")
}

// getAuthToken reads the auth token from NIKS3_AUTH_TOKEN_FILE or the default XDG path.
// The file should contain the token as a single line (trailing whitespace is trimmed).
func getAuthToken() (string, error) {
	tokenFile := os.Getenv("NIKS3_AUTH_TOKEN_FILE")
	if tokenFile == "" {
		tokenFile = defaultAuthTokenPath()
		if tokenFile == "" {
			return "", nil
		}
		if _, err := os.Stat(tokenFile); os.IsNotExist(err) {
			return "", nil
		}
	}

	data, err := os.ReadFile(tokenFile)
	if err != nil {
		return "", fmt.Errorf("reading auth token from file %q: %w", tokenFile, err)
	}

	return strings.TrimSpace(string(data)), nil
}

// resolveAuthToken returns the token from the file at flagTokenPath if set,
// otherwise falls back to flagToken (from --auth-token or the env/XDG default).
func resolveAuthToken(flagToken, flagTokenPath string) (string, error) {
	if flagTokenPath != "" {
		data, err := os.ReadFile(flagTokenPath)
		if err != nil {
			return "", fmt.Errorf("reading auth token file: %w", err)
		}
		return strings.TrimSpace(string(data)), nil
	}
	return flagToken, nil
}

// commonFlags holds pointers to flags shared across all subcommands.
type commonFlags struct {
	serverURL *string
	authToken *string
	debug     *bool
	help      *bool
}

// addCommonFlags registers --server-url, --auth-token, --debug, and -h/--help
// on the given FlagSet and returns pointers to them.
func addCommonFlags(fs *flag.FlagSet, defaultAuthToken string) commonFlags {
	fs.Usage = func() {} // Suppress default usage; each command prints its own.
	cf := commonFlags{
		serverURL: fs.String("server-url", os.Getenv("NIKS3_SERVER_URL"), "Server URL"),
		authToken: fs.String("auth-token", defaultAuthToken, "Auth token"),
		debug:     fs.Bool("debug", false, "Enable debug logging"),
		help:      fs.Bool("help", false, "Show help"),
	}
	fs.BoolVar(cf.help, "h", false, "Show help")
	return cf
}

func requireServerURL(url string) error {
	if url == "" {
		return errors.New("server URL is required (use --server-url or NIKS3_SERVER_URL env var)")
	}
	return nil
}

func requireAuthToken(token string) error {
	if token == "" {
		return errors.New("auth token is required (use --auth-token, --auth-token-path, NIKS3_AUTH_TOKEN_FILE, or $XDG_CONFIG_HOME/niks3/auth-token)")
	}
	return nil
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: niks3 <command> [flags]")
	fmt.Fprintln(os.Stderr, "\nCommands:")
	fmt.Fprintln(os.Stderr, "  push    Upload paths to S3-compatible binary cache")
	fmt.Fprintln(os.Stderr, "  listen  Listen on a socket for paths to upload (for post-build-hook)")
	fmt.Fprintln(os.Stderr, "  gc      Run garbage collection on old closures")
	fmt.Fprintln(os.Stderr, "\nGlobal flags:")
	fmt.Fprintln(os.Stderr, "  -h, --help    Show help")
	fmt.Fprintln(os.Stderr, "\nUse 'niks3 <command> --help' for more information about a command.")
}

const authTokenHelp = `  --auth-token string
        Auth token (default: reads from $XDG_CONFIG_HOME/niks3/auth-token or NIKS3_AUTH_TOKEN_FILE)
        WARNING: tokens passed on the command line are visible in /proc and shell history;
        prefer --auth-token-path or NIKS3_AUTH_TOKEN_FILE`

const authTokenPathHelp = `  --auth-token-path string
        Path to file containing the auth token (preferred over --auth-token)`

func printPushHelp() {
	fmt.Fprintln(os.Stderr, "Usage: niks3 push [flags] <store-paths...>")
	fmt.Fprintln(os.Stderr, "\nUpload Nix store paths to S3-compatible binary cache.")
	fmt.Fprintln(os.Stderr, "\nFlags:")
	fmt.Fprintln(os.Stderr, "  --server-url string")
	fmt.Fprintln(os.Stderr, "        Server URL (can also use NIKS3_SERVER_URL env var)")
	fmt.Fprintln(os.Stderr, authTokenHelp)
	fmt.Fprintln(os.Stderr, "  --max-concurrent-uploads int")
	fmt.Fprintln(os.Stderr, "        Maximum concurrent uploads (default: 30)")
	fmt.Fprintln(os.Stderr, "  --verify-s3-integrity")
	fmt.Fprintln(os.Stderr, "        Verify that objects in database actually exist in S3 before skipping upload")
	fmt.Fprintln(os.Stderr, "  --debug")
	fmt.Fprintln(os.Stderr, "        Enable debug logging (includes HTTP requests/responses)")
	fmt.Fprintln(os.Stderr, "  -h, --help")
	fmt.Fprintln(os.Stderr, "        Show this help message")
}

func printGcHelp() {
	fmt.Fprintln(os.Stderr, "Usage: niks3 gc [flags]")
	fmt.Fprintln(os.Stderr, "\nRun garbage collection on old closures and failed uploads.")
	fmt.Fprintln(os.Stderr, "\nFlags:")
	fmt.Fprintln(os.Stderr, "  --server-url string")
	fmt.Fprintln(os.Stderr, "        Server URL (can also use NIKS3_SERVER_URL env var)")
	fmt.Fprintln(os.Stderr, authTokenHelp)
	fmt.Fprintln(os.Stderr, authTokenPathHelp)
	fmt.Fprintln(os.Stderr, "  --older-than string")
	fmt.Fprintln(os.Stderr, "        Delete closures older than this duration (default: '720h' for 30 days)")
	fmt.Fprintln(os.Stderr, "  --failed-uploads-older-than string")
	fmt.Fprintln(os.Stderr, "        Delete failed uploads older than this duration (default: '6h')")
	fmt.Fprintln(os.Stderr, "  --force")
	fmt.Fprintln(os.Stderr, "        Force immediate deletion without grace period")
	fmt.Fprintln(os.Stderr, "        WARNING: may delete objects still being uploaded")
	fmt.Fprintln(os.Stderr, "  --debug")
	fmt.Fprintln(os.Stderr, "        Enable debug logging (includes HTTP requests/responses)")
	fmt.Fprintln(os.Stderr, "  -h, --help")
	fmt.Fprintln(os.Stderr, "        Show this help message")
}

func printListenHelp() {
	fmt.Fprintln(os.Stderr, "Usage: niks3 listen [flags]")
	fmt.Fprintln(os.Stderr, "\nListen on a unix datagram socket for store paths to upload.")
	fmt.Fprintln(os.Stderr, "Designed to work with niks3-post-build-hook and systemd socket activation.")
	fmt.Fprintln(os.Stderr, "\nWhen running standalone (without socket activation), the process must have")
	fmt.Fprintln(os.Stderr, "write access to the socket directory to create and remove the socket file.")
	fmt.Fprintln(os.Stderr, "\nFlags:")
	fmt.Fprintln(os.Stderr, "  --server-url string")
	fmt.Fprintln(os.Stderr, "        Server URL (can also use NIKS3_SERVER_URL env var)")
	fmt.Fprintln(os.Stderr, authTokenHelp)
	fmt.Fprintln(os.Stderr, authTokenPathHelp)
	fmt.Fprintln(os.Stderr, "  --socket string")
	fmt.Fprintf(os.Stderr, "        Socket path (default: %s)\n", client.DefaultSocketPath)
	fmt.Fprintln(os.Stderr, "  --batch-size int")
	fmt.Fprintln(os.Stderr, "        Number of paths per push batch (default: 50)")
	fmt.Fprintln(os.Stderr, "  --batch-timeout string")
	fmt.Fprintln(os.Stderr, "        Max wait before pushing a partial batch (default: \"10s\")")
	fmt.Fprintln(os.Stderr, "  --idle-exit-timeout string")
	fmt.Fprintln(os.Stderr, "        Exit after this duration with no paths received; \"0\" to disable (default: \"60s\")")
	fmt.Fprintln(os.Stderr, "  --max-errors int")
	fmt.Fprintln(os.Stderr, "        Exit after this many consecutive push errors (default: 5)")
	fmt.Fprintln(os.Stderr, "  --max-concurrent-uploads int")
	fmt.Fprintln(os.Stderr, "        Maximum concurrent uploads (default: 30)")
	fmt.Fprintln(os.Stderr, "  --verify-s3-integrity")
	fmt.Fprintln(os.Stderr, "        Verify that objects in database actually exist in S3 before skipping upload")
	fmt.Fprintln(os.Stderr, "  --debug")
	fmt.Fprintln(os.Stderr, "        Enable debug logging (includes HTTP requests/responses)")
	fmt.Fprintln(os.Stderr, "  -h, --help")
	fmt.Fprintln(os.Stderr, "        Show this help message")
}

func run() error {
	if len(os.Args) < 2 {
		printUsage()
		return errors.New("no command provided")
	}

	// Handle global --help or -h before reading auth files.
	if os.Args[1] == "--help" || os.Args[1] == "-h" || os.Args[1] == "help" {
		printUsage()
		os.Exit(0)
	}

	// Get default auth token from environment/XDG (after help check so
	// --help never reads auth files or produces auth errors).
	defaultAuthToken, err := getAuthToken()
	if err != nil {
		return err
	}

	switch os.Args[1] {
	case "push":
		pushCmd := flag.NewFlagSet("push", flag.ContinueOnError)
		cf := addCommonFlags(pushCmd, defaultAuthToken)
		maxConcurrent := pushCmd.Int("max-concurrent-uploads", 30, "Maximum concurrent uploads")
		verifyS3Integrity := pushCmd.Bool("verify-s3-integrity", false, "Verify S3 integrity")

		if err := pushCmd.Parse(os.Args[2:]); err != nil {
			if err == flag.ErrHelp {
				printPushHelp()
				os.Exit(0)
			}
			return fmt.Errorf("parsing flags: %w", err)
		}
		if *cf.help {
			printPushHelp()
			os.Exit(0)
		}

		setupLogger(*cf.debug)

		if err := requireServerURL(*cf.serverURL); err != nil {
			return err
		}
		if err := requireAuthToken(*cf.authToken); err != nil {
			return err
		}

		paths := pushCmd.Args()
		if len(paths) == 0 {
			return errors.New("at least one store path is required")
		}

		return pushCommand(*cf.serverURL, *cf.authToken, paths, *maxConcurrent, *verifyS3Integrity, *cf.debug)

	case "gc":
		gcCmd := flag.NewFlagSet("gc", flag.ContinueOnError)
		cf := addCommonFlags(gcCmd, defaultAuthToken)
		authTokenPath := gcCmd.String("auth-token-path", "", "Path to auth token file")
		olderThan := gcCmd.String("older-than", "720h", "Delete closures older than this duration")
		pendingOlderThan := gcCmd.String("failed-uploads-older-than", "6h", "Delete failed uploads older than this duration")
		force := gcCmd.Bool("force", false, "Force immediate deletion without grace period")

		if err := gcCmd.Parse(os.Args[2:]); err != nil {
			if err == flag.ErrHelp {
				printGcHelp()
				os.Exit(0)
			}
			return fmt.Errorf("parsing flags: %w", err)
		}
		if *cf.help {
			printGcHelp()
			os.Exit(0)
		}

		setupLogger(*cf.debug)

		if err := requireServerURL(*cf.serverURL); err != nil {
			return err
		}

		token, err := resolveAuthToken(*cf.authToken, *authTokenPath)
		if err != nil {
			return err
		}
		if err := requireAuthToken(token); err != nil {
			return err
		}

		return gcCommand(*cf.serverURL, token, *olderThan, *pendingOlderThan, *force, *cf.debug)

	case "listen":
		listenCmd := flag.NewFlagSet("listen", flag.ContinueOnError)
		cf := addCommonFlags(listenCmd, defaultAuthToken)
		authTokenPath := listenCmd.String("auth-token-path", "", "Path to auth token file")
		listenSocket := listenCmd.String("socket", client.DefaultSocketPath, "Socket path")
		listenBatchSize := listenCmd.Int("batch-size", 50, "Number of paths per push batch")
		listenBatchTimeout := listenCmd.String("batch-timeout", "10s", "Max wait before pushing a partial batch")
		listenIdleExitTimeout := listenCmd.String("idle-exit-timeout", "60s", "Exit after this duration with no paths; \"0\" to disable")
		listenMaxErrors := listenCmd.Int("max-errors", 5, "Exit after this many consecutive push errors")
		listenMaxConcurrent := listenCmd.Int("max-concurrent-uploads", 30, "Maximum concurrent uploads")
		listenVerifyS3 := listenCmd.Bool("verify-s3-integrity", false, "Verify S3 integrity")

		if err := listenCmd.Parse(os.Args[2:]); err != nil {
			if err == flag.ErrHelp {
				printListenHelp()
				os.Exit(0)
			}
			return fmt.Errorf("parsing flags: %w", err)
		}
		if *cf.help {
			printListenHelp()
			os.Exit(0)
		}

		setupLogger(*cf.debug)

		if err := requireServerURL(*cf.serverURL); err != nil {
			return err
		}

		token, err := resolveAuthToken(*cf.authToken, *authTokenPath)
		if err != nil {
			return err
		}
		if err := requireAuthToken(token); err != nil {
			return err
		}

		batchTimeout, err := time.ParseDuration(*listenBatchTimeout)
		if err != nil {
			return fmt.Errorf("parsing --batch-timeout: %w", err)
		}

		var idleTimeout time.Duration
		if *listenIdleExitTimeout != "0" {
			idleTimeout, err = time.ParseDuration(*listenIdleExitTimeout)
			if err != nil {
				return fmt.Errorf("parsing --idle-exit-timeout: %w", err)
			}
		}

		return listenCommand(*cf.serverURL, token, *listenSocket, *listenBatchSize, batchTimeout, idleTimeout, *listenMaxErrors, *listenMaxConcurrent, *listenVerifyS3, *cf.debug)

	default:
		return fmt.Errorf("unknown command: %s", os.Args[1])
	}
}

func pushCommand(serverURL, authToken string, paths []string, maxConcurrent int, verifyS3Integrity bool, debug bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if maxConcurrent < 1 {
		maxConcurrent = 1
	}

	c, err := client.NewClient(ctx, serverURL, authToken)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	c.MaxConcurrentNARUploads = maxConcurrent
	c.VerifyS3Integrity = verifyS3Integrity

	if debug {
		c.SetDebugHTTP(true)
	}

	if err := c.PushPaths(ctx, paths); err != nil {
		return fmt.Errorf("pushing paths: %w", err)
	}

	return nil
}

func gcCommand(serverURL, authToken, olderThan, pendingOlderThan string, force bool, debug bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := client.NewClient(ctx, serverURL, authToken)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	if debug {
		c.SetDebugHTTP(true)
	}

	if force {
		slog.Warn("WARNING: Force mode enabled - objects will be deleted immediately without grace period")
		slog.Warn("This may delete objects that are currently being uploaded or referenced")
	}

	slog.Info("Starting garbage collection", "older-than", olderThan, "failed-uploads-older-than", pendingOlderThan, "force", force)

	stats, err := c.RunGarbageCollection(ctx, olderThan, pendingOlderThan, force)
	if err != nil {
		return fmt.Errorf("running garbage collection: %w", err)
	}

	slog.Info("Garbage collection completed successfully",
		"failed-uploads-deleted", stats.FailedUploadsDeleted,
		"old-closures-deleted", stats.OldClosuresDeleted,
		"objects-marked-for-deletion", stats.ObjectsMarkedForDeletion,
		"objects-deleted-after-grace-period", stats.ObjectsDeletedAfterGracePeriod,
		"objects-failed-to-delete", stats.ObjectsFailedToDelete,
	)

	return nil
}

func listenCommand(serverURL, authToken, socketPath string, batchSize int, batchTimeout, idleTimeout time.Duration, maxErrors, maxConcurrent int, verifyS3Integrity bool, debug bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if maxConcurrent < 1 {
		maxConcurrent = 1
	}

	// Acquire socket (systemd activation or self-created).
	conn, activated, err := client.GetSocket(socketPath)
	if err != nil {
		return fmt.Errorf("acquiring socket: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if !activated {
		// Clean up self-created socket on exit.
		defer func() { _ = os.Remove(socketPath) }()
	}

	slog.Info("Listening for store paths",
		"socket", socketPath,
		"socket-activated", activated,
		"batch-size", batchSize,
		"batch-timeout", batchTimeout,
		"idle-exit-timeout", idleTimeout,
		"max-errors", maxErrors,
	)

	c, err := client.NewClient(ctx, serverURL, authToken)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	c.MaxConcurrentNARUploads = maxConcurrent
	c.VerifyS3Integrity = verifyS3Integrity

	if debug {
		c.SetDebugHTTP(true)
	}

	l := client.NewListener(conn, c.PushPaths, client.ListenerConfig{
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
		IdleTimeout:  idleTimeout,
		MaxErrors:    maxErrors,
	})

	if err := l.Run(ctx); err != nil {
		return fmt.Errorf("listener: %w", err)
	}

	return nil
}
