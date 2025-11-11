package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

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

// getAuthToken reads the auth token from NIKS3_AUTH_TOKEN_FILE.
// The file should contain the token as a single line (trailing whitespace is trimmed).
func getAuthToken() (string, error) {
	tokenFile := os.Getenv("NIKS3_AUTH_TOKEN_FILE")
	if tokenFile == "" {
		return "", nil
	}

	data, err := os.ReadFile(tokenFile)
	if err != nil {
		return "", fmt.Errorf("reading auth token from file %q: %w", tokenFile, err)
	}

	return strings.TrimSpace(string(data)), nil
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: niks3 <command> [flags]")
	fmt.Fprintln(os.Stderr, "\nCommands:")
	fmt.Fprintln(os.Stderr, "  push    Upload paths to S3-compatible binary cache")
	fmt.Fprintln(os.Stderr, "  gc      Run garbage collection on old closures")
	fmt.Fprintln(os.Stderr, "\nGlobal flags:")
	fmt.Fprintln(os.Stderr, "  -h, --help    Show help")
	fmt.Fprintln(os.Stderr, "\nUse 'niks3 <command> --help' for more information about a command.")
}

func printPushHelp() {
	fmt.Fprintln(os.Stderr, "Usage: niks3 push [flags] <store-paths...>")
	fmt.Fprintln(os.Stderr, "\nUpload Nix store paths to S3-compatible binary cache.")
	fmt.Fprintln(os.Stderr, "\nFlags:")
	fmt.Fprintln(os.Stderr, "  --server-url string")
	fmt.Fprintln(os.Stderr, "        Server URL (can also use NIKS3_SERVER_URL env var)")
	fmt.Fprintln(os.Stderr, "  --auth-token string")
	fmt.Fprintln(os.Stderr, "        Auth token (can also use NIKS3_AUTH_TOKEN_FILE env var)")
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
	fmt.Fprintln(os.Stderr, "  --auth-token string")
	fmt.Fprintln(os.Stderr, "        Auth token (can also use NIKS3_AUTH_TOKEN_FILE env var)")
	fmt.Fprintln(os.Stderr, "  --auth-token-path string")
	fmt.Fprintln(os.Stderr, "        Path to auth token file")
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

func run() error {
	// Check for global help flag
	if len(os.Args) < 2 {
		printUsage()
		return errors.New("no command provided")
	}

	// Handle global --help or -h
	if os.Args[1] == "--help" || os.Args[1] == "-h" || os.Args[1] == "help" {
		printUsage()
		os.Exit(0)
	}

	// Get default auth token from environment (file or var)
	defaultAuthToken, err := getAuthToken()
	if err != nil {
		return err
	}

	// Define flags for push command
	pushCmd := flag.NewFlagSet("push", flag.ContinueOnError)
	pushCmd.Usage = func() {} // Suppress default usage, we'll handle it
	pushServerURL := pushCmd.String("server-url", os.Getenv("NIKS3_SERVER_URL"), "Server URL (can also use NIKS3_SERVER_URL env var)")
	pushAuthToken := pushCmd.String("auth-token", defaultAuthToken, "Auth token (can also use NIKS3_AUTH_TOKEN_FILE env var)")
	maxConcurrent := pushCmd.Int("max-concurrent-uploads", 30, "Maximum concurrent uploads")
	verifyS3Integrity := pushCmd.Bool("verify-s3-integrity", false, "Verify that objects in database actually exist in S3 before skipping upload")
	pushDebug := pushCmd.Bool("debug", false, "Enable debug logging (includes HTTP requests/responses)")
	pushHelp := pushCmd.Bool("help", false, "Show help")
	pushCmd.BoolVar(pushHelp, "h", false, "Show help")

	// Define flags for gc command
	gcCmd := flag.NewFlagSet("gc", flag.ContinueOnError)
	gcCmd.Usage = func() {} // Suppress default usage, we'll handle it
	gcServerURL := gcCmd.String("server-url", os.Getenv("NIKS3_SERVER_URL"), "Server URL (can also use NIKS3_SERVER_URL env var)")
	gcAuthToken := gcCmd.String("auth-token", defaultAuthToken, "Auth token (can also use NIKS3_AUTH_TOKEN_FILE env var)")
	gcAuthTokenPath := gcCmd.String("auth-token-path", "", "Path to auth token file")
	olderThan := gcCmd.String("older-than", "720h", "Delete closures older than this duration (e.g., '720h' for 30 days)")
	pendingOlderThan := gcCmd.String("failed-uploads-older-than", "6h", "Delete failed uploads older than this duration (e.g., '6h' for 6 hours)")
	force := gcCmd.Bool("force", false, "Force immediate deletion without grace period (WARNING: may delete objects still being uploaded)")
	gcDebug := gcCmd.Bool("debug", false, "Enable debug logging (includes HTTP requests/responses)")
	gcHelp := gcCmd.Bool("help", false, "Show help")
	gcCmd.BoolVar(gcHelp, "h", false, "Show help")

	// Parse command
	switch os.Args[1] {
	case "push":
		if err := pushCmd.Parse(os.Args[2:]); err != nil {
			if err == flag.ErrHelp {
				printPushHelp()
				os.Exit(0)
			}
			return fmt.Errorf("parsing flags: %w", err)
		}

		if *pushHelp {
			printPushHelp()
			os.Exit(0)
		}

		// Setup logger with debug level if requested
		setupLogger(*pushDebug)

		if *pushServerURL == "" {
			return errors.New("server URL is required (use --server-url or NIKS3_SERVER_URL env var)")
		}

		if *pushAuthToken == "" {
			return errors.New("auth token is required (use --auth-token or NIKS3_AUTH_TOKEN_FILE env var)")
		}

		paths := pushCmd.Args()
		if len(paths) == 0 {
			return errors.New("at least one store path is required")
		}

		return pushCommand(*pushServerURL, *pushAuthToken, paths, *maxConcurrent, *verifyS3Integrity, *pushDebug)

	case "gc":
		if err := gcCmd.Parse(os.Args[2:]); err != nil {
			if err == flag.ErrHelp {
				printGcHelp()
				os.Exit(0)
			}
			return fmt.Errorf("parsing flags: %w", err)
		}

		if *gcHelp {
			printGcHelp()
			os.Exit(0)
		}

		// Setup logger with debug level if requested
		setupLogger(*gcDebug)

		if *gcServerURL == "" {
			return errors.New("server URL is required (use --server-url or NIKS3_SERVER_URL env var)")
		}

		// Handle auth token from file if specified
		token := *gcAuthToken

		if *gcAuthTokenPath != "" {
			tokenData, err := os.ReadFile(*gcAuthTokenPath)
			if err != nil {
				return fmt.Errorf("reading auth token file: %w", err)
			}

			token = strings.TrimSpace(string(tokenData))
		}

		if token == "" {
			return errors.New("auth token is required (use --auth-token, --auth-token-path, or NIKS3_AUTH_TOKEN_FILE env var)")
		}

		return gcCommand(*gcServerURL, token, *olderThan, *pendingOlderThan, *force, *gcDebug)

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

	// Create client
	c, err := client.NewClient(ctx, serverURL, authToken)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	// Set maximum concurrent uploads
	c.MaxConcurrentNARUploads = maxConcurrent
	c.VerifyS3Integrity = verifyS3Integrity

	// Enable HTTP debug logging if requested
	if debug {
		c.SetDebugHTTP(true)
	}

	// Use the high-level PushPaths method
	if err := c.PushPaths(ctx, paths); err != nil {
		return fmt.Errorf("pushing paths: %w", err)
	}

	return nil
}

func gcCommand(serverURL, authToken, olderThan, pendingOlderThan string, force bool, debug bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Create client
	c, err := client.NewClient(ctx, serverURL, authToken)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	// Enable HTTP debug logging if requested
	if debug {
		c.SetDebugHTTP(true)
	}

	if force {
		slog.Warn("WARNING: Force mode enabled - objects will be deleted immediately without grace period")
		slog.Warn("This may delete objects that are currently being uploaded or referenced")
	}

	slog.Info("Starting garbage collection", "older-than", olderThan, "failed-uploads-older-than", pendingOlderThan, "force", force)

	// Run garbage collection
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
