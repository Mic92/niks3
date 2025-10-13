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

func run() error {
	// Get default auth token from environment (file or var)
	defaultAuthToken, err := getAuthToken()
	if err != nil {
		return err
	}

	// Define flags
	pushCmd := flag.NewFlagSet("push", flag.ExitOnError)
	serverURL := pushCmd.String("server-url", os.Getenv("NIKS3_SERVER_URL"), "Server URL (can also use NIKS3_SERVER_URL env var)")
	authToken := pushCmd.String("auth-token", defaultAuthToken, "Auth token (can also use NIKS3_AUTH_TOKEN_FILE env var)")
	maxConcurrent := pushCmd.Int("max-concurrent-uploads", 30, "Maximum concurrent uploads")

	// Parse command
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: niks3 push [flags] <store-paths...>")
		fmt.Fprintln(os.Stderr, "\nCommands:")
		fmt.Fprintln(os.Stderr, "  push    Upload paths to S3-compatible binary cache")

		return errors.New("no command provided")
	}

	switch os.Args[1] {
	case "push":
		if err := pushCmd.Parse(os.Args[2:]); err != nil {
			return fmt.Errorf("parsing flags: %w", err)
		}

		if *serverURL == "" {
			return errors.New("server URL is required (use --server-url or NIKS3_SERVER_URL env var)")
		}

		if *authToken == "" {
			return errors.New("auth token is required (use --auth-token or NIKS3_AUTH_TOKEN_FILE env var)")
		}

		paths := pushCmd.Args()
		if len(paths) == 0 {
			return errors.New("at least one store path is required")
		}

		return pushCommand(*serverURL, *authToken, paths, *maxConcurrent)

	default:
		return fmt.Errorf("unknown command: %s", os.Args[1])
	}
}

func pushCommand(serverURL, authToken string, paths []string, maxConcurrent int) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if maxConcurrent < 1 {
		maxConcurrent = 1
	}

	// Create client
	c, err := client.NewClient(serverURL, authToken)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	// Set maximum concurrent uploads
	c.MaxConcurrentNARUploads = maxConcurrent

	// Use the high-level PushPaths method
	if err := c.PushPaths(ctx, paths); err != nil {
		return fmt.Errorf("pushing paths: %w", err)
	}

	return nil
}
