package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Mic92/niks3/client"
	"github.com/Mic92/niks3/cmdutil"
)

func main() {
	if err := run(); err != nil {
		slog.Error("Fatal error", "error", err)
		os.Exit(1)
	}
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
	fmt.Fprintln(os.Stderr, cmdutil.AuthTokenHelp)
	fmt.Fprintln(os.Stderr, cmdutil.AuthTokenPathHelp)
	fmt.Fprintln(os.Stderr, cmdutil.AuthTokenScriptHelp)
	fmt.Fprintln(os.Stderr, "  --max-concurrent-uploads int")
	fmt.Fprintln(os.Stderr, "        Maximum concurrent uploads (default: 30)")
	fmt.Fprintln(os.Stderr, "  --verify-s3-integrity")
	fmt.Fprintln(os.Stderr, "        Verify that objects in database actually exist in S3 before skipping upload")
	fmt.Fprintln(os.Stderr, cmdutil.TLSHelp)
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
	fmt.Fprintln(os.Stderr, cmdutil.AuthTokenHelp)
	fmt.Fprintln(os.Stderr, cmdutil.AuthTokenPathHelp)
	fmt.Fprintln(os.Stderr, cmdutil.AuthTokenScriptHelp)
	fmt.Fprintln(os.Stderr, "  --older-than string")
	fmt.Fprintln(os.Stderr, "        Delete closures older than this duration (default: '720h' for 30 days)")
	fmt.Fprintln(os.Stderr, "  --failed-uploads-older-than string")
	fmt.Fprintln(os.Stderr, "        Delete failed uploads older than this duration (default: '6h')")
	fmt.Fprintln(os.Stderr, "  --force")
	fmt.Fprintln(os.Stderr, "        Force immediate deletion without grace period")
	fmt.Fprintln(os.Stderr, "        WARNING: may delete objects still being uploaded")
	fmt.Fprintln(os.Stderr, cmdutil.TLSHelp)
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

	switch os.Args[1] {
	case "push":
		pushCmd := flag.NewFlagSet("push", flag.ContinueOnError)
		cf := cmdutil.AddCommonFlags(pushCmd)
		maxConcurrent := pushCmd.Int("max-concurrent-uploads", 30, "Maximum concurrent uploads")
		verifyS3Integrity := pushCmd.Bool("verify-s3-integrity", false, "Verify S3 integrity")
		tf := cmdutil.AddTLSFlags(pushCmd)

		if err := pushCmd.Parse(os.Args[2:]); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				printPushHelp()
				os.Exit(0)
			}

			return fmt.Errorf("parsing flags: %w", err)
		}

		if *cf.Help {
			printPushHelp()
			os.Exit(0)
		}

		cmdutil.SetupLogger(*cf.Debug)

		if err := cmdutil.RequireServerURL(*cf.ServerURL); err != nil {
			return err //nolint:wrapcheck // cmdutil errors are already user-facing
		}

		ts, err := cf.TokenSource(pushCmd, tf)
		if err != nil {
			return err //nolint:wrapcheck // cmdutil errors are already user-facing
		}

		paths := pushCmd.Args()
		if len(paths) == 0 {
			return errors.New("at least one store path is required")
		}

		return pushCommand(*cf.ServerURL, ts, paths, *maxConcurrent, *verifyS3Integrity, *cf.Debug, tf)

	case "gc":
		gcCmd := flag.NewFlagSet("gc", flag.ContinueOnError)
		cf := cmdutil.AddCommonFlags(gcCmd)
		olderThan := gcCmd.String("older-than", "720h", "Delete closures older than this duration")
		pendingOlderThan := gcCmd.String("failed-uploads-older-than", "6h", "Delete failed uploads older than this duration")
		force := gcCmd.Bool("force", false, "Force immediate deletion without grace period")
		tf := cmdutil.AddTLSFlags(gcCmd)

		if err := gcCmd.Parse(os.Args[2:]); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				printGcHelp()
				os.Exit(0)
			}

			return fmt.Errorf("parsing flags: %w", err)
		}

		if *cf.Help {
			printGcHelp()
			os.Exit(0)
		}

		cmdutil.SetupLogger(*cf.Debug)

		if err := cmdutil.RequireServerURL(*cf.ServerURL); err != nil {
			return err //nolint:wrapcheck // cmdutil errors are already user-facing
		}

		ts, err := cf.TokenSource(gcCmd, tf)
		if err != nil {
			return err //nolint:wrapcheck // cmdutil errors are already user-facing
		}

		return gcCommand(*cf.ServerURL, ts, *olderThan, *pendingOlderThan, *force, *cf.Debug, tf)

	default:
		return fmt.Errorf("unknown command: %s", os.Args[1])
	}
}

func pushCommand(serverURL string, ts client.TokenSource, paths []string, maxConcurrent int, verifyS3Integrity bool, debug bool, tf cmdutil.TLSFlags) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if maxConcurrent < 1 {
		maxConcurrent = 1
	}

	c, err := client.NewClientWithTokenSource(ctx, serverURL, ts)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	if err := tf.Configure(c); err != nil {
		return err //nolint:wrapcheck // cmdutil errors are already user-facing
	}

	c.MaxConcurrentNARUploads = maxConcurrent
	c.VerifyS3Integrity = verifyS3Integrity

	if debug {
		c.SetDebugHTTP(true)
	}

	if _, err := c.PushPaths(ctx, paths); err != nil {
		return fmt.Errorf("pushing paths: %w", err)
	}

	return nil
}

func gcCommand(serverURL string, ts client.TokenSource, olderThan, pendingOlderThan string, force bool, debug bool, tf cmdutil.TLSFlags) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := client.NewClientWithTokenSource(ctx, serverURL, ts)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	if err := tf.Configure(c); err != nil {
		return err //nolint:wrapcheck // cmdutil errors are already user-facing
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

	slog.Info(
		"Garbage collection completed successfully",
		"failed-uploads-deleted", stats.FailedUploadsDeleted,
		"old-closures-deleted", stats.OldClosuresDeleted,
		"objects-marked-for-deletion", stats.ObjectsMarkedForDeletion,
		"objects-deleted-after-grace-period", stats.ObjectsDeletedAfterGracePeriod,
		"objects-failed-to-delete", stats.ObjectsFailedToDelete,
	)

	return nil
}
