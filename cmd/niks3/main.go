package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"

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
	fmt.Fprintln(os.Stderr, "  pins    Manage pins (list, delete)")
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
	fmt.Fprintln(os.Stderr, "  --no-closure")
	fmt.Fprintln(os.Stderr, "        Upload exactly the given store paths instead of their closures.")
	fmt.Fprintln(os.Stderr, "        Each path becomes its own garbage collection root. Callers must pass")
	fmt.Fprintln(os.Stderr, "        every path they want cached, including dependencies; anything left out")
	fmt.Fprintln(os.Stderr, "        is only usable if some other push already uploaded it.")
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
		pinName := pushCmd.String("pin", "", "Create a named pin for the pushed closure")
		noClosure := pushCmd.Bool("no-closure", false, "Upload only the given paths, not their closures")
		tf := cmdutil.AddTLSFlags(pushCmd)

		ts, err := cmdutil.ParseCommand(pushCmd, cf, tf, os.Args[2:], printPushHelp)
		if err != nil {
			return err //nolint:wrapcheck // cmdutil errors are already user-facing
		}

		paths := pushCmd.Args()
		if len(paths) == 0 {
			return errors.New("at least one store path is required")
		}

		if *pinName != "" && len(paths) > 1 {
			return errors.New("--pin requires exactly one store path")
		}

		// A pin protects paths reachable from its root narinfo. Under
		// --no-closure that is one path, not the closure the user meant.
		if *pinName != "" && *noClosure {
			return errors.New("--pin cannot be combined with --no-closure")
		}

		return pushCommand(*cf.ServerURL, ts, paths, pushOptions{
			MaxConcurrent:     *maxConcurrent,
			VerifyS3Integrity: *verifyS3Integrity,
			NoClosure:         *noClosure,
			PinName:           *pinName,
			Debug:             *cf.Debug,
			TLS:               tf,
		})

	case "gc":
		gcCmd := flag.NewFlagSet("gc", flag.ContinueOnError)
		cf := cmdutil.AddCommonFlags(gcCmd)
		olderThan := gcCmd.String("older-than", "720h", "Delete closures older than this duration")
		pendingOlderThan := gcCmd.String("failed-uploads-older-than", "6h", "Delete failed uploads older than this duration")
		force := gcCmd.Bool("force", false, "Force immediate deletion without grace period")
		tf := cmdutil.AddTLSFlags(gcCmd)

		ts, err := cmdutil.ParseCommand(gcCmd, cf, tf, os.Args[2:], printGcHelp)
		if err != nil {
			return err //nolint:wrapcheck // cmdutil errors are already user-facing
		}

		return gcCommand(*cf.ServerURL, ts, *olderThan, *pendingOlderThan, *force, *cf.Debug, tf)

	case "pins":
		if len(os.Args) < 3 {
			printPinsHelp()

			return errors.New("missing subcommand (create, list, or delete)")
		}

		pinsCmd := flag.NewFlagSet("pins", flag.ContinueOnError)
		cf := cmdutil.AddCommonFlags(pinsCmd)
		namesOnly := pinsCmd.Bool("names-only", false, "Output only pin names (list only)")
		jsonOutput := pinsCmd.Bool("json", false, "Output as JSON (list only)")
		tf := cmdutil.AddTLSFlags(pinsCmd)

		subcommand := os.Args[2]

		ts, err := cmdutil.ParseCommand(pinsCmd, cf, tf, os.Args[3:], printPinsHelp)
		if err != nil {
			return err //nolint:wrapcheck // cmdutil errors are already user-facing
		}

		switch subcommand {
		case "create":
			args := pinsCmd.Args()
			if len(args) < 2 {
				return errors.New("create requires a pin name and store path")
			}

			return pinsCreateCommandNew(*cf.ServerURL, ts, args[0], args[1], *cf.Debug, tf)
		case "list":
			return pinsListCommandNew(*cf.ServerURL, ts, namesOnly, jsonOutput, *cf.Debug, tf)
		case "delete":
			args := pinsCmd.Args()
			if len(args) < 1 {
				return errors.New("delete requires a pin name")
			}

			return pinsDeleteCommandNew(*cf.ServerURL, ts, args[0], *cf.Debug, tf)
		default:
			return fmt.Errorf("unknown pins subcommand: %s", subcommand)
		}

	default:
		return fmt.Errorf("unknown command: %s", os.Args[1])
	}
}

// pushOptions carries the tunables of the push subcommand.
type pushOptions struct {
	MaxConcurrent     int
	VerifyS3Integrity bool
	NoClosure         bool
	PinName           string
	Debug             bool
	TLS               cmdutil.TLSFlags
}

func pushCommand(serverURL string, ts client.TokenSource, paths []string, opts pushOptions) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	maxConcurrent := max(opts.MaxConcurrent, 1)

	c, err := cmdutil.NewClient(ctx, serverURL, ts, opts.TLS, opts.Debug)
	if err != nil {
		return err //nolint:wrapcheck // cmdutil errors are already user-facing
	}

	c.MaxConcurrentNARUploads = maxConcurrent
	c.VerifyS3Integrity = opts.VerifyS3Integrity
	c.NoClosure = opts.NoClosure

	if _, err := c.PushPaths(ctx, paths); err != nil {
		return fmt.Errorf("pushing paths: %w", err)
	}

	if pinName := opts.PinName; pinName != "" {
		// The server only accepts store paths, but users typically pass a
		// ./result symlink as produced by nix-build.
		storePath, err := c.ResolveStorePath(paths[0])
		if err != nil {
			return fmt.Errorf("resolving store path for pin %q: %w", pinName, err)
		}

		if err := c.CreatePin(ctx, pinName, storePath); err != nil {
			return fmt.Errorf("creating pin %q: %w", pinName, err)
		}

		slog.Info("Created pin", "name", pinName, "store_path", storePath)
	}

	return nil
}

func gcCommand(serverURL string, ts client.TokenSource, olderThan, pendingOlderThan string, force bool, debug bool, tf cmdutil.TLSFlags) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := cmdutil.NewClient(ctx, serverURL, ts, tf, debug)
	if err != nil {
		return err //nolint:wrapcheck // cmdutil errors are already user-facing
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

func printPinsHelp() {
	fmt.Fprintln(os.Stderr, "Usage: niks3 pins <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "\nManage pins that protect closures from garbage collection.")
	fmt.Fprintln(os.Stderr, "\nSubcommands:")
	fmt.Fprintln(os.Stderr, "  create <name> <store-path>  Create a pin for an existing store path")
	fmt.Fprintln(os.Stderr, "  list                        List all pins")
	fmt.Fprintln(os.Stderr, "  delete <name>               Delete a pin by name")
	fmt.Fprintln(os.Stderr, "\nFlags:")
	fmt.Fprintln(os.Stderr, "  --server-url string")
	fmt.Fprintln(os.Stderr, "        Server URL (can also use NIKS3_SERVER_URL env var)")
	fmt.Fprintln(os.Stderr, "  --auth-token string")
	fmt.Fprintln(os.Stderr, "        Auth token (default: $XDG_CONFIG_HOME/niks3/auth-token or NIKS3_AUTH_TOKEN_FILE)")
	fmt.Fprintln(os.Stderr, "  --names-only")
	fmt.Fprintln(os.Stderr, "        Output only pin names, one per line (for scripting, list only)")
	fmt.Fprintln(os.Stderr, "  --json")
	fmt.Fprintln(os.Stderr, "        Output as JSON (list only)")
	fmt.Fprintln(os.Stderr, "  --debug")
	fmt.Fprintln(os.Stderr, "        Enable debug logging")
	fmt.Fprintln(os.Stderr, "  -h, --help")
	fmt.Fprintln(os.Stderr, "        Show this help message")
}

func pinsCreateCommandNew(serverURL string, ts client.TokenSource, name, storePath string, debug bool, tf cmdutil.TLSFlags) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := cmdutil.NewClient(ctx, serverURL, ts, tf, debug)
	if err != nil {
		return err //nolint:wrapcheck // cmdutil errors are already user-facing
	}

	resolvedPath, err := c.ResolveStorePath(storePath)
	if err != nil {
		return fmt.Errorf("resolving store path: %w", err)
	}

	if err := c.CreatePin(ctx, name, resolvedPath); err != nil {
		return fmt.Errorf("creating pin: %w", err)
	}

	slog.Info("Created pin", "name", name, "store_path", resolvedPath)

	return nil
}

func pinsListCommandNew(serverURL string, ts client.TokenSource, namesOnly, jsonOutput *bool, debug bool, tf cmdutil.TLSFlags) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := cmdutil.NewClient(ctx, serverURL, ts, tf, debug)
	if err != nil {
		return err //nolint:wrapcheck // cmdutil errors are already user-facing
	}

	pins, err := c.ListPins(ctx)
	if err != nil {
		return fmt.Errorf("listing pins: %w", err)
	}

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")

		return enc.Encode(pins) //nolint:wrapcheck // direct output
	}

	if *namesOnly {
		for _, pin := range pins {
			fmt.Println(pin.Name)
		}

		return nil
	}

	if len(pins) == 0 {
		_, _ = fmt.Fprintln(os.Stdout, "No pins found")

		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "NAME\tSTORE PATH\tUPDATED AT")

	for _, pin := range pins {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", pin.Name, pin.StorePath, pin.UpdatedAt)
	}

	_ = w.Flush()

	return nil
}

func pinsDeleteCommandNew(serverURL string, ts client.TokenSource, name string, debug bool, tf cmdutil.TLSFlags) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := cmdutil.NewClient(ctx, serverURL, ts, tf, debug)
	if err != nil {
		return err //nolint:wrapcheck // cmdutil errors are already user-facing
	}

	if err := c.DeletePin(ctx, name); err != nil {
		return fmt.Errorf("deleting pin: %w", err)
	}

	slog.Info("Deleted pin", "name", name)

	return nil
}
