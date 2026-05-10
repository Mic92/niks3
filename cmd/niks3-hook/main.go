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
	"time"

	"github.com/Mic92/niks3/client"
	"github.com/Mic92/niks3/cmdutil"
	"github.com/Mic92/niks3/hook"
)

// socketPath can be overridden at build time via ldflags:
//
//	-ldflags "-X main.socketPath=/custom/path"
var socketPath = hook.DefaultSocketPath //nolint:gochecknoglobals // ldflags override

func main() {
	if err := run(); err != nil {
		slog.Error("Fatal error", "error", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: niks3-hook <command> [flags]")
	fmt.Fprintln(os.Stderr, "\nCommands:")
	fmt.Fprintln(os.Stderr, "  send    Send store paths to the upload daemon (called by Nix post-build-hook)")
	fmt.Fprintln(os.Stderr, "  serve   Run the upload daemon (accepts paths, queues, and uploads)")
	fmt.Fprintln(os.Stderr, "\nUse 'niks3-hook <command> --help' for more information about a command.")
}

func printSendHelp() {
	fmt.Fprintln(os.Stderr, "Usage: niks3-hook send [flags]")
	fmt.Fprintln(os.Stderr, "\nSend store paths from OUT_PATHS to the upload daemon via unix socket.")
	fmt.Fprintln(os.Stderr, "Always exits 0 to avoid affecting Nix builds. Errors are logged to stderr.")
	fmt.Fprintln(os.Stderr, "\nFlags:")
	fmt.Fprintf(os.Stderr, "  --socket string\n        Unix socket path (default: %s)\n", socketPath)
	fmt.Fprintln(os.Stderr, "  -h, --help")
	fmt.Fprintln(os.Stderr, "        Show this help message")
}

func printServeHelp() {
	fmt.Fprintln(os.Stderr, "Usage: niks3-hook serve [flags]")
	fmt.Fprintln(os.Stderr, "\nRun the upload daemon. Listens on a unix stream socket, queues paths in SQLite,")
	fmt.Fprintln(os.Stderr, "and uploads them to the niks3 server in the background.")
	fmt.Fprintln(os.Stderr, "\nFlags:")
	fmt.Fprintln(os.Stderr, "  --server-url string")
	fmt.Fprintln(os.Stderr, "        Server URL (can also use NIKS3_SERVER_URL env var)")
	fmt.Fprintln(os.Stderr, cmdutil.AuthTokenHelp)
	fmt.Fprintln(os.Stderr, cmdutil.AuthTokenPathHelp)
	fmt.Fprintln(os.Stderr, cmdutil.AuthTokenScriptHelp)
	fmt.Fprintf(os.Stderr, "  --socket string\n        Unix socket path (default: %s)\n", hook.DefaultSocketPath)
	fmt.Fprintln(os.Stderr, "  --db-path string")
	fmt.Fprintln(os.Stderr, "        SQLite database path (default: /var/lib/niks3-hook/upload-queue.db)")
	fmt.Fprintln(os.Stderr, "  --batch-size int")
	fmt.Fprintln(os.Stderr, "        Paths per upload batch (default: 50)")
	fmt.Fprintln(os.Stderr, "  --idle-exit-timeout string")
	fmt.Fprintln(os.Stderr, `        Exit after no activity; "0" to disable (default: "60s")`)
	fmt.Fprintln(os.Stderr, "  --max-concurrent-uploads int")
	fmt.Fprintln(os.Stderr, "        Concurrent upload limit (default: 30)")
	fmt.Fprintln(os.Stderr, "  --verify-s3-integrity")
	fmt.Fprintln(os.Stderr, "        Verify S3 objects before skipping")
	fmt.Fprintln(os.Stderr, cmdutil.TLSHelp)
	fmt.Fprintln(os.Stderr, "  --debug")
	fmt.Fprintln(os.Stderr, "        Enable debug logging")
	fmt.Fprintln(os.Stderr, "  -h, --help")
	fmt.Fprintln(os.Stderr, "        Show this help message")
}

func run() error {
	if len(os.Args) < 2 {
		printUsage()

		return errors.New("no command provided")
	}

	switch os.Args[1] {
	case "--help", "-h", "help":
		printUsage()

		return nil
	case "send":
		return runSend()
	case "serve":
		return runServe()
	default:
		printUsage()

		return fmt.Errorf("unknown command: %s", os.Args[1])
	}
}

func runSend() error {
	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	fs.Usage = func() {}

	socket := fs.String("socket", socketPath, "Unix socket path")
	help := fs.Bool("help", false, "Show help")
	fs.BoolVar(help, "h", false, "Show help")

	if err := fs.Parse(os.Args[2:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printSendHelp()
			os.Exit(0)
		}

		return fmt.Errorf("parsing flags: %w", err)
	}

	if *help {
		printSendHelp()
		os.Exit(0)
	}

	outPaths := os.Getenv("OUT_PATHS")

	paths := strings.Fields(outPaths)
	if len(paths) == 0 {
		return nil
	}

	// Always exit 0: log errors to stderr but never return an error.
	if err := hook.SendPaths(*socket, paths); err != nil {
		fmt.Fprintf(os.Stderr, "niks3-hook send: %v\n", err)
	}

	return nil
}

func runServe() error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	cf := cmdutil.AddCommonFlags(fs)
	socket := fs.String("socket", hook.DefaultSocketPath, "Unix socket path")
	dbPath := fs.String("db-path", "/var/lib/niks3-hook/upload-queue.db", "SQLite database path")
	batchSize := fs.Int("batch-size", 50, "Paths per upload batch")
	idleExitTimeout := fs.String("idle-exit-timeout", "60s", "Exit after no activity; \"0\" to disable")
	maxConcurrent := fs.Int("max-concurrent-uploads", 30, "Concurrent upload limit")
	verifyS3 := fs.Bool("verify-s3-integrity", false, "Verify S3 integrity")
	tf := cmdutil.AddTLSFlags(fs)

	if err := fs.Parse(os.Args[2:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printServeHelp()
			os.Exit(0)
		}

		return fmt.Errorf("parsing flags: %w", err)
	}

	if *cf.Help {
		printServeHelp()
		os.Exit(0)
	}

	cmdutil.SetupLogger(*cf.Debug)

	if err := cmdutil.RequireServerURL(*cf.ServerURL); err != nil {
		return err //nolint:wrapcheck // cmdutil errors are already user-facing
	}

	ts, err := cf.TokenSource(fs, tf)
	if err != nil {
		return err //nolint:wrapcheck // cmdutil errors are already user-facing
	}

	var idleTimeout time.Duration
	if *idleExitTimeout != "0" {
		idleTimeout, err = time.ParseDuration(*idleExitTimeout)
		if err != nil {
			return fmt.Errorf("parsing --idle-exit-timeout: %w", err)
		}
	}

	if *maxConcurrent < 1 {
		*maxConcurrent = 1
	}

	// Open the SQLite queue.
	queue, err := hook.OpenQueue(*dbPath)
	if err != nil {
		return fmt.Errorf("opening queue: %w", err)
	}

	defer func() { _ = queue.Close() }()

	// Check for leftover items from a previous run.
	if count, err := queue.Count(); err == nil && count > 0 {
		slog.Info("Resuming with pending paths from previous run", "pending", count)
	}

	// Set up signal handling.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Create the niks3 client.
	c, err := client.NewClientWithTokenSource(ctx, *cf.ServerURL, ts)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	if err := tf.Configure(c); err != nil {
		return err //nolint:wrapcheck // cmdutil errors are already user-facing
	}

	c.MaxConcurrentNARUploads = *maxConcurrent

	c.VerifyS3Integrity = *verifyS3
	if *cf.Debug {
		c.SetDebugHTTP(true)
	}

	// Notification channels: the server notifies both the worker and the
	// idle timer when new paths are queued. Separate channels avoid the
	// race where one consumer steals the signal from the other.
	workerNotify := make(chan struct{}, 1)
	idleNotify := make(chan struct{}, 1)

	// QueueFunc: enqueue paths in SQLite and notify worker + idle timer.
	queueFunc := func(paths []string) error {
		if err := queue.Enqueue(paths); err != nil {
			return fmt.Errorf("enqueueing paths: %w", err)
		}
		// Non-blocking sends to wake both consumers.
		select {
		case workerNotify <- struct{}{}:
		default:
		}

		select {
		case idleNotify <- struct{}{}:
		default:
		}

		return nil
	}

	// Acquire socket.
	ln, activated, err := hook.GetListener(*socket)
	if err != nil {
		return fmt.Errorf("acquiring socket: %w", err)
	}

	defer func() { _ = ln.Close() }()

	if !activated {
		defer func() { _ = os.Remove(*socket) }()
	}

	slog.Info(
		"niks3-hook serve starting",
		"socket", *socket,
		"socket-activated", activated,
		"db-path", *dbPath,
		"batch-size", *batchSize,
		"idle-exit-timeout", idleTimeout,
	)

	// Start the upload worker.
	worker := hook.NewWorker(queue, c.PushPaths, *batchSize, workerNotify)
	workerDone := make(chan struct{})

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	go func() {
		defer close(workerDone)

		worker.Run(workerCtx)
	}()

	// Start the socket server.
	srv := hook.NewServer(ln, queueFunc)

	// Idle exit: cancel the context when both the socket is idle and the queue is empty.
	if idleTimeout > 0 {
		go func() {
			idleTimer := time.NewTimer(idleTimeout)
			defer idleTimer.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-idleNotify:
					// Activity detected — reset the idle timer.
					if !idleTimer.Stop() {
						select {
						case <-idleTimer.C:
						default:
						}
					}

					idleTimer.Reset(idleTimeout)
				case <-idleTimer.C:
					// Timer fired — only exit if the queue is empty.
					if worker.QueueEmpty() { //nolint:contextcheck // Queue.Count uses background context; local SQLite op
						slog.Info("Idle timeout reached and queue is empty, shutting down")
						stop()

						return
					}
					// Queue is not empty; reset the timer and keep waiting.
					slog.Debug("Idle timeout fired but queue is non-empty, waiting")
					idleTimer.Reset(idleTimeout)
				}
			}
		}()
	}

	// Serve blocks until context is cancelled.
	_ = srv.Serve(ctx)

	// Wait for worker to finish draining.
	workerCancel()
	<-workerDone

	slog.Info("niks3-hook serve stopped")

	return nil
}
