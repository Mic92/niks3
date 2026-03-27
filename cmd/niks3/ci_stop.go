package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// runCIStop is invoked by the action's post step. It dials the daemon socket,
// sends "stop", and blocks until the daemon replies with stats (after draining).
//
// If the daemon is already dead (dial fails), that's not an error — the job
// might have been a fork PR with no daemon started, or the daemon crashed.
// Either way the post step shouldn't fail; push failures are cache misses,
// not CI gates.
//
// Stop is also the dispatch point for storescan mode: if STATE_mode says
// storescan, it diffs the store snapshot and pushes instead of dialing.
func runCIStop(timeout time.Duration) error {
	mode := ghaGetState(stateKeyMode)
	debug := ghaGetInputBool("debug")

	switch mode {
	case modeDaemon:
		socket := ghaGetState(stateKeySocket)

		return stopDaemon(socket, timeout)

	case modeStorescan:
		snapshot := ghaGetState(stateKeySnapshot)
		serverURL := ghaGetState(stateKeyServerURL)
		audience := ghaGetState(stateKeyAudience)

		return storescanDiff(snapshot, serverURL, audience, debug)

	case modeNone, "":
		// No push configured (fork PR, skip-push, or setup never ran).
		return nil

	default:
		return fmt.Errorf("unknown mode %q in STATE_mode", mode)
	}
}

func stopDaemon(socket string, timeout time.Duration) error {
	logPath := ghaGetState(stateKeyDaemonLog)

	d := net.Dialer{Timeout: 5 * time.Second}

	conn, err := d.DialContext(context.Background(), "unix", socket)
	if err != nil {
		// Daemon not running — not an error at the action level.
		ghaWarningf("niks3 daemon not reachable at %s: %v", socket, err)

		return nil
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(timeout))

	if _, err := fmt.Fprintln(conn, wireCmdStop); err != nil {
		return fmt.Errorf("sending stop: %w", err)
	}

	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			ghaWarningf("niks3 daemon drain timed out after %v (daemon log: %s)", timeout, logPath)
			killDaemon()

			return nil
		}

		return fmt.Errorf("reading drain reply: %w", err)
	}

	reportStats(reply, logPath)

	return nil
}

// reportStats parses "stats <pushed> <skipped> <queued>" and emits a ::notice::.
func reportStats(reply, logPath string) {
	fields := strings.Fields(reply)
	if len(fields) != 4 || fields[0] != "stats" {
		ghaNoticef("niks3 daemon drained: %s", strings.TrimSpace(reply))

		return
	}

	pushed, _ := strconv.Atoi(fields[1])
	skipped, _ := strconv.Atoi(fields[2])
	queued, _ := strconv.Atoi(fields[3])

	if skipped > 0 {
		ghaWarningf("niks3: pushed %d/%d paths, %d skipped (see %s)", pushed, queued, skipped, logPath)
	} else {
		ghaNoticef("niks3: pushed %d/%d paths", pushed, queued)
	}
}

// killDaemon SIGKILLs the daemon process group if we saved its pid at setup.
// Used only on drain timeout — a clean stop goes through the socket.
func killDaemon() {
	pidStr := ghaGetState(stateKeyDaemonPID)
	if pidStr == "" {
		return
	}

	pid, err := strconv.Atoi(pidStr)
	if err != nil || pid <= 0 {
		return
	}

	// Negative pid targets the process group — the daemon was started with
	// Setpgid so its child `nix` subprocesses go too.
	_ = syscall.Kill(-pid, syscall.SIGKILL)
}
