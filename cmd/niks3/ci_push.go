package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// runCIPush is what the Nix post-build-hook executes. It reads $OUT_PATHS
// (space-separated, per `man nix.conf`), dials the daemon socket, writes a
// "push <path> <path>...\n" line, waits for "ok\n", and exits.
//
// It ALWAYS exits 0, even on error. Per `man nix.conf`, a non-zero hook exit
// stops Nix from scheduling any further builds for the entire session —
// turning a dead daemon (a cache miss next run) into a wasted CI hour.
func runCIPush(socket string) {
	outPaths := strings.Fields(os.Getenv("OUT_PATHS"))
	if len(outPaths) == 0 {
		return
	}

	if err := pushToSocket(socket, outPaths); err != nil {
		fmt.Fprintf(os.Stderr, "niks3 ci push: %v (build continues)\n", err)
	}
}

func pushToSocket(socket string, paths []string) error {
	d := net.Dialer{Timeout: 5 * time.Second}

	conn, err := d.DialContext(context.Background(), "unix", socket)
	if err != nil {
		return fmt.Errorf("dial %s: %w", socket, err)
	}

	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	if _, err := fmt.Fprintf(conn, "%s %s\n", wireCmdPush, strings.Join(paths, " ")); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("read ack: %w", err)
	}

	if strings.TrimSpace(reply) != "ok" {
		return fmt.Errorf("daemon replied %q, expected ok", strings.TrimSpace(reply))
	}

	return nil
}
