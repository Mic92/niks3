package main

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/Mic92/niks3/client"
)

// socketPath is the path to the niks3 upload socket.
// It defaults to client.DefaultSocketPath but can be overridden at build time
// via -ldflags "-X main.socketPath=/custom/path".
var socketPath = client.DefaultSocketPath

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "niks3-post-build-hook: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	return sendPaths(socketPath, os.Getenv("OUT_PATHS"))
}

// sendPaths sends each space-separated path from outPaths as a datagram to socketPath.
// Returns an error if the socket cannot be connected to. Per-datagram write errors
// are silently ignored.
func sendPaths(socket, outPaths string) error {
	if outPaths == "" {
		return nil
	}

	conn, err := net.Dial("unixgram", socket)
	if err != nil {
		return fmt.Errorf("connecting to socket %s: %w", socket, err)
	}
	defer func() { _ = conn.Close() }()

	for _, path := range strings.Fields(outPaths) {
		_, _ = conn.Write([]byte(path))
	}

	return nil
}
