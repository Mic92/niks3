package hook

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

const sendTimeout = 5 * time.Second

// SendPaths connects to the unix stream socket at socketPath, sends the given
// store paths as a JSON request, reads the JSON acknowledgement, and returns
// any error. The caller should always exit 0 regardless of the error to avoid
// affecting Nix builds.
func SendPaths(socketPath string, paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()

	dialer := net.Dialer{Timeout: sendTimeout}

	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return fmt.Errorf("connecting to socket %s: %w", socketPath, err)
	}

	defer func() { _ = conn.Close() }()

	// Set a deadline for the entire send+receive operation.
	if err := conn.SetDeadline(time.Now().Add(sendTimeout)); err != nil {
		return fmt.Errorf("setting deadline: %w", err)
	}

	// Send the request.
	req := Request{Paths: paths}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	// Read the response.
	var resp Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return fmt.Errorf("reading response: %w", err)
	}

	if resp.Status != "ok" {
		return fmt.Errorf("server error: %s", resp.Message)
	}

	return nil
}
