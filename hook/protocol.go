// Package hook implements the niks3-hook protocol, client, server, and queue.
package hook

// Request is sent from the hook client (niks3-hook send) to the server (niks3-hook serve).
// One JSON object per line over a unix stream connection.
type Request struct {
	Paths []string `json:"paths"`
}

// Response is sent from the server back to the hook client.
type Response struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}
