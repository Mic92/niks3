package api

import "errors"

// ErrStaleClaim means another worker took over the build. Discard the result.
var ErrStaleClaim = errors.New("stale build claim")

const (
	ClaimBuilt     = "built"
	ClaimFailed    = "failed"
	ClaimBuild     = "build"
	ClaimWait      = "wait"
	ClaimHeartbeat = "hb"
)

type ClaimRequest struct {
	Outputs []string `json:"outputs"` // narinfo keys
	// Inputs (narinfo keys) get their GC age refreshed when a build is granted.
	Inputs []string `json:"inputs,omitempty"`
	Token  int64    `json:"token,omitempty"`
}

// ClaimStatus is one NDJSON line on the claim stream.
type ClaimStatus struct {
	Status string `json:"status"`
	Token  int64  `json:"token,omitempty"`
	Kind   string `json:"kind,omitempty"`
}

// FailRequest releases a claim. A non-empty Kind is reported to clients
// currently waiting on the same build. Nothing is remembered, so a later
// request retries.
type FailRequest struct {
	ClaimToken int64  `json:"claim_token"`
	Kind       string `json:"kind"`
}
