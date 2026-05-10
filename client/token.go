package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// TokenSource yields a bearer token for authenticating to the niks3 server.
//
// Implementations may cache; callers should treat each call as cheap and
// invoke it immediately before issuing a request rather than holding the
// result.
type TokenSource func(ctx context.Context) (string, error)

// NoToken returns a TokenSource that yields no token. Use when the
// transport itself carries the credential, e.g. an mTLS client cert.
func NoToken() TokenSource {
	return func(context.Context) (string, error) { return "", nil }
}

// StaticToken returns a TokenSource that always yields tok and never refreshes.
// An empty tok is valid: it represents anonymous access.
func StaticToken(tok string) TokenSource {
	return func(context.Context) (string, error) { return tok, nil }
}

// fileRereadInterval bounds how often FileToken hits the filesystem when
// the cache is warm. Mirrors client-go's CachedFileTokenSource (1 min poll
// for projected service account tokens).
const fileRereadInterval = time.Minute

// FileToken returns a TokenSource that re-reads path lazily.
//
// The file is re-read at most once per minute. This matches the Kubernetes
// bound-service-account-token pattern: an external process (systemd timer,
// vault-agent, kubelet) keeps the file fresh; the consumer only needs to
// notice eventually.
//
// Trailing whitespace is trimmed. An empty file is an error: a token file
// that exists is expected to hold a token.
func FileToken(path string) TokenSource {
	var (
		mu     sync.Mutex
		cached string
		readAt time.Time
	)

	return func(context.Context) (string, error) {
		mu.Lock()
		defer mu.Unlock()

		if cached != "" && time.Since(readAt) < fileRereadInterval {
			return cached, nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("reading auth token file %q: %w", path, err)
		}

		tok := strings.TrimSpace(string(data))
		if tok == "" {
			return "", fmt.Errorf("auth token file %q is empty", path)
		}

		cached, readAt = tok, time.Now()

		return tok, nil
	}
}

// scriptOutput is the JSON document a token script must print to stdout.
// Logs and progress should go to stderr; stdout must contain exactly one
// JSON object. This mirrors the Kubernetes ExecCredential and AWS
// credential_process conventions, which proved easier to author and debug
// than marker-line protocols.
type scriptOutput struct {
	Token string `json:"token"`
	// ExpiresAt, when set, lets niks3 cache the token and refresh before
	// expiry. RFC 3339. When absent the script is rerun on every Token call,
	// which is the right behavior for scripts that cache internally (aws
	// sts, vault).
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// scriptRefreshFraction controls how early to refresh relative to the
// observed token lifetime. 0.75 means refresh after 75% of the lifetime has
// elapsed — same heuristic kubelet uses for projected tokens.
const scriptRefreshFraction = 0.75

// ScriptToken returns a TokenSource that obtains tokens by running a command.
//
// The command string is split with shell-like quoting rules (single/double
// quotes, backslash escapes — but no expansion or substitution) and exec'd
// directly, matching the AWS credential_process and Kubernetes ExecCredential
// conventions. It must print a JSON object to stdout (see scriptOutput) and
// may log freely to stderr. When the output carries expires_at the token is
// cached until 75% of its lifetime has elapsed; otherwise the command runs on
// every Token call.
//
// Wrap in `sh -c '...'` explicitly if shell features (pipes, variables) are
// needed.
func ScriptToken(script string) TokenSource {
	return scriptToken(script, time.Now)
}

func scriptToken(script string, now func() time.Time) TokenSource {
	var (
		mu        sync.Mutex
		cached    string
		refreshAt time.Time // zero ⇒ no caching
	)

	return func(ctx context.Context) (string, error) {
		mu.Lock()
		defer mu.Unlock()

		if cached != "" && !refreshAt.IsZero() && now().Before(refreshAt) {
			return cached, nil
		}

		out, err := runTokenScript(ctx, script)
		if err != nil {
			return "", err
		}

		if out.Token == "" {
			return "", errors.New("auth token script returned empty token")
		}

		cached, refreshAt = out.Token, time.Time{}

		if out.ExpiresAt != nil {
			if lifetime := out.ExpiresAt.Sub(now()); lifetime > 0 {
				refreshAt = now().Add(time.Duration(float64(lifetime) * scriptRefreshFraction))
			}
		}

		return cached, nil
	}
}

func runTokenScript(ctx context.Context, script string) (*scriptOutput, error) {
	argv, err := shellSplit(script)
	if err != nil {
		return nil, fmt.Errorf("parsing auth token script: %w", err)
	}

	if len(argv) == 0 {
		return nil, errors.New("auth token script is empty")
	}

	//nolint:gosec // command is user-provided config, like git's credential.helper
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stderr = os.Stderr

	stdout, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("running auth token script: %w", err)
	}

	var out scriptOutput
	if err := json.Unmarshal(stdout, &out); err != nil {
		return nil, fmt.Errorf("parsing auth token script output (expected JSON object on stdout): %w", err)
	}

	return &out, nil
}
