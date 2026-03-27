package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"
)

// ErrNotInGitHubActions is returned when the GitHub Actions OIDC environment
// variables are not set. This happens outside of GitHub Actions, or in fork
// PRs where id-token:write is not granted.
var ErrNotInGitHubActions = errors.New("GitHub Actions OIDC env vars not set (ACTIONS_ID_TOKEN_REQUEST_TOKEN/_URL)")

// GitHubOIDCTokenSource fetches OIDC tokens from the GitHub Actions runtime.
//
// Tokens expire after roughly 5–10 minutes, while a single CI job can run for
// much longer (VM tests, large builds). Callers should fetch a fresh token
// per push batch rather than once at setup.
type GitHubOIDCTokenSource struct {
	// Audience is sent as the `audience` query parameter. Must match the
	// audience configured on the niks3 server for the GitHub issuer.
	Audience string

	// HTTPClient is used for the token request. If nil, a client with a 15s
	// timeout is used.
	HTTPClient *http.Client
}

// Token fetches a fresh OIDC token from the GitHub Actions endpoint.
func (s *GitHubOIDCTokenSource) Token(ctx context.Context) (string, error) {
	reqToken := os.Getenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
	reqURL := os.Getenv("ACTIONS_ID_TOKEN_REQUEST_URL")

	if reqToken == "" || reqURL == "" {
		return "", ErrNotInGitHubActions
	}

	// The request URL already contains a query string (?audience is appended
	// to an existing ?... by GitHub's own actions/core implementation, so
	// we do the same). url.Values handles escaping.
	u, err := url.Parse(reqURL)
	if err != nil {
		return "", fmt.Errorf("parsing ACTIONS_ID_TOKEN_REQUEST_URL: %w", err)
	}

	q := u.Query()
	q.Set("audience", s.Audience)
	u.RawQuery = q.Encode()

	// The URL is supplied by the GitHub Actions runtime itself via env; an
	// attacker who can set ACTIONS_ID_TOKEN_REQUEST_URL already owns the job.
	//nolint:gosec // G704: URL from trusted GHA runtime env, not user input
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("building OIDC request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+reqToken)
	req.Header.Set("Accept", "application/json")

	client := s.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	resp, err := client.Do(req) //nolint:gosec // see above
	if err != nil {
		return "", fmt.Errorf("fetching OIDC token: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("OIDC endpoint returned %s", resp.Status)
	}

	var body struct {
		Value string `json:"value"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("decoding OIDC response: %w", err)
	}

	if body.Value == "" {
		return "", errors.New("OIDC response missing 'value' field")
	}

	return body.Value, nil
}
