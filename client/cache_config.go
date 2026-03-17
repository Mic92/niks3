package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/Mic92/niks3/api"
)

// FetchCacheConfig retrieves substituter URL, public keys, and OIDC audience
// from the niks3 server's unauthenticated /api/cache-config endpoint.
func FetchCacheConfig(ctx context.Context, serverURL, issuer string) (*api.CacheConfig, error) {
	u, err := url.Parse(serverURL)
	if err != nil {
		return nil, fmt.Errorf("parsing server URL: %w", err)
	}

	u = u.JoinPath("api", "cache-config")
	u.RawQuery = url.Values{"issuer": {issuer}}.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("building request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching cache-config: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned %s", resp.Status)
	}

	var cfg api.CacheConfig
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &cfg, nil
}
