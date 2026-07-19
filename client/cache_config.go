package client

import (
	"context"
	"fmt"
	"net/http"

	"github.com/Mic92/niks3/api"
)

// GetCacheConfig fetches the public cache configuration from the server
// (GET /api/cache-config), including the maximum accepted NAR size.
func (c *Client) GetCacheConfig(ctx context.Context) (*api.CacheConfig, error) {
	cfg := &api.CacheConfig{}

	url := c.baseURL.JoinPath("/api/cache-config")
	if err := c.doJSONRequest(ctx, http.MethodGet, url.String(), nil, cfg, http.StatusOK); err != nil {
		return nil, fmt.Errorf("fetching cache config: %w", err)
	}

	return cfg, nil
}
