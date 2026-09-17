package client

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Mic92/niks3/api"
)

// Long-running pushers (--stdin, upload hook) still see config changes within a minute.
const cacheConfigTTL = time.Minute

// GetCacheConfig fetches the public cache configuration from the server
// (GET /api/cache-config), including the maximum accepted NAR size.
// Cached for cacheConfigTTL.
func (c *Client) GetCacheConfig(ctx context.Context) (*api.CacheConfig, error) {
	c.cacheConfigMu.Lock()
	defer c.cacheConfigMu.Unlock()

	if c.cacheConfig != nil && time.Since(c.cacheConfigAt) < cacheConfigTTL {
		return c.cacheConfig, nil
	}

	cfg := &api.CacheConfig{}

	url := c.baseURL.JoinPath("/api/cache-config")
	if err := c.doJSONRequest(ctx, http.MethodGet, url.String(), nil, cfg, http.StatusOK); err != nil {
		return nil, fmt.Errorf("fetching cache config: %w", err)
	}

	c.cacheConfig, c.cacheConfigAt = cfg, time.Now()

	return cfg, nil
}

type skippedUploadsRequest struct {
	Paths    uint64 `json:"paths"`
	NarBytes uint64 `json:"nar_bytes"`
}

// ReportSkippedUploads tells the server how many store paths were skipped
// due to the max NAR size limit, so it can expose the numbers as metrics.
// Best effort: failures are logged and the push continues.
func (c *Client) ReportSkippedUploads(ctx context.Context, paths, narBytes uint64) {
	if paths == 0 {
		return
	}

	url := c.baseURL.JoinPath("/api/uploads/skipped")

	err := c.doJSONRequest(ctx, http.MethodPost, url.String(),
		skippedUploadsRequest{Paths: paths, NarBytes: narBytes}, nil, http.StatusOK, http.StatusNoContent)
	if err != nil {
		slog.Warn("Failed to report skipped uploads", "error", err)
	}
}
