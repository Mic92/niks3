package client

import (
	"context"
	"fmt"
	"net/http"

	"github.com/Mic92/niks3/api"
)

// Present returns the store paths whose closure is already cached.
func (c *Client) Present(ctx context.Context, paths []string) (map[string]bool, error) {
	keys := make([]string, 0, len(paths))
	byKey := make(map[string]string, len(paths))

	for _, p := range paths {
		hash, err := GetStorePathHash(p)
		if err != nil {
			return nil, err
		}

		key := hash + ".narinfo"
		keys = append(keys, key)
		byKey[key] = p
	}

	var resp api.PresentResponse
	if err := c.doJSONRequest(ctx, http.MethodPost, c.baseURL.JoinPath("api/objects/present").String(),
		api.PresentRequest{Keys: keys}, &resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("querying present paths: %w", err)
	}

	present := make(map[string]bool, len(resp.Present))
	for _, key := range resp.Present {
		present[byKey[key]] = true
	}

	return present, nil
}
