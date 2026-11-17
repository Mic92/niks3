package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/Mic92/niks3/api"
)

// ClosuresExist returns the store paths that were pushed as closures.
func (c *Client) ClosuresExist(ctx context.Context, paths []string) (map[string]bool, error) {
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

	req := api.ExistsRequest{Keys: keys}

	var resp api.ExistsResponse

	err := c.doJSONRequest(ctx, http.MethodPost, c.baseURL.JoinPath("api/closures/exists").String(), req, &resp, http.StatusOK)

	// An older server has the endpoint under its former name.
	var status *HTTPStatusError
	if errors.As(err, &status) && (status.StatusCode == http.StatusNotFound || status.StatusCode == http.StatusMethodNotAllowed) {
		var former api.PresentResponse

		err = c.doJSONRequest(ctx, http.MethodPost, c.baseURL.JoinPath("api/objects/present").String(), req, &former, http.StatusOK)
		resp.Exists = former.Present
	}

	if err != nil {
		return nil, fmt.Errorf("querying pushed closures: %w", err)
	}

	exists := make(map[string]bool, len(resp.Exists))
	for _, key := range resp.Exists {
		exists[byKey[key]] = true
	}

	return exists, nil
}
