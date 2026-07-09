package client

import (
	"context"
	"log/slog"
	"net/http"
)

type createPinRequest struct {
	StorePath string `json:"store_path"`
}

// PinInfo represents a pin's information.
type PinInfo struct {
	Name      string `json:"name"`
	StorePath string `json:"store_path"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// CreatePin creates or updates a pin that maps a name to a store path.
// The pin protects the associated closure from garbage collection and
// makes the store path retrievable via curl cache.domain.tld/pins/<name>.
func (c *Client) CreatePin(ctx context.Context, name, storePath string) error {
	reqURL := c.baseURL.JoinPath("api/pins", name)

	reqBody := createPinRequest{
		StorePath: storePath,
	}

	if err := c.doJSONRequest(ctx, http.MethodPost, reqURL.String(), reqBody, nil, http.StatusOK, http.StatusNoContent); err != nil {
		return err
	}

	slog.Debug("Created pin", "name", name, "store_path", storePath)

	return nil
}

// ListPins returns all pins from the server.
func (c *Client) ListPins(ctx context.Context) ([]PinInfo, error) {
	reqURL := c.baseURL.JoinPath("api/pins")

	var pins []PinInfo
	if err := c.doJSONRequest(ctx, http.MethodGet, reqURL.String(), nil, &pins, http.StatusOK); err != nil {
		return nil, err
	}

	slog.Debug("Listed pins", "count", len(pins))

	return pins, nil
}

// DeletePin deletes a pin by name.
func (c *Client) DeletePin(ctx context.Context, name string) error {
	reqURL := c.baseURL.JoinPath("api/pins", name)

	if err := c.doJSONRequest(ctx, http.MethodDelete, reqURL.String(), nil, nil, http.StatusOK, http.StatusNoContent); err != nil {
		return err
	}

	slog.Debug("Deleted pin", "name", name)

	return nil
}
