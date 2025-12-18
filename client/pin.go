package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
)

type createPinRequest struct {
	StorePath string `json:"store_path"`
}

// PinInfo represents a pin's information.
type PinInfo struct {
	Name       string `json:"name"`
	NarinfoKey string `json:"narinfo_key"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
}

// CreatePin creates or updates a pin that maps a name to a store path.
// The pin protects the associated closure from garbage collection and
// makes the store path retrievable via curl cache.domain.tld/pins/<name>.
func (c *Client) CreatePin(ctx context.Context, name, storePath string) error {
	reqURL := c.baseURL.JoinPath("api/pins", name)

	reqBody := createPinRequest{
		StorePath: storePath,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshaling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.authToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.DoWithRetry(ctx, req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK, http.StatusNoContent); err != nil {
		return err
	}

	slog.Debug("Created pin", "name", name, "store_path", storePath)

	return nil
}

// ListPins returns all pins from the server.
func (c *Client) ListPins(ctx context.Context) ([]PinInfo, error) {
	reqURL := c.baseURL.JoinPath("api/pins")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.authToken)

	resp, err := c.DoWithRetry(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK); err != nil {
		return nil, err
	}

	var pins []PinInfo
	if err := json.NewDecoder(resp.Body).Decode(&pins); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	slog.Debug("Listed pins", "count", len(pins))

	return pins, nil
}

// DeletePin deletes a pin by name.
func (c *Client) DeletePin(ctx context.Context, name string) error {
	reqURL := c.baseURL.JoinPath("api/pins", name)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, reqURL.String(), http.NoBody)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.authToken)

	resp, err := c.DoWithRetry(ctx, req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK, http.StatusNoContent); err != nil {
		return err
	}

	slog.Debug("Deleted pin", "name", name)

	return nil
}
