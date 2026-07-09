package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// doJSONRequest sends a request to the server API and checks the response
// status. If reqBody is non-nil it is JSON-encoded and sent with a JSON
// content type. If respBody is non-nil the response is JSON-decoded into it.
func (c *Client) doJSONRequest(
	ctx context.Context,
	method, url string,
	reqBody, respBody any,
	acceptedStatuses ...int,
) error {
	var body io.Reader = http.NoBody

	if reqBody != nil {
		jsonData, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("marshaling request: %w", err)
		}

		body = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.DoServerRequest(ctx, req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, acceptedStatuses...); err != nil {
		return err
	}

	if respBody != nil {
		if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
			return fmt.Errorf("decoding response: %w", err)
		}
	}

	return nil
}
