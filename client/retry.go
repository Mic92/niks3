package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RetryConfig holds retry configuration for HTTP requests.
type RetryConfig struct {
	MaxRetries     int           // Maximum number of retry attempts (0 = no retries)
	InitialBackoff time.Duration // Initial backoff duration
	MaxBackoff     time.Duration // Maximum backoff duration
	Multiplier     float64       // Backoff multiplier for each retry
	Jitter         float64       // Random jitter factor (0.0-1.0)
}

// DefaultRetryConfig returns sensible defaults for retry configuration.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:     5,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2.0,
		Jitter:         0.1,
	}
}

// isRetryableStatus determines if an HTTP status code should trigger a retry.
func isRetryableStatus(statusCode int) bool {
	// Retry on server errors and specific gateway errors
	switch statusCode {
	case http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout,      // 504
		http.StatusInsufficientStorage, // 507
		http.StatusRequestTimeout,      // 408
		429:                            // Too Many Requests (rate limiting)
		return true
	default:
		return false
	}
}

// isRetryableError determines if an error should trigger a retry.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Network errors, timeouts, connection issues are retryable
	// This includes context.DeadlineExceeded from request timeouts
	// but NOT context.Canceled (user canceled the operation)
	if errors.Is(err, context.Canceled) {
		return false
	}

	// All other errors (network errors, EOF, connection reset, etc.) are retryable
	return true
}

// calculateBackoff calculates the backoff duration for a given attempt.
func (c *RetryConfig) calculateBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return c.InitialBackoff
	}

	// Calculate exponential backoff
	backoff := float64(c.InitialBackoff) * math.Pow(c.Multiplier, float64(attempt))

	// Cap at max backoff
	if backoff > float64(c.MaxBackoff) {
		backoff = float64(c.MaxBackoff)
	}

	// Add jitter to prevent thundering herd
	if c.Jitter > 0 {
		//nolint:gosec // math/rand is fine for jitter (not cryptographic)
		jitter := backoff * c.Jitter * (rand.Float64()*2 - 1) // +/- jitter%
		backoff += jitter
	}

	return time.Duration(backoff)
}

// closeResponseBody safely drains and closes an HTTP response body.
func closeResponseBody(body io.ReadCloser) {
	if body == nil {
		return
	}

	if _, err := io.Copy(io.Discard, body); err != nil {
		slog.Warn("Failed to drain response body", "error", err)
	}

	if err := body.Close(); err != nil {
		slog.Warn("Failed to close response body", "error", err)
	}
}

// retryAfterDuration parses the Retry-After header and returns the duration to wait.
// The Retry-After header can be either:
// - A number of seconds (e.g., "120")
// - An HTTP-date (e.g., "Fri, 31 Dec 1999 23:59:59 GMT")
// Returns 0 if the header is missing, invalid, or in the past.
func retryAfterDuration(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}

	retryAfter := resp.Header.Get("Retry-After")
	if retryAfter == "" {
		return 0
	}

	// Try parsing as seconds (integer)
	if seconds, err := strconv.ParseInt(strings.TrimSpace(retryAfter), 10, 64); err == nil {
		if seconds > 0 {
			return time.Duration(seconds) * time.Second
		}

		return 0
	}

	// Try parsing as HTTP-date
	if retryTime, err := http.ParseTime(retryAfter); err == nil {
		duration := time.Until(retryTime)
		if duration > 0 {
			return duration
		}

		return 0
	}

	// Invalid format
	return 0
}

// DoWithRetry executes an HTTP request with exponential backoff retry logic.
// The request body will be read and stored for retries if necessary.
// If the request is to the API server (matches baseURL host), the Authorization header is automatically added.
func (c *Client) DoWithRetry(ctx context.Context, req *http.Request) (*http.Response, error) {
	// Add auth header for API requests (same host as baseURL)
	if c.authToken != "" && req.URL.Host == c.baseURL.Host {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	// If retries are disabled, just do the request once
	if c.Retry.MaxRetries <= 0 {
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("executing request: %w", err)
		}

		return resp, nil
	}

	// Store request body for retries (if it exists and is seekable)
	var bodyBytes []byte

	if req.Body != nil && req.Body != http.NoBody {
		var err error

		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("reading request body for retry: %w", err)
		}

		if err := req.Body.Close(); err != nil {
			slog.Warn("Failed to close request body", "error", err)
		}
	}

	var lastErr error

	var lastResp *http.Response

	for attempt := 0; attempt <= c.Retry.MaxRetries; attempt++ {
		// Recreate request body for retry attempts
		if bodyBytes != nil {
			req.Body = io.NopCloser(newBytesReader(bodyBytes))
			req.ContentLength = int64(len(bodyBytes))
		}

		// Execute request
		resp, err := c.httpClient.Do(req)

		// Success case
		if err == nil && !isRetryableStatus(resp.StatusCode) {
			return resp, nil
		}

		// Store error/response for potential final return
		lastErr = err
		lastResp = resp

		// Determine if we should retry
		shouldRetry := false
		if err != nil {
			shouldRetry = isRetryableError(err)
		} else if isRetryableStatus(resp.StatusCode) {
			shouldRetry = true
			// Close the response body before retrying
			closeResponseBody(resp.Body)
		}

		// Check if we've exhausted retries
		if !shouldRetry || attempt == c.Retry.MaxRetries {
			if err != nil {
				return nil, fmt.Errorf("request failed after retries: %w", err)
			}

			return resp, nil
		}

		// Calculate backoff
		backoff := c.Retry.calculateBackoff(attempt)

		// Honor server-provided Retry-After header if present
		if resp != nil {
			if ra := retryAfterDuration(resp); ra > backoff {
				backoff = ra
			}
		}

		// Log retry attempt
		if err != nil {
			slog.Warn("Request failed, retrying",
				"attempt", attempt+1,
				"max_attempts", c.Retry.MaxRetries+1,
				"backoff", backoff,
				"error", err,
				"url", req.URL.Redacted())
		} else {
			slog.Warn("Request returned retryable status, retrying",
				"attempt", attempt+1,
				"max_attempts", c.Retry.MaxRetries+1,
				"backoff", backoff,
				"status", resp.StatusCode,
				"url", req.URL.Redacted())
		}

		// Wait before retry (check context cancellation)
		select {
		case <-ctx.Done():
			// Context canceled, return immediately
			if lastResp != nil {
				closeResponseBody(lastResp.Body)
			}

			return nil, fmt.Errorf("context canceled during retry: %w", ctx.Err())
		case <-time.After(backoff):
			// Continue to next retry
		}
	}

	// Should never reach here, but return last error/response
	if lastErr != nil {
		return nil, lastErr
	}

	return lastResp, nil
}

// bytesReader is a wrapper to make []byte seekable for request retries.
type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}

	n := copy(p, r.data[r.pos:])
	r.pos += n

	return n, nil
}

func (r *bytesReader) Close() error {
	return nil
}
