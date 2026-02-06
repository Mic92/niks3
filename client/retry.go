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

	"github.com/Mic92/niks3/ratelimit"
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

// DoServerRequest executes an HTTP request to the niks3 server with rate limiting and retry.
func (c *Client) DoServerRequest(ctx context.Context, req *http.Request) (*http.Response, error) {
	return c.doWithRetry(ctx, req, c.ServerRateLimiter)
}

// DoS3Request executes an HTTP request to S3 (presigned URL) with rate limiting and retry.
func (c *Client) DoS3Request(ctx context.Context, req *http.Request) (*http.Response, error) {
	return c.doWithRetry(ctx, req, c.S3RateLimiter)
}

// DoWithRetry executes an HTTP request with exponential backoff retry logic.
//
// Deprecated: Use DoServerRequest or DoS3Request instead to get proper rate limiting.
func (c *Client) DoWithRetry(ctx context.Context, req *http.Request) (*http.Response, error) {
	return c.doWithRetry(ctx, req, c.ServerRateLimiter)
}

// recordLimiterFeedback updates the rate limiter based on the HTTP response status.
func recordLimiterFeedback(limiter *ratelimit.AdaptiveRateLimiter, statusCode int) {
	if limiter == nil {
		return
	}

	switch {
	case statusCode == http.StatusTooManyRequests || statusCode == http.StatusServiceUnavailable:
		limiter.RecordThrottle()
	case statusCode >= 200 && statusCode < 300:
		limiter.RecordSuccess()
	}
}

// waitForLimiter blocks until the rate limiter allows a request.
func waitForLimiter(ctx context.Context, limiter *ratelimit.AdaptiveRateLimiter) error {
	if limiter == nil {
		return nil
	}

	if err := limiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter: %w", err)
	}

	return nil
}

// doWithRetry executes an HTTP request with adaptive rate limiting and exponential backoff retry.
// The request body will be read and stored for retries if necessary.
func (c *Client) doWithRetry(ctx context.Context, req *http.Request, limiter *ratelimit.AdaptiveRateLimiter) (*http.Response, error) {
	// If retries are disabled, just do the request once
	if c.Retry.MaxRetries <= 0 {
		return c.doOnce(ctx, req, limiter)
	}

	// Require GetBody for retries so we can replay the body without
	// copying it to the heap. http.NewRequest sets GetBody automatically
	// for *bytes.Reader and *strings.Reader, which all callers use.
	// This avoids copying mmap'd data to the heap on retries.
	if req.Body != nil && req.Body != http.NoBody && req.GetBody == nil {
		return nil, errors.New("request with body must have GetBody set for retry support")
	}

	var lastErr error

	var lastResp *http.Response

	for attempt := 0; attempt <= c.Retry.MaxRetries; attempt++ {
		if err := waitForLimiter(ctx, limiter); err != nil {
			return nil, err
		}

		// Reset body for retry attempts using GetBody
		if req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, fmt.Errorf("getting request body for retry: %w", err)
			}

			req.Body = body
		}

		// Execute request
		resp, err := c.httpClient.Do(req)

		// Success case
		if err == nil && !isRetryableStatus(resp.StatusCode) {
			recordLimiterFeedback(limiter, resp.StatusCode)

			return resp, nil
		}

		// Record throttle on 429 or 503
		if err == nil {
			recordLimiterFeedback(limiter, resp.StatusCode)
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

// doOnce executes a single HTTP request without retries, with rate limiting feedback.
func (c *Client) doOnce(ctx context.Context, req *http.Request, limiter *ratelimit.AdaptiveRateLimiter) (*http.Response, error) {
	if err := waitForLimiter(ctx, limiter); err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}

	recordLimiterFeedback(limiter, resp.StatusCode)

	return resp, nil
}
