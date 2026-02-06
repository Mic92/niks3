package client_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Mic92/niks3/client"
	"github.com/Mic92/niks3/ratelimit"
)

func newTestClientWithRetries(httpClient *http.Client, maxRetries int) *client.Client {
	return client.NewTestClient(httpClient, client.RetryConfig{
		MaxRetries:     maxRetries,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     1 * time.Millisecond,
		Multiplier:     1.0,
		Jitter:         0,
	})
}

// TestRateLimiterFeedback verifies that the rate limiter is updated correctly
// based on HTTP response status codes: throttle on 429/503, success on 2xx,
// and no change on other non-retryable statuses like 400.
func TestRateLimiterFeedback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		status        int
		maxRetries    int
		expectEnabled bool
	}{
		{
			name:          "429 enables limiter",
			status:        http.StatusTooManyRequests,
			maxRetries:    1,
			expectEnabled: true,
		},
		{
			name:          "503 enables limiter",
			status:        http.StatusServiceUnavailable,
			maxRetries:    1,
			expectEnabled: true,
		},
		{
			name:          "200 does not enable limiter",
			status:        http.StatusOK,
			maxRetries:    0,
			expectEnabled: false,
		},
		{
			name:          "400 does not enable limiter",
			status:        http.StatusBadRequest,
			maxRetries:    0,
			expectEnabled: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			c := newTestClientWithRetries(srv.Client(), tc.maxRetries)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
			if err != nil {
				t.Fatal(err)
			}

			resp, err := c.DoServerRequest(t.Context(), req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if err := resp.Body.Close(); err != nil {
				t.Errorf("closing response body: %v", err)
			}

			got := c.ServerRateLimiter.IsEnabled()
			if got != tc.expectEnabled {
				t.Errorf("limiter.IsEnabled() = %v, want %v after status %d", got, tc.expectEnabled, tc.status)
			}
		})
	}
}

// TestRateLimiterFeedback_400DoesNotCountAsSuccess verifies that a 400
// response does not advance the success counter when the limiter is already
// enabled. This prevents client errors from being mistaken for available
// server capacity.
func TestRateLimiterFeedback_400DoesNotCountAsSuccess(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	c := newTestClientWithRetries(srv.Client(), 0)

	// Force-enable the limiter, then verify a 400 doesn't change the rate.
	c.ServerRateLimiter.RecordThrottle()

	rateBefore := c.ServerRateLimiter.CurrentRate()

	// Send RateRecoveryAfter requests — if 400 counted as success the rate
	// would increase after this many calls.
	for range ratelimit.RateRecoveryAfter {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := c.DoServerRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := resp.Body.Close(); err != nil {
			t.Errorf("closing response body: %v", err)
		}
	}

	rateAfter := c.ServerRateLimiter.CurrentRate()
	if rateAfter != rateBefore {
		t.Errorf("rate changed after %d 400s: before=%f after=%f", ratelimit.RateRecoveryAfter, rateBefore, rateAfter)
	}
}
