package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"

	minio "github.com/minio/minio-go/v7"
	"golang.org/x/time/rate"
)

// Rate limiting constants.
// These are exported for testing purposes.
const (
	S3RateMin            = 5.0   // Floor: never go below this
	S3RateMax            = 500.0 // Ceiling: never exceed this
	S3RateBackoffFactor  = 0.7   // Reduce to 70% on 429 (like AWS SDK beta)
	S3RateRecoveryFactor = 1.1   // +10% on sustained success
	S3RateRecoveryAfter  = 10    // Successful requests before recovery
)

// AdaptiveRateLimiter implements adaptive rate limiting for S3 requests.
// It starts disabled (or with an initial rate) and adapts based on throttle responses.
type AdaptiveRateLimiter struct {
	mu           sync.Mutex
	limiter      *rate.Limiter
	enabled      bool    // starts false if initialRate=0, enabled on first 429
	currentRate  float64 // current rate limit
	successCount int64   // consecutive successful requests
}

// NewAdaptiveRateLimiter creates a new adaptive rate limiter.
// If initialRate is 0, the limiter starts disabled and enables on first throttle.
// If initialRate > 0, the limiter starts enabled at that rate.
func NewAdaptiveRateLimiter(initialRate float64) *AdaptiveRateLimiter {
	a := &AdaptiveRateLimiter{}

	if initialRate > 0 {
		// Clamp to bounds
		if initialRate < S3RateMin {
			initialRate = S3RateMin
		}

		if initialRate > S3RateMax {
			initialRate = S3RateMax
		}

		a.enabled = true
		a.currentRate = initialRate
		a.limiter = rate.NewLimiter(rate.Limit(initialRate), int(initialRate))
	}

	return a
}

// Wait blocks until a request is allowed or the context is canceled.
// If the limiter is disabled, it returns immediately.
func (a *AdaptiveRateLimiter) Wait(ctx context.Context) error {
	a.mu.Lock()
	enabled := a.enabled
	limiter := a.limiter
	a.mu.Unlock()

	if !enabled || limiter == nil {
		return nil
	}

	return limiter.Wait(ctx) //nolint:wrapcheck // context errors should not be wrapped
}

// RecordSuccess tracks a successful request. After enough consecutive successes,
// the rate limit is gradually increased.
func (a *AdaptiveRateLimiter) RecordSuccess() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.enabled {
		return
	}

	a.successCount++

	if a.successCount >= S3RateRecoveryAfter {
		a.successCount = 0

		newRate := a.currentRate * S3RateRecoveryFactor
		if newRate > S3RateMax {
			newRate = S3RateMax
		}

		if newRate != a.currentRate {
			a.currentRate = newRate
			a.limiter.SetLimit(rate.Limit(newRate))
			a.limiter.SetBurst(int(newRate))
			slog.Debug("S3 rate limiter recovered", "rate", newRate)
		}
	}
}

// RecordThrottle enables the limiter (if not already) and reduces the rate.
// This should be called when an S3 429 or throttling error is received.
func (a *AdaptiveRateLimiter) RecordThrottle() {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Reset success count on any throttle
	a.successCount = 0

	if !a.enabled {
		// First throttle - enable at a conservative rate
		a.enabled = true
		a.currentRate = S3RateMin
		a.limiter = rate.NewLimiter(rate.Limit(S3RateMin), int(S3RateMin))
		slog.Warn("S3 rate limiter enabled after throttle", "rate", S3RateMin)

		return
	}

	// Already enabled - back off
	newRate := a.currentRate * S3RateBackoffFactor
	if newRate < S3RateMin {
		newRate = S3RateMin
	}

	a.currentRate = newRate
	a.limiter.SetLimit(rate.Limit(newRate))
	a.limiter.SetBurst(int(newRate))
	slog.Warn("S3 rate limiter backed off", "rate", newRate)
}

// IsEnabled returns whether the rate limiter is currently active.
func (a *AdaptiveRateLimiter) IsEnabled() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.enabled
}

// CurrentRate returns the current rate limit (requests per second).
// Returns 0 if disabled.
func (a *AdaptiveRateLimiter) CurrentRate() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.enabled {
		return 0
	}

	return a.currentRate
}

// handleS3Error checks if an error is a rate limit and returns appropriate HTTP response.
// Returns true if the error was handled (caller should return), false if caller should
// handle the error itself. It checks the error message for rate limit indicators since
// errors may be wrapped as they bubble up through the call stack.
func (s *Service) handleS3Error(w http.ResponseWriter, err error, operation string) bool {
	if err == nil {
		return false
	}

	// Check if any error in the chain is a rate limit error
	if isRateLimitError(err) {
		// Record throttle to adapt the rate limiter
		s.S3RateLimiter.RecordThrottle()

		slog.Warn("S3 rate limit hit", "operation", operation, "error", err)
		w.Header().Set("Retry-After", "2")
		http.Error(w, "S3 rate limit exceeded, please retry", http.StatusTooManyRequests)

		return true
	}

	return false
}

// isRateLimitError checks if a minio error (or any error in a wrapped chain)
// is a rate limit/throttle response. S3 uses 503 with "SlowDown" error code.
// Some S3-compatible providers may use 429 or other codes.
func isRateLimitError(err error) bool {
	for err != nil {
		if isMinioRateLimitError(err) {
			return true
		}

		err = errors.Unwrap(err)
	}

	return false
}

// isMinioRateLimitError checks a single error (not unwrapped) for rate limit indicators.
func isMinioRateLimitError(err error) bool {
	errResp := minio.ToErrorResponse(err)

	// Check S3 error codes (primary detection method)
	switch errResp.Code {
	case "SlowDown", "SlowDownRead", "SlowDownWrite",
		"Throttling", "ThrottlingException",
		"RequestThrottled", "RequestLimitExceeded":
		return true
	}

	// Some S3-compatible providers use 429 instead of 503+SlowDown
	if errResp.StatusCode == http.StatusTooManyRequests {
		return true
	}

	return false
}
