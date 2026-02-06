package ratelimit

import (
	"context"
	"log/slog"
	"sync"

	"golang.org/x/time/rate"
)

// Rate limiting constants.
// These are exported for testing purposes.
const (
	RateMin            = 5.0   // Floor: never go below this
	RateMax            = 500.0 // Ceiling: never exceed this
	RateBackoffFactor  = 0.7   // Reduce to 70% on 429 (like AWS SDK beta)
	RateRecoveryFactor = 1.1   // +10% on sustained success
	RateRecoveryAfter  = 10    // Successful requests before recovery
)

// AdaptiveRateLimiter implements adaptive rate limiting for HTTP requests.
// It starts disabled (or with an initial rate) and adapts based on throttle responses.
type AdaptiveRateLimiter struct {
	mu           sync.Mutex
	limiter      *rate.Limiter
	enabled      bool    // starts false if initialRate=0, enabled on first 429
	currentRate  float64 // current rate limit
	successCount int64   // consecutive successful requests
	name         string  // optional name for log messages
}

// NewAdaptiveRateLimiter creates a new adaptive rate limiter.
// If initialRate is 0, the limiter starts disabled and enables on first throttle.
// If initialRate > 0, the limiter starts enabled at that rate.
func NewAdaptiveRateLimiter(initialRate float64, name string) *AdaptiveRateLimiter {
	a := &AdaptiveRateLimiter{
		name: name,
	}

	if initialRate > 0 {
		// Clamp to bounds
		if initialRate < RateMin {
			initialRate = RateMin
		}

		if initialRate > RateMax {
			initialRate = RateMax
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

	if a.successCount >= RateRecoveryAfter {
		a.successCount = 0

		newRate := a.currentRate * RateRecoveryFactor
		if newRate > RateMax {
			newRate = RateMax
		}

		if newRate != a.currentRate {
			a.currentRate = newRate
			a.limiter.SetLimit(rate.Limit(newRate))
			a.limiter.SetBurst(int(newRate))
			slog.Debug("Rate limiter recovered", "name", a.name, "rate", newRate)
		}
	}
}

// RecordThrottle enables the limiter (if not already) and reduces the rate.
// This should be called when a 429 or throttling error is received.
func (a *AdaptiveRateLimiter) RecordThrottle() {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Reset success count on any throttle
	a.successCount = 0

	if !a.enabled {
		// First throttle - enable at a conservative rate
		a.enabled = true
		a.currentRate = RateMin
		a.limiter = rate.NewLimiter(rate.Limit(RateMin), int(RateMin))
		slog.Warn("Rate limiter enabled after throttle", "name", a.name, "rate", RateMin)

		return
	}

	// Already enabled - back off
	newRate := a.currentRate * RateBackoffFactor
	if newRate < RateMin {
		newRate = RateMin
	}

	a.currentRate = newRate
	a.limiter.SetLimit(rate.Limit(newRate))
	a.limiter.SetBurst(int(newRate))
	slog.Warn("Rate limiter backed off", "name", a.name, "rate", newRate)
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
