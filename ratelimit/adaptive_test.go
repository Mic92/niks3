package ratelimit_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Mic92/niks3/ratelimit"
)

func TestAdaptiveRateLimiter_ThreadSafety(t *testing.T) {
	t.Parallel()

	limiter := ratelimit.NewAdaptiveRateLimiter(100, "test")

	var wg sync.WaitGroup

	// Spawn multiple goroutines doing concurrent operations
	for range 10 {
		wg.Add(3)

		// Goroutine doing waits
		go func() {
			defer wg.Done()

			for range 50 {
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
				_ = limiter.Wait(ctx)

				cancel()
			}
		}()

		// Goroutine doing success records
		go func() {
			defer wg.Done()

			for range 50 {
				limiter.RecordSuccess()
			}
		}()

		// Goroutine doing throttle records
		go func() {
			defer wg.Done()

			for range 10 {
				limiter.RecordThrottle()
			}
		}()
	}

	wg.Wait()

	// Just verify it didn't panic and rate is within bounds
	r := limiter.CurrentRate()
	if r < ratelimit.RateMin || r > ratelimit.RateMax {
		t.Errorf("Rate %f out of bounds [%f, %f]", r, ratelimit.RateMin, ratelimit.RateMax)
	}
}
