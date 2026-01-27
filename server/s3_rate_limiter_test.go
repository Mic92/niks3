package server_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
)

func TestAdaptiveRateLimiter_ThreadSafety(t *testing.T) {
	t.Parallel()

	limiter := server.NewAdaptiveRateLimiter(100)

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
	rate := limiter.CurrentRate()
	if rate < server.S3RateMin || rate > server.S3RateMax {
		t.Errorf("Rate %f out of bounds [%f, %f]", rate, server.S3RateMin, server.S3RateMax)
	}
}

func TestAdaptiveRateLimiter_WaitRespectsContext(t *testing.T) {
	t.Parallel()

	// Create limiter with very low rate
	limiter := server.NewAdaptiveRateLimiter(1) // 1 req/s

	// First call succeeds
	ctx := t.Context()
	err := limiter.Wait(ctx)
	ok(t, err)

	// Second call with immediate timeout should fail
	ctxTimeout, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
	defer cancel()

	// Give it a moment to expire
	time.Sleep(1 * time.Millisecond)

	err = limiter.Wait(ctxTimeout)
	if err == nil {
		t.Error("Expected context deadline error")
	}
}
