package client_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/client"
)

// TestCreatePendingClosure_RetriesOn409 verifies that the client backs off and
// retries when the server returns HTTP 409 (duplicate multipart upload still in
// progress), eventually succeeding once the lock is released.
func TestCreatePendingClosure_RetriesOn409(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/pending_closures" {
			http.Error(w, "unexpected path", http.StatusNotFound)

			return
		}

		n := attempts.Add(1)
		if n < 3 {
			http.Error(w, "multipart upload already in progress for this object", http.StatusConflict)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		if err := json.NewEncoder(w).Encode(map[string]any{
			"id":              "42",
			"started_at":      "2026-06-26T00:00:00Z",
			"pending_objects": map[string]any{},
		}); err != nil {
			t.Errorf("encoding response: %v", err)
		}
	}))
	defer srv.Close()

	conflictRetry := client.RetryConfig{
		MaxRetries:     5,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
		Multiplier:     2.0,
		Jitter:         0,
	}

	c, err := client.NewTestClientForServer(srv.URL, conflictRetry)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreatePendingClosure(context.Background(), "test.narinfo", nil, false)
	if err != nil {
		t.Fatalf("CreatePendingClosure failed: %v", err)
	}

	if resp.ID != "42" {
		t.Errorf("unexpected ID: %q", resp.ID)
	}

	if got := attempts.Load(); got != 3 {
		t.Errorf("expected 3 attempts, got %d", got)
	}
}

// TestCreatePendingClosure_409ExhaustsRetries verifies that we surface the 409
// body to the caller after exhausting the retry budget, instead of looping
// forever.
func TestCreatePendingClosure_409ExhaustsRetries(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		http.Error(w, "multipart upload already in progress for this object", http.StatusConflict)
	}))
	defer srv.Close()

	conflictRetry := client.RetryConfig{
		MaxRetries:     2,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
		Multiplier:     2.0,
		Jitter:         0,
	}

	c, err := client.NewTestClientForServer(srv.URL, conflictRetry)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.CreatePendingClosure(context.Background(), "test.narinfo", nil, false)
	if err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}

	if !strings.Contains(err.Error(), "409") {
		t.Errorf("expected error to mention 409, got: %v", err)
	}

	// 1 initial attempt + MaxRetries retries = 3 total
	if got := attempts.Load(); got != 3 {
		t.Errorf("expected 3 attempts, got %d", got)
	}
}

// TestCreatePendingClosure_409RespectsContext verifies that an in-flight 409
// backoff is interrupted promptly when the caller cancels the context, instead
// of waiting out the full backoff window.
func TestCreatePendingClosure_409RespectsContext(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "multipart upload already in progress for this object", http.StatusConflict)
	}))
	defer srv.Close()

	conflictRetry := client.RetryConfig{
		MaxRetries:     10,
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2.0,
		Jitter:         0,
	}

	c, err := client.NewTestClientForServer(srv.URL, conflictRetry)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()

	_, err = c.CreatePendingClosure(ctx, "test.narinfo", nil, false)
	if err == nil {
		t.Fatal("expected error from canceled context, got nil")
	}

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("expected prompt cancellation, took %v", elapsed)
	}
}
