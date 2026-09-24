package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
)

func TestProxyWriteTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		size int64
		want time.Duration
	}{
		// Floor for tiny objects: never less than the slack.
		{"narinfo", 500, 5 * time.Minute},
		// 1 GiB at 100 kB/s floor + 5 min slack ≈ 3 h.
		{"1 GiB nar", 1 << 30, 5*time.Minute + (1<<30)/100_000*time.Second},
		// 10 GiB ≈ 30 h.
		{"10 GiB nar", 10 << 30, 5*time.Minute + (10<<30)/100_000*time.Second},
		// Zero / unknown size still gets the slack.
		{"unknown size", 0, 5 * time.Minute},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := server.ProxyWriteTimeout(tc.size)
			if got != tc.want {
				t.Errorf("ProxyWriteTimeout(%d) = %v, want %v", tc.size, got, tc.want)
			}
		})
	}
}

// A pending-closure response is written after one S3 call per new object.
// After a throttle the limiter runs at its floor, so the deadline must grow
// with the closure or a large push is cut off after its rows were created.
func TestPendingClosureWriteTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		objects int
		want    time.Duration
	}{
		{"empty", 0, time.Minute},
		{"negative", -1, time.Minute},
		// 400 NARs at the 5 rps floor take 80 s; the old fixed 60 s cut this off.
		{"400 objects", 400, time.Minute + 80*time.Second},
		{"670k objects", 670_000, time.Minute + 134_000*time.Second},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := server.PendingClosureWriteTimeout(tc.objects); got != tc.want {
				t.Errorf("PendingClosureWriteTimeout(%d) = %v, want %v", tc.objects, got, tc.want)
			}
		})
	}

	// The handler must apply it. Behind a server whose WriteTimeout runs out
	// while the handler is still working, the response is otherwise cut off
	// after the pending closure was created.
	t.Run("handler extends the server's deadline", func(t *testing.T) {
		t.Parallel()

		service := createTestService(t)
		defer service.Close()

		const writeTimeout = 200 * time.Millisecond

		service.SetTestHookBeforePendingInsert(func() { time.Sleep(4 * writeTimeout) })

		ts := httptest.NewUnstartedServer(http.HandlerFunc(service.CreatePendingClosureHandler))
		ts.Config.WriteTimeout = writeTimeout
		ts.Start()

		defer ts.Close()

		hash := strings.Repeat("w", 32)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/api/pending_closures",
			strings.NewReader(closureBody(hash, narKeyFor(hash))))
		ok(t, err)

		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("response cut off by the server's write timeout: %v", err)
		}

		defer func() { _ = resp.Body.Close() }()

		var pc server.PendingClosureResponse
		if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&pc) != nil || pc.ID == "" {
			t.Fatalf("status=%d, pending closure %+v: response incomplete", resp.StatusCode, pc)
		}
	})
}
