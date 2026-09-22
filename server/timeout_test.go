package server_test

import (
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
}
