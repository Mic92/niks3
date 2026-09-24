package server_test

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server"
)

// A GC run must end when the server shuts down. It holds a pool connection
// for its advisory lock, and Pool.Close blocks until that connection is
// released, so an uncancellable run would hang the process until SIGKILL.
//
// Shutdown is tried before the run starts and while the run is blocked at
// each kind of point a long run spends its time in: an S3 request of the
// sweep and a row lock in the mark. A run that only checks for shutdown up
// front passes the first case and hangs in the others.
func TestGCEndsOnShutdown(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// block makes the run block somewhere. It returns a channel that is
		// closed once the run does and a func that lifts the block (the test
		// calls it before Pool.Close, which would wait for a lock holder).
		// nil means shutdown precedes the run.
		block func(t *testing.T, service *server.Service, orphan string) (<-chan struct{}, func())
	}{
		{"before the run", nil},
		{"blocked in the sweep's S3 delete", blockGCInSweepDelete},
		{"blocked on a row lock in the mark", blockGCInMark},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			service := createTestService(t)

			streams, stopStreams := context.WithCancel(t.Context())
			defer stopStreams()

			service.Streams = streams

			orphan := "nar/" + strings.Repeat("a", 52) + ".nar.zst"

			createOrphanedObjects(t, service, []struct {
				key  string
				refs []string
			}{
				{key: orphan, refs: []string{}},
			})

			var blocked <-chan struct{}

			release := func() {}

			if tc.block != nil {
				blocked, release = tc.block(t, service, orphan)
			} else {
				stopStreams()
			}

			gcDone := make(chan api.GCTaskStatus, 1)

			go func() { gcDone <- service.RunGCForTest(24*time.Hour, 24*time.Hour, true) }()

			if blocked != nil {
				select {
				case <-blocked:
				case st := <-gcDone:
					t.Fatalf("GC finished before reaching the blocking point: state=%s", st.State)
				case <-time.After(10 * time.Second):
					t.Fatal("GC never reached the blocking point")
				}

				stopStreams()
			}

			select {
			case st := <-gcDone:
				if st.State != api.GCTaskStateFailed {
					t.Fatalf("GC ran to completion after shutdown: state=%s", st.State)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("GC still running 5s after shutdown")
			}

			release()

			done := make(chan struct{})

			go func() {
				service.Close()
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Pool.Close() hangs after GC was cancelled by shutdown")
			}
		})
	}
}

// blockGCInSweepDelete holds the sweep's multi-object delete in S3 until the
// request's context ends or the block is lifted.
func blockGCInSweepDelete(t *testing.T, service *server.Service, _ string) (<-chan struct{}, func()) {
	t.Helper()

	reached := make(chan struct{})
	release := make(chan struct{})

	var once, released sync.Once

	lift := func() { released.Do(func() { close(release) }) }
	t.Cleanup(lift)

	service.MinioClient = interceptedS3Client(t, func(r *http.Request, next http.RoundTripper) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Query().Has("delete") {
			once.Do(func() { close(reached) })

			select {
			case <-r.Context().Done():
				return nil, r.Context().Err()
			case <-release:
			}
		}

		return next.RoundTrip(r)
	})

	return reached, lift
}

// blockGCInMark holds a row lock on the orphan, which the mark's FOR UPDATE
// scan waits for, until the block is lifted.
func blockGCInMark(t *testing.T, service *server.Service, orphan string) (<-chan struct{}, func()) {
	t.Helper()

	ctx := context.WithoutCancel(t.Context())

	tx, err := service.Pool.Begin(ctx)
	ok(t, err)

	lift := func() { _ = tx.Rollback(ctx) }
	t.Cleanup(lift)

	_, err = tx.Exec(ctx, "SELECT 1 FROM objects WHERE key = $1 FOR UPDATE", orphan)
	ok(t, err)

	reached := make(chan struct{})

	go func() {
		defer close(reached)

		for ctx.Err() == nil {
			var waiting int
			if err := service.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
				WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting); err != nil || waiting > 0 {
				return
			}

			time.Sleep(10 * time.Millisecond)
		}
	}()

	return reached, lift
}
