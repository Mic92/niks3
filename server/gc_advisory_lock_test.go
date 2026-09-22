package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server"
)

// TestGCAdvisoryLockBlocksConcurrentRun checks GC fails fast when another
// instance already holds the advisory lock.
func TestGCAdvisoryLockBlocksConcurrentRun(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	// Hold the lock on a dedicated connection to simulate another instance.
	conn, err := service.Pool.Acquire(ctx)
	ok(t, err)

	defer conn.Release()

	var acquired bool

	err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", server.GCAdvisoryLockKey).Scan(&acquired)
	ok(t, err)

	if !acquired {
		t.Fatal("expected to acquire GC advisory lock for the simulated peer")
	}

	defer func() {
		_, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", server.GCAdvisoryLockKey)
		ok(t, err)
	}()

	// A replica with no task of its own reports the peer's run while the
	// lock is held, so a client polling through a load balancer is not told
	// that no collection has ever run.
	statusReq := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		service.GCStatusHandler(w, httptest.NewRequestWithContext(ctx, http.MethodGet, "/api/gc/status", nil))

		return w
	}

	w := statusReq()
	if w.Code != http.StatusOK {
		t.Fatalf("status while a peer holds the lock: %d %s", w.Code, w.Body.String())
	}

	var remote api.GCTaskStatus
	ok(t, json.Unmarshal(w.Body.Bytes(), &remote))

	if remote.State != api.GCTaskStateRunning || remote.Phase != api.GCTaskPhaseOtherReplica {
		t.Fatalf("status while a peer holds the lock = %+v, want running on another replica", remote)
	}

	// GC must not proceed while the lock is held elsewhere.
	status := service.RunGCForTest(720*time.Hour, 6*time.Hour, false)

	if status.State != api.GCTaskStateFailed {
		t.Fatalf("expected GC to fail while advisory lock is held, got state %q", status.State)
	}

	if !strings.Contains(status.Error, "already running") {
		t.Fatalf("expected lock-held error, got %q", status.Error)
	}

	// With no task and no lock there is nothing to report.
	service.GCTasks = server.NewGCTaskStore()

	_, err = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", server.GCAdvisoryLockKey)
	ok(t, err)

	if w := statusReq(); w.Code != http.StatusNotFound {
		t.Fatalf("status with no task and no lock: %d %s", w.Code, w.Body.String())
	}

	// Re-take the lock so the deferred unlock has something to release.
	err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", server.GCAdvisoryLockKey).Scan(&acquired)
	ok(t, err)
}
