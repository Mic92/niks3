package client_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/client"
)

// Behind a load balancer the start request and the polls can reach
// different replicas. A replica without the task reports the run while the
// shared lock is held and 404 afterwards; the client must take that as the
// run having ended rather than fail.
func TestRunGarbageCollection_FinishedOnAnotherReplica(t *testing.T) {
	t.Parallel()

	var polls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/api/closures":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(api.GCTaskStatus{State: api.GCTaskStateRunning, Phase: api.GCTaskPhaseCleanupOldClosures})
		case r.Method == http.MethodGet && r.URL.Path == "/api/gc/status":
			if polls.Add(1) == 1 {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(api.GCTaskStatus{State: api.GCTaskStateRunning, Phase: api.GCTaskPhaseOtherReplica})

				return
			}

			http.Error(w, "no garbage collection has run yet", http.StatusNotFound)
		default:
			http.Error(w, "unexpected request "+r.URL.Path, http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	c, err := client.NewTestClientForServer(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	c.GCPollInterval = time.Millisecond

	stats, err := c.RunGarbageCollection(t.Context(), "0s", "", false)
	if err != nil {
		t.Fatalf("RunGarbageCollection: %v", err)
	}

	if stats == nil {
		t.Fatal("no stats returned")
	}

	if n := polls.Load(); n != 2 {
		t.Errorf("polled %d times, want 2", n)
	}
}

// A 404 while the run was last seen on this replica is still an error.
func TestRunGarbageCollection_NotFoundAfterLocalRun(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(api.GCTaskStatus{State: api.GCTaskStateRunning, Phase: api.GCTaskPhaseCleanupOldClosures})

			return
		}

		http.Error(w, "no garbage collection has run yet", http.StatusNotFound)
	}))
	defer srv.Close()

	c, err := client.NewTestClientForServer(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	c.GCPollInterval = time.Millisecond

	if _, err := c.RunGarbageCollection(t.Context(), "0s", "", false); err == nil {
		t.Fatal("a 404 after a run seen on this replica must be an error")
	}
}
