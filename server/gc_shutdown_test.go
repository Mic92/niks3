package server_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/api"
)

// A GC run must end when the server shuts down. It holds a pool connection
// for its advisory lock, and Pool.Close blocks until that connection is
// released, so an uncancellable run would hang the process until SIGKILL.
func TestGCEndsOnShutdown(t *testing.T) {
	t.Parallel()

	service := createTestService(t)

	streams, stopStreams := context.WithCancel(t.Context())
	service.Streams = streams

	createOrphanedObjects(t, service, []struct {
		key  string
		refs []string
	}{
		{key: "nar/" + strings.Repeat("a", 52) + ".nar.zst", refs: []string{}},
	})

	// Shutdown already happened when the run starts.
	stopStreams()

	st := service.RunGCForTest(24*time.Hour, 24*time.Hour, true)
	if st.State != api.GCTaskStateFailed {
		t.Fatalf("GC ran to completion after shutdown: state=%s", st.State)
	}

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
}
