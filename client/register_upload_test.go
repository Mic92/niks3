package client_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/client"
)

// A registration outlives the push that started it, but not indefinitely:
// against a server that accepts the request and never answers, it must give
// up on its own, or WaitRegistrations (deferred by every push command) keeps
// the process alive after Ctrl-C and a full registrations group blocks the
// upload workers.
func TestRegisterUploadedObject_BoundedAgainstSilentServer(t *testing.T) {
	t.Parallel()

	var (
		received atomic.Int32
		stop     = make(chan struct{})
	)

	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		received.Add(1)

		select {
		case <-stop:
		case <-r.Context().Done():
		}
	}))
	// Unblock the handlers before Close waits for them.
	defer srv.Close()
	defer close(stop)

	c, err := client.NewTestClientForServer(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	// Long enough for the requests to reach the server on a loaded builder
	// under the race detector: at 200ms, some runs saw none of them.
	const timeout = 2 * time.Second

	c.SetRegistrationTimeout(timeout)

	// The push that registers is cancelled (Ctrl-C) while the requests hang.
	pushCtx, cancelPush := context.WithCancel(t.Context())

	start := time.Now()

	const registrations = 3
	for range registrations {
		c.RegisterUploadedObject(pushCtx, "abc.narinfo")
	}

	cancelPush()

	done := make(chan struct{})

	go func() {
		defer close(done)

		c.WaitRegistrations()
	}()

	select {
	case <-done:
	case <-time.After(5 * timeout):
		t.Fatal("WaitRegistrations did not return: registrations against a silent server are unbounded")
	}

	// The requests were sent and survived the push's cancellation; it was
	// the bound, not the cancellation, that ended them.
	if n := received.Load(); n != registrations {
		t.Errorf("server saw %d registrations, want %d", n, registrations)
	}

	if elapsed := time.Since(start); elapsed < timeout {
		t.Errorf("registrations ended after %v, before their bound: cancelled with the push", elapsed)
	}
}
