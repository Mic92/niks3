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
	defer srv.Close()
	defer close(stop)

	c, err := client.NewTestClientForServer(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	c.SetRegistrationTimeout(200 * time.Millisecond)

	pushCtx, cancelPush := context.WithCancel(t.Context())

	const registrations = 3
	for range registrations {
		c.RegisterUploadedObject(pushCtx, "abc.narinfo")
	}

	cancelPush()

	start := time.Now()
	done := make(chan struct{})

	go func() {
		defer close(done)

		c.WaitRegistrations()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitRegistrations did not return: registrations against a silent server are unbounded")
	}

	if n := received.Load(); n != registrations {
		t.Errorf("server saw %d registrations, want %d", n, registrations)
	}

	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Errorf("registrations ended after %v, before their bound: cancelled with the push", elapsed)
	}
}
