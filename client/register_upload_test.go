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

	stop := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
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

	c.SetRegistrationTimeout(100 * time.Millisecond)

	for range 3 {
		c.RegisterUploadedObject(t.Context(), "abc.narinfo")
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		c.WaitRegistrations()
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("WaitRegistrations did not return: registrations against a silent server are unbounded")
	}
}

func TestRegisterUploadedObject_OutlivesPush(t *testing.T) {
	t.Parallel()

	var received atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c, err := client.NewTestClientForServer(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	pushCtx, cancelPush := context.WithCancel(t.Context())
	cancelPush()

	c.RegisterUploadedObject(pushCtx, "abc.narinfo")
	c.WaitRegistrations()

	if n := received.Load(); n != 1 {
		t.Fatalf("server saw %d registrations after the push was cancelled, want 1", n)
	}
}
