package client_test

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Mic92/niks3/client"
)

// Regression: one TCP connection per request exhausted ephemeral ports in pods.
func TestRegisterUploadedObjectReusesConnections(t *testing.T) {
	t.Parallel()

	var conns atomic.Int64

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}` + "\n"))
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)

	c, err := client.NewTestClientForServer(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	const requests = 2000
	for range requests {
		c.RegisterUploadedObject(t.Context(), "x.narinfo")
	}

	c.WaitRegistrations()

	if got := conns.Load(); got > 100 {
		t.Fatalf("%d requests opened %d connections", requests, got)
	}
}
