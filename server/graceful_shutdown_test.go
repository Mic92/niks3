package server_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
)

// TestGracefulShutdownDrainsInflight verifies that an in-flight request is
// allowed to finish after the shutdown signal, and that serve returns once it
// completes.
func TestGracefulShutdownDrainsInflight(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	ok(t, err)

	addr := ln.Addr().String()

	shutdownCtx, triggerShutdown := context.WithCancel(t.Context())

	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-releaseRequest
		_, _ = w.Write([]byte("done"))
	})

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}

	var (
		serveErr error
		wg       sync.WaitGroup
	)

	// serve takes ownership of the already-bound listener, so there is no
	// bind/close/rebind race.
	wg.Go(func() {
		serveErr = server.ServeForTest(shutdownCtx, srv, ln)
	})

	respCh := make(chan string, 1)

	go func() {
		resp, err := http.Get("http://" + addr + "/slow") //nolint:noctx
		if err != nil {
			respCh <- "error: " + err.Error()

			return
		}
		defer func() { _ = resp.Body.Close() }()

		body, _ := io.ReadAll(resp.Body)
		respCh <- string(body)
	}()

	// Wait for the request to reach the handler, but fail fast if the request
	// errored out instead of hanging here forever.
	select {
	case <-requestStarted:
	case body := <-respCh:
		t.Fatalf("request did not reach handler: %q", body)
	case <-time.After(5 * time.Second):
		t.Fatal("request never reached handler")
	}

	// Signal shutdown while the request is still in flight.
	triggerShutdown()

	// Give serve a moment to enter Shutdown; the in-flight request must still
	// be allowed to complete.
	time.Sleep(50 * time.Millisecond)
	close(releaseRequest)

	select {
	case body := <-respCh:
		if body != "done" {
			t.Fatalf("in-flight request was not drained cleanly, got %q", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request did not complete")
	}

	wg.Wait()

	if serveErr != nil {
		t.Fatalf("serve returned error: %v", serveErr)
	}
}
