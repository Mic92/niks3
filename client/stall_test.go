package client_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Mic92/niks3/client"
)

const testStall = time.Minute

// pipes is a listener whose connections live in memory, so the tests run on
// the fake clock.
type pipes struct {
	conns  chan net.Conn
	closed chan struct{}
}

func (p pipes) Accept() (net.Conn, error) {
	select {
	case conn := <-p.conns:
		return conn, nil
	case <-p.closed:
		return nil, net.ErrClosed
	}
}

func (p pipes) Close() error {
	close(p.closed)

	return nil
}

func (pipes) Addr() net.Addr { return &net.UnixAddr{} }

func (p pipes) dial(context.Context, string, string) (net.Conn, error) {
	local, remote := net.Pipe()

	select {
	case p.conns <- remote:
		return local, nil
	case <-p.closed:
		return nil, net.ErrClosed
	}
}

// serve returns a transport to a server that speaks HTTP/2 without TLS. stop
// is closed when the test ends.
func serve(t *testing.T, handler func(r *http.Request, stop <-chan struct{})) *http.Transport {
	t.Helper()

	listener := pipes{conns: make(chan net.Conn), closed: make(chan struct{})}
	stop := make(chan struct{})

	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)

	srv := &http.Server{
		Protocols:         protocols,
		ReadHeaderTimeout: testStall,
		Handler:           http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { handler(r, stop) }),
	}

	go func() { _ = srv.Serve(listener) }()

	transport := client.NewStallingTransport(testStall, listener.dial)
	transport.Protocols = protocols

	t.Cleanup(func() {
		close(stop)
		transport.CloseIdleConnections()

		_ = srv.Close()
	})

	return transport
}

func request(t *testing.T, transport *http.Transport, path string, body io.Reader) error {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, "http://niks3"+path, body)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		return err //nolint:wrapcheck
	}

	return resp.Body.Close() //nolint:wrapcheck
}

func TestRequestToASilentPeerFails(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		silent := func(context.Context, string, string) (net.Conn, error) {
			local, _ := net.Pipe()

			return local, nil
		}

		if err := request(t, client.NewStallingTransport(testStall, silent), "/", nil); err == nil {
			t.Fatal("request succeeded")
		}
	})
}

// slowBody takes half the stall timeout for every byte.
type slowBody struct{ left int }

func (b *slowBody) Read(p []byte) (int, error) {
	if b.left == 0 {
		return 0, io.EOF
	}

	time.Sleep(testStall / 2) //nolint:mnd

	b.left--
	p[0] = 0

	return 1, nil
}

// The upload takes longer than the stall timeout, and data moves all the time.
func TestSlowUploadSucceeds(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		transport := serve(t, func(r *http.Request, _ <-chan struct{}) {
			_, _ = io.Copy(io.Discard, r.Body)
		})

		if err := request(t, transport, "/", &slowBody{left: 6}); err != nil { //nolint:mnd
			t.Fatal(err)
		}
	})
}

// With HTTP/2 one connection carries all requests, so it keeps moving data
// while one of them gets no answer.
func TestRequestWithoutAnAnswerFailsOnABusyConnection(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		transport := serve(t, func(r *http.Request, stop <-chan struct{}) {
			if r.URL.Path == "/hang" {
				<-stop
			}
		})

		// Stops by itself so a request that hangs ends the test.
		busy := make(chan struct{})

		go func() {
			defer close(busy)

			for range 50 {
				time.Sleep(testStall / 10) //nolint:mnd

				_ = request(t, transport, "/busy", nil)
			}
		}()

		defer func() { <-busy }()

		start := time.Now()

		if err := request(t, transport, "/hang", nil); err == nil || time.Since(start) != testStall {
			t.Fatalf("request ended after %s: %v", time.Since(start), err)
		}
	})
}
