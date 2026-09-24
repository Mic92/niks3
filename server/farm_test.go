package server_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pipeWriter struct {
	*io.PipeWriter

	header http.Header
}

func (p *pipeWriter) Header() http.Header { return p.header }
func (p *pipeWriter) WriteHeader(int)     {}
func (p *pipeWriter) Flush()              {}

type leadStream struct {
	t      *testing.T
	cancel context.CancelFunc
	lines  chan api.LeadStatus
	done   chan struct{}
}

func scanLead(r io.Reader, lines chan<- api.LeadStatus) {
	defer close(lines)

	sc := bufio.NewScanner(r)
	for sc.Scan() {
		var st api.LeadStatus
		if json.Unmarshal(sc.Bytes(), &st) == nil {
			lines <- st
		}
	}
}

func openLead(t *testing.T, s *server.Service) *leadStream {
	t.Helper()

	return openLeadAs(t, s, false)
}

func (l *leadStream) next() api.LeadStatus {
	l.t.Helper()

	select {
	case st, open := <-l.lines:
		if !open {
			l.t.Fatal("lead stream closed")
		}

		return st
	case <-time.After(5 * time.Second):
		l.t.Fatal("timeout waiting for lead status")
	}

	return api.LeadStatus{}
}

// until skips heartbeats until want matches, for at most ten seconds: a new
// holder of the lock stays quiet for as many heartbeats as the ping timeout
// needs, which depends on the heartbeat.
func (l *leadStream) until(want api.LeadStatus) {
	l.t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		if l.next() == want {
			return
		}
	}

	l.t.Fatalf("never saw %+v", want)
}

func (l *leadStream) close() {
	l.cancel()
	<-l.done
}

func TestLeadElectsOneAndHandsOver(t *testing.T) {
	t.Parallel()

	s := createTestService(t)
	defer s.Close()

	a := openLead(t, s)
	a.until(api.LeadStatus{Lead: true})

	b := openLead(t, s)
	b.until(api.LeadStatus{Lead: false})

	// Still exactly one leader a few beats later.
	for range 3 {
		if st := a.next(); !st.Lead {
			t.Fatalf("a lost lead: %+v", st)
		}

		if st := b.next(); st.Lead {
			t.Fatalf("two leaders: %+v", st)
		}
	}

	// The old leader notices a lost connection only on its next heartbeat,
	// and on a dead connection a ping timeout after that, so the new one
	// must not announce before both have passed since the lock became free,
	// or the two overlap.
	released := time.Now()

	a.close()
	b.until(api.LeadStatus{Lead: true})

	if since, bound := time.Since(released), server.LeadHeartbeat()+server.LeadPingTimeout(); since < bound {
		t.Fatalf("b announced leadership %s after a released it, within a heartbeat and a ping timeout (%s)", since, bound)
	}

	c := openLead(t, s)
	c.until(api.LeadStatus{Lead: false})
	c.close()
	b.close()
}

func openLeadAs(t *testing.T, s *server.Service, incumbent bool) *leadStream {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	pr, pw := io.Pipe()
	ls := &leadStream{t: t, cancel: cancel, lines: make(chan api.LeadStatus, 64), done: make(chan struct{})}

	go func() {
		defer close(ls.done)
		defer func() { _ = pw.Close() }()

		body, err := json.Marshal(api.LeadRequest{Incumbent: incumbent})
		if err != nil {
			panic(err)
		}

		r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/farm/lead", bytes.NewReader(body))
		s.LeadHandler(&pipeWriter{PipeWriter: pw, header: http.Header{}}, r)
	}()

	go scanLead(pr, ls.lines)

	return ls
}

// After a restart the previous leader gets the lock back even if a standby
// reconnects first.
func TestLeadIncumbentWinsAfterRestart(t *testing.T) { //nolint:paralleltest // mutates startedAt
	s := createTestService(t)
	defer s.Close()

	server.RestartedNow(10 * server.LeadHeartbeat())

	standby := openLeadAs(t, s, false)
	standby.until(api.LeadStatus{Lead: false})

	incumbent := openLeadAs(t, s, true)
	incumbent.until(api.LeadStatus{Lead: true})

	for range 12 {
		if st := standby.next(); st.Lead {
			t.Fatalf("standby took the lock from the incumbent")
		}
	}

	incumbent.close()
	standby.until(api.LeadStatus{Lead: true})
	standby.close()
}

func TestLeadEndsOnShutdown(t *testing.T) {
	t.Parallel()

	s := createTestService(t)
	defer s.Close()

	streams, stop := context.WithCancel(t.Context())
	s.Streams = streams

	a := openLead(t, s)
	a.until(api.LeadStatus{Lead: true})
	stop()

	select {
	case <-a.done:
	case <-time.After(5 * time.Second):
		t.Fatal("stream survived shutdown")
	}
}

// cutProxy forwards TCP connections to the test Postgres until cut, then
// swallows traffic both ways without closing anything: what a client sees
// when the database host crashes or fails over, or the network partitions.
type cutProxy struct {
	ln  net.Listener
	cut atomic.Bool

	mu    sync.Mutex
	conns []net.Conn
}

func startCutProxy(t *testing.T) *cutProxy {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	ok(t, err)

	p := &cutProxy{ln: ln}
	socket := filepath.Join(testPostgresServer.tempDir, ".s.PGSQL.5432")

	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}

			var d net.Dialer

			upstream, err := d.DialContext(t.Context(), "unix", socket)
			if err != nil {
				_ = client.Close()

				continue
			}

			p.mu.Lock()
			p.conns = append(p.conns, client, upstream)
			p.mu.Unlock()

			go p.pipe(upstream, client)
			go p.pipe(client, upstream)
		}
	}()

	return p
}

func (p *cutProxy) pipe(dst io.Writer, src io.Reader) {
	buf := make([]byte, 32<<10)

	for {
		n, err := src.Read(buf)
		if n > 0 && !p.cut.Load() {
			if _, err := dst.Write(buf[:n]); err != nil {
				return
			}
		}

		if err != nil {
			return
		}
	}
}

func (p *cutProxy) close() {
	_ = p.ln.Close()

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, c := range p.conns {
		_ = c.Close()
	}
}

// A leader's lock connection can die without a reset, and the lock with it:
// the database host crashed or failed over, or Postgres dropped a session it
// could no longer reach. The leader's ping on that connection then hangs for
// the kernel's retransmission timeout while its stream stays open, and the
// successor announces. The old stream must end before the new one leads.
func TestLeadEndsWhenItsConnectionHangs(t *testing.T) { //nolint:paralleltest // lengthens the heartbeat
	defer server.SetLeadHeartbeat(200 * time.Millisecond)()

	s := createTestService(t)
	defer s.Close()

	proxy := startCutProxy(t)

	cfg := s.Pool.Config().ConnConfig
	viaProxy, err := pgxpool.New(t.Context(),
		fmt.Sprintf("postgres://%s@%s/%s?sslmode=disable", cfg.User, proxy.ln.Addr(), cfg.Database))
	ok(t, err)

	defer viaProxy.Close()

	a := openLead(t, &server.Service{Pool: viaProxy})
	defer a.close()
	// Unblock a leader stuck on the dead connection before waiting for it.
	defer proxy.close()

	a.until(api.LeadStatus{Lead: true})

	b := openLead(t, s)
	defer b.close()

	b.until(api.LeadStatus{Lead: false})

	proxy.cut.Store(true)

	// Postgres drops the leader's session and its lock; the leader hears
	// nothing of it.
	var terminated bool
	ok(t, s.Pool.QueryRow(t.Context(), `
		SELECT coalesce(bool_and(pg_terminate_backend(pid, 5000)), false) FROM pg_locks
		WHERE locktype = 'advisory'
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&terminated))

	if !terminated {
		t.Fatal("leader's session was not terminated")
	}

	b.until(api.LeadStatus{Lead: true})

	select {
	case <-a.done:
	default:
		t.Fatal("two leaders: the old leader's stream is still open after its successor announced")
	}
}
