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

// A new leader must not announce on the heartbeat that took the lock, nor
// until the old leader's ping has had time to fail: the old leader notices a
// lost connection only on its next heartbeat, and on a dead connection a ping
// timeout after that, so the two would overlap. The standby's stream is
// gated, so the test knows which heartbeat ran while the old leader still
// held the lock and can let the next one run only once the lock is free: that
// next heartbeat is the one that takes the lock, and it and the quiet
// heartbeats after it must still say false.
func TestLeadElectsOneAndHandsOver(t *testing.T) {
	t.Parallel()

	s := createTestService(t)
	// Registered before the streams, so on a failure they are closed first
	// and Close does not wait forever for their lock connections.
	t.Cleanup(s.Close)

	a := openLead(t, s)
	a.until(api.LeadStatus{Lead: true})

	b := openLeadGated(t, s)

	// Still exactly one leader a few beats later.
	for range 3 {
		if st := a.next(); !st.Lead {
			t.Fatalf("a lost lead: %+v", st)
		}

		if st := b.held(); st.Lead {
			t.Fatalf("two leaders: %+v", st)
		}

		b.release()
	}

	// b is held inside a heartbeat whose lock attempt ran while a led.
	if st := b.held(); st.Lead {
		t.Fatalf("two leaders: %+v", st)
	}

	a.close()
	waitLeadLockFree(t, s)
	b.release()

	// The old leader runs its next heartbeat within one heartbeat of losing
	// the lock and its ping may take a ping timeout to fail, so b must say
	// false on the heartbeat that takes the lock and on every one until both
	// have passed.
	quiet := 1 + int((server.LeadPingTimeout()+server.LeadHeartbeat()-1)/server.LeadHeartbeat())

	for beat := range quiet {
		if st := b.held(); st.Lead {
			t.Fatalf("b announced leadership on heartbeat %d after taking the lock, before the old leader's ping could fail", beat)
		}

		b.release()
	}

	if st := b.held(); !st.Lead {
		t.Fatalf("b did not announce leadership %d heartbeats after taking the lock: %+v", quiet, st)
	}

	b.release()

	c := openLead(t, s)
	c.until(api.LeadStatus{Lead: false})
	c.close()
	b.close()
}

// gatedLead is a lead stream whose handler blocks in each status write until
// the test releases it, so the test decides when the next heartbeat runs.
type gatedLead struct {
	t       *testing.T
	cancel  context.CancelFunc
	lines   chan api.LeadStatus
	resume  chan struct{}
	done    chan struct{}
	pending bool
}

type gatedWriter struct {
	ctx    context.Context //nolint:containedctx // the stream's lifetime
	header http.Header
	g      *gatedLead
}

func (w *gatedWriter) Header() http.Header { return w.header }
func (w *gatedWriter) WriteHeader(int)     {}
func (w *gatedWriter) Flush()              {}

func (w *gatedWriter) Write(p []byte) (int, error) {
	var st api.LeadStatus
	if err := json.Unmarshal(p, &st); err != nil {
		return 0, err //nolint:wrapcheck // test writer
	}

	select {
	case w.g.lines <- st:
	case <-w.ctx.Done():
		return 0, w.ctx.Err() //nolint:wrapcheck // test writer
	}

	select {
	case <-w.g.resume:
		return len(p), nil
	case <-w.ctx.Done():
		return 0, w.ctx.Err() //nolint:wrapcheck // test writer
	}
}

func openLeadGated(t *testing.T, s *server.Service) *gatedLead {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	g := &gatedLead{t: t, cancel: cancel, lines: make(chan api.LeadStatus), resume: make(chan struct{}), done: make(chan struct{})}

	go func() {
		defer close(g.done)

		r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/farm/lead", nil)
		s.LeadHandler(&gatedWriter{ctx: ctx, header: http.Header{}, g: g}, r)
	}()

	t.Cleanup(g.close)

	return g
}

// held returns the status the handler is writing; the handler stays blocked
// in that write, its lock attempt for the heartbeat already made, until
// release.
func (g *gatedLead) held() api.LeadStatus {
	g.t.Helper()

	if g.pending {
		g.t.Fatal("held called twice without release")
	}

	select {
	case st := <-g.lines:
		g.pending = true

		return st
	case <-g.done:
		g.t.Fatal("lead stream closed")
	case <-time.After(5 * time.Second):
		g.t.Fatal("timeout waiting for lead status")
	}

	return api.LeadStatus{}
}

// release lets the handler finish the held write and run its next heartbeat.
func (g *gatedLead) release() {
	g.t.Helper()

	g.pending = false

	select {
	case g.resume <- struct{}{}:
	case <-g.done:
		g.t.Fatal("lead stream closed")
	}
}

func (g *gatedLead) close() {
	g.cancel()
	<-g.done
}

// waitLeadLockFree waits until no session holds the farm lead lock on the
// service's database. A closed connection's session, and its lock, end only
// once its backend has noticed.
func waitLeadLockFree(t *testing.T, s *server.Service) {
	t.Helper()

	key := uint64(server.FarmLeadLockKey)
	deadline := time.Now().Add(5 * time.Second)

	for {
		var held bool
		ok(t, s.Pool.QueryRow(t.Context(), `SELECT EXISTS (
			SELECT 1 FROM pg_locks
			WHERE locktype = 'advisory' AND granted
			  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
			  AND objsubid = 1 AND classid = $1 AND objid = $2)`,
			int64(key>>32), int64(key&0xffffffff)).Scan(&held))

		if !held {
			return
		}

		if time.Now().After(deadline) {
			t.Fatal("the old leader's lock was never released")
		}

		time.Sleep(5 * time.Millisecond)
	}
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

	t.Cleanup(ls.close)

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
