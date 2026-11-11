package server_test

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
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

// until skips heartbeats until the stream says lead, for at most ten seconds.
func (l *leadStream) until(lead bool) api.LeadStatus {
	l.t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		if st := l.next(); st.Lead == lead {
			return st
		}
	}

	l.t.Fatalf("never saw lead: %v", lead)

	return api.LeadStatus{}
}

func (l *leadStream) close() {
	l.cancel()

	go func() {
		for range l.lines {
		}
	}()

	<-l.done
}

func TestLeadElectsOneAndHandsOver(t *testing.T) {
	t.Parallel()

	s := createTestService(t)
	defer s.Close()

	a := openLead(t, s)
	defer a.close()

	a.until(true)

	b := openLead(t, s)
	defer b.close()

	b.until(false)

	// Still exactly one leader a few beats later.
	for range 3 {
		if st := a.next(); !st.Lead {
			t.Fatalf("a lost lead: %+v", st)
		}

		if st := b.next(); st.Lead {
			t.Fatalf("two leaders: %+v", st)
		}
	}

	// The old leader needs a heartbeat, or a ping timeout, to notice the loss.
	// The new one must not announce before then.
	released := time.Now()

	a.close()
	b.until(true)

	if since, bound := time.Since(released), server.LeadHeartbeat()+server.LeadPingTimeout(); since < bound {
		t.Fatalf("b announced leadership %s after a released it, within a heartbeat and a ping timeout (%s)", since, bound)
	}

	c := openLead(t, s)
	c.until(false)
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
	standby.until(false)

	incumbent := openLeadAs(t, s, true)
	incumbent.until(true)

	for range 12 {
		if st := standby.next(); st.Lead {
			t.Fatalf("standby took the lock from the incumbent")
		}
	}

	incumbent.close()
	standby.until(true)
	standby.close()
}

// The Postgres backend that holds the lead lock, or 0.
func leadHolder(t *testing.T, s *server.Service) int {
	t.Helper()

	var pid int
	ok(t, s.Pool.QueryRow(t.Context(), `
		SELECT coalesce(max(pid), 0) FROM pg_locks
		WHERE locktype = 'advisory' AND granted AND objid = $1
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`,
		server.FarmLeadLockKey&0xffffffff).Scan(&pid))

	return pid
}

// The standby holds the lock but nobody has heard of it yet.
func TestLeadUnannouncedStandbyYieldsToIncumbent(t *testing.T) {
	t.Parallel()

	s := createTestService(t)
	defer s.Close()

	leader := openLead(t, s)
	leader.until(true)

	standby := openLead(t, s)
	defer standby.close()

	standby.until(false)

	was := leadHolder(t, s)
	leader.close()

	for now := was; now == was || now == 0; now = leadHolder(t, s) {
		time.Sleep(time.Millisecond)
	}

	back := openLeadAs(t, s, true)
	defer back.close()

	back.until(true)

	for range 2 * int(server.LeadPingTimeout()/server.LeadHeartbeat()) {
		if standby.next().Lead {
			t.Fatal("the standby leads although the incumbent came back before it said so")
		}
	}
}

// A restart closes every stream of the old process, so the incumbent that
// takes the lock back has no predecessor to wait out. Waiting anyway made the
// farm scheduler yield its role on every niks3 restart.
func TestLeadIncumbentAnnouncesAtOnce(t *testing.T) {
	t.Parallel()

	s := createTestService(t)
	defer s.Close()

	incumbent := openLeadAs(t, s, true)
	defer incumbent.close()

	if st := incumbent.next(); !st.Lead {
		t.Fatalf("incumbent was told %+v on its first heartbeat, want Lead: true", st)
	}
}

func TestLeadGenerationGrowsWithEveryGrant(t *testing.T) {
	t.Parallel()

	s := createTestService(t)
	defer s.Close()

	a := openLead(t, s)
	first := a.until(true).Generation

	b := openLead(t, s)
	defer b.close()

	if st := b.until(false); st.Generation != 0 {
		t.Fatalf("a standby was told generation %d", st.Generation)
	}

	if again := a.next().Generation; again != first {
		t.Fatalf("generation changed from %d to %d while a led", first, again)
	}

	a.close()

	second := b.until(true).Generation
	b.close()

	// The same scheduler coming back is a new grant too.
	c := openLeadAs(t, s, true)
	defer c.close()

	third := c.until(true).Generation

	if first == 0 || second <= first || third <= second {
		t.Fatalf("generations %d, %d, %d do not grow", first, second, third)
	}
}

func TestLeadEndsOnShutdown(t *testing.T) {
	t.Parallel()

	s := createTestService(t)
	defer s.Close()

	streams, stop := context.WithCancel(t.Context())
	s.Streams = streams

	a := openLead(t, s)
	a.until(true)
	stop()

	select {
	case <-a.done:
	case <-time.After(5 * time.Second):
		t.Fatal("stream survived shutdown")
	}
}

// cutProxy forwards TCP to the test Postgres until cut, then swallows traffic
// without closing anything, as a crashed host or a partition does.
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
	// Postgres names its socket after PGPORT.
	port := cmp.Or(os.Getenv("PGPORT"), "5432")
	socket := filepath.Join(testPostgresServer.tempDir, ".s.PGSQL."+port)

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

// A leader's lock connection can die silently, and the lock with it. The old
// stream must end before the successor announces.
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

	a.until(true)

	b := openLead(t, s)
	defer b.close()

	b.until(false)

	proxy.cut.Store(true)

	// Postgres drops the leader's session and lock without telling the leader.
	var terminated bool
	ok(t, s.Pool.QueryRow(t.Context(), `
		SELECT coalesce(bool_and(pg_terminate_backend(pid, 5000)), false) FROM pg_locks
		WHERE locktype = 'advisory'
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&terminated))

	if !terminated {
		t.Fatal("leader's session was not terminated")
	}

	b.until(true)

	select {
	case <-a.done:
	default:
		t.Fatal("two leaders: the old leader's stream is still open after its successor announced")
	}
}
