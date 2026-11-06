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

// until skips heartbeats until want matches, for at most ten seconds.
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
	defer a.close()

	a.until(api.LeadStatus{Lead: true})

	b := openLead(t, s)
	defer b.close()

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

	// The old leader needs a heartbeat, or a ping timeout, to notice the loss.
	// The new one must not announce before then.
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

	return openLeadInFarm(t, s, incumbent, "")
}

func openLeadInFarm(t *testing.T, s *server.Service, incumbent bool, farmID string) *leadStream {
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

		path := "/api/farm/lead"
		if farmID != "" {
			path += "/" + farmID
		}
		r := httptest.NewRequestWithContext(ctx, http.MethodPost, path, bytes.NewReader(body))
		r.SetPathValue("farmID", farmID)
		s.LeadHandler(&pipeWriter{PipeWriter: pw, header: http.Header{}}, r)
	}()

	go scanLead(pr, ls.lines)

	return ls
}

func TestLeadScopedToFarm(t *testing.T) {
	t.Parallel()

	s := createTestService(t)
	defer s.Close()

	legacy := openLeadInFarm(t, s, true, "")
	defer legacy.close()
	legacy.until(api.LeadStatus{Lead: true})

	arm := openLeadInFarm(t, s, true, "arm")
	defer arm.close()
	arm.until(api.LeadStatus{Lead: true})

	x86 := openLeadInFarm(t, s, true, "x86")
	defer x86.close()
	x86.until(api.LeadStatus{Lead: true})

	otherX86 := openLeadInFarm(t, s, true, "x86")
	defer otherX86.close()
	otherX86.until(api.LeadStatus{Lead: false})

	otherLegacy := openLeadInFarm(t, s, true, "")
	defer otherLegacy.close()
	otherLegacy.until(api.LeadStatus{Lead: false})

	x86.close()
	otherX86.until(api.LeadStatus{Lead: true})
	// Taking over x86 must not disturb either independent leader.
	if st := arm.next(); !st.Lead {
		t.Fatalf("arm lost leadership: %+v", st)
	}
	if st := legacy.next(); !st.Lead {
		t.Fatalf("legacy farm lost leadership: %+v", st)
	}
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

	a.until(api.LeadStatus{Lead: true})

	b := openLead(t, s)
	defer b.close()

	b.until(api.LeadStatus{Lead: false})

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

	b.until(api.LeadStatus{Lead: true})

	select {
	case <-a.done:
	default:
		t.Fatal("two leaders: the old leader's stream is still open after its successor announced")
	}
}
