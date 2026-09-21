package server_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server"
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

// until skips heartbeats until want matches.
func (l *leadStream) until(want api.LeadStatus) {
	l.t.Helper()

	for range 20 {
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

	a.close()
	b.until(api.LeadStatus{Lead: true})

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
