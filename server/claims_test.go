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

// Generous so a loaded CI box cannot starve holders into staleness (3 beats).
const testHeartbeat = 500 * time.Millisecond

func commitTestClosure(t *testing.T, service *server.Service, hash string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	narinfoKey := hash + ".narinfo"
	narKey := narKeyFor(hash)
	resp := createPendingClosure(t, service, map[string]any{
		"closure": narinfoKey,
		"objects": []map[string]any{
			{"key": narinfoKey, "type": "narinfo", "refs": []string{narKey}},
			{"key": narKey, "type": "nar", "refs": []string{}},
		},
	})
	meta := uploadPendingObjects(ctx, t, service, resp, hash, narKey)
	commitPendingClosure(t, service, resp.ID, meta)

	return narinfoKey
}

func postJSON(t *testing.T, handler http.HandlerFunc, path string, body any, want int, out any) {
	t.Helper()

	raw, err := json.Marshal(body)
	ok(t, err)

	check := checkStatusCode(want)
	rr := testRequest(t, &TestRequest{method: "POST", path: path, body: raw, handler: handler, checkResponse: &check})

	if out != nil && rr.Body.Len() > 0 {
		ok(t, json.Unmarshal(rr.Body.Bytes(), out))
	}
}

type pipeWriter struct {
	*io.PipeWriter

	header http.Header
}

func (p *pipeWriter) Header() http.Header { return p.header }
func (p *pipeWriter) WriteHeader(int)     {}
func (p *pipeWriter) Flush()              {}

func newPipe() (*io.PipeReader, *pipeWriter) {
	pr, pw := io.Pipe()

	return pr, &pipeWriter{PipeWriter: pw, header: http.Header{}}
}

type claimStream struct {
	t      *testing.T
	cancel context.CancelFunc
	lines  chan api.ClaimStatus
}

func openClaim(t *testing.T, s *server.Service, req api.ClaimRequest) *claimStream {
	t.Helper()

	body, err := json.Marshal(req)
	ok(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	pr, pw := newPipe()
	cs := &claimStream{t: t, cancel: cancel, lines: make(chan api.ClaimStatus, 64)}

	go func() {
		defer func() { _ = pw.Close() }()

		r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/builds/claim", bytes.NewReader(body))
		s.ClaimHandler(pw, r)
	}()

	go func() {
		defer close(cs.lines)

		sc := bufio.NewScanner(pr)
		for sc.Scan() {
			var st api.ClaimStatus
			if json.Unmarshal(sc.Bytes(), &st) == nil {
				cs.lines <- st
			}
		}
	}()

	return cs
}

// next returns the next non-heartbeat line.
func (c *claimStream) next() api.ClaimStatus {
	c.t.Helper()

	deadline := time.After(5 * time.Second)

	for {
		select {
		case st, open := <-c.lines:
			if !open {
				c.t.Fatal("claim stream closed")
			}

			if st.Status != api.ClaimHeartbeat {
				return st
			}
		case <-deadline:
			c.t.Fatal("timeout waiting for claim status")
		}
	}
}

func (c *claimStream) expect(status string) api.ClaimStatus {
	c.t.Helper()

	st := c.next()
	if st.Status != status {
		c.t.Fatalf("status = %q (%+v), want %q", st.Status, st, status)
	}

	return st
}

func (c *claimStream) close() { c.cancel() }

func claimReq(outputs ...string) api.ClaimRequest {
	return api.ClaimRequest{Outputs: outputs}
}

func newClaimService(t *testing.T) *server.Service {
	t.Helper()

	s := createTestService(t)
	s.ClaimHeartbeat = testHeartbeat
	s.StartClaims(t.Context())

	return s
}

func completeWithToken(t *testing.T, s *server.Service, hash string, token int64, want int) {
	t.Helper()

	narinfoKey := hash + ".narinfo"
	narKey := narKeyFor(hash)
	resp := createPendingClosure(t, s, map[string]any{
		"closure": narinfoKey,
		"objects": []map[string]any{
			{"key": narinfoKey, "type": "narinfo", "refs": []string{narKey}},
			{"key": narKey, "type": "nar", "refs": []string{}},
		},
	})

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	meta := uploadPendingObjects(ctx, t, s, resp, hash, narKey)
	signBody, err := json.Marshal(map[string]any{"narinfos": meta})
	ok(t, err)
	testRequest(t, &TestRequest{
		method: "POST", path: "/api/pending_closures/" + resp.ID + "/sign", body: signBody,
		handler: s.SignNarinfosHandler, pathValues: map[string]string{"id": resp.ID},
	})

	body, err := json.Marshal(map[string]any{"claim_token": token})
	ok(t, err)

	check := checkStatusCode(want)
	testRequest(t, &TestRequest{
		method: "POST", path: "/api/pending_closures/" + resp.ID + "/complete", body: body,
		handler: s.CommitPendingClosureHandler, pathValues: map[string]string{"id": resp.ID},
		checkResponse: &check,
	})
}

func TestClaim_BuildWaitComplete(t *testing.T) {
	t.Parallel()

	s := newClaimService(t)
	defer s.Close()

	hash := "00000000000000000000000000000010"
	out := hash + ".narinfo"

	a := openClaim(t, s, claimReq(out))
	defer a.close()

	token := a.expect(api.ClaimBuild).Token

	b := openClaim(t, s, claimReq(out))
	defer b.close()
	b.expect(api.ClaimWait)

	// re-claim by holder is idempotent
	a2 := openClaim(t, s, api.ClaimRequest{Outputs: []string{out}, Token: token})
	defer a2.close()

	if got := a2.expect(api.ClaimBuild).Token; got != token {
		t.Fatalf("re-claim token = %d, want %d", got, token)
	}

	completeWithToken(t, s, hash, token+1, http.StatusConflict)
	completeWithToken(t, s, hash, token, http.StatusNoContent)
	b.expect(api.ClaimBuilt)

	c := openClaim(t, s, claimReq(out))
	defer c.close()
	c.expect(api.ClaimBuilt)
}

func TestClaim_GCMarkedOutputCountsAsAbsent(t *testing.T) {
	t.Parallel()

	s := newClaimService(t)
	defer s.Close()

	out := commitTestClosure(t, s, "00000000000000000000000000000011")

	a := openClaim(t, s, claimReq(out))
	defer a.close()
	a.expect(api.ClaimBuilt)

	_, err := s.Pool.Exec(t.Context(), "UPDATE objects SET deleted_at = now(), first_deleted_at = now() WHERE key = $1", out)
	ok(t, err)

	b := openClaim(t, s, claimReq(out))
	defer b.close()
	b.expect(api.ClaimBuild)
}

func TestClaim_TooManyStreams(t *testing.T) {
	t.Parallel()

	s := newClaimService(t)
	defer s.Close()

	s.MaxClaimStreams = 1

	a := openClaim(t, s, claimReq("00000000000000000000000000000019.narinfo"))
	defer a.close()
	a.expect(api.ClaimBuild)

	body, err := json.Marshal(claimReq("00000000000000000000000000000020.narinfo"))
	ok(t, err)

	rr := httptest.NewRecorder()
	s.ClaimHandler(rr, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/builds/claim", bytes.NewReader(body)))

	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") == "" {
		t.Fatalf("got %d %q, want 503 with Retry-After", rr.Code, rr.Header().Get("Retry-After"))
	}
}

// A dropped holder stream must not release the claim: the worker is still
// building and will publish with its token. It re-enters with the token
// before the row goes stale, and the waiter is only promoted by staleness.
func TestClaim_HolderDisconnectKeepsClaim(t *testing.T) {
	t.Parallel()

	s := newClaimService(t)
	defer s.Close()

	out := "00000000000000000000000000000011.narinfo"

	a := openClaim(t, s, claimReq(out))
	token := a.expect(api.ClaimBuild).Token

	b := openClaim(t, s, claimReq(out))
	defer b.close()
	b.expect(api.ClaimWait)

	a.close()
	time.Sleep(testHeartbeat)

	a2 := openClaim(t, s, api.ClaimRequest{Outputs: []string{out}, Token: token})
	defer a2.close()

	if st := a2.expect(api.ClaimBuild); st.Token != token {
		t.Fatalf("reconnect got token %d, want %d", st.Token, token)
	}

	a2.close()

	start := time.Now()

	if st := b.expect(api.ClaimBuild); st.Token == token || time.Since(start) < 2*testHeartbeat {
		t.Fatalf("waiter promoted after %v with token %d, want steal after staleness", time.Since(start), st.Token)
	}
}

func TestClaim_FailWakesWaitersButIsNotRemembered(t *testing.T) {
	t.Parallel()

	s := newClaimService(t)
	defer s.Close()

	out := "00000000000000000000000000000013.narinfo"

	a := openClaim(t, s, claimReq(out))
	defer a.close()

	token := a.expect(api.ClaimBuild).Token

	b := openClaim(t, s, claimReq(out))
	defer b.close()
	b.expect(api.ClaimWait)

	postJSON(t, s.FailHandler, "/api/builds/fail",
		api.FailRequest{ClaimToken: token + 1, Kind: "PermanentFailure"}, http.StatusConflict, nil)
	postJSON(t, s.FailHandler, "/api/builds/fail",
		api.FailRequest{ClaimToken: token, Kind: "PermanentFailure"}, http.StatusNoContent, nil)

	if st := b.expect(api.ClaimFailed); st.Kind != "PermanentFailure" {
		t.Fatalf("waiter got %+v", st)
	}

	// a later request is a retry and builds again
	c := openClaim(t, s, claimReq(out))
	defer c.close()
	c.expect(api.ClaimBuild)
}

func TestClaim_FailWithoutKindReleases(t *testing.T) {
	t.Parallel()

	s := newClaimService(t)
	defer s.Close()

	out := "00000000000000000000000000000014.narinfo"

	a := openClaim(t, s, claimReq(out))
	defer a.close()

	token := a.expect(api.ClaimBuild).Token

	b := openClaim(t, s, claimReq(out))
	defer b.close()
	b.expect(api.ClaimWait)

	postJSON(t, s.FailHandler, "/api/builds/fail",
		api.FailRequest{ClaimToken: token}, http.StatusNoContent, nil)
	b.expect(api.ClaimBuild)
}

func TestClaim_StaleHeartbeatStolen(t *testing.T) {
	t.Parallel()

	s := newClaimService(t)
	defer s.Close()

	out := "00000000000000000000000000000015.narinfo"

	a := openClaim(t, s, claimReq(out))
	defer a.close()

	old := a.expect(api.ClaimBuild).Token

	// Simulate the holder's niks3 instance dying without cleanup.
	_, err := s.Pool.Exec(t.Context(), "UPDATE claims SET heartbeat_at = now() - interval '1 hour'")
	ok(t, err)

	b := openClaim(t, s, claimReq(out))
	defer b.close()

	if tok := b.expect(api.ClaimBuild).Token; tok == old {
		t.Fatal("stolen claim must get a fresh token")
	}
}

func TestClaim_TwoInstances(t *testing.T) {
	t.Parallel()

	s1 := newClaimService(t)
	defer s1.Close()

	s2 := server.CloneServiceForTest(t.Context(), s1)
	s2.ClaimHeartbeat = testHeartbeat
	s2.StartClaims(t.Context())

	defer s2.Close()

	hash := "00000000000000000000000000000016"
	out := hash + ".narinfo"

	a := openClaim(t, s1, claimReq(out))
	defer a.close()

	token := a.expect(api.ClaimBuild).Token

	b := openClaim(t, s2, claimReq(out))
	defer b.close()
	b.expect(api.ClaimWait)

	// holder re-claims via the other instance (e.g. after LB failover)
	a2 := openClaim(t, s2, api.ClaimRequest{Outputs: []string{out}, Token: token})
	defer a2.close()
	a2.expect(api.ClaimBuild)

	completeWithToken(t, s2, hash, token, http.StatusNoContent)
	b.expect(api.ClaimBuilt)
}

func TestClaim_InputsTouched(t *testing.T) {
	t.Parallel()

	s := newClaimService(t)
	defer s.Close()

	ctx := t.Context()
	input := commitTestClosure(t, s, "00000000000000000000000000000017")
	old := time.Now().UTC().Add(-48 * time.Hour)
	_, err := s.Pool.Exec(ctx, "UPDATE closures SET updated_at = $1 WHERE key = $2", old, input)
	ok(t, err)

	out := "00000000000000000000000000000018.narinfo"
	a := openClaim(t, s, api.ClaimRequest{Outputs: []string{out}, Inputs: []string{input}})

	defer a.close()

	a.expect(api.ClaimBuild)

	s.RunGCForTest(24*time.Hour, time.Hour, true)

	var n int

	ok(t, s.Pool.QueryRow(ctx, "SELECT count(*) FROM closures WHERE key = $1", input).Scan(&n))

	if n != 1 {
		t.Fatal("claimed input was garbage-collected")
	}
}

// The real server wraps handlers for metrics and sets a WriteTimeout. The
// stream must flush through the wrapper and outlive the timeout.
func TestClaim_StreamsThroughServer(t *testing.T) {
	t.Parallel()

	s := newClaimService(t)
	defer s.Close()

	srv := httptest.NewUnstartedServer(server.NewMetrics().Instrument(http.HandlerFunc(s.ClaimHandler)))
	srv.Config.WriteTimeout = testHeartbeat
	srv.Start()

	defer srv.Close()

	body, err := json.Marshal(claimReq("streamsthroughserver000000000000"))
	ok(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL, bytes.NewReader(body))
	ok(t, err)

	resp, err := http.DefaultClient.Do(req)
	ok(t, err)

	defer func() { _ = resp.Body.Close() }()

	sc := bufio.NewScanner(resp.Body)
	for i := range 4 {
		if !sc.Scan() {
			t.Fatalf("stream ended after %d lines: %v", i, sc.Err())
		}
	}
}
