package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/api"
)

// Only closure roots are reported present. A narinfo that exists just as a
// dependency of another closure is collected with that closure, so a client
// must push it to make it a root of its own.
func TestPresentReportsOnlyClosureRoots(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	root := strings.Repeat("a", 32) + ".narinfo"
	dep := strings.Repeat("b", 32) + ".narinfo"
	tombstoned := strings.Repeat("c", 32) + ".narinfo"

	_, err := service.Pool.Exec(ctx,
		`INSERT INTO objects (key, refs) VALUES ($1, $2), ($3, '{}')`, root, []string{dep}, dep)
	ok(t, err)

	_, err = service.Pool.Exec(ctx,
		`INSERT INTO objects (key, deleted_at, first_deleted_at) VALUES ($1, now(), now())`, tombstoned)
	ok(t, err)

	_, err = service.Pool.Exec(ctx,
		`INSERT INTO closures (key, updated_at) VALUES ($1, now() - interval '1 day'), ($2, now())`, root, tombstoned)
	ok(t, err)

	body, err := json.Marshal(api.PresentRequest{Keys: []string{root, dep, tombstoned}})
	ok(t, err)

	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/objects/present", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	service.PresentHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	var resp api.PresentResponse
	ok(t, json.Unmarshal(w.Body.Bytes(), &resp))

	if len(resp.Present) != 1 || resp.Present[0] != root {
		t.Fatalf("present=%v, want only the closure root %s", resp.Present, root)
	}

	var touched bool
	ok(t, service.Pool.QueryRow(ctx,
		"SELECT updated_at > now() - interval '1 minute' FROM closures WHERE key = $1", root).Scan(&touched))

	if !touched {
		t.Errorf("present closure %s was not touched", root)
	}
}

// A closure that GC is deleting while the present check runs must not be
// reported present: the client would skip the push and the hook would drop
// the path, while the closure's objects go on to be swept.
func TestPresentNotReportedWhileGCDeletesClosure(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	root := strings.Repeat("d", 32) + ".narinfo"

	_, err := service.Pool.Exec(ctx, `INSERT INTO objects (key, refs) VALUES ($1, '{}')`, root)
	ok(t, err)

	_, err = service.Pool.Exec(ctx,
		`INSERT INTO closures (key, updated_at) VALUES ($1, now() - interval '30 days')`, root)
	ok(t, err)

	// GC has locked the row for deletion but not yet committed.
	gcTx, err := service.Pool.Begin(ctx)
	ok(t, err)

	defer func() { _ = gcTx.Rollback(ctx) }()

	_, err = gcTx.Exec(ctx, `DELETE FROM closures WHERE updated_at < now() - interval '1 day'`)
	ok(t, err)

	body, err := json.Marshal(api.PresentRequest{Keys: []string{root}})
	ok(t, err)

	type result struct {
		code int
		body []byte
	}

	done := make(chan result, 1)

	go func() {
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/objects/present", strings.NewReader(string(body)))
		w := httptest.NewRecorder()
		service.PresentHandler(w, req)
		done <- result{code: w.Code, body: w.Body.Bytes()}
	}()

	// The present check must block behind the delete rather than answer from
	// a snapshot in which the closure still exists.
	select {
	case res := <-done:
		t.Fatalf("present answered while GC held the closure row: status=%d body=%s", res.code, res.body)
	case <-time.After(500 * time.Millisecond):
	}

	ok(t, gcTx.Commit(ctx))

	res := <-done
	if res.code != http.StatusOK {
		t.Fatalf("status=%d body=%s", res.code, res.body)
	}

	var resp api.PresentResponse
	ok(t, json.Unmarshal(res.body, &resp))

	if len(resp.Present) != 0 {
		t.Fatalf("present=%v, want none: GC deleted the closure", resp.Present)
	}

	// The response must still be a JSON array, not null, so clients that
	// range over it keep working.
	if !strings.Contains(string(res.body), `"present":[]`) {
		t.Errorf("empty present list must encode as [], got %s", res.body)
	}
}
