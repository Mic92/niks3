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

// A narinfo that exists only as a dependency is not present, it must be pushed.
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
	seededWithoutS3(t, root, dep)

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
		"SELECT updated_at > timezone('UTC', now()) - interval '1 minute' FROM closures WHERE key = $1", root).Scan(&touched))

	if !touched {
		t.Errorf("present closure %s was not touched", root)
	}
}

// If GC is deleting a closure, the present check must not report it.
// Otherwise the client skips the push and the closure is lost.
func TestPresentNotReportedWhileGCDeletesClosure(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	root := strings.Repeat("d", 32) + ".narinfo"

	_, err := service.Pool.Exec(ctx, `INSERT INTO objects (key, refs) VALUES ($1, '{}')`, root)
	ok(t, err)
	seededWithoutS3(t, root)

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

	// The handler has to wait for GC's lock. A plain SELECT would answer from
	// a snapshot that still contains the closure.
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

	// Clients expect a JSON array, so an empty result must be [] and not null.
	if !strings.Contains(string(res.body), `"present":[]`) {
		t.Errorf("empty present list must encode as [], got %s", res.body)
	}
}
