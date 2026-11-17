package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server/pg"
)

// The three endpoints differ in what they say about a path that was only
// pushed as a dependency.
func TestExistsTellsClosuresFromTheirDependencies(t *testing.T) {
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

	body, err := json.Marshal(api.ExistsRequest{Keys: []string{root, dep, tombstoned}})
	ok(t, err)

	ask := func(handler http.HandlerFunc, answer any) {
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/", strings.NewReader(string(body)))
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}

		ok(t, json.Unmarshal(w.Body.Bytes(), answer))
	}

	var objects, closures api.ExistsResponse

	ask(service.ObjectsExistHandler, &objects)
	slices.Sort(objects.Exists)

	if !slices.Equal(objects.Exists, []string{root, dep}) {
		t.Fatalf("objects=%v, want the closure and its dependency", objects.Exists)
	}

	var untouched bool
	ok(t, service.Pool.QueryRow(ctx,
		"SELECT updated_at < timezone('UTC', now()) - interval '1 hour' FROM closures WHERE key = $1", root).Scan(&untouched))

	if !untouched {
		t.Errorf("asking for objects touched the closure %s", root)
	}

	ask(service.ClosuresExistHandler, &closures)

	if !slices.Equal(closures.Exists, []string{root}) {
		t.Fatalf("closures=%v, want only %s", closures.Exists, root)
	}

	var former api.PresentResponse

	ask(service.PresentHandler, &former)

	if !slices.Equal(former.Present, closures.Exists) {
		t.Fatalf("/api/objects/present answered %v, /api/closures/exists %v", former.Present, closures.Exists)
	}

	var touched bool
	ok(t, service.Pool.QueryRow(ctx,
		"SELECT updated_at > timezone('UTC', now()) - interval '1 minute' FROM closures WHERE key = $1", root).Scan(&touched))

	if !touched {
		t.Errorf("present closure %s was not touched", root)
	}
}

func TestPresentRefreshesOnlyStaleClosures(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	fresh := strings.Repeat("e", 32) + ".narinfo"
	stale := strings.Repeat("f", 32) + ".narinfo"

	_, err := service.Pool.Exec(ctx, `INSERT INTO objects (key, refs) VALUES ($1, '{}'), ($2, '{}')`, fresh, stale)
	ok(t, err)

	_, err = service.Pool.Exec(ctx,
		`INSERT INTO closures (key, updated_at) VALUES
		 ($1, timezone('UTC', now()) - interval '1 minute'), ($2, timezone('UTC', now()) - interval '1 hour')`, fresh, stale)
	ok(t, err)

	present, err := pg.New(service.Pool).TouchPresentClosures(ctx, []string{fresh, stale})
	ok(t, err)

	if len(present) != 2 {
		t.Fatalf("present=%v, want both closures", present)
	}

	var freshKept, staleRefreshed bool

	ok(t, service.Pool.QueryRow(ctx,
		`SELECT
		   (SELECT updated_at < timezone('UTC', now()) - interval '30 seconds' FROM closures WHERE key = $1),
		   (SELECT updated_at > timezone('UTC', now()) - interval '30 seconds' FROM closures WHERE key = $2)`,
		fresh, stale).Scan(&freshKept, &staleRefreshed))

	if !freshKept {
		t.Error("a closure touched a minute ago was rewritten")
	}

	if !staleRefreshed {
		t.Error("a closure touched an hour ago was not refreshed")
	}
}

// If GC is deleting a closure, the present check must not report it.
// Otherwise the client skips the push and the closure is lost.
func TestPresentNotReportedWhileGCDeletesClosure(t *testing.T) {
	t.Parallel()

	// The check waits for GC even when it does not rewrite the row.
	for name, age := range map[string]string{"stale": "30 days", "fresh": "1 minute"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			presentWaitsForGC(t, age)
		})
	}
}

func presentWaitsForGC(t *testing.T, age string) {
	t.Helper()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	root := strings.Repeat("d", 32) + ".narinfo"

	_, err := service.Pool.Exec(ctx, `INSERT INTO objects (key, refs) VALUES ($1, '{}')`, root)
	ok(t, err)

	_, err = service.Pool.Exec(ctx,
		`INSERT INTO closures (key, updated_at) VALUES ($1, timezone('UTC', now()) - $2::interval)`, root, age)
	ok(t, err)

	// GC has locked the row for deletion but not yet committed.
	gcTx, err := service.Pool.Begin(ctx)
	ok(t, err)

	defer func() { _ = gcTx.Rollback(ctx) }()

	_, err = gcTx.Exec(ctx, `DELETE FROM closures WHERE key = $1`, root)
	ok(t, err)

	body, err := json.Marshal(api.ExistsRequest{Keys: []string{root}})
	ok(t, err)

	type result struct {
		code int
		body []byte
	}

	done := make(chan result, 1)

	go func() {
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/closures/exists", strings.NewReader(string(body)))
		w := httptest.NewRecorder()
		service.ClosuresExistHandler(w, req)
		done <- result{code: w.Code, body: w.Body.Bytes()}
	}()

	// The handler has to wait for GC's lock. A plain SELECT would answer from
	// a snapshot that still contains the closure.
	select {
	case res := <-done:
		t.Fatalf("answered while GC held the closure row: status=%d body=%s", res.code, res.body)
	case <-time.After(500 * time.Millisecond):
	}

	ok(t, gcTx.Commit(ctx))

	res := <-done
	if res.code != http.StatusOK {
		t.Fatalf("status=%d body=%s", res.code, res.body)
	}

	var resp api.ExistsResponse
	ok(t, json.Unmarshal(res.body, &resp))

	if len(resp.Exists) != 0 {
		t.Fatalf("exists=%v, want none: GC deleted the closure", resp.Exists)
	}

	// Clients expect a JSON array, so an empty result must be [] and not null.
	if !strings.Contains(string(res.body), `"exists":[]`) {
		t.Errorf("empty list must encode as [], got %s", res.body)
	}
}
