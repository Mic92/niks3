package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
