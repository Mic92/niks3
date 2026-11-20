package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/oidc"
)

const staticTokenForTests = "static-api-token-at-least-36-chars-long"

// pin creates or updates a pin through CreatePinHandler HTTP POST.
// Asserts 204 on success. hash is 32 nix base32 chars (e.g. 32 'a's).
func pin(t *testing.T, service *server.Service, name, hash string) {
	t.Helper()

	body := map[string]string{
		"store_path": "/nix/store/" + hash + "-test",
	}
	bodyBytes, err := json.Marshal(body)
	ok(t, err)

	req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/pins/"+name, bytes.NewBuffer(bodyBytes))
	req.SetPathValue("name", name)
	req.Header.Set("Authorization", "Bearer "+staticTokenForTests)

	w := httptest.NewRecorder()
	service.RequireScope(oidc.ScopeWrite, service.CreatePinHandler).ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("pin(%s, %s) failed: status=%d body=%s", name, hash, w.Code, w.Body.String())
	}
}

// setupGraphObjects inserts objects and closures for testing.
func setupGraphObjects(t *testing.T, service *server.Service, objects map[string][]string, closureKeys []string) {
	t.Helper()

	ctx := t.Context()

	// Insert objects with proper numbered parameters
	if len(objects) > 0 {
		var query strings.Builder
		query.WriteString("INSERT INTO objects (key, refs) VALUES ")
		args := make([]any, 0, 2*len(objects))
		first := true
		paramNum := 1

		for key, refs := range objects {
			if !first {
				query.WriteString(", ")
			}
			fmt.Fprintf(&query, "($%d, $%d)", paramNum, paramNum+1)
			args = append(args, key, refs)
			paramNum += 2
			first = false
		}

		_, err := service.Pool.Exec(ctx, query.String(), args...)
		ok(t, err)
	}

	// Insert closures
	if len(closureKeys) > 0 {
		var query strings.Builder
		query.WriteString("INSERT INTO closures (key, updated_at) VALUES ")
		args := make([]any, 0, len(closureKeys))

		for i := range closureKeys {
			if i > 0 {
				query.WriteString(", ")
			}
			fmt.Fprintf(&query, "($%d, now())", i+1)
			args = append(args, closureKeys[i])
		}

		_, err := service.Pool.Exec(ctx, query.String(), args...)
		ok(t, err)
	}
}

func unpin(t *testing.T, service *server.Service, name string) {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/api/pins/"+name, nil)
	req.SetPathValue("name", name)
	req.Header.Set("Authorization", "Bearer "+staticTokenForTests)

	w := httptest.NewRecorder()
	service.RequireScope(oidc.ScopeWrite, service.DeletePinHandler).ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("unpin(%s) failed: status=%d body=%s", name, w.Code, w.Body.String())
	}
}

func narinfoOf(letter string) string { return strings.Repeat(letter, 32) + ".narinfo" }

// pinService has the narinfos a, b and c. a references its NAR, itself and b.
func pinService(t *testing.T) *server.Service {
	t.Helper()

	service := createTestServiceWithAuth(t, staticTokenForTests)
	t.Cleanup(service.Close)

	service.PinReadGrace = time.Hour

	setupGraphObjects(t, service, map[string][]string{
		narinfoOf("a"): {"nar/a.nar.zst", narinfoOf("a"), narinfoOf("b")},
		narinfoOf("b"): {"nar/b.nar.zst"},
		narinfoOf("c"): {"nar/c.nar.zst"},
	}, []string{narinfoOf("a"), narinfoOf("b"), narinfoOf("c")})

	return service
}

func assertReads(t *testing.T, service *server.Service, pins string, want map[string]bool) {
	t.Helper()

	for key, readable := range want {
		got, err := service.PinsReach(t.Context(), []string{pins}, key)
		ok(t, err)

		if got != readable {
			t.Errorf("%s may read %s: %v, want %v", pins, key, got, readable)
		}
	}
}

func movedAgo(t *testing.T, service *server.Service, ago string) {
	t.Helper()

	_, err := service.Pool.Exec(t.Context(), "UPDATE pins SET moved_at = now() - $1::interval", ago)
	ok(t, err)
}

func TestPinReadsWhatItsTargetReaches(t *testing.T) {
	t.Parallel()

	service := pinService(t)
	pin(t, service, "host-1", strings.Repeat("a", 32))
	pin(t, service, "other", strings.Repeat("c", 32))

	assertReads(t, service, "host-1", map[string]bool{
		narinfoOf("a"): true, "nar/a.nar.zst": true, narinfoOf("b"): true, "nar/b.nar.zst": true,
		narinfoOf("c"): false, "nar/c.nar.zst": false,
	})
	assertReads(t, service, "host-2", map[string]bool{narinfoOf("a"): false})
	assertReads(t, service, "host-*", map[string]bool{narinfoOf("a"): true, narinfoOf("c"): false})

	unpin(t, service, "host-1")
	assertReads(t, service, "host-1", map[string]bool{narinfoOf("a"): false})
}

func TestPinReadsItsPreviousTargetDuringTheGrace(t *testing.T) {
	t.Parallel()

	service := pinService(t)
	pin(t, service, "host-1", strings.Repeat("a", 32))
	pin(t, service, "host-1", strings.Repeat("c", 32))
	// Setting a pin to what it points at is not a move.
	pin(t, service, "host-1", strings.Repeat("c", 32))

	movedAgo(t, service, "30 minutes")
	assertReads(t, service, "host-1", map[string]bool{narinfoOf("a"): true, narinfoOf("c"): true})

	movedAgo(t, service, "2 hours")
	assertReads(t, service, "host-1", map[string]bool{narinfoOf("a"): false, narinfoOf("c"): true})

	// Only the latest move has a grace.
	pin(t, service, "host-1", strings.Repeat("b", 32))
	assertReads(t, service, "host-1", map[string]bool{narinfoOf("a"): false, narinfoOf("c"): true, narinfoOf("b"): true})
}

// closures hands out the closure of a target and counts the walks.
type closures struct {
	of    map[string][]string
	walks map[string]int
}

func (c *closures) load(_ context.Context, target string) ([]string, error) {
	c.walks[target]++

	return c.of[target], nil
}

func (c *closures) reaches(t *testing.T, sets *server.PinSets, target, key string) bool {
	t.Helper()

	found, err := sets.Reaches(t.Context(), target, key, c.load)
	ok(t, err)

	return found
}

func TestPinSetIsWalkedOnceAndRefreshed(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var sets server.PinSets

		db := &closures{of: map[string][]string{"a": {"a", "nar"}}, walks: map[string]int{}}

		for range 100 {
			if !db.reaches(t, &sets, "a", "nar") || db.reaches(t, &sets, "a", "log") {
				t.Fatal("wrong answer from the set")
			}
		}

		if db.walks["a"] != 1 {
			t.Fatalf("the closure was walked %d times", db.walks["a"])
		}

		// A later push added the log.
		db.of["a"] = []string{"a", "nar", "log"}

		time.Sleep(server.PinSetLifetime)

		if !db.reaches(t, &sets, "a", "log") {
			t.Fatal("the set was not refreshed")
		}
	})
}

func TestPinSetIsDroppedWhenOld(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var sets server.PinSets

		db := &closures{of: map[string][]string{"a": {"a"}, "b": {"b"}}, walks: map[string]int{}}
		db.reaches(t, &sets, "a", "a")

		time.Sleep(server.PinSetLifetime - time.Minute)
		db.reaches(t, &sets, "b", "b")
		time.Sleep(2 * time.Minute)
		synctest.Wait()

		if sets.Held() != 1 {
			t.Fatalf("%d keys held, want only those of b", sets.Held())
		}

		time.Sleep(2 * server.PinSetLifetime)
		synctest.Wait()

		if sets.Held() != 0 {
			t.Fatalf("%d keys held after every set got old", sets.Held())
		}
	})
}

func TestGlobToLike(t *testing.T) {
	t.Parallel()

	cases := []struct {
		pattern string
		want    string
	}{
		{"host-*", "host-%"},
		{"web_?", `web\__`},
		{"normal", "normal"},
		{"*", "%"},
		{"?", "_"},
		{"%", `\%`},
		{`\_`, `\\\_`},
		{`\*`, `\\%`},
		{`\?`, `\\_`},
		{`\\`, `\\\\`},
		{`host\`, `host\\`},
	}

	for _, c := range cases {
		got := server.GlobToLike(c.pattern)
		if got != c.want {
			t.Errorf("GlobToLike(%q) = %q, want %q", c.pattern, got, c.want)
		}
	}
}
