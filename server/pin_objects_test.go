package server_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/oidc"
	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgtype"
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

// TestPinObjectsReachable verifies that PinMayRead returns true for all objects
// reachable from a pin's target, and false for unrelated objects.
func TestPinObjectsReachable(t *testing.T) {
	t.Parallel()
	service := createTestServiceWithAuth(t, staticTokenForTests)
	defer service.Close()

	ctx := t.Context()

	// Create the closure graph:
	// a.narinfo -> nar/a.nar.zst, b.narinfo (b references nar/b)
	// b.narinfo -> nar/b.nar.zst; c.narinfo (unrelated)
	aKey := strings.Repeat("a", 32) + ".narinfo"
	bKey := strings.Repeat("b", 32) + ".narinfo"
	cKey := strings.Repeat("c", 32) + ".narinfo"
	narAKey := "nar/" + strings.Repeat("a", 32) + "00000000000000000000.nar.zst"
	narBKey := "nar/" + strings.Repeat("b", 32) + "00000000000000000000.nar.zst"

	setupGraphObjects(t, service, map[string][]string{
		aKey:    {narAKey, bKey},
		bKey:    {narBKey},
		cKey:    {},
		narAKey: {},
		narBKey: {},
	}, []string{aKey, bKey, cKey, narAKey, narBKey})

	// Pin host-1 to a.narinfo through CreatePinHandler
	pin(t, service, "host-1", strings.Repeat("a", 32))

	queryFn := pg.New(service.Pool)
	grace := pgtype.Interval{Valid: true, Microseconds: 0}

	// Check a.narinfo
	canRead, err := queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-1"},
		Key:      aKey,
		Grace:    grace,
	})
	ok(t, err)
	if !canRead {
		t.Errorf("PinMayRead([\"host-1\"], %q) = false, want true", aKey)
	}

	// Check b.narinfo
	canRead, err = queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-1"},
		Key:      bKey,
		Grace:    grace,
	})
	ok(t, err)
	if !canRead {
		t.Errorf("PinMayRead([\"host-1\"], %q) = false, want true", bKey)
	}

	// Check nar/a
	canRead, err = queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-1"},
		Key:      narAKey,
		Grace:    grace,
	})
	ok(t, err)
	if !canRead {
		t.Errorf("PinMayRead([\"host-1\"], %q) = false, want true", narAKey)
	}

	// Check nar/b
	canRead, err = queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-1"},
		Key:      narBKey,
		Grace:    grace,
	})
	ok(t, err)
	if !canRead {
		t.Errorf("PinMayRead([\"host-1\"], %q) = false, want true", narBKey)
	}

	// Check c.narinfo (unrelated, should be false)
	canRead, err = queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-1"},
		Key:      cKey,
		Grace:    grace,
	})
	ok(t, err)
	if canRead {
		t.Errorf("PinMayRead([\"host-1\"], %q) = true, want false", cKey)
	}
}

// TestPinObjectsGrace verifies that grace period works correctly for moved pins.
func TestPinObjectsGrace(t *testing.T) {
	t.Parallel()
	service := createTestServiceWithAuth(t, staticTokenForTests)
	defer service.Close()

	ctx := t.Context()

	aKey := strings.Repeat("a", 32) + ".narinfo"
	cKey := strings.Repeat("c", 32) + ".narinfo"

	setupGraphObjects(t, service, map[string][]string{
		aKey: {},
		cKey: {},
	}, []string{aKey, cKey})

	// Pin host-1 to a.narinfo
	pin(t, service, "host-1", strings.Repeat("a", 32))

	// Move pin to c.narinfo
	pin(t, service, "host-1", strings.Repeat("c", 32))

	queryFn := pg.New(service.Pool)

	// Age the moved_at timestamp to 30 minutes ago (within 1h grace)
	_, err := service.Pool.Exec(ctx,
		`UPDATE pins SET moved_at = now() - interval '30 minutes' WHERE name = 'host-1'`)
	ok(t, err)

	// With grace 1h, a.narinfo should still be readable (within grace)
	grace1h := pgtype.Interval{Valid: true, Microseconds: 3600 * 1_000_000}
	canRead, err := queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-1"},
		Key:      aKey,
		Grace:    grace1h,
	})
	ok(t, err)
	if !canRead {
		t.Errorf("PinMayRead with grace 1h for previous target a = false, want true")
	}

	// With grace 0, a.narinfo should NOT be readable (outside grace)
	grace0 := pgtype.Interval{Valid: true, Microseconds: 0}
	canRead, err = queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-1"},
		Key:      aKey,
		Grace:    grace0,
	})
	ok(t, err)
	if canRead {
		t.Errorf("PinMayRead with grace 0 for previous target a = true, want false")
	}

	// With grace 0, c.narinfo should be readable (current target)
	canRead, err = queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-1"},
		Key:      cKey,
		Grace:    grace0,
	})
	ok(t, err)
	if !canRead {
		t.Errorf("PinMayRead with grace 0 for current target c = false, want true")
	}
}

// TestPinObjectsTwoMoves verifies behavior with multiple pin moves.
func TestPinObjectsTwoMoves(t *testing.T) {
	t.Parallel()
	service := createTestServiceWithAuth(t, staticTokenForTests)
	defer service.Close()

	ctx := t.Context()

	aKey := strings.Repeat("a", 32) + ".narinfo"
	cKey := strings.Repeat("c", 32) + ".narinfo"
	dKey := strings.Repeat("d", 32) + ".narinfo"

	setupGraphObjects(t, service, map[string][]string{
		aKey: {},
		cKey: {},
		dKey: {},
	}, []string{aKey, cKey, dKey})

	// Move: a -> c -> d through CreatePinHandler
	pin(t, service, "host-1", strings.Repeat("a", 32))
	pin(t, service, "host-1", strings.Repeat("c", 32))
	pin(t, service, "host-1", strings.Repeat("d", 32))

	// Age the last move (c -> d) to 30 minutes ago (within 1h grace)
	_, err := service.Pool.Exec(ctx,
		`UPDATE pins SET moved_at = now() - interval '30 minutes' WHERE name = 'host-1'`)
	ok(t, err)

	queryFn := pg.New(service.Pool)
	grace1h := pgtype.Interval{Valid: true, Microseconds: 3600 * 1_000_000}

	// a.narinfo should be false (PrunePinObjects removes it, only current + previous tracked)
	canRead, err := queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-1"},
		Key:      aKey,
		Grace:    grace1h,
	})
	ok(t, err)
	if canRead {
		t.Errorf("PinMayRead after two moves: a.narinfo = true, want false (not in previous/current)")
	}

	// c.narinfo should be true (it's the previous target, still within grace period)
	canRead, err = queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-1"},
		Key:      cKey,
		Grace:    grace1h,
	})
	ok(t, err)
	if !canRead {
		t.Errorf("PinMayRead after two moves: c.narinfo = false, want true (previous target within grace)")
	}
}

// TestPinObjectsSameTarget verifies that re-pinning to the same target doesn't reset moved_at.
func TestPinObjectsSameTarget(t *testing.T) {
	t.Parallel()
	service := createTestServiceWithAuth(t, staticTokenForTests)
	defer service.Close()

	ctx := t.Context()

	aKey := strings.Repeat("a", 32) + ".narinfo"
	cKey := strings.Repeat("c", 32) + ".narinfo"

	setupGraphObjects(t, service, map[string][]string{
		aKey: {},
		cKey: {},
	}, []string{aKey, cKey})

	// Pin a -> c -> c through CreatePinHandler
	pin(t, service, "host-1", strings.Repeat("a", 32))
	pin(t, service, "host-1", strings.Repeat("c", 32))
	pin(t, service, "host-1", strings.Repeat("c", 32))

	// Age moved_at to 30 minutes ago
	_, err := service.Pool.Exec(ctx,
		`UPDATE pins SET moved_at = now() - interval '30 minutes' WHERE name = 'host-1'`)
	ok(t, err)

	queryFn := pg.New(service.Pool)
	grace1h := pgtype.Interval{Valid: true, Microseconds: 3600 * 1_000_000}

	// a.narinfo should still be readable (it's the previous target, within grace)
	canRead, err := queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-1"},
		Key:      aKey,
		Grace:    grace1h,
	})
	ok(t, err)
	if !canRead {
		t.Errorf("PinMayRead with same target repins: a.narinfo = false, want true")
	}
}

// TestPinObjectsSelfReference verifies that objects can reference themselves.
func TestPinObjectsSelfReference(t *testing.T) {
	t.Parallel()
	service := createTestServiceWithAuth(t, staticTokenForTests)
	defer service.Close()

	aKey := strings.Repeat("a", 32) + ".narinfo"

	// a.narinfo references itself
	setupGraphObjects(t, service, map[string][]string{
		aKey: {aKey},
	}, []string{aKey})

	// Pin succeeds (asserts 204)
	pin(t, service, "host-1", strings.Repeat("a", 32))

	// a.narinfo is readable
	ctx := t.Context()
	queryFn := pg.New(service.Pool)
	grace := pgtype.Interval{Valid: true, Microseconds: 0}
	canRead, err := queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-1"},
		Key:      aKey,
		Grace:    grace,
	})
	ok(t, err)
	if !canRead {
		t.Errorf("PinMayRead for self-referencing object = false, want true")
	}
}

// TestPinObjectsOtherPin verifies that patterns are used for pin matching.
func TestPinObjectsOtherPin(t *testing.T) {
	t.Parallel()
	service := createTestServiceWithAuth(t, staticTokenForTests)
	defer service.Close()

	ctx := t.Context()

	aKey := strings.Repeat("a", 32) + ".narinfo"
	cKey := strings.Repeat("c", 32) + ".narinfo"

	setupGraphObjects(t, service, map[string][]string{
		aKey: {},
		cKey: {},
	}, []string{aKey, cKey})

	// Pin host-1 to a and host-2 to c through CreatePinHandler
	pin(t, service, "host-1", strings.Repeat("a", 32))
	pin(t, service, "host-2", strings.Repeat("c", 32))

	queryFn := pg.New(service.Pool)
	grace := pgtype.Interval{Valid: true, Microseconds: 0}

	// host-2 exact match cannot read a.narinfo
	canRead, err := queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-2"},
		Key:      aKey,
		Grace:    grace,
	})
	ok(t, err)
	if canRead {
		t.Errorf("PinMayRead([\"host-2\"], a) = true, want false (different pin)")
	}

	// host-% pattern matches both, so host-% can read a.narinfo
	canRead, err = queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-%"},
		Key:      aKey,
		Grace:    grace,
	})
	ok(t, err)
	if !canRead {
		t.Errorf("PinMayRead([\"host-%%\"], a) = false, want true (pattern matches)")
	}
}

// TestPinObjectsDelete verifies that DeletePin removes access to pinned objects.
func TestPinObjectsDelete(t *testing.T) {
	t.Parallel()
	service := createTestServiceWithAuth(t, staticTokenForTests)
	defer service.Close()

	ctx := t.Context()

	aKey := strings.Repeat("a", 32) + ".narinfo"

	setupGraphObjects(t, service, map[string][]string{
		aKey: {},
	}, []string{aKey})

	// Pin host-1 to a.narinfo through CreatePinHandler
	pin(t, service, "host-1", strings.Repeat("a", 32))

	// Delete the pin through DeletePinHandler
	req := httptest.NewRequestWithContext(ctx, "DELETE", "/api/pins/host-1", nil)
	req.SetPathValue("name", "host-1")
	req.Header.Set("Authorization", "Bearer "+staticTokenForTests)

	w := httptest.NewRecorder()
	service.RequireScope(oidc.ScopeAdmin, service.DeletePinHandler).ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("DeletePin failed: status=%d", w.Code)
	}

	// a.narinfo should not be readable
	queryFn := pg.New(service.Pool)
	grace := pgtype.Interval{Valid: true, Microseconds: 0}
	canRead, err := queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-1"},
		Key:      aKey,
		Grace:    grace,
	})
	ok(t, err)
	if canRead {
		t.Errorf("PinMayRead after DeletePin = true, want false")
	}
}

// TestPinObjectsBackfill verifies that the BackfillPinObjects query populates rows for existing pins.
func TestPinObjectsBackfill(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	// Create two pins with disjoint closures
	a1Key := strings.Repeat("a", 32) + ".narinfo"
	b1Key := strings.Repeat("b", 32) + ".narinfo"
	c1Key := strings.Repeat("c", 32) + ".narinfo"
	d1Key := strings.Repeat("d", 32) + ".narinfo"

	setupGraphObjects(t, service, map[string][]string{
		a1Key: {b1Key},
		b1Key: {},
		c1Key: {d1Key},
		d1Key: {},
	}, []string{a1Key, b1Key, c1Key, d1Key})

	// Insert pins directly into database (this tests the migration statement)
	_, err := service.Pool.Exec(ctx,
		`INSERT INTO pins (name, narinfo_key, store_path, created_at, updated_at) VALUES
		 ($1, $2, '/nix/store/dummy1', now(), now()),
		 ($3, $4, '/nix/store/dummy2', now(), now())`,
		"pin-1", a1Key, "pin-2", c1Key)
	ok(t, err)

	// Run BackfillPinObjects for all pins
	queryFn := pg.New(service.Pool)
	err = queryFn.BackfillPinObjects(ctx)
	ok(t, err)

	grace := pgtype.Interval{Valid: true, Microseconds: 0}

	// pin-1 can read a1 and b1, but not c1 or d1
	for key, shouldRead := range map[string]bool{
		a1Key: true,
		b1Key: true,
		c1Key: false,
		d1Key: false,
	} {
		canRead, err := queryFn.PinMayRead(ctx, pg.PinMayReadParams{
			Patterns: []string{"pin-1"},
			Key:      key,
			Grace:    grace,
		})
		ok(t, err)
		if canRead != shouldRead {
			t.Errorf("PinMayRead(pin-1, %q) = %v, want %v", key, canRead, shouldRead)
		}
	}

	// pin-2 can read c1 and d1, but not a1 or b1
	for key, shouldRead := range map[string]bool{
		a1Key: false,
		b1Key: false,
		c1Key: true,
		d1Key: true,
	} {
		canRead, err := queryFn.PinMayRead(ctx, pg.PinMayReadParams{
			Patterns: []string{"pin-2"},
			Key:      key,
			Grace:    grace,
		})
		ok(t, err)
		if canRead != shouldRead {
			t.Errorf("PinMayRead(pin-2, %q) = %v, want %v", key, canRead, shouldRead)
		}
	}
}

// TestPinObjectsSharedTarget verifies that grace expiration is scoped per pin
// and DeleteExpiredPinObjects removes expired previous targets only.
func TestPinObjectsSharedTarget(t *testing.T) {
	t.Parallel()
	service := createTestServiceWithAuth(t, staticTokenForTests)
	defer service.Close()

	ctx := t.Context()

	aKey := strings.Repeat("a", 32) + ".narinfo"
	cKey := strings.Repeat("c", 32) + ".narinfo"

	setupGraphObjects(t, service, map[string][]string{
		aKey: {},
		cKey: {},
	}, []string{aKey, cKey})

	// Pin both host-1 and host-2 to a.narinfo through CreatePinHandler
	pin(t, service, "host-1", strings.Repeat("a", 32))
	pin(t, service, "host-2", strings.Repeat("a", 32))

	// Move host-1 to c.narinfo
	pin(t, service, "host-1", strings.Repeat("c", 32))

	// Age host-1's moved_at to 2 hours ago (outside grace)
	_, err := service.Pool.Exec(ctx,
		`UPDATE pins SET moved_at = now() - interval '2 hours' WHERE name = 'host-1'`)
	ok(t, err)

	queryFn := pg.New(service.Pool)
	grace1h := pgtype.Interval{Valid: true, Microseconds: 3600 * 1_000_000}

	// Before DeleteExpiredPinObjects: verify deletion happens correctly
	deleted, err := queryFn.DeleteExpiredPinObjects(ctx, grace1h)
	ok(t, err)
	if deleted <= 0 {
		t.Errorf("DeleteExpiredPinObjects deleted %d rows, want > 0", deleted)
	}

	// After DeleteExpiredPinObjects: host-1 cannot read a (rows deleted)
	canRead, err := queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-1"},
		Key:      aKey,
		Grace:    grace1h,
	})
	ok(t, err)
	if canRead {
		t.Errorf("After delete: PinMayRead([\"host-1\"], a) = true, want false (rows deleted)")
	}

	// But host-2 can still read a.narinfo (it's the current target)
	canRead, err = queryFn.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: []string{"host-2"},
		Key:      aKey,
		Grace:    grace1h,
	})
	ok(t, err)
	if !canRead {
		t.Errorf("After delete: PinMayRead([\"host-2\"], a) = false, want true (current target)")
	}
}

// TestGlobToLike tests the glob to like pattern conversion function.
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
