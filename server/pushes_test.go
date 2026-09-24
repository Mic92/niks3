package server_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/signing"
)

// pkgObjects returns the narinfo and NAR objects of one store path.
func pkgObjects(hash string, refs ...string) []map[string]any {
	narinfoRefs := make([]string, 0, 1+len(refs))
	narinfoRefs = append(narinfoRefs, narKeyFor(hash))
	for _, r := range refs {
		narinfoRefs = append(narinfoRefs, r+".narinfo")
	}

	return []map[string]any{
		{"key": hash + ".narinfo", "type": "narinfo", "refs": narinfoRefs},
		{"key": narKeyFor(hash), "type": "nar", "refs": []string{}},
	}
}

func createPush(t *testing.T, service *server.Service, roots []string, objects ...[]map[string]any) server.PendingClosureResponse {
	t.Helper()

	var all []map[string]any
	for _, o := range objects {
		all = append(all, o...)
	}

	body, err := json.Marshal(map[string]any{"roots": roots, "objects": all})
	ok(t, err)

	rr := testRequest(t, &TestRequest{
		method:  "POST",
		path:    "/api/pushes",
		body:    body,
		handler: service.CreatePushHandler,
	})

	var resp server.PendingClosureResponse

	ok(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	return resp
}

// completePush uploads what the push was offered, as the client does, and
// completes it.
func completePush(t *testing.T, service *server.Service, push server.PendingClosureResponse, status int) {
	t.Helper()

	for key, pendingObject := range push.PendingObjects {
		if pendingObject.MultipartInfo != nil {
			handleMultipartUpload(t.Context(), t, key, pendingObject, service)
		} else {
			handlePresignedUpload(t.Context(), t, pendingObject.PresignedURL)
		}
	}

	check := checkStatusCode(status)
	testRequest(t, &TestRequest{
		method:        "POST",
		path:          "/api/pushes/" + push.ID + "/complete",
		handler:       service.CompletePushHandler,
		pathValues:    map[string]string{"id": push.ID},
		checkResponse: &check,
	})
}

func countRows(t *testing.T, service *server.Service, query string, args ...any) int {
	t.Helper()

	var n int

	ok(t, service.Pool.QueryRow(t.Context(), query, args...).Scan(&n))

	return n
}

// Roots that share dependencies store each object once, not once per root.
func TestPush_OverlappingRootsStoreOneRowPerKey(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	base := strings.Repeat("a", 32)
	rootA := strings.Repeat("b", 32)
	rootB := strings.Repeat("c", 32)

	resp := createPush(t, service, []string{rootA + ".narinfo", rootB + ".narinfo"},
		pkgObjects(base), pkgObjects(rootA, base), pkgObjects(rootB, base))

	if len(resp.PendingObjects) != 6 {
		t.Errorf("pending objects = %d, want 6", len(resp.PendingObjects))
	}

	if n := countRows(t, service, "SELECT count(*) FROM pending_closures"); n != 1 {
		t.Errorf("pending closures = %d, want 1", n)
	}

	if n := countRows(t, service, "SELECT count(*) FROM pending_objects"); n != 6 {
		t.Errorf("pending object rows = %d, want 6", n)
	}
}

// Completing a push publishes one closure row per root.
func TestPush_CompleteCommitsEveryRoot(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	base := strings.Repeat("a", 32)
	rootA := strings.Repeat("b", 32)
	rootB := strings.Repeat("c", 32)

	resp := createPush(t, service, []string{rootA + ".narinfo", rootB + ".narinfo"},
		pkgObjects(base), pkgObjects(rootA, base), pkgObjects(rootB, base))
	completePush(t, service, resp, http.StatusNoContent)

	if n := countRows(t, service, "SELECT count(*) FROM closures WHERE key = ANY($1)",
		[]string{rootA + ".narinfo", rootB + ".narinfo"}); n != 2 {
		t.Errorf("closure rows = %d, want 2", n)
	}

	if n := countRows(t, service, "SELECT count(*) FROM objects"); n != 6 {
		t.Errorf("objects = %d, want 6", n)
	}

	if n := countRows(t, service, "SELECT count(*) FROM pending_objects"); n != 0 {
		t.Errorf("pending object rows = %d, want 0", n)
	}
}

// A key that was live when the push started is not offered for upload, but
// the push still holds a pending row for it. A GC that runs before the commit,
// even one that ages out the only closure reaching the key and sweeps with
// force, must leave it alone, and the commit then succeeds.
func TestPush_SkippedKeySurvivesGCBeforeCommit(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	base := strings.Repeat("a", 32)
	first := strings.Repeat("b", 32)
	second := strings.Repeat("c", 32)

	one := createPush(t, service, []string{first + ".narinfo"}, pkgObjects(base), pkgObjects(first, base))
	completePush(t, service, one, http.StatusNoContent)

	two := createPush(t, service, []string{second + ".narinfo"}, pkgObjects(base), pkgObjects(second, base))

	if len(two.PendingObjects) != 2 {
		t.Fatalf("second push pending objects = %d, want 2 (base is live)", len(two.PendingObjects))
	}

	time.Sleep(50 * time.Millisecond)

	// The first closure ages out; only the second push still reaches base.
	st := service.RunGCForTest(0, 24*time.Hour, true)
	if st.State != "succeeded" {
		t.Fatalf("GC failed: %s", st.Error)
	}

	for _, key := range []string{base + ".narinfo", narKeyFor(base)} {
		if !objectIsLive(t, service, key) {
			t.Errorf("%s was collected while a push held it", key)
		}
	}

	completePush(t, service, two, http.StatusNoContent)

	if n := countRows(t, service, "SELECT count(*) FROM closures WHERE key = $1", second+".narinfo"); n != 1 {
		t.Errorf("closure rows for the second push = %d, want 1", n)
	}
}

func TestPush_RejectsBadRequests(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	t.Cleanup(func() { service.Close() })

	base := strings.Repeat("a", 32)

	cases := map[string]map[string]any{
		"no roots":            {"roots": []string{}, "objects": pkgObjects(base)},
		"no objects":          {"roots": []string{base + ".narinfo"}, "objects": []any{}},
		"bad root":            {"roots": []string{base}, "objects": pkgObjects(base)},
		"root not in objects": {"roots": []string{strings.Repeat("b", 32) + ".narinfo"}, "objects": pkgObjects(base)},
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			raw, err := json.Marshal(body)
			ok(t, err)

			check := checkStatusCode(http.StatusBadRequest)
			testRequest(t, &TestRequest{
				method:        "POST",
				path:          "/api/pushes",
				body:          raw,
				handler:       service.CreatePushHandler,
				checkResponse: &check,
			})
		})
	}
}

// The client signs a push's narinfos through the push's own route.
func TestPush_SignsNarinfosOfItsPendingObjects(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	key, err := signing.ParseKey(testSigningSecret)
	ok(t, err)

	service.SigningKeys = []*signing.Key{key}

	hash := strings.Repeat("a", 32)
	resp := createPush(t, service, []string{hash + ".narinfo"}, pkgObjects(hash))

	body, err := json.Marshal(map[string]any{"narinfos": map[string]any{
		hash + ".narinfo": map[string]any{
			"store_path":  "/nix/store/" + hash + "-test",
			"url":         narKeyFor(hash),
			"compression": "zstd",
			"nar_hash":    "sha256:0000000000000000000000000000000000000000000000000000",
			"nar_size":    1000,
			"references":  []string{},
		},
	}})
	ok(t, err)

	rr := testRequest(t, &TestRequest{
		method:     "POST",
		path:       "/api/pushes/" + resp.ID + "/sign",
		body:       body,
		handler:    service.SignNarinfosHandler,
		pathValues: map[string]string{"id": resp.ID},
	})

	var signed struct {
		Signatures map[string][]string `json:"signatures"`
	}

	ok(t, json.Unmarshal(rr.Body.Bytes(), &signed))

	if len(signed.Signatures[hash+".narinfo"]) != 1 {
		t.Errorf("signatures = %v, want one for %s.narinfo", signed.Signatures, hash)
	}
}
