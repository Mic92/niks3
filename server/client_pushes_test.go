package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/client"
)

type pushCalls struct {
	pushes  atomic.Int32
	closure atomic.Int32
}

// pushOverlappingPaths pushes a dependency and two paths that share it
// through a server that counts the create calls. announce controls whether
// the server advertises POST /api/pushes.
func pushOverlappingPaths(t *testing.T, announce bool) *pushCalls {
	t.Helper()

	testService := createTestServiceWithAuth(t, testAuthToken)
	t.Cleanup(func() { testService.Close() })

	ok(t, testService.InitializeBucket(t.Context()))

	mux := http.NewServeMux()
	registerTestHandlers(mux, testService)

	calls := &pushCalls{}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/pushes":
			calls.pushes.Add(1)

			if !announce {
				http.NotFound(w, r)

				return
			}
		case "/api/pending_closures":
			calls.closure.Add(1)
		case "/api/cache-config":
			if !announce {
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, r)

				var cfg api.CacheConfig

				ok(t, json.Unmarshal(rec.Body.Bytes(), &cfg))

				cfg.Pushes = false
				w.Header().Set("Content-Type", "application/json")
				ok(t, json.NewEncoder(w).Encode(cfg))

				return
			}
		}

		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)

	ctx := t.Context()
	nixEnv := setupIsolatedNixStore(t)

	dep := `builtins.toFile "shared-dep" "shared dependency"`
	depPath := nixEvalStorePath(t, nixEnv, dep)
	pathA := nixEvalStorePath(t, nixEnv, `builtins.toFile "a" "${`+dep+`}"`)
	pathB := nixEvalStorePath(t, nixEnv, `builtins.toFile "b" "${`+dep+`}"`)

	c, err := client.NewClient(ctx, ts.URL, testAuthToken)
	ok(t, err)

	c.NixEnv = nixEnv

	_, err = c.PushPaths(ctx, []string{pathA, pathB})
	ok(t, err)

	for _, p := range []string{depPath, pathA, pathB} {
		hash, err := client.GetStorePathHash(p)
		ok(t, err)
		verifyNarinfoInS3(ctx, t, testService, hash, p)
	}

	return calls
}

// One push for all roots, not one pending closure per path.
func TestClientPushesUseOnePush(t *testing.T) {
	t.Parallel()

	calls := pushOverlappingPaths(t, true)

	if n := calls.pushes.Load(); n != 1 {
		t.Errorf("POST /api/pushes calls = %d, want 1", n)
	}

	if n := calls.closure.Load(); n != 0 {
		t.Errorf("POST /api/pending_closures calls = %d, want 0", n)
	}
}

// A server that does not announce pushes gets the closure flow.
func TestClientFallsBackToClosures(t *testing.T) {
	t.Parallel()

	calls := pushOverlappingPaths(t, false)

	if n := calls.pushes.Load(); n != 0 {
		t.Errorf("POST /api/pushes calls = %d, want 0", n)
	}

	if n := calls.closure.Load(); n == 0 {
		t.Errorf("POST /api/pending_closures was not used")
	}
}
