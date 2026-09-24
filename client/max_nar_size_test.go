package client_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/client"
)

func TestFilterOversizedClosures(t *testing.T) {
	t.Parallel()

	const store = "/nix/store/"

	small := store + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-small"
	image := store + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-vm-image"
	wrapper := store + "cccccccccccccccccccccccccccccccc-wrapper"

	pathInfos := map[string]*client.PathInfo{
		small:   {Path: small, NarSize: 1000},
		image:   {Path: image, NarSize: 5000},
		wrapper: {Path: wrapper, NarSize: 100, References: []string{image, small}},
	}

	t.Run("no limit keeps everything", func(t *testing.T) {
		t.Parallel()

		kept, infos, skipped := client.FilterOversizedClosures([]string{wrapper, small}, pathInfos, 0)
		if skipped.Paths != 0 || skipped.NarBytes != 0 {
			t.Errorf("skipped = %+v, want none", skipped)
		}

		if len(kept) != 2 || len(infos) != 3 {
			t.Errorf("kept = %v, infos = %d, want all", kept, len(infos))
		}
	})

	t.Run("closure with oversized dependency is skipped", func(t *testing.T) {
		t.Parallel()

		kept, infos, skipped := client.FilterOversizedClosures([]string{wrapper, small}, pathInfos, 2000)
		if skipped.Paths != 2 || skipped.NarBytes != 5100 {
			t.Errorf("skipped = %+v, want 2 paths / 5100 bytes", skipped)
		}

		if !slices.Equal(kept, []string{small}) {
			t.Errorf("kept = %v, want only %s", kept, small)
		}

		if len(infos) != 1 || infos[small] == nil {
			t.Errorf("pruned infos = %v, want only %s", infos, small)
		}
	})

	t.Run("all closures skipped", func(t *testing.T) {
		t.Parallel()

		kept, infos, skipped := client.FilterOversizedClosures([]string{wrapper}, pathInfos, 50)
		if skipped.Paths != 3 || skipped.NarBytes != 6100 {
			t.Errorf("skipped = %+v, want 3 paths / 6100 bytes", skipped)
		}

		if len(kept) != 0 || len(infos) != 0 {
			t.Errorf("kept = %v, infos = %v, want none", kept, infos)
		}
	})
}

// isolatedNixEnv returns an environment for nix commands that use a store of
// their own under a temporary directory.
func isolatedNixEnv(t *testing.T) []string {
	t.Helper()

	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	for _, dir := range []string{"store", "state/nix/profiles", "var/log/nix/drvs", "etc", "cache"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	env := make([]string, 0, len(os.Environ())+9)

	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "NIX_") && !strings.HasPrefix(e, "_NIX_") && !strings.HasPrefix(e, "XDG_CACHE_HOME=") {
			env = append(env, e)
		}
	}

	return append(env,
		"NIX_STORE_DIR="+root+"/store",
		"NIX_STATE_DIR="+root+"/state",
		"NIX_LOG_DIR="+root+"/var/log/nix",
		"NIX_CONF_DIR="+root+"/etc",
		"XDG_CACHE_HOME="+root+"/cache",
		"NIX_REMOTE=",
		"_NIX_TEST_NO_SANDBOX=1",
		"NIX_CONFIG=substituters =\nsandbox = false",
	)
}

// What PushPaths reports is what the hook daemon removes from its queue. A
// dependency of a closure skipped for size fits the limit and may be queued
// on its own, so it must not be reported: only the closures that went up and
// the paths whose own closure can never go up.
func TestPushPathsReportsOnlyUnuploadablePathsOfSkippedClosure(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("nix-instantiate"); err != nil {
		t.Skip("nix-instantiate not available")
	}

	nixEnv := isolatedNixEnv(t)

	// wrapper references small, which fits the limit, and big, which does
	// not. Neither has a deriver, so no build is needed.
	expr := `let
	  small = builtins.toFile "small" "small";
	  big = builtins.toFile "big" (builtins.concatStringsSep "" (builtins.genList (_: "x") 8192));
	in { inherit small big; wrapper = builtins.toFile "wrapper" "${small} ${big}"; }`

	cmd := exec.CommandContext(t.Context(), "nix-instantiate", "--eval", "--strict", "--json", "--read-write-mode", "--expr", expr)
	cmd.Env = nixEnv

	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("creating store paths: %v", err)
	}

	var paths struct {
		Small   string `json:"small"`
		Big     string `json:"big"`
		Wrapper string `json:"wrapper"`
	}
	if err := json.Unmarshal(out, &paths); err != nil {
		t.Fatalf("parsing %s: %v", out, err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/cache-config":
			_ = json.NewEncoder(w).Encode(api.CacheConfig{MaxNarSize: 4096})
		case "/api/objects/present":
			_ = json.NewEncoder(w).Encode(api.PresentResponse{})
		case "/api/uploads/skipped":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(ts.Close)

	c, err := client.NewTestClientForServer(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	c.NixEnv = nixEnv

	handled, err := c.PushPaths(t.Context(), []string{paths.Wrapper})
	if err != nil {
		t.Fatalf("PushPaths: %v", err)
	}

	slices.Sort(handled)

	want := []string{paths.Big, paths.Wrapper}
	slices.Sort(want)

	if !slices.Equal(handled, want) {
		t.Errorf("PushPaths reported %v, want %v (not the dependency %s that fits)", handled, want, paths.Small)
	}
}
