package client_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/Mic92/niks3/client"
)

// Store path hashes must be 32 chars; narHash values are real base64 SHA-256
// digests so getNARKey can convert them to Nix32.
const (
	pathA = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-app"
	pathB = "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-libfoo"
	pathC = "/nix/store/cccccccccccccccccccccccccccccccc-gcc"

	hashA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hashB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	hashC = "cccccccccccccccccccccccccccccccc"
)

// pathInfoFixture builds A -> B plus a standalone C, standing in for a
// build-only dependency that nothing in the runtime graph points at.
func pathInfoFixture(t *testing.T) map[string]*client.PathInfo {
	t.Helper()

	const jsonInput = `{
		"` + pathA + `": {
			"narHash": "sha256-ypeBEsobvcr6wjGzmiPcTaeG7/gUfE5yuYB3ha/uSLs=",
			"narSize": 100,
			"references": ["` + pathB + `"]
		},
		"` + pathB + `": {
			"narHash": "sha256-PiPoFgA5WUoziU9lZOGxNIu9egCI1CxKy3PurtWcAJ0=",
			"narSize": 200,
			"references": []
		},
		"` + pathC + `": {
			"narHash": "sha256-Ln0sA6lQeuJl7PW1NWiFpTOTogKdJBOUmXJloaJa78Y=",
			"narSize": 300,
			"references": []
		}
	}`

	pathInfos, err := client.ParsePathInfoJSON([]byte(jsonInput))
	if err != nil {
		t.Fatalf("parsing fixture path info: %v", err)
	}

	return pathInfos
}

// closureByKey indexes prepared closures by their root narinfo key.
func closureByKey(closures []client.ClosureInfo) map[string]client.ClosureInfo {
	byKey := make(map[string]client.ClosureInfo, len(closures))
	for _, c := range closures {
		byKey[c.NarinfoKey] = c
	}

	return byKey
}

// narinfoKeys returns the sorted narinfo object keys of a closure.
func narinfoKeys(closure client.ClosureInfo) []string {
	var keys []string

	for _, obj := range closure.Objects {
		if obj.Type == client.ObjectTypeNarinfo {
			keys = append(keys, obj.Key)
		}
	}

	slices.Sort(keys)

	return keys
}

func TestPrepareClosuresNoClosure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pathInfos := pathInfoFixture(t)
	roots := []string{pathA, pathB, pathC}

	t.Run("closure mode pulls in transitive references", func(t *testing.T) {
		t.Parallel()

		result, err := client.PrepareClosures(ctx, roots, pathInfos, nil, false)
		if err != nil {
			t.Fatalf("PrepareClosures: %v", err)
		}

		byKey := closureByKey(result.Closures)

		got := narinfoKeys(byKey[hashA+".narinfo"])
		want := []string{hashA + ".narinfo", hashB + ".narinfo"}

		if !slices.Equal(got, want) {
			t.Errorf("closure for A = %v, want %v", got, want)
		}
	})

	t.Run("no-closure keeps each path self-contained", func(t *testing.T) {
		t.Parallel()

		result, err := client.PrepareClosures(ctx, roots, pathInfos, nil, true)
		if err != nil {
			t.Fatalf("PrepareClosures: %v", err)
		}

		if len(result.Closures) != len(roots) {
			t.Fatalf("got %d closures, want %d", len(result.Closures), len(roots))
		}

		byKey := closureByKey(result.Closures)

		// A references B, but --no-closure must not pull B's objects in:
		// a reference-closed set would otherwise grow payloads quadratically.
		for _, tc := range []struct{ name, hash string }{
			{"A", hashA},
			{"B", hashB},
			{"C", hashC},
		} {
			closure, ok := byKey[tc.hash+".narinfo"]
			if !ok {
				t.Fatalf("no closure for %s", tc.name)
			}

			got := narinfoKeys(closure)
			want := []string{tc.hash + ".narinfo"}

			if !slices.Equal(got, want) {
				t.Errorf("closure for %s = %v, want %v", tc.name, got, want)
			}

			// narinfo, nar and .ls, and nothing belonging to another path.
			if len(closure.Objects) != 3 {
				t.Errorf("closure for %s has %d objects, want 3", tc.name, len(closure.Objects))
			}
		}
	})

	t.Run("no-closure narinfos still record their references", func(t *testing.T) {
		t.Parallel()

		// GC walks objects.refs from every gcroot, so A's narinfo must still
		// point at B even though B was uploaded as an independent closure.
		result, err := client.PrepareClosures(ctx, roots, pathInfos, nil, true)
		if err != nil {
			t.Fatalf("PrepareClosures: %v", err)
		}

		closure := closureByKey(result.Closures)[hashA+".narinfo"]

		var refs []string

		for _, obj := range closure.Objects {
			if obj.Key == hashA+".narinfo" {
				refs = obj.Refs
			}
		}

		if !slices.Contains(refs, hashB+".narinfo") {
			t.Errorf("A's narinfo refs = %v, want it to contain %s", refs, hashB+".narinfo")
		}
	})
}

func TestChunkStorePaths(t *testing.T) {
	t.Parallel()

	t.Run("keeps a small set in one chunk", func(t *testing.T) {
		t.Parallel()

		paths := []string{pathA, pathB, pathC}

		chunks := slices.Collect(client.ChunkStorePaths(paths, client.MaxPathInfoArgBytes))
		if len(chunks) != 1 {
			t.Fatalf("got %d chunks, want 1", len(chunks))
		}

		if !slices.Equal(chunks[0], paths) {
			t.Errorf("chunk = %v, want %v", chunks[0], paths)
		}
	})

	t.Run("splits on the byte budget and preserves every path", func(t *testing.T) {
		t.Parallel()

		// Budget fits two paths per chunk: each costs len(path)+1.
		budget := 2 * (len(pathA) + 1)
		paths := []string{pathA, pathA, pathA, pathA, pathA}

		chunks := slices.Collect(client.ChunkStorePaths(paths, budget))
		if len(chunks) != 3 {
			t.Fatalf("got %d chunks, want 3", len(chunks))
		}

		var total int

		for _, chunk := range chunks {
			if len(chunk) > 2 {
				t.Errorf("chunk of %d paths exceeds budget of 2", len(chunk))
			}

			total += len(chunk)
		}

		if total != len(paths) {
			t.Errorf("chunks cover %d paths, want %d", total, len(paths))
		}
	})

	t.Run("emits an oversized path rather than dropping it", func(t *testing.T) {
		t.Parallel()

		long := "/nix/store/" + strings.Repeat("x", 500)

		chunks := slices.Collect(client.ChunkStorePaths([]string{long}, 10))
		if len(chunks) != 1 || len(chunks[0]) != 1 || chunks[0][0] != long {
			t.Errorf("chunks = %v, want the single oversized path", chunks)
		}
	})

	t.Run("handles an empty input", func(t *testing.T) {
		t.Parallel()

		chunks := slices.Collect(client.ChunkStorePaths(nil, client.MaxPathInfoArgBytes))
		if len(chunks) != 0 {
			t.Errorf("got %d chunks, want none", len(chunks))
		}
	})
}
