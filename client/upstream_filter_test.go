package client_test

import (
	"slices"
	"testing"

	"github.com/Mic92/niks3/client"
)

func TestFilterUpstreamClosure(t *testing.T) {
	t.Parallel()

	const store = "/nix/store/"

	root := store + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-root"
	upstream := store + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-upstream"
	upstreamDependency := store + "cccccccccccccccccccccccccccccccc-upstream-dependency"
	localDependency := store + "dddddddddddddddddddddddddddddddd-local-dependency"

	pathInfos := map[string]*client.PathInfo{
		root: {
			Path:       root,
			NarSize:    100,
			References: []string{upstream, localDependency},
		},
		upstream: {
			Path:       upstream,
			NarSize:    200,
			References: []string{upstreamDependency},
			Signatures: []string{"cache.nixos.org-1:signature"},
		},
		upstreamDependency: {
			Path:    upstreamDependency,
			NarSize: 300,
		},
		localDependency: {
			Path:    localDependency,
			NarSize: 400,
		},
	}

	t.Run("empty configuration disables filtering", func(t *testing.T) {
		t.Parallel()

		roots, infos, filtered := client.FilterUpstreamClosure([]string{root}, pathInfos, nil)
		if !slices.Equal(roots, []string{root}) || len(infos) != len(pathInfos) {
			t.Fatalf("roots = %v, infos = %d, want unchanged closure", roots, len(infos))
		}

		if filtered.Paths != 0 || filtered.NarBytes != 0 {
			t.Errorf("filtered = %+v, want none", filtered)
		}
	})

	t.Run("signed path cuts its dependency subtree", func(t *testing.T) {
		t.Parallel()

		roots, infos, filtered := client.FilterUpstreamClosure(
			[]string{root},
			pathInfos,
			[]string{"cache.nixos.org-1"},
		)
		if !slices.Equal(roots, []string{root}) {
			t.Fatalf("roots = %v, want only %s", roots, root)
		}

		if len(infos) != 2 || infos[root] == nil || infos[localDependency] == nil {
			t.Errorf("infos = %v, want root and local dependency", infos)
		}

		if filtered.Paths != 2 || filtered.NarBytes != 500 {
			t.Errorf("filtered = %+v, want 2 paths / 500 bytes", filtered)
		}
	})

	t.Run("separate root keeps a path below an upstream boundary", func(t *testing.T) {
		t.Parallel()

		roots, infos, filtered := client.FilterUpstreamClosure(
			[]string{root, upstreamDependency},
			pathInfos,
			[]string{"cache.nixos.org-1"},
		)
		if !slices.Equal(roots, []string{root, upstreamDependency}) {
			t.Fatalf("roots = %v, want both local roots", roots)
		}

		if len(infos) != 3 || infos[upstream] != nil {
			t.Errorf("infos = %v, want every path except signed upstream", infos)
		}

		if filtered.Paths != 1 || filtered.NarBytes != 200 {
			t.Errorf("filtered = %+v, want 1 path / 200 bytes", filtered)
		}
	})

	t.Run("signed root removes its whole closure", func(t *testing.T) {
		t.Parallel()

		roots, infos, filtered := client.FilterUpstreamClosure(
			[]string{upstream},
			map[string]*client.PathInfo{
				upstream:           pathInfos[upstream],
				upstreamDependency: pathInfos[upstreamDependency],
			},
			[]string{"cache.nixos.org-1"},
		)
		if len(roots) != 0 || len(infos) != 0 {
			t.Fatalf("roots = %v, infos = %v, want empty closure", roots, infos)
		}

		if filtered.Paths != 2 || filtered.NarBytes != 500 {
			t.Errorf("filtered = %+v, want 2 paths / 500 bytes", filtered)
		}
	})

	t.Run("key names match exactly", func(t *testing.T) {
		t.Parallel()

		roots, infos, filtered := client.FilterUpstreamClosure(
			[]string{upstream},
			map[string]*client.PathInfo{
				upstream: {
					Path:       upstream,
					Signatures: []string{"cache.nixos.org-10:signature", "malformed"},
				},
			},
			[]string{"cache.nixos.org-1"},
		)
		if !slices.Equal(roots, []string{upstream}) || len(infos) != 1 {
			t.Fatalf("roots = %v, infos = %v, want unchanged closure", roots, infos)
		}

		if filtered.Paths != 0 {
			t.Errorf("filtered = %+v, want none", filtered)
		}
	})

	t.Run("any configured key can match", func(t *testing.T) {
		t.Parallel()

		roots, infos, filtered := client.FilterUpstreamClosure(
			[]string{upstream},
			map[string]*client.PathInfo{
				upstream: {
					Path:       upstream,
					NarSize:    200,
					Signatures: []string{"example.org-1:signature"},
				},
			},
			[]string{"cache.nixos.org-1", "example.org-1"},
		)
		if len(roots) != 0 || len(infos) != 0 {
			t.Fatalf("roots = %v, infos = %v, want filtered closure", roots, infos)
		}

		if filtered.Paths != 1 || filtered.NarBytes != 200 {
			t.Errorf("filtered = %+v, want 1 path / 200 bytes", filtered)
		}
	})
}
