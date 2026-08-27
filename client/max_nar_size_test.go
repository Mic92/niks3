package client_test

import (
	"slices"
	"testing"

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

		kept, infos, skipped := client.FilterOversizedClosures([]string{wrapper, small}, pathInfos, 0, false)
		if skipped.Paths != 0 || skipped.NarBytes != 0 {
			t.Errorf("skipped = %+v, want none", skipped)
		}

		if len(kept) != 2 || len(infos) != 3 {
			t.Errorf("kept = %v, infos = %d, want all", kept, len(infos))
		}
	})

	t.Run("closure with oversized dependency is skipped", func(t *testing.T) {
		t.Parallel()

		kept, infos, skipped := client.FilterOversizedClosures([]string{wrapper, small}, pathInfos, 2000, false)
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

		kept, infos, skipped := client.FilterOversizedClosures([]string{wrapper}, pathInfos, 50, false)
		if skipped.Paths != 3 || skipped.NarBytes != 6100 {
			t.Errorf("skipped = %+v, want 3 paths / 6100 bytes", skipped)
		}

		if len(kept) != 0 || len(infos) != 0 {
			t.Errorf("kept = %v, infos = %v, want none", kept, infos)
		}
	})

	t.Run("no-closure judges each path on its own size", func(t *testing.T) {
		t.Parallel()

		// wrapper references the oversized image but is uploaded on its own,
		// so it must not inherit the dependency's size.
		roots := []string{wrapper, small, image}

		kept, infos, skipped := client.FilterOversizedClosures(roots, pathInfos, 2000, true)
		if !slices.Equal(kept, []string{wrapper, small}) {
			t.Errorf("kept = %v, want %v", kept, []string{wrapper, small})
		}

		if skipped.Paths != 1 || skipped.NarBytes != 5000 {
			t.Errorf("skipped = %+v, want 1 path / 5000 bytes", skipped)
		}

		if len(infos) != 2 || infos[wrapper] == nil || infos[small] == nil {
			t.Errorf("pruned infos = %v, want %s and %s", infos, wrapper, small)
		}
	})
}
