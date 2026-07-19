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

		kept, infos := client.FilterOversizedClosures([]string{wrapper, small}, pathInfos, 0)
		if len(kept) != 2 || len(infos) != 3 {
			t.Errorf("kept = %v, infos = %d, want all", kept, len(infos))
		}
	})

	t.Run("closure with oversized dependency is skipped", func(t *testing.T) {
		t.Parallel()

		kept, infos := client.FilterOversizedClosures([]string{wrapper, small}, pathInfos, 2000)
		if !slices.Equal(kept, []string{small}) {
			t.Errorf("kept = %v, want only %s", kept, small)
		}

		if len(infos) != 1 || infos[small] == nil {
			t.Errorf("pruned infos = %v, want only %s", infos, small)
		}
	})

	t.Run("all closures skipped", func(t *testing.T) {
		t.Parallel()

		kept, infos := client.FilterOversizedClosures([]string{wrapper}, pathInfos, 50)
		if len(kept) != 0 || len(infos) != 0 {
			t.Errorf("kept = %v, infos = %v, want none", kept, infos)
		}
	})
}
