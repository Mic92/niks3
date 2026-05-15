package client_test

import (
	"testing"

	"github.com/Mic92/niks3/client"
)

func TestPartSizeForNAR(t *testing.T) {
	t.Parallel()

	const (
		mib     = 1 << 20
		gib     = 1 << 30
		tib     = 1 << 40
		minPart = client.MultipartPartSize
	)

	tests := []struct {
		name    string
		narSize uint64
		want    int
	}{
		{"zero stays at minimum", 0, minPart},
		{"small stays at minimum", 1 * mib, minPart},
		// 80 GiB / 9000 ~= 9.1 MiB -> rounds up to 10 MiB, exactly the minimum.
		{"80 GiB fits at minimum", 80 * gib, minPart},
		// 115 GiB / 9000 ~= 13.1 MiB -> 16 MiB step. This is the size from issue #387.
		{"115 GiB needs larger parts", 115 * gib, 16 * mib},
		// 1 TiB / 9000 ~= 116.5 MiB -> 128 MiB
		{"1 TiB", 1 * tib, 128 * mib},
		// 5 TiB (S3 max object) / 9000 ~= 582.6 MiB -> 592 MiB
		{"5 TiB S3 max object", 5 * tib, 592 * mib},
		// Absurd: capped at S3 max part size
		{"capped at 5 GiB", 100 * tib, 5 * gib},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := client.PartSizeForNAR(tc.narSize)
			if got != tc.want {
				t.Errorf("PartSizeForNAR(%d) = %d, want %d", tc.narSize, got, tc.want)
			}

			if got < minPart {
				t.Errorf("PartSizeForNAR(%d) = %d, below minimum %d", tc.narSize, got, minPart)
			}

			if got > 5*gib {
				t.Errorf("PartSizeForNAR(%d) = %d, above S3 max part size", tc.narSize, got)
			}
		})
	}
}
