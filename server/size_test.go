package server_test

import (
	"testing"

	"github.com/Mic92/niks3/server"
)

func TestParseSize(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want uint64
		err  bool
	}{
		{"", 0, false},
		{"0", 0, false},
		{"1024", 1024, false},
		{"100K", 100 << 10, false},
		{"512M", 512 << 20, false},
		{"512MiB", 512 << 20, false},
		{"2G", 2 << 30, false},
		{"2g", 2 << 30, false},
		{"1.5G", 1610612736, false},
		{"1T", 1 << 40, false},
		{"abc", 0, true},
		{"-1G", 0, true},
	}

	for _, tc := range cases {
		got, err := server.ParseSize(tc.in)
		if tc.err {
			if err == nil {
				t.Errorf("ParseSize(%q): expected error, got %d", tc.in, got)
			}

			continue
		}

		if err != nil {
			t.Errorf("ParseSize(%q): unexpected error: %v", tc.in, err)

			continue
		}

		if got != tc.want {
			t.Errorf("ParseSize(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
