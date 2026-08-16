package cmdutil_test

import (
	"flag"
	"slices"
	"testing"

	"github.com/Mic92/niks3/cmdutil"
)

func TestAddUpstreamCacheKeyNameFlag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want []string
	}{
		{name: "omitted", args: nil, want: nil},
		{
			name: "one key",
			args: []string{"--upstream-cache-key-name", "cache.nixos.org-1"},
			want: []string{"cache.nixos.org-1"},
		},
		{
			name: "repeated",
			args: []string{
				"--upstream-cache-key-name", "cache.nixos.org-1",
				"--upstream-cache-key-name", "example.org-1",
			},
			want: []string{"cache.nixos.org-1", "example.org-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			got := cmdutil.AddUpstreamCacheKeyNameFlag(fs)

			if err := fs.Parse(tt.args); err != nil {
				t.Fatalf("Parse(%v): %v", tt.args, err)
			}

			if !slices.Equal(*got, tt.want) {
				t.Errorf("key names = %v, want %v", *got, tt.want)
			}
		})
	}
}
