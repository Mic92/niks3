package configdir_test

import (
	"path/filepath"
	"testing"

	"github.com/Mic92/niks3/client/configdir"
)

func TestDir(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, tc := range []struct {
		name string
		xdg  string
		want string
	}{
		{name: "override", xdg: filepath.Join(home, "custom config"), want: filepath.Join(home, "custom config", "niks3")},
		{name: "fallback", want: filepath.Join(home, ".config", "niks3")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("XDG_CONFIG_HOME", tc.xdg)
			got, err := configdir.Dir()
			if err != nil || got != tc.want {
				t.Fatalf("Dir() = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}
