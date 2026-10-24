package cmdutil_test

import (
	"path/filepath"
	"testing"

	"github.com/Mic92/niks3/client/oidclogin"
	"github.com/Mic92/niks3/cmdutil"
)

func TestAuthConfigDirectories(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, xdg := range []string{filepath.Join(home, "override"), ""} {
		t.Setenv("XDG_CONFIG_HOME", xdg)
		base := xdg
		if base == "" {
			base = filepath.Join(home, ".config")
		}
		manager, err := oidclogin.DefaultManager()
		if err != nil {
			t.Fatal(err)
		}
		if manager.Dir != filepath.Join(base, "niks3", "oidc") {
			t.Fatalf("unexpected OIDC directory: %s", manager.Dir)
		}
		if cmdutil.DefaultAuthTokenPath() != filepath.Join(base, "niks3", "auth-token") {
			t.Fatal("token files and OIDC profiles use different config directories")
		}
	}
}
