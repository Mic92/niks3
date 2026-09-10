package client_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Mic92/niks3/client"
)

func caseHackDir(t *testing.T, names ...string) string {
	t.Helper()

	saved := client.SetUseCaseHack(true)

	t.Cleanup(func() { client.SetUseCaseHack(saved) })

	// nix-store --dump refuses symlinked prefixes (macOS /var -> /private/var).
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range names {
		err := os.WriteFile(filepath.Join(root, name), []byte(name), 0o600)
		if err != nil {
			t.Fatal(err)
		}
	}

	return root
}

//nolint:paralleltest // flips the useCaseHack global
func TestDumpPathCaseHackMatchesNix(t *testing.T) {
	if _, err := exec.LookPath("nix-store"); err != nil {
		t.Skip("nix-store not available")
	}

	root := caseHackDir(t,
		"XT_CONNMARK.h",
		"xt_connmark.h~nix~case~hack~1",
		"Xt_connmark.h~nix~case~hack~2",
		"a!", // sorts between "a" and "a~"
		"a~nix~case~hack~12",
		"c~nix~case~hack~x~nix~case~hack~2",
	)

	var got bytes.Buffer

	_, err := client.DumpPathWithListing(&got, root)
	if err != nil {
		t.Fatal(err)
	}

	want, err := exec.CommandContext(t.Context(),
		"nix-store", "--option", "use-case-hack", "true", "--dump", root).Output()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("NAR differs from nix-store --dump: %d vs %d bytes", got.Len(), len(want))
	}
}

//nolint:paralleltest // flips the useCaseHack global
func TestDumpPathCaseHackCollision(t *testing.T) {
	root := caseHackDir(t, "same", "same~nix~case~hack~1")

	_, err := client.DumpPathWithListing(&bytes.Buffer{}, root)
	if err == nil {
		t.Fatal("expected file name collision")
	}
}
