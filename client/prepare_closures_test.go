package client_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/Mic92/niks3/client"
)

// Every per-path object must be referenced by the path's narinfo, otherwise
// server-side GC reaps it while the closure is still live.
func TestPrepareClosuresNarinfoReferencesAllSiblings(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	storeDir := filepath.Join(root, "store")
	drvName := "dddddddddddddddddddddddddddddddd-hello.drv"
	drvPath := filepath.Join(storeDir, drvName)
	outPath := filepath.Join(storeDir, "pppppppppppppppppppppppppppppppp-hello")

	logDir := filepath.Join(root, "var", "log", "nix", "drvs", drvName[:2])
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(logDir, drvName[2:]), []byte("log"), 0o600); err != nil {
		t.Fatal(err)
	}

	infos, err := client.ParsePathInfoJSON([]byte(`{"` + outPath + `":{
		"narHash":"sha256-FePFYIVMuycIqe+eptI3PwuHY2SracpXWhLJRi0k7JU=",
		"narSize":1,"references":[],"deriver":"` + drvPath + `"}}`))
	if err != nil {
		t.Fatal(err)
	}

	res, err := client.PrepareClosures(t.Context(), []string{outPath}, infos, nil)
	if err != nil {
		t.Fatal(err)
	}

	objs := res.Closures[0].Objects

	var narinfo client.ObjectWithRefs

	for _, o := range objs {
		if o.Type == client.ObjectTypeNarinfo {
			narinfo = o
		}
	}

	if _, ok := res.LogPathsByKey["log/"+drvName]; !ok {
		t.Fatalf("build log not discovered: %v", res.LogPathsByKey)
	}

	for _, o := range objs {
		if o.Type == client.ObjectTypeNarinfo {
			continue
		}

		if !slices.Contains(narinfo.Refs, o.Key) {
			t.Errorf("narinfo does not reference %s (%s)", o.Key, o.Type)
		}
	}
}
