package client_test

import (
	"strings"
	"testing"

	"github.com/Mic92/niks3/client"
)

// Narinfos name references and the deriver by base name, whatever the
// store directory: Nix rejects absolute paths there.
func TestGenerateNarinfoContentBaseNames(t *testing.T) {
	t.Parallel()

	for _, storeDir := range []string{"/nix/store", "/tmp/test/store"} {
		deriver := storeDir + "/3d4ls1qwmdj2ixrfz0k9bwxi8hq5nnw0-hello.drv"
		meta := &client.NarinfoMetadata{
			StorePath:   storeDir + "/0b65fpa7amsf86ma3c5y40zgjx1843ia-hello",
			URL:         "nar/abc.nar.zst",
			Compression: "zstd",
			NarHash:     "sha256:abc",
			NarSize:     1,
			References: []string{
				storeDir + "/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-glibc",
				storeDir + "/0b65fpa7amsf86ma3c5y40zgjx1843ia-hello",
			},
			Deriver: &deriver,
		}

		content := client.GenerateNarinfoContent(meta, nil)

		for _, want := range []string{
			"References: 0b65fpa7amsf86ma3c5y40zgjx1843ia-hello zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-glibc\n",
			"Deriver: 3d4ls1qwmdj2ixrfz0k9bwxi8hq5nnw0-hello.drv\n",
		} {
			if !strings.Contains(content, want) {
				t.Errorf("store %s: narinfo lacks %q:\n%s", storeDir, want, content)
			}
		}
	}
}
