package signing_test

import (
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server/signing"
)

// Throwaway key; the fixture signatures were made by Nix with it.
const (
	realisationTestSecretKey = "niks3-test-1:hFH+zw6wjbdaPIxcL5+q4Dm3tOUDgTpjHOe/32BUiVvsD1g3wcIoU+zVFzJ43s5fR5jW91j0GcNfxVwLUwDj3Q==" //nolint:gosec // throwaway test key
	realisationTestPublicKey = "niks3-test-1:7A9YN8HCKFPs1RcyeN7OX0eY1vdY9BnDX8VcC1MA490="
)

type realisationFixture struct {
	name        string
	realisation signing.Realisation
	key         string
	fingerprint string
	nixSig      string
}

func realisationFixtures() []realisationFixture {
	return []realisationFixture{
		{
			// nix 2.35.3: `nix realisation info --json <drv>^out`
			name: "build trace (nix 2.35)",
			realisation: signing.Realisation{
				DrvPath:    "3ny5n3w5lva8f9saqkcjrdgpx5zvpgv7-ca-fixture.drv",
				OutputName: "out",
				OutPath:    "l7cppcmmzf8ikklbkwx0dsnxsqsndlr7-ca-fixture",
			},
			key:         "build-trace-v2/3ny5n3w5lva8f9saqkcjrdgpx5zvpgv7-ca-fixture.drv/out.doi",
			fingerprint: `{"key":{"drvPath":"3ny5n3w5lva8f9saqkcjrdgpx5zvpgv7-ca-fixture.drv","outputName":"out"},"value":{"outPath":"l7cppcmmzf8ikklbkwx0dsnxsqsndlr7-ca-fixture"}}`,
			nixSig:      "sp+I8xi2JdVFeLI2axOffkMMQtWFV9kUZV076O10UTMcQG2yBdgr6zpJ2oxNz3zDB5cTaV0GB35P/wGogvtsAw==",
		},
		{
			// nix 2.36pre20260912 (master)
			name: "build trace (nix master)",
			realisation: signing.Realisation{
				DrvPath:    "6gmrcmazav19h16qgqg0n96ka6d5r129-ca-fixture.drv",
				OutputName: "out",
				OutPath:    "0qjrj88xs3qjxqp83xjhll9d6avqji0k-ca-fixture",
			},
			key:         "build-trace-v2/6gmrcmazav19h16qgqg0n96ka6d5r129-ca-fixture.drv/out.doi",
			fingerprint: `{"key":{"drvPath":"6gmrcmazav19h16qgqg0n96ka6d5r129-ca-fixture.drv","outputName":"out"},"value":{"outPath":"0qjrj88xs3qjxqp83xjhll9d6avqji0k-ca-fixture"}}`,
			nixSig:      "0IOAcLHT808Iothv28rZZ89IiFUmAzBTVQsQWkS7HoUPUSpBPogfneq65rjv8/fmIqD2A4/TiTwrUyXKmUvQAQ==",
		},
		{
			// nix 2.34.9
			name: "realisation (nix 2.34)",
			realisation: signing.Realisation{
				ID:      "sha256:512885212ca5cbb73870c2f224efd06d13de313d2e7c15056b53521f788764a6!out",
				OutPath: "zvi3467pwnaq46bmjw4lxkfdd415xc0g-ca-fixture",
			},
			key:         "realisations/sha256:512885212ca5cbb73870c2f224efd06d13de313d2e7c15056b53521f788764a6!out.doi",
			fingerprint: `{"dependentRealisations":{},"id":"sha256:512885212ca5cbb73870c2f224efd06d13de313d2e7c15056b53521f788764a6!out","outPath":"zvi3467pwnaq46bmjw4lxkfdd415xc0g-ca-fixture"}`,
			nixSig:      "KreZVIap7uvMmo04LS66ZchZ85wNqkrbaIFzPkErnKu+LQNnSVnZvNw+H58fuv9rsnIYV0O2LAkjdj9C7ANwBw==",
		},
	}
}

func TestRealisationFingerprintMatchesNix(t *testing.T) {
	t.Parallel()

	_, encoded, _ := strings.Cut(realisationTestPublicKey, ":")
	publicKey, err := base64.StdEncoding.DecodeString(encoded)
	ok(t, err)

	key, err := signing.ParseKey(realisationTestSecretKey)
	ok(t, err)

	for _, tc := range realisationFixtures() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.realisation.Key(); got != tc.key {
				t.Errorf("Key() = %q, want %q", got, tc.key)
			}

			fingerprint, err := signing.GenerateRealisationFingerprint(&tc.realisation)
			ok(t, err)
			if string(fingerprint) != tc.fingerprint {
				t.Fatalf("fingerprint = %s, want %s", fingerprint, tc.fingerprint)
			}

			nixSig, err := base64.StdEncoding.DecodeString(tc.nixSig)
			ok(t, err)
			if !ed25519.Verify(publicKey, fingerprint, nixSig) {
				t.Fatal("Nix's signature does not verify against our fingerprint")
			}

			// Ed25519 is deterministic.
			signatures, err := key.SignFingerprints(t.Context(), map[string][]byte{tc.key: fingerprint})
			ok(t, err)
			if want := "niks3-test-1:" + tc.nixSig; signatures[tc.key] != want {
				t.Errorf("signature = %q, want %q", signatures[tc.key], want)
			}
		})
	}
}

func TestRealisationFingerprintRejectsInvalid(t *testing.T) {
	t.Parallel()

	for name, r := range map[string]signing.Realisation{
		"no out path":          {DrvPath: "x-a.drv", OutputName: "out"},
		"full out path":        {DrvPath: "x-a.drv", OutputName: "out", OutPath: "/nix/store/x-a"},
		"drv not a drv":        {DrvPath: "x-a", OutputName: "out", OutPath: "x-a"},
		"drv with directory":   {DrvPath: "/nix/store/x-a.drv", OutputName: "out", OutPath: "x-a"},
		"no output name":       {DrvPath: "x-a.drv", OutPath: "x-a"},
		"output name slash":    {DrvPath: "x-a.drv", OutputName: "../out", OutPath: "x-a"},
		"id and drv":           {ID: "sha256:00!out", DrvPath: "x-a.drv", OutputName: "out", OutPath: "x-a"},
		"id without output":    {ID: "sha256:00", OutPath: "x-a"},
		"build trace has deps": {DrvPath: "x-a.drv", OutputName: "out", OutPath: "x-a", DependentRealisations: map[string]string{"a": "b"}},
	} {
		if _, err := signing.GenerateRealisationFingerprint(&r); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func ok(tb testing.TB, err error) {
	tb.Helper()

	if err != nil {
		tb.Fatal(err)
	}
}
