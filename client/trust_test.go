package client_test

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"slices"
	"testing"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/client"
	"github.com/Mic92/niks3/server/signing"
)

type testKey struct {
	name string
	priv ed25519.PrivateKey
}

func newTestKey(name string, seed byte) testKey {
	return testKey{name: name, priv: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seed}, ed25519.SeedSize))}
}

func (k testKey) public() string {
	pub, _ := k.priv.Public().(ed25519.PublicKey)

	return k.name + ":" + base64.StdEncoding.EncodeToString(pub)
}

func (k testKey) sign(t *testing.T, info *client.PathInfo, as string) string {
	t.Helper()

	narHash, err := client.ConvertHashToNix32(info.NarHash.String())
	if err != nil {
		t.Fatal(err)
	}

	fingerprint, err := signing.GenerateFingerprint(&signing.NarInfo{
		StorePath: info.Path, NarHash: narHash, NarSize: info.NarSize, References: info.References,
	})
	if err != nil {
		t.Fatal(err)
	}

	return as + ":" + base64.StdEncoding.EncodeToString(ed25519.Sign(k.priv, fingerprint))
}

func testPathInfo(t *testing.T, name string) *client.PathInfo {
	t.Helper()

	infos, err := client.ParsePathInfoJSON([]byte(`{"/nix/store/` + name + `":{"narHash":"sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=","narSize":2,"references":[]}}`))
	if err != nil {
		t.Fatal(err)
	}

	return infos["/nix/store/"+name]
}

func TestUntrustedPaths(t *testing.T) {
	t.Parallel()

	server := newTestKey("server-test-1", 1)
	upstream := newTestKey("upstream-test-1", 2)
	other := newTestKey("other-test-1", 3)

	policy := &api.CacheConfig{PublicKeys: []string{server.public()}, UpstreamPublicKeys: []string{upstream.public()}}

	for _, tc := range []struct {
		name      string
		cfg       *api.CacheConfig
		prepare   func(info *client.PathInfo)
		untrusted bool
	}{
		{name: "server without policy accepts anything", cfg: &api.CacheConfig{PublicKeys: []string{server.public()}}},
		{name: "built locally", cfg: policy, prepare: func(info *client.PathInfo) { info.Ultimate = true }},
		{name: "signed by the server key", cfg: policy, prepare: func(info *client.PathInfo) {
			info.Signatures = []string{server.sign(t, info, server.name)}
		}},
		{name: "signed by an upstream key", cfg: policy, prepare: func(info *client.PathInfo) {
			info.Signatures = []string{other.sign(t, info, other.name), upstream.sign(t, info, upstream.name)}
		}},
		{name: "unsigned substitute", cfg: policy, untrusted: true},
		{name: "signed only by another key", cfg: policy, untrusted: true, prepare: func(info *client.PathInfo) {
			info.Signatures = []string{other.sign(t, info, other.name)}
		}},
		{name: "another key's signature under the server key name", cfg: policy, untrusted: true, prepare: func(info *client.PathInfo) {
			info.Signatures = []string{other.sign(t, info, server.name)}
		}},
		{name: "server signature over different contents", cfg: policy, untrusted: true, prepare: func(info *client.PathInfo) {
			info.Signatures = []string{server.sign(t, info, server.name)}
			info.NarSize++
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			info := testPathInfo(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-dependency")
			if tc.prepare != nil {
				tc.prepare(info)
			}

			got, err := client.UntrustedPaths(tc.cfg, map[string]*client.PathInfo{info.Path: info})
			if err != nil {
				t.Fatal(err)
			}

			var want []string
			if tc.untrusted {
				want = []string{info.Path}
			}

			if !slices.Equal(got, want) {
				t.Errorf("untrusted = %v, want %v", got, want)
			}
		})
	}
}

func TestUntrustedPathsContentAddressed(t *testing.T) {
	t.Parallel()

	infos, err := client.ParsePathInfoJSON([]byte(`{"/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-source":{"narHash":"sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=","narSize":2,"references":[],"ca":"fixed:r:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s"}}`))
	if err != nil {
		t.Fatal(err)
	}

	cfg := &api.CacheConfig{UpstreamPublicKeys: []string{newTestKey("upstream-test-1", 2).public()}}

	got, err := client.UntrustedPaths(cfg, infos)
	if err != nil || len(got) != 0 {
		t.Errorf("untrusted = %v, %v; want none", got, err)
	}
}

func TestParsePathInfoUltimate(t *testing.T) {
	t.Parallel()

	infos, err := client.ParsePathInfoJSON([]byte(`{"/nix/store/cccccccccccccccccccccccccccccccc-out":{"narHash":"sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=","narSize":2,"references":[],"ultimate":true}}`))
	if err != nil {
		t.Fatal(err)
	}

	if !infos["/nix/store/cccccccccccccccccccccccccccccccc-out"].Ultimate {
		t.Error("ultimate not parsed")
	}
}
