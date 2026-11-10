package client

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/Mic92/niks3/api"
)

// untrustedPaths returns the paths the server's signing policy refuses: when
// it advertises upstream keys, every path must be built locally, be
// content-addressed, or carry a valid signature from a server or upstream key.
// Otherwise the server would re-sign paths substituted under a key it does
// not trust.
func untrustedPaths(cfg *api.CacheConfig, pathInfos map[string]*PathInfo) ([]string, error) {
	if len(cfg.UpstreamPublicKeys) == 0 {
		return nil, nil
	}

	keys, err := parsePublicKeys(slices.Concat(cfg.PublicKeys, cfg.UpstreamPublicKeys))
	if err != nil {
		return nil, fmt.Errorf("parsing server trust policy: %w", err)
	}

	var untrusted []string

	for path, info := range pathInfos {
		if info.Ultimate || (info.CA != nil && info.CA.String() != "") {
			continue
		}

		fingerprint, err := narinfoFingerprint(path, info)
		if err != nil {
			return nil, err
		}

		if !signedByAny(keys, fingerprint, info.Signatures) {
			untrusted = append(untrusted, path)
		}
	}

	slices.Sort(untrusted)

	return untrusted, nil
}

func parsePublicKeys(keys []string) (map[string]ed25519.PublicKey, error) {
	parsed := make(map[string]ed25519.PublicKey, len(keys))

	for _, key := range keys {
		name, encoded, ok := strings.Cut(key, ":")
		if !ok || name == "" {
			return nil, fmt.Errorf("public key %q is not name:base64", key)
		}

		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil || len(raw) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("public key %s is not a base64 Ed25519 key", name)
		}

		parsed[name] = raw
	}

	return parsed, nil
}

// narinfoFingerprint is the message Nix signs: 1;path;narHash;narSize;refs.
func narinfoFingerprint(path string, info *PathInfo) ([]byte, error) {
	narHash, err := ConvertHashToNix32(info.NarHash.String())
	if err != nil {
		return nil, fmt.Errorf("converting NAR hash of %s: %w", path, err)
	}

	refs := slices.Sorted(slices.Values(info.References))

	return []byte("1;" + path + ";" + narHash + ";" + strconv.FormatUint(info.NarSize, 10) + ";" + strings.Join(refs, ",")), nil
}

func signedByAny(keys map[string]ed25519.PublicKey, fingerprint []byte, signatures []string) bool {
	for _, signature := range signatures {
		name, encoded, _ := strings.Cut(signature, ":")

		key, ok := keys[name]
		if !ok {
			continue
		}

		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err == nil && ed25519.Verify(key, fingerprint, raw) {
			return true
		}
	}

	return false
}

// UntrustedPathsError reports closure paths the server's signing policy refuses.
type UntrustedPathsError struct {
	Paths []string
}

func (e *UntrustedPathsError) Error() string {
	return fmt.Sprintf("refusing to push %d path(s) neither built locally, content-addressed, nor signed by a key the server trusts: %s",
		len(e.Paths), strings.Join(e.Paths, ", "))
}
