package client

import (
	"fmt"
	"slices"
	"strings"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server/signing"
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

	keys, err := signing.ParsePublicKeys(slices.Concat(cfg.PublicKeys, cfg.UpstreamPublicKeys))
	if err != nil {
		return nil, fmt.Errorf("parsing server trust policy: %w", err)
	}

	var untrusted []string

	for path, info := range pathInfos {
		if info.Ultimate || (info.CA != nil && info.CA.String() != "") {
			continue
		}

		narHash, err := ConvertHashToNix32(info.NarHash.String())
		if err != nil {
			return nil, fmt.Errorf("converting NAR hash of %s: %w", path, err)
		}

		ok, err := keys.Verifies(&signing.NarInfo{
			StorePath:  path,
			NarHash:    narHash,
			NarSize:    info.NarSize,
			References: info.References,
		}, info.Signatures)
		if err != nil {
			return nil, fmt.Errorf("verifying %s: %w", path, err)
		}

		if !ok {
			untrusted = append(untrusted, path)
		}
	}

	slices.Sort(untrusted)

	return untrusted, nil
}

// UntrustedPathsError reports closure paths the server's signing policy refuses.
type UntrustedPathsError struct {
	Paths []string
}

func (e *UntrustedPathsError) Error() string {
	return fmt.Sprintf("refusing to push %d path(s) neither built locally, content-addressed, nor signed by a key the server trusts: %s",
		len(e.Paths), strings.Join(e.Paths, ", "))
}
