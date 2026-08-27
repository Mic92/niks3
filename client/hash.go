package client

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

var hashSizes = map[string]int{ //nolint:gochecknoglobals // lookup table
	"md5": 16, "sha1": 20, "sha256": 32, "sha512": 64, "blake3": 32,
}

// Hash is a Nix hash decoded to raw bytes from any of Nix's encodings.
type Hash struct {
	Algorithm string
	Digest    []byte
}

// ParseHash parses "<algo>:<digest>" or "<algo>-<digest>", detecting the
// digest encoding by length like libutil/hash.cc.
func ParseHash(s string) (Hash, error) {
	i := strings.IndexAny(s, ":-")
	if i < 0 {
		return Hash{}, fmt.Errorf("hash %q lacks an algorithm prefix", s)
	}

	format := "" // detect
	if s[i] == '-' {
		format = "base64"
	}

	return decodeDigest(s[:i], format, s[i+1:])
}

func decodeDigest(algo, format, digest string) (Hash, error) {
	size, ok := hashSizes[algo]
	if !ok {
		return Hash{}, fmt.Errorf("unknown hash algorithm %q", algo)
	}

	if format == "" {
		switch len(digest) {
		case hex.EncodedLen(size):
			format = "base16"
		case nixBase32Len(size):
			format = "nix32"
		case base64.StdEncoding.EncodedLen(size):
			format = "base64"
		default:
			return Hash{}, fmt.Errorf("hash %q has wrong length for %s", digest, algo)
		}
	}

	var (
		raw []byte
		err error
	)

	switch format {
	case "base16", "hex":
		raw, err = hex.DecodeString(digest)
	case "nix32", "base32":
		raw, err = DecodeNixBase32(digest)
	case "base64", "sri":
		raw, err = base64.StdEncoding.DecodeString(digest)
	default:
		return Hash{}, fmt.Errorf("unknown hash format %q", format)
	}

	if err != nil {
		return Hash{}, fmt.Errorf("decoding %s %s digest: %w", format, algo, err)
	}

	if len(raw) != size {
		return Hash{}, fmt.Errorf("%s digest has %d bytes, want %d", algo, len(raw), size)
	}

	return Hash{Algorithm: algo, Digest: raw}, nil
}

// UnmarshalJSON accepts the string form and the Nix 2.33+ structured form.
func (h *Hash) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		parsed, err := ParseHash(s)
		if err != nil {
			return err
		}

		*h = parsed

		return nil
	}

	var obj struct {
		Algorithm string `json:"algorithm"`
		Format    string `json:"format"`
		Hash      string `json:"hash"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("hash must be either a string or structured object: %w", err)
	}

	parsed, err := decodeDigest(obj.Algorithm, obj.Format, obj.Hash)
	if err != nil {
		return err
	}

	*h = parsed

	return nil
}

// Nix32 returns "<algo>:<nix32>", the form used in narinfo files.
func (h *Hash) Nix32() string { return h.Algorithm + ":" + EncodeNixBase32(h.Digest) }

func (h *Hash) BareNix32() string { return EncodeNixBase32(h.Digest) }

// SRI returns "<algo>-<base64>".
func (h *Hash) SRI() string {
	return h.Algorithm + "-" + base64.StdEncoding.EncodeToString(h.Digest)
}

func (h *Hash) String() string { return h.SRI() }
