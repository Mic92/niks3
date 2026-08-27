package server

import (
	"regexp"
	"strings"
)

// Nix base32 alphabet: 0123456789abcdfghijklmnpqrsvwxyz
// (note: no 'e', 'o', 't', 'u' — differs from standard base32).
const nixBase32Alphabet = "0123456789abcdfghijklmnpqrsvwxyz"

// ObjectClass describes one kind of cache object. Proxy response headers
// come from here, never from writer-controlled S3 metadata.
type ObjectClass struct {
	// UploadType is what clients declare. Empty means server-owned, not writable.
	UploadType  string
	ContentType string
	// Zstd objects are stored with Content-Encoding: zstd.
	Zstd         bool
	Redirectable bool
	match        func(string) bool
}

func reMatch(expr string) func(string) bool {
	re := regexp.MustCompile(expr)

	return re.MatchString
}

func exact(s string) func(string) bool {
	return func(k string) bool { return k == s }
}

var objectClasses = []*ObjectClass{ //nolint:gochecknoglobals // lookup table
	{
		UploadType: "narinfo", ContentType: "text/x-nix-narinfo", Zstd: true,
		match: reMatch(`^[` + nixBase32Alphabet + `]{32}\.narinfo$`),
	},
	{
		UploadType: "nar", ContentType: "application/x-nix-nar", Redirectable: true,
		match: reMatch(`^nar/[` + nixBase32Alphabet + `]{52}\.nar(\.zst|\.xz|\.bz2)?$`),
	},
	{
		UploadType: "listing", ContentType: "application/json", Zstd: true,
		match: reMatch(`^[` + nixBase32Alphabet + `]{32}\.ls$`),
	},
	// name alphabet matches nix's nameRegexStr in src/libstore/path.cc
	{
		UploadType: "build_log", ContentType: "text/plain; charset=utf-8", Zstd: true,
		match: reMatch(`^log/[a-zA-Z0-9+._?=-]+\.drv$`),
	},
	{
		UploadType: "realisation", ContentType: "application/json", Zstd: true,
		match: reMatch(`^realisations/[a-z0-9]+:[a-zA-Z0-9+/=]+![a-zA-Z0-9+._?=-]+\.doi$`),
	},
	{ContentType: "text/x-nix-cache-info", match: exact("nix-cache-info")},
	{ContentType: "text/html; charset=utf-8", match: exact("index.html")},
	{ContentType: "text/plain; charset=utf-8", match: reMatch(`^pins/[a-zA-Z0-9._-]+$`)},
}

func hasTraversal(key string) bool {
	// Store-path names can legitimately contain ".." (e.g. hm_..zlogout.drv),
	// so only whole segments count.
	return key == "" || key == ".." || strings.HasPrefix(key, "/") || strings.HasPrefix(key, "../") ||
		strings.HasSuffix(key, "/..") || strings.Contains(key, "/../")
}

// ClassifyCacheKey returns the class of a readable cache object, or nil.
func ClassifyCacheKey(key string) *ObjectClass {
	if hasTraversal(key) {
		return nil
	}

	for _, c := range objectClasses {
		if c.match(key) {
			return c
		}
	}

	return nil
}

// IsValidCachePath reports whether key is served by the read proxy.
func IsValidCachePath(key string) bool {
	return ClassifyCacheKey(key) != nil
}

// IsValidUploadKey reports whether a client may upload key as objType.
func IsValidUploadKey(key, objType string) bool {
	c := ClassifyCacheKey(key)

	return c != nil && c.UploadType != "" && c.UploadType == objType
}
