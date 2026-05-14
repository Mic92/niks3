package server

import "strings"

// IsValidUploadKey reports whether a client may request a presigned upload
// for the given object key and declared type.
//
// This is the write-side counterpart of IsValidCachePath. It is stricter:
// the key must match the exact pattern for its declared type, and
// server-owned files (nix-cache-info, index.html) are never client-writable.
// Without this check an authenticated client could obtain presigned PUT URLs
// for arbitrary S3 keys — overwriting nix-cache-info, hosting attacker HTML
// under the cache origin, or poisoning unrelated objects.
func IsValidUploadKey(key, objType string) bool {
	if key == "" {
		return false
	}

	if strings.HasPrefix(key, "/") || strings.HasPrefix(key, "../") ||
		strings.HasSuffix(key, "/..") || strings.Contains(key, "/../") {
		return false
	}

	switch objType {
	case "narinfo":
		return narinfoRe.MatchString(key)
	case "nar":
		return narRe.MatchString(key)
	case "listing":
		return lsRe.MatchString(key)
	case "build_log":
		return logRe.MatchString(key)
	case "realisation":
		return realisationsRe.MatchString(key)
	default:
		return false
	}
}
