package server

import "strings"

// Object types a client may declare for an upload. They select the key
// pattern in IsValidUploadKey and the upload strategy in the pending
// closure handler.
const (
	objectTypeNarinfo     = "narinfo"
	objectTypeNar         = "nar"
	objectTypeListing     = "listing"
	objectTypeBuildLog    = "build_log"
	objectTypeRealisation = "realisation"
)

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
	case objectTypeNarinfo:
		return narinfoRe.MatchString(key)
	case objectTypeNar:
		return narRe.MatchString(key)
	case objectTypeListing:
		return lsRe.MatchString(key)
	case objectTypeBuildLog:
		return logRe.MatchString(key)
	case objectTypeRealisation:
		return realisationsRe.MatchString(key)
	default:
		return false
	}
}
