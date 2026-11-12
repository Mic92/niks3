package server

import "strings"

// globToLike converts a glob pattern to a SQL LIKE pattern.
// Glob patterns use * and ? as wildcards; SQL LIKE uses % and _.
// Backslash, percent, and underscore are literal characters in glob patterns
// but special in SQL LIKE, so they are escaped with backslash.
// * → %, ? → _, and \, %, _ are all escaped with \.
func globToLike(pattern string) string {
	var result strings.Builder

	for _, ch := range []byte(pattern) {
		switch ch {
		case '*':
			result.WriteByte('%')
		case '?':
			result.WriteByte('_')
		case '%', '_', '\\':
			result.WriteByte('\\')
			result.WriteByte(ch)
		default:
			result.WriteByte(ch)
		}
	}

	return result.String()
}
