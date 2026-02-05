package oidc

// GlobMatchForTesting exports globMatch for testing purposes.
func GlobMatchForTesting(pattern, str string) bool {
	return globMatch(pattern, str)
}
