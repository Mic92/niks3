package oidc

import (
	"errors"
	"fmt"
	"strings"
)

// validateBoundClaims checks that all bound claims match.
// All specified claims must match (AND logic).
// Each claim value must match at least one pattern (OR logic within a claim).
func validateBoundClaims(claims map[string]any, boundClaims map[string][]string) error {
	for claimName, allowedPatterns := range boundClaims {
		value, err := getClaim(claims, claimName)
		if err != nil {
			return fmt.Errorf("required claim %q not found", claimName)
		}

		// Normalize value to string slice (some claims can be string or []string)
		values := normalizeToStringSlice(value)
		if len(values) == 0 {
			return fmt.Errorf("claim %q has no value", claimName)
		}

		// Check if ANY value matches ANY pattern
		matched := false

		for _, v := range values {
			for _, pattern := range allowedPatterns {
				if matchGlob(pattern, v) {
					matched = true

					break
				}
			}

			if matched {
				break
			}
		}

		if !matched {
			return fmt.Errorf("claim %q value %v not in allowed patterns %v", claimName, values, allowedPatterns)
		}
	}

	return nil
}

// validateBoundSubject checks that the subject matches one of the bound patterns.
func validateBoundSubject(claims map[string]any, boundSubject []string) error {
	if len(boundSubject) == 0 {
		return nil
	}

	sub, ok := claims["sub"].(string)
	if !ok || sub == "" {
		return errors.New("token missing 'sub' claim")
	}

	for _, pattern := range boundSubject {
		if matchGlob(pattern, sub) {
			return nil
		}
	}

	return fmt.Errorf("subject %q not in allowed patterns %v", sub, boundSubject)
}

// getClaim retrieves a claim from the claims map.
// Supports dot notation for nested claims (e.g., "github.repository").
func getClaim(claims map[string]any, name string) (any, error) {
	// Check for direct key first
	if val, ok := claims[name]; ok {
		return val, nil
	}

	// Try dot notation for nested claims
	parts := strings.Split(name, ".")
	if len(parts) == 1 {
		return nil, errors.New("claim not found")
	}

	var current any = claims
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, errors.New("claim not found")
		}

		current, ok = m[part]
		if !ok {
			return nil, errors.New("claim not found")
		}
	}

	return current, nil
}

// normalizeToStringSlice converts a claim value to a string slice.
func normalizeToStringSlice(value any) []string {
	switch v := value.(type) {
	case string:
		return []string{v}
	case []any:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}

		return result
	case []string:
		return v
	default:
		// Try to convert to string representation
		return []string{fmt.Sprintf("%v", v)}
	}
}

// matchGlob performs glob pattern matching.
// Supports * (matches any sequence of characters) and ? (matches any single character).
func matchGlob(pattern, value string) bool {
	return globMatch(pattern, value)
}

// globMatch implements glob matching with * and ? wildcards.
func globMatch(pattern, str string) bool {
	// Empty pattern only matches empty string
	if pattern == "" {
		return str == ""
	}

	// If pattern is just *, it matches everything
	if pattern == "*" {
		return true
	}

	pIdx := 0
	sIdx := 0
	starIdx := -1
	matchIdx := 0

	for sIdx < len(str) {
		switch {
		case pIdx < len(pattern) && (pattern[pIdx] == '?' || pattern[pIdx] == str[sIdx]):
			// Characters match or pattern has ?
			pIdx++
			sIdx++
		case pIdx < len(pattern) && pattern[pIdx] == '*':
			// Star found, record position
			starIdx = pIdx
			matchIdx = sIdx
			pIdx++
		case starIdx != -1:
			// No match, but we have a star to fall back to
			pIdx = starIdx + 1
			matchIdx++
			sIdx = matchIdx
		default:
			// No match
			return false
		}
	}

	// Check for remaining characters in pattern (should all be *)
	for pIdx < len(pattern) && pattern[pIdx] == '*' {
		pIdx++
	}

	return pIdx == len(pattern)
}
