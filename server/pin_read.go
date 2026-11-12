package server

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgtype"
)

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

// PinReadRule maps an mTLS subject DN glob to a pin name glob pattern.
type PinReadRule struct {
	Subject string
	Pin     string
}

var pinValidationRegex = regexp.MustCompile(`^[a-zA-Z0-9._*?-]*$`)

// ParsePinReadRule parses a rule of the form "subject=pin".
// Splits at the last "=". Errors if either side is empty or if the pin part,
// with {cn} removed, has characters other than [a-zA-Z0-9._*?-].
// A pin pattern may be only {cn} (it is empty after {cn} is removed, and that is valid).
func ParsePinReadRule(s string) (PinReadRule, error) {
	idx := strings.LastIndexByte(s, '=')
	if idx < 0 {
		return PinReadRule{}, fmt.Errorf("pin read rule missing '=': %s", s)
	}

	subject := s[:idx]
	pin := s[idx+1:]

	if subject == "" {
		return PinReadRule{}, errors.New("pin read rule subject is empty")
	}

	if pin == "" {
		return PinReadRule{}, errors.New("pin read rule pin is empty")
	}

	// Validate pin pattern (excluding {cn})
	pinWithoutCN := strings.ReplaceAll(pin, "{cn}", "")
	if !pinValidationRegex.MatchString(pinWithoutCN) {
		return PinReadRule{}, fmt.Errorf("pin read rule pin contains invalid characters: %s", pin)
	}

	return PinReadRule{Subject: subject, Pin: pin}, nil
}

// commonName extracts the first CN= attribute from a DN in "CN=x,O=y" form.
// Returns the CN value and true if found, or "", false otherwise.
// Works with both native mTLS (pkix.Name.String()) and nginx $ssl_client_s_dn (RFC 2253).
// Handles escaped characters (\X for any X) in DN values.
func commonName(dn string) (string, bool) {
	var parts []string
	var current strings.Builder

	// Split on unescaped commas only. When we see \, skip the next character.
	for i := 0; i < len(dn); i++ {
		switch {
		case dn[i] == '\\' && i+1 < len(dn):
			// Escaped character - include both backslash and next char
			current.WriteByte(dn[i])
			i++
			current.WriteByte(dn[i])
		case dn[i] == ',':
			// Unescaped comma - split here
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteByte(dn[i])
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if cn, ok := strings.CutPrefix(part, "CN="); ok {
			return cn, true
		}
	}

	return "", false
}

// parsePinReadRules parses a slice of "subject=pin" rule strings into PinReadRule structs.
// Returns an error if any rule is invalid.
func parsePinReadRules(raw []string) ([]PinReadRule, error) {
	rules := make([]PinReadRule, 0, len(raw))
	for _, rule := range raw {
		parsed, err := ParsePinReadRule(rule)
		if err != nil {
			return nil, fmt.Errorf("invalid --mtls-read-pin: %w", err)
		}
		rules = append(rules, parsed)
	}

	return rules, nil
}

// pinScopedMayRead checks if a principal with pin-scoped read access can read a key.
// It returns true if the key is reachable from one of the principal's readable pins.
// The principal's readPins are glob patterns that are converted to SQL LIKE patterns.
func (s *Service) pinScopedMayRead(ctx context.Context, p principal, key string) (bool, error) {
	if len(p.readPins) == 0 {
		return false, nil
	}

	// Convert glob patterns to SQL LIKE patterns
	likePatterns := make([]string, len(p.readPins))
	for i, pin := range p.readPins {
		likePatterns[i] = globToLike(pin)
	}

	grace := s.pinReadGraceInterval()

	queries := pg.New(s.Pool)
	can, err := queries.PinMayRead(ctx, pg.PinMayReadParams{
		Patterns: likePatterns,
		Key:      key,
		Grace:    grace,
	})
	if err != nil {
		return false, fmt.Errorf("pin read check failed: %w", err)
	}

	return can, nil
}

// pinReadGraceInterval returns a pgtype.Interval representing the pin read grace period.
func (s *Service) pinReadGraceInterval() pgtype.Interval {
	return pgtype.Interval{
		Valid:        true,
		Microseconds: s.PinReadGrace.Microseconds(),
	}
}
