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
		return PinReadRule{}, fmt.Errorf("want <subject-glob>=<pin-glob>, got %s", s)
	}

	subject := s[:idx]
	pin := s[idx+1:]

	if subject == "" {
		return PinReadRule{}, errors.New("the subject glob is empty")
	}

	if pin == "" {
		return PinReadRule{}, errors.New("the pin glob is empty")
	}

	// Validate pin pattern (excluding {cn})
	pinWithoutCN := strings.ReplaceAll(pin, "{cn}", "")
	if !pinValidationRegex.MatchString(pinWithoutCN) {
		return PinReadRule{}, fmt.Errorf("invalid character in pin glob %s", pin)
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

// pinReadRulesFlag collects repeated --mtls-read-pin flags.
type pinReadRulesFlag []PinReadRule

func (f *pinReadRulesFlag) String() string { return fmt.Sprint(*f) }

func (f *pinReadRulesFlag) Set(value string) error {
	rule, err := ParsePinReadRule(value)
	if err != nil {
		return err
	}

	*f = append(*f, rule)

	return nil
}

// pinScopedMayRead reports whether key is reachable from a pin the principal
// may read.
func (s *Service) pinScopedMayRead(ctx context.Context, p principal, key string) (bool, error) {
	patterns := make([]string, len(p.readPins))
	for i, pin := range p.readPins {
		patterns[i] = globToLike(pin)
	}

	targets, err := pg.New(s.Pool).GetReadablePinTargets(ctx, pg.GetReadablePinTargetsParams{
		Patterns: patterns,
		Grace:    pgtype.Interval{Valid: true, Microseconds: s.PinReadGrace.Microseconds()},
	})
	if err != nil {
		return false, fmt.Errorf("looking up pin targets: %w", err)
	}

	for _, target := range targets {
		reached, err := s.pinSets.reaches(ctx, target, key, pg.New(s.Pool).GetReachableKeys)
		if err != nil {
			return false, fmt.Errorf("walking the closure of %s: %w", target, err)
		}

		if reached {
			return true, nil
		}
	}

	return false, nil
}
