package client

import (
	"fmt"
	"strings"
)

// shellSplit tokenises s like a POSIX shell would, honouring single quotes,
// double quotes, and backslash escapes. It does NOT do parameter expansion,
// globbing, or command substitution — this runs on a credential command
// string, where surprise expansion would be a security footgun.
//
// Semantics:
//   - whitespace outside quotes separates words
//   - 'single quoted' text is taken literally; backslash is not special
//   - "double quoted" text allows backslash to escape the next byte
//   - backslash outside quotes escapes the next byte
//   - adjacent fragments concatenate: a"b"'c' ⇒ abc
//
// Returns an error on an unterminated quote or trailing backslash.
func shellSplit(s string) ([]string, error) {
	var (
		words   []string
		cur     strings.Builder
		started bool // cur holds a word in progress (handles "" as a word)
	)

	const (
		stPlain = iota
		stSingle
		stDouble
	)

	state := stPlain
	escaped := false

	for _, r := range s {
		if escaped {
			cur.WriteRune(r)

			started = true
			escaped = false

			continue
		}

		switch state {
		case stPlain:
			switch r {
			case '\\':
				escaped = true
				started = true
			case '\'':
				state = stSingle
				started = true
			case '"':
				state = stDouble
				started = true
			case ' ', '\t', '\n':
				if started {
					words = append(words, cur.String())
					cur.Reset()

					started = false
				}
			default:
				cur.WriteRune(r)

				started = true
			}
		case stSingle:
			if r == '\'' {
				state = stPlain
			} else {
				cur.WriteRune(r)
			}
		case stDouble:
			switch r {
			case '\\':
				escaped = true
			case '"':
				state = stPlain
			default:
				cur.WriteRune(r)
			}
		}
	}

	if escaped {
		return nil, fmt.Errorf("trailing backslash in %q", s)
	}

	if state != stPlain {
		return nil, fmt.Errorf("unterminated quote in %q", s)
	}

	if started {
		words = append(words, cur.String())
	}

	return words, nil
}
