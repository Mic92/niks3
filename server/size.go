package server

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseSize parses a byte size like "2G", "512MiB", "100K", or a plain number
// of bytes. Suffixes are base-1024, case-insensitive, with optional trailing
// "iB"/"B". Empty string returns 0 (unlimited).
func ParseSize(value string) (uint64, error) {
	s := strings.TrimSpace(value)
	if s == "" {
		return 0, nil
	}

	upper := strings.ToUpper(s)
	upper = strings.TrimSuffix(upper, "IB")
	upper = strings.TrimSuffix(upper, "B")

	multiplier := uint64(1)

	if len(upper) > 0 {
		switch upper[len(upper)-1] {
		case 'K':
			multiplier = 1 << 10
		case 'M':
			multiplier = 1 << 20
		case 'G':
			multiplier = 1 << 30
		case 'T':
			multiplier = 1 << 40
		}

		if multiplier > 1 {
			upper = upper[:len(upper)-1]
		}
	}

	num, err := strconv.ParseFloat(strings.TrimSpace(upper), 64)
	if err != nil || num < 0 {
		return 0, fmt.Errorf("invalid size %q: expected a number with optional K/M/G/T suffix", value)
	}

	return uint64(num * float64(multiplier)), nil
}
