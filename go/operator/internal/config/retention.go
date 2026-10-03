package config

import (
	"fmt"
	"math"
	"strconv"
)

var retentionUnitSeconds = map[byte]int64{
	'w': 7 * 24 * 60 * 60,
	'd': 24 * 60 * 60,
	'h': 60 * 60,
	'm': 60,
	's': 1,
}

// parseRetention parses durations such as "14d", "2w", or "1w3d12h" into
// seconds. Units are w, d, h, m, and s; a day is always 24 hours.
func parseRetention(value string) (int64, error) {
	if value == "" {
		return 0, fmt.Errorf("retention is empty")
	}
	var total int64
	for rest := value; rest != ""; {
		digits := 0
		for digits < len(rest) && rest[digits] >= '0' && rest[digits] <= '9' {
			digits++
		}
		if digits == 0 || digits == len(rest) {
			return 0, fmt.Errorf("retention %q must be a sequence of <number><unit> with units w, d, h, m, s", value)
		}
		unit, ok := retentionUnitSeconds[rest[digits]]
		if !ok {
			return 0, fmt.Errorf("retention %q has unknown unit %q; use w, d, h, m, or s", value, rest[digits])
		}
		amount, err := strconv.ParseInt(rest[:digits], 10, 64)
		if err != nil || amount > (math.MaxInt64-total)/unit {
			return 0, fmt.Errorf("retention %q is too large", value)
		}
		total += amount * unit
		rest = rest[digits+1:]
	}
	if total == 0 {
		return 0, fmt.Errorf("retention %q must be positive", value)
	}
	return total, nil
}

// renderRetention prefers the duration form and falls back to the
// deprecated seconds field; nil keeps data forever.
func renderRetention(retention string, deprecatedSeconds *int64) (*int64, error) {
	if retention == "" {
		return deprecatedSeconds, nil
	}
	seconds, err := parseRetention(retention)
	if err != nil {
		return nil, err
	}
	return &seconds, nil
}
