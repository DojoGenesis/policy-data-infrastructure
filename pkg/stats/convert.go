package stats

import (
	"fmt"
)

// NullSentinel is the integer value used in source data to represent suppressed/missing integer fields.
const NullSentinel = -666666666

// SafeFloat converts an interface{} value to *float64.
// Returns nil for missing or suppressed values: nil, "", "*".
func SafeFloat(v interface{}) *float64 {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case string:
		if val == "" || val == "*" {
			return nil
		}
		var f float64
		if _, err := fmt.Sscanf(val, "%f", &f); err != nil {
			return nil
		}
		return &f
	case float64:
		return &val
	case float32:
		f := float64(val)
		return &f
	case int:
		f := float64(val)
		return &f
	case int32:
		f := float64(val)
		return &f
	case int64:
		f := float64(val)
		return &f
	default:
		return nil
	}
}

// SafeInt converts an interface{} value to *int.
// Returns nil for nil, "*", "", and values equal to NullSentinel (-666666666).
func SafeInt(v interface{}) *int {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case string:
		if val == "" || val == "*" {
			return nil
		}
		var i int
		if _, err := fmt.Sscanf(val, "%d", &i); err != nil {
			return nil
		}
		if i == NullSentinel {
			return nil
		}
		return &i
	case int:
		if val == NullSentinel {
			return nil
		}
		return &val
	case int32:
		i := int(val)
		if i == NullSentinel {
			return nil
		}
		return &i
	case int64:
		i := int(val)
		if i == NullSentinel {
			return nil
		}
		return &i
	case float64:
		i := int(val)
		if i == NullSentinel {
			return nil
		}
		return &i
	default:
		return nil
	}
}

// SafePct computes numerator/denominator as *float64.
// Returns nil if either argument is nil or denominator is zero.
func SafePct(numerator, denominator *float64) *float64 {
	if numerator == nil || denominator == nil || *denominator == 0 {
		return nil
	}
	result := *numerator / *denominator
	return &result
}
