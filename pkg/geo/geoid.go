package geo

import (
	"fmt"
	"unicode"
)

// GEOIDInfo contains the structured components extracted from a raw FIPS GEOID
// string. Fields are only populated for levels at or below the component's
// position in the hierarchy (e.g. TractCode is empty for a state GEOID).
type GEOIDInfo struct {
	GEOID          string
	Level          Level
	StateFIPS      string
	CountyFIPS     string
	TractCode      string
	BlockGroupCode string
}

// validLengths maps accepted GEOID lengths to their corresponding Level.
// Length 2  → state (SS)
// Length 5  → county (SS+CCC)
// Length 11 → tract (SS+CCC+TTTTTT)
// Length 12 → block group (SS+CCC+TTTTTT+B)
var validLengths = map[int]Level{
	2:  State,
	5:  County,
	11: Tract,
	12: BlockGroup,
}

// ValidateGEOID checks that geoid consists entirely of ASCII digits and has a
// recognised length. It does not verify that the FIPS codes refer to real
// geographic entities.
func ValidateGEOID(geoid string) error {
	if len(geoid) == 0 {
		return fmt.Errorf("geo: empty GEOID")
	}
	for _, r := range geoid {
		if !unicode.IsDigit(r) {
			return fmt.Errorf("geo: GEOID %q contains non-digit character %q", geoid, r)
		}
	}
	if _, ok := validLengths[len(geoid)]; !ok {
		return fmt.Errorf("geo: GEOID %q has unrecognised length %d (valid: 2, 5, 11, 12)", geoid, len(geoid))
	}
	return nil
}

// ParseGEOID parses a raw FIPS GEOID string and returns a populated GEOIDInfo.
// Returns an error if ValidateGEOID fails.
func ParseGEOID(geoid string) (*GEOIDInfo, error) {
	if err := ValidateGEOID(geoid); err != nil {
		return nil, err
	}

	level := validLengths[len(geoid)]
	info := &GEOIDInfo{
		GEOID: geoid,
		Level: level,
	}

	// Every GEOID starts with a 2-digit state FIPS code.
	info.StateFIPS = geoid[:2]

	switch level {
	case County:
		info.CountyFIPS = geoid[2:5]
	case Tract:
		info.CountyFIPS = geoid[2:5]
		info.TractCode = geoid[5:11]
	case BlockGroup:
		info.CountyFIPS = geoid[2:5]
		info.TractCode = geoid[5:11]
		info.BlockGroupCode = geoid[11:12]
	}

	return info, nil
}

// ParentGEOID returns the GEOID of the parent geography for the given geoid.
// The mapping is:
//
//	block_group (12) → tract (11)
//	tract       (11) → county (5)
//	county       (5) → state (2)
//	state        (2) → "" (nation has no GEOID in the FIPS scheme)
//
// An error is returned when the GEOID is invalid.
func ParentGEOID(geoid string) (string, error) {
	if err := ValidateGEOID(geoid); err != nil {
		return "", err
	}

	switch len(geoid) {
	case 12:
		return geoid[:11], nil
	case 11:
		return geoid[:5], nil
	case 5:
		return geoid[:2], nil
	case 2:
		// States have no FIPS parent.
		return "", nil
	}

	// ValidateGEOID ensures we never reach here.
	return "", fmt.Errorf("geo: unexpected GEOID length %d", len(geoid))
}

// IsChild returns true when child is a direct or indirect descendant of
// parent in the FIPS hierarchy. Both GEOIDs must be valid; invalid inputs
// always return false.
func IsChild(parent, child string) bool {
	if err := ValidateGEOID(parent); err != nil {
		return false
	}
	if err := ValidateGEOID(child); err != nil {
		return false
	}
	// child must be longer than parent to be a descendant.
	if len(child) <= len(parent) {
		return false
	}
	// A valid child's GEOID starts with the parent's GEOID.
	return child[:len(parent)] == parent
}
