package geo

import (
	"testing"
)

func TestValidateGEOID(t *testing.T) {
	valid := []string{
		"55",            // state  (Wisconsin)
		"55025",         // county (Dane County, WI)
		"55025002100",   // tract
		"550250021001",  // block group
	}
	for _, g := range valid {
		if err := ValidateGEOID(g); err != nil {
			t.Errorf("ValidateGEOID(%q): unexpected error: %v", g, err)
		}
	}

	invalid := []string{
		"",              // empty
		"5",             // length 1
		"555",           // length 3
		"5502",          // length 4
		"5502500",       // length 7
		"5502500210",    // length 10
		"5502500210012", // length 13
		"AB",            // non-digit
		"5502A",         // non-digit in county
		"55 25",         // space
	}
	for _, g := range invalid {
		if err := ValidateGEOID(g); err == nil {
			t.Errorf("ValidateGEOID(%q): expected error, got nil", g)
		}
	}
}

func TestParseGEOID(t *testing.T) {
	tests := []struct {
		geoid          string
		wantLevel      Level
		wantState      string
		wantCounty     string
		wantTract      string
		wantBlockGroup string
	}{
		{
			geoid:     "55",
			wantLevel: State,
			wantState: "55",
		},
		{
			geoid:      "55025",
			wantLevel:  County,
			wantState:  "55",
			wantCounty: "025",
		},
		{
			geoid:      "55025002100",
			wantLevel:  Tract,
			wantState:  "55",
			wantCounty: "025",
			wantTract:  "002100",
		},
		{
			geoid:          "550250021001",
			wantLevel:      BlockGroup,
			wantState:      "55",
			wantCounty:     "025",
			wantTract:      "002100",
			wantBlockGroup: "1",
		},
	}

	for _, tc := range tests {
		info, err := ParseGEOID(tc.geoid)
		if err != nil {
			t.Errorf("ParseGEOID(%q): unexpected error: %v", tc.geoid, err)
			continue
		}
		if info.GEOID != tc.geoid {
			t.Errorf("ParseGEOID(%q).GEOID = %q, want %q", tc.geoid, info.GEOID, tc.geoid)
		}
		if info.Level != tc.wantLevel {
			t.Errorf("ParseGEOID(%q).Level = %q, want %q", tc.geoid, info.Level, tc.wantLevel)
		}
		if info.StateFIPS != tc.wantState {
			t.Errorf("ParseGEOID(%q).StateFIPS = %q, want %q", tc.geoid, info.StateFIPS, tc.wantState)
		}
		if info.CountyFIPS != tc.wantCounty {
			t.Errorf("ParseGEOID(%q).CountyFIPS = %q, want %q", tc.geoid, info.CountyFIPS, tc.wantCounty)
		}
		if info.TractCode != tc.wantTract {
			t.Errorf("ParseGEOID(%q).TractCode = %q, want %q", tc.geoid, info.TractCode, tc.wantTract)
		}
		if info.BlockGroupCode != tc.wantBlockGroup {
			t.Errorf("ParseGEOID(%q).BlockGroupCode = %q, want %q", tc.geoid, info.BlockGroupCode, tc.wantBlockGroup)
		}
	}
}

func TestParseGEOIDErrors(t *testing.T) {
	bad := []string{"", "5", "55A", "123456789012345"}
	for _, g := range bad {
		if _, err := ParseGEOID(g); err == nil {
			t.Errorf("ParseGEOID(%q): expected error, got nil", g)
		}
	}
}

func TestParentGEOID(t *testing.T) {
	tests := []struct {
		geoid      string
		wantParent string
	}{
		{"550250021001", "55025002100"}, // block group → tract
		{"55025002100", "55025"},        // tract → county
		{"55025", "55"},                 // county → state
		{"55", ""},                      // state → nation (no GEOID)
	}

	for _, tc := range tests {
		got, err := ParentGEOID(tc.geoid)
		if err != nil {
			t.Errorf("ParentGEOID(%q): unexpected error: %v", tc.geoid, err)
			continue
		}
		if got != tc.wantParent {
			t.Errorf("ParentGEOID(%q) = %q, want %q", tc.geoid, got, tc.wantParent)
		}
	}
}

func TestParentGEOIDError(t *testing.T) {
	if _, err := ParentGEOID("bad"); err == nil {
		t.Error("ParentGEOID(\"bad\"): expected error for invalid GEOID")
	}
}

func TestIsChild(t *testing.T) {
	tests := []struct {
		parent string
		child  string
		want   bool
	}{
		// Direct relationships.
		{"55", "55025", true},            // state → county
		{"55025", "55025002100", true},   // county → tract
		{"55025002100", "550250021001", true}, // tract → block group

		// Indirect (ancestor) relationships.
		{"55", "55025002100", true},      // state → tract
		{"55", "550250021001", true},     // state → block group

		// Not a child (different state).
		{"27", "55025", false},
		{"55", "27025", false},

		// Same GEOID (not strictly a child).
		{"55025", "55025", false},

		// Parent longer than child (reversed).
		{"55025", "55", false},

		// Invalid GEOIDs.
		{"", "55025", false},
		{"55025", "", false},
		{"bad", "55025", false},
		{"55025", "bad", false},
	}

	for _, tc := range tests {
		got := IsChild(tc.parent, tc.child)
		if got != tc.want {
			t.Errorf("IsChild(%q, %q) = %v, want %v", tc.parent, tc.child, got, tc.want)
		}
	}
}
