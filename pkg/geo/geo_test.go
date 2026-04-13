package geo

import (
	"testing"
)

func TestLevelFromString(t *testing.T) {
	tests := []struct {
		input   string
		want    Level
		wantErr bool
	}{
		{"nation", Nation, false},
		{"state", State, false},
		{"county", County, false},
		{"tract", Tract, false},
		{"block_group", BlockGroup, false},
		{"ward", Ward, false},
		{"", "", true},
		{"Region", "", true},
		{"STATE", "", true}, // case-sensitive
		{"unknown", "", true},
	}

	for _, tc := range tests {
		got, err := LevelFromString(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Errorf("LevelFromString(%q): expected error, got nil", tc.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("LevelFromString(%q): unexpected error: %v", tc.input, err)
			continue
		}
		if got != tc.want {
			t.Errorf("LevelFromString(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestLevels(t *testing.T) {
	levels := Levels()
	if len(levels) != 6 {
		t.Fatalf("Levels() returned %d levels, want 6", len(levels))
	}
	want := []Level{Nation, State, County, Tract, BlockGroup, Ward}
	for i, w := range want {
		if levels[i] != w {
			t.Errorf("Levels()[%d] = %q, want %q", i, levels[i], w)
		}
	}
	// Verify the returned slice is a copy.
	levels[0] = "modified"
	if Levels()[0] != Nation {
		t.Error("Levels() returned a reference to internal slice")
	}
}

func TestParentLevel(t *testing.T) {
	tests := []struct {
		input   Level
		want    Level
		wantOK  bool
	}{
		{Nation, "", false},
		{State, Nation, true},
		{County, State, true},
		{Tract, County, true},
		{BlockGroup, Tract, true},
		{Ward, BlockGroup, true},
	}

	for _, tc := range tests {
		got, ok := ParentLevel(tc.input)
		if ok != tc.wantOK {
			t.Errorf("ParentLevel(%q): ok = %v, want %v", tc.input, ok, tc.wantOK)
		}
		if ok && got != tc.want {
			t.Errorf("ParentLevel(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}

	// Unknown level.
	if _, ok := ParentLevel("bogus"); ok {
		t.Error("ParentLevel(\"bogus\"): expected ok=false for unknown level")
	}
}

func TestChildLevel(t *testing.T) {
	tests := []struct {
		input  Level
		want   Level
		wantOK bool
	}{
		{Nation, State, true},
		{State, County, true},
		{County, Tract, true},
		{Tract, BlockGroup, true},
		{BlockGroup, Ward, true},
		{Ward, "", false},
	}

	for _, tc := range tests {
		got, ok := ChildLevel(tc.input)
		if ok != tc.wantOK {
			t.Errorf("ChildLevel(%q): ok = %v, want %v", tc.input, ok, tc.wantOK)
		}
		if ok && got != tc.want {
			t.Errorf("ChildLevel(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}

	// Unknown level.
	if _, ok := ChildLevel("bogus"); ok {
		t.Error("ChildLevel(\"bogus\"): expected ok=false for unknown level")
	}
}

func TestParentChildInverse(t *testing.T) {
	// Verifies that ParentLevel and ChildLevel are consistent inverses.
	levels := Levels()
	for i := 1; i < len(levels); i++ {
		child := levels[i]
		parent, ok := ParentLevel(child)
		if !ok {
			t.Errorf("ParentLevel(%q): expected ok=true", child)
			continue
		}
		// Going back down from parent should reach the same child.
		c, ok2 := ChildLevel(parent)
		if !ok2 {
			t.Errorf("ChildLevel(%q): expected ok=true", parent)
			continue
		}
		if c != child {
			t.Errorf("ChildLevel(ParentLevel(%q)) = %q, want %q", child, c, child)
		}
	}
}
