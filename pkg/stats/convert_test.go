package stats

import "testing"

func TestSafeFloat_values(t *testing.T) {
	cases := []struct {
		input    interface{}
		wantNil  bool
		wantVal  float64
	}{
		{nil, true, 0},
		{"", true, 0},
		{"*", true, 0},
		{"bogus", true, 0},
		{3.14, false, 3.14},
		{float32(2.5), false, 2.5},
		{int(7), false, 7},
		{int32(8), false, 8},
		{int64(9), false, 9},
		{"1.5", false, 1.5},
	}
	for _, c := range cases {
		got := SafeFloat(c.input)
		if c.wantNil && got != nil {
			t.Errorf("SafeFloat(%v): want nil, got %v", c.input, *got)
		}
		if !c.wantNil {
			if got == nil {
				t.Errorf("SafeFloat(%v): got nil, want %.4f", c.input, c.wantVal)
			} else if *got != c.wantVal {
				t.Errorf("SafeFloat(%v): want %.4f, got %.4f", c.input, c.wantVal, *got)
			}
		}
	}
}

func TestSafeInt_values(t *testing.T) {
	cases := []struct {
		input   interface{}
		wantNil bool
		wantVal int
	}{
		{nil, true, 0},
		{"", true, 0},
		{"*", true, 0},
		{NullSentinel, true, 0},
		{int32(NullSentinel), true, 0},
		{int64(NullSentinel), true, 0},
		{float64(NullSentinel), true, 0},
		{5, false, 5},
		{int32(3), false, 3},
		{int64(4), false, 4},
		{float64(6), false, 6},
		{"10", false, 10},
	}
	for _, c := range cases {
		got := SafeInt(c.input)
		if c.wantNil && got != nil {
			t.Errorf("SafeInt(%v): want nil, got %v", c.input, *got)
		}
		if !c.wantNil {
			if got == nil {
				t.Errorf("SafeInt(%v): got nil, want %d", c.input, c.wantVal)
			} else if *got != c.wantVal {
				t.Errorf("SafeInt(%v): want %d, got %d", c.input, c.wantVal, *got)
			}
		}
	}
}

func TestSafePct_basic(t *testing.T) {
	num := 3.0
	den := 4.0
	got := SafePct(&num, &den)
	if got == nil {
		t.Fatal("expected non-nil")
	}
	if *got != 0.75 {
		t.Errorf("want 0.75, got %.4f", *got)
	}
}

func TestSafePct_nil_inputs(t *testing.T) {
	den := 4.0
	if SafePct(nil, &den) != nil {
		t.Error("nil numerator should yield nil")
	}
	num := 3.0
	if SafePct(&num, nil) != nil {
		t.Error("nil denominator should yield nil")
	}
}

func TestSafePct_zero_denominator(t *testing.T) {
	num := 3.0
	den := 0.0
	if SafePct(&num, &den) != nil {
		t.Error("zero denominator should yield nil")
	}
}
