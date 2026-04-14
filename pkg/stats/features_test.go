package stats

import (
	"math"
	"testing"
)

func fp(v float64) *float64 { return &v }

func TestICE(t *testing.T) {
	tests := []struct {
		name       string
		privileged, deprived, total *float64
		want       *float64
	}{
		{"equal", fp(50), fp(50), fp(100), fp(0.0)},
		{"all privileged", fp(100), fp(0), fp(100), fp(1.0)},
		{"all deprived", fp(0), fp(100), fp(100), fp(-1.0)},
		{"mixed", fp(80), fp(20), fp(100), fp(0.6)},
		{"nil input", nil, fp(50), fp(100), nil},
		{"zero total", fp(50), fp(50), fp(0), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ICE(tt.privileged, tt.deprived, tt.total)
			if tt.want == nil {
				if got != nil {
					t.Errorf("expected nil, got %v", *got)
				}
				return
			}
			if got == nil {
				t.Fatal("expected non-nil")
			}
			if math.Abs(*got-*tt.want) > 1e-9 {
				t.Errorf("got %v, want %v", *got, *tt.want)
			}
		})
	}
}

func TestDissimilarityIndex(t *testing.T) {
	// Perfect segregation
	d, _ := DissimilarityIndex([]*float64{fp(100), fp(0)}, []*float64{fp(0), fp(100)})
	if d == nil || math.Abs(*d-1.0) > 1e-9 {
		t.Errorf("perfect segregation: got %v, want 1.0", d)
	}
	// Perfect integration
	d2, _ := DissimilarityIndex([]*float64{fp(50), fp(50)}, []*float64{fp(50), fp(50)})
	if d2 == nil || math.Abs(*d2) > 1e-9 {
		t.Errorf("integration: got %v, want 0.0", d2)
	}
}

func TestCoefficientOfVariation(t *testing.T) {
	cv := CoefficientOfVariation(fp(1000), fp(100))
	if cv == nil {
		t.Fatal("expected non-nil")
	}
	expected := (100.0 / 1.645) / 1000.0
	if math.Abs(*cv-expected) > 1e-4 {
		t.Errorf("got %v, want %v", *cv, expected)
	}
	if ReliabilityLevel(cv) != "high" {
		t.Errorf("expected high, got %s", ReliabilityLevel(cv))
	}
}
