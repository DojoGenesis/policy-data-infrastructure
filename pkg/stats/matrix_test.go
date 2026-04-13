package stats

import (
	"math"
	"testing"
)

func TestTranspose_square(t *testing.T) {
	A := [][]float64{{1, 2}, {3, 4}}
	At := Transpose(A)
	if At[0][0] != 1 || At[0][1] != 3 || At[1][0] != 2 || At[1][1] != 4 {
		t.Errorf("transpose wrong: %v", At)
	}
}

func TestTranspose_rectangular(t *testing.T) {
	A := [][]float64{{1, 2, 3}, {4, 5, 6}}
	At := Transpose(A)
	// At should be 3×2
	if len(At) != 3 || len(At[0]) != 2 {
		t.Fatalf("shape wrong: %dx%d", len(At), len(At[0]))
	}
	if At[2][1] != 6 {
		t.Errorf("At[2][1] should be 6, got %.1f", At[2][1])
	}
}

func TestMatMult_identity(t *testing.T) {
	A := [][]float64{{1, 2}, {3, 4}}
	I := [][]float64{{1, 0}, {0, 1}}
	AI := MatMult(A, I)
	for i := range A {
		for j := range A[i] {
			if AI[i][j] != A[i][j] {
				t.Errorf("A*I != A at [%d][%d]: got %.1f", i, j, AI[i][j])
			}
		}
	}
}

func TestMatMult_known(t *testing.T) {
	A := [][]float64{{1, 2}, {3, 4}}
	B := [][]float64{{5, 6}, {7, 8}}
	C := MatMult(A, B)
	// [[1*5+2*7, 1*6+2*8], [3*5+4*7, 3*6+4*8]] = [[19,22],[43,50]]
	expected := [][]float64{{19, 22}, {43, 50}}
	for i := range expected {
		for j := range expected[i] {
			if C[i][j] != expected[i][j] {
				t.Errorf("[%d][%d]: want %.1f, got %.1f", i, j, expected[i][j], C[i][j])
			}
		}
	}
}

func TestGaussJordanInvert_2x2(t *testing.T) {
	M := [][]float64{{2, 1}, {5, 3}}
	// inverse = [[3,-1],[-5,2]]
	inv, err := GaussJordanInvert(M)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := [][]float64{{3, -1}, {-5, 2}}
	for i := range expected {
		for j := range expected[i] {
			if math.Abs(inv[i][j]-expected[i][j]) > 1e-9 {
				t.Errorf("[%d][%d]: want %.1f, got %.6f", i, j, expected[i][j], inv[i][j])
			}
		}
	}
}

func TestGaussJordanInvert_identity_result(t *testing.T) {
	// M * M^-1 should be I
	M := [][]float64{{4, 7}, {2, 6}}
	inv, err := GaussJordanInvert(M)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	prod := MatMult(M, inv)
	n := len(M)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			expected := 0.0
			if i == j {
				expected = 1.0
			}
			if math.Abs(prod[i][j]-expected) > 1e-9 {
				t.Errorf("M*Minv[%d][%d] = %.6f, want %.1f", i, j, prod[i][j], expected)
			}
		}
	}
}

func TestGaussJordanInvert_singular(t *testing.T) {
	M := [][]float64{{1, 2}, {2, 4}} // row 2 = 2 * row 1 → singular
	_, err := GaussJordanInvert(M)
	if err == nil {
		t.Error("expected error for singular matrix, got nil")
	}
}

func TestGaussJordanInvert_3x3(t *testing.T) {
	M := [][]float64{
		{1, 2, 3},
		{0, 1, 4},
		{5, 6, 0},
	}
	inv, err := GaussJordanInvert(M)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	prod := MatMult(M, inv)
	n := 3
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			want := 0.0
			if i == j {
				want = 1.0
			}
			if math.Abs(prod[i][j]-want) > 1e-9 {
				t.Errorf("M*Minv[%d][%d] = %.8f, want %.1f", i, j, prod[i][j], want)
			}
		}
	}
}
