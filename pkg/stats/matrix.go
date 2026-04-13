package stats

import (
	"fmt"
	"math"
)

// Transpose returns the transpose of matrix A.
// A must be non-empty and rectangular.
func Transpose(A [][]float64) [][]float64 {
	rows := len(A)
	cols := len(A[0])
	result := make([][]float64, cols)
	for i := range result {
		result[i] = make([]float64, rows)
	}
	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			result[j][i] = A[i][j]
		}
	}
	return result
}

// MatMult multiplies matrices A (ra×ca) and B (ca×cb).
func MatMult(A, B [][]float64) [][]float64 {
	ra := len(A)
	ca := len(A[0])
	cb := len(B[0])
	result := make([][]float64, ra)
	for i := range result {
		result[i] = make([]float64, cb)
		for j := 0; j < cb; j++ {
			s := 0.0
			for k := 0; k < ca; k++ {
				s += A[i][k] * B[k][j]
			}
			result[i][j] = s
		}
	}
	return result
}

// GaussJordanInvert inverts an n×n matrix using Gauss-Jordan elimination
// with partial pivoting. Returns an error if the matrix is singular.
func GaussJordanInvert(M [][]float64) ([][]float64, error) {
	n := len(M)
	// build augmented matrix [M | I]
	aug := make([][]float64, n)
	for i := range aug {
		aug[i] = make([]float64, 2*n)
		copy(aug[i], M[i])
		aug[i][n+i] = 1.0
	}

	for col := 0; col < n; col++ {
		// partial pivot: find row with max abs value in this column
		pivot := col
		for r := col + 1; r < n; r++ {
			if math.Abs(aug[r][col]) > math.Abs(aug[pivot][col]) {
				pivot = r
			}
		}
		aug[col], aug[pivot] = aug[pivot], aug[col]

		div := aug[col][col]
		if math.Abs(div) < 1e-12 {
			return nil, fmt.Errorf("matrix is singular at column %d (pivot=%.2e)", col, div)
		}

		// normalize pivot row
		for k := 0; k < 2*n; k++ {
			aug[col][k] /= div
		}

		// eliminate column in all other rows
		for row := 0; row < n; row++ {
			if row == col {
				continue
			}
			factor := aug[row][col]
			for k := 0; k < 2*n; k++ {
				aug[row][k] -= factor * aug[col][k]
			}
		}
	}

	// extract right half — copy to avoid sharing memory with aug workspace
	inv := make([][]float64, n)
	for i := range inv {
		inv[i] = make([]float64, n)
		copy(inv[i], aug[i][n:])
	}
	return inv, nil
}

// MatDiag returns the diagonal elements of a square matrix.
func MatDiag(A [][]float64) []float64 {
	n := len(A)
	d := make([]float64, n)
	for i := 0; i < n; i++ {
		d[i] = A[i][i]
	}
	return d
}
