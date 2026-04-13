package stats

import "fmt"

// InteractionOLS adds interaction terms (column products) to the design matrix X,
// then runs OLS.
//
// X is an n×p matrix. interactions is a slice of column-index pairs [i, j] (0-based).
// For each pair, a new column X[:,i]*X[:,j] is appended to X before running OLS.
// Returns an error if any column index is out of range.
func InteractionOLS(X [][]float64, y []float64, interactions [][2]int) (*OLSResult, error) {
	n := len(X)
	if n == 0 {
		return nil, fmt.Errorf("InteractionOLS: empty X")
	}
	p := len(X[0])

	// validate interaction indices
	for _, pair := range interactions {
		if pair[0] < 0 || pair[0] >= p || pair[1] < 0 || pair[1] >= p {
			return nil, fmt.Errorf("InteractionOLS: column index out of range: [%d, %d] for p=%d", pair[0], pair[1], p)
		}
	}

	// build augmented design matrix
	augX := make([][]float64, n)
	for i, row := range X {
		newRow := make([]float64, p+len(interactions))
		copy(newRow, row)
		for k, pair := range interactions {
			newRow[p+k] = row[pair[0]] * row[pair[1]]
		}
		augX[i] = newRow
	}

	return OLS(augX, y)
}
