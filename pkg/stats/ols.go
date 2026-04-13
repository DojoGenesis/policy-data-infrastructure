package stats

import (
	"fmt"
	"math"
)

// OLSResult holds the full output of an OLS regression.
type OLSResult struct {
	// Betas are the estimated coefficients (length = number of columns in X).
	Betas []float64
	// RSquared is the coefficient of determination.
	RSquared float64
	// Predictions are the fitted values yhat_i for each observation.
	Predictions []float64
	// Residuals are y_i - yhat_i, rounded to 4 decimal places.
	Residuals []float64
	// StdErrors are the standard errors of each coefficient.
	StdErrors []float64
	// TStats are coefficient / SE for each coefficient.
	TStats []float64
	// PValues are two-tailed p-values computed from the t-distribution.
	PValues []float64
}

// OLS fits ordinary least squares: beta = (X'X)^{-1} X'y.
//
// XRows is an n×p matrix (each row is one observation; include a column of 1s for intercept).
// yVals is length n.
// Returns an error if X'X is singular or if inputs are inconsistent.
func OLS(XRows [][]float64, yVals []float64) (*OLSResult, error) {
	n := len(yVals)
	if n == 0 {
		return nil, fmt.Errorf("OLS: empty y")
	}
	if len(XRows) != n {
		return nil, fmt.Errorf("OLS: X has %d rows but y has %d elements", len(XRows), n)
	}
	p := len(XRows[0])
	if n <= p {
		return nil, fmt.Errorf("OLS: n (%d) must exceed p (%d)", n, p)
	}

	Xt := Transpose(XRows)
	XtX := MatMult(Xt, XRows)
	// Xty: p×1 column vector, built as p×1 slice-of-slices
	yCols := make([][]float64, n)
	for i, v := range yVals {
		yCols[i] = []float64{v}
	}
	XtY := MatMult(Xt, yCols) // p×1

	XtXi, err := GaussJordanInvert(XtX)
	if err != nil {
		return nil, fmt.Errorf("OLS: X'X is singular: %w", err)
	}

	betaMat := MatMult(XtXi, XtY) // p×1
	betas := make([]float64, p)
	for i := range betas {
		betas[i] = betaMat[i][0]
	}

	// fitted values
	preds := make([]float64, n)
	for i := 0; i < n; i++ {
		s := 0.0
		for j := 0; j < p; j++ {
			s += XRows[i][j] * betas[j]
		}
		preds[i] = s
	}

	// compute R²
	yMean := 0.0
	for _, v := range yVals {
		yMean += v
	}
	yMean /= float64(n)

	ssTot, ssRes := 0.0, 0.0
	for i := 0; i < n; i++ {
		d := yVals[i] - yMean
		ssTot += d * d
		r := yVals[i] - preds[i]
		ssRes += r * r
	}

	rSq := 0.0
	if ssTot > 0 {
		rSq = 1.0 - ssRes/ssTot
	}

	// residuals rounded to 4 decimal places
	residuals := make([]float64, n)
	for i := 0; i < n; i++ {
		residuals[i] = math.Round((yVals[i]-preds[i])*1e4) / 1e4
	}

	// standard errors: SE_j = sqrt(sigma^2 * (X'X)^{-1}_{jj})
	df := float64(n - p)
	sigma2 := ssRes / df

	diag := MatDiag(XtXi)
	stdErrors := make([]float64, p)
	tStats := make([]float64, p)
	pValues := make([]float64, p)

	for j := 0; j < p; j++ {
		variance := sigma2 * diag[j]
		if variance < 0 {
			variance = 0
		}
		se := math.Sqrt(variance)
		stdErrors[j] = se
		if se > 0 {
			tStats[j] = betas[j] / se
		}
		pValues[j] = tDistPValue(tStats[j], df)
	}

	return &OLSResult{
		Betas:       betas,
		RSquared:    rSq,
		Predictions: preds,
		Residuals:   residuals,
		StdErrors:   stdErrors,
		TStats:      tStats,
		PValues:     pValues,
	}, nil
}

// tDistPValue computes a two-tailed p-value for a t-statistic with df degrees of freedom.
// Uses an approximation via the regularized incomplete beta function (Abramowitz & Stegun).
func tDistPValue(t, df float64) float64 {
	if df <= 0 {
		return 1.0
	}
	x := df / (df + t*t)
	// regularized incomplete beta I_x(df/2, 1/2)
	ib := incompleteBeta(x, df/2, 0.5)
	p := ib
	if p > 1 {
		p = 1
	}
	if p < 0 {
		p = 0
	}
	return p
}

// incompleteBeta computes the regularized incomplete beta function I_x(a, b)
// using a continued fraction representation (accurate for most statistical uses).
func incompleteBeta(x, a, b float64) float64 {
	if x < 0 || x > 1 {
		return 0
	}
	if x == 0 {
		return 0
	}
	if x == 1 {
		return 1
	}

	// use symmetry relation when x > (a+1)/(a+b+2)
	if x > (a+1)/(a+b+2) {
		return 1 - incompleteBeta(1-x, b, a)
	}

	lgab, _ := math.Lgamma(a + b)
	lga, _ := math.Lgamma(a)
	lgb, _ := math.Lgamma(b)
	lbeta := lgab - lga - lgb
	front := math.Exp(math.Log(x)*a+math.Log(1-x)*b-lbeta) / a

	// Lentz continued fraction
	const maxIter = 200
	const eps = 3e-7

	f := 1.0
	C := 1.0
	D := 1 - (a+b)*x/(a+1)
	if math.Abs(D) < 1e-30 {
		D = 1e-30
	}
	D = 1 / D
	f = D

	for m := 1; m <= maxIter; m++ {
		mf := float64(m)
		// even step
		num := mf * (b - mf) * x / ((a + 2*mf - 1) * (a + 2*mf))
		D = 1 + num*D
		if math.Abs(D) < 1e-30 {
			D = 1e-30
		}
		C = 1 + num/C
		if math.Abs(C) < 1e-30 {
			C = 1e-30
		}
		D = 1 / D
		delta := C * D
		f *= delta

		// odd step
		num = -(a + mf) * (a + b + mf) * x / ((a + 2*mf) * (a + 2*mf + 1))
		D = 1 + num*D
		if math.Abs(D) < 1e-30 {
			D = 1e-30
		}
		C = 1 + num/C
		if math.Abs(C) < 1e-30 {
			C = 1e-30
		}
		D = 1 / D
		delta = C * D
		f *= delta

		if math.Abs(delta-1) < eps {
			break
		}
	}

	return front * f
}
