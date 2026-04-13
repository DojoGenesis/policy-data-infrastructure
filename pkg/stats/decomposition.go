package stats

import "fmt"

// DecompositionResult holds the output of a Blinder-Oaxaca decomposition.
type DecompositionResult struct {
	// MeanA is the mean of yA (predicted by OLS on group A).
	MeanA float64
	// MeanB is the mean of yB (predicted by OLS on group B).
	MeanB float64
	// Gap is MeanA - MeanB.
	Gap float64
	// Endowment is the part of the gap explained by differences in X (characteristics).
	Endowment float64
	// Coefficients is the part of the gap explained by differences in coefficients (returns).
	Coefficients float64
	// Interaction is the joint effect of differing X and differing coefficients.
	Interaction float64
	// EndowmentPct is Endowment as a fraction of Gap (0 if Gap == 0).
	EndowmentPct float64
	// CoefficientsPct is Coefficients as a fraction of Gap (0 if Gap == 0).
	CoefficientsPct float64
}

// BlinderOaxaca performs a generalized two-group Blinder-Oaxaca decomposition.
//
// xA and yAVals are the design matrix and outcome for group A.
// xB and yBVals are the design matrix and outcome for group B.
// Both design matrices must include a constant column if an intercept is desired.
//
// The three-way decomposition (Oaxaca 1973 / Ransom 1988) is:
//
//	Gap = Endowment + Coefficients + Interaction
//	Endowment    = (mean_xA - mean_xB)' * beta_B
//	Coefficients = mean_xB' * (beta_A - beta_B)
//	Interaction  = (mean_xA - mean_xB)' * (beta_A - beta_B)
func BlinderOaxaca(xA [][]float64, yAVals []float64, xB [][]float64, yBVals []float64) (*DecompositionResult, error) {
	if len(xA) == 0 || len(xB) == 0 {
		return nil, fmt.Errorf("BlinderOaxaca: empty group")
	}

	resA, err := OLS(xA, yAVals)
	if err != nil {
		return nil, fmt.Errorf("BlinderOaxaca: OLS on group A: %w", err)
	}
	resB, err := OLS(xB, yBVals)
	if err != nil {
		return nil, fmt.Errorf("BlinderOaxaca: OLS on group B: %w", err)
	}

	p := len(resA.Betas)

	// column means of X for each group
	meanXA := columnMeans(xA, p)
	meanXB := columnMeans(xB, p)

	// outcome means
	meanA := colMean(yAVals)
	meanB := colMean(yBVals)
	gap := meanA - meanB

	// endowment = (meanXA - meanXB)' * betaB
	// coefficients = meanXB' * (betaA - betaB)
	// interaction = (meanXA - meanXB)' * (betaA - betaB)
	endowment := 0.0
	coefficients := 0.0
	interaction := 0.0
	for j := 0; j < p; j++ {
		dX := meanXA[j] - meanXB[j]
		dB := resA.Betas[j] - resB.Betas[j]
		endowment += dX * resB.Betas[j]
		coefficients += meanXB[j] * dB
		interaction += dX * dB
	}

	endowmentPct := 0.0
	coefficientsPct := 0.0
	if gap != 0 {
		endowmentPct = endowment / gap
		coefficientsPct = coefficients / gap
	}

	return &DecompositionResult{
		MeanA:           meanA,
		MeanB:           meanB,
		Gap:             gap,
		Endowment:       endowment,
		Coefficients:    coefficients,
		Interaction:     interaction,
		EndowmentPct:    endowmentPct,
		CoefficientsPct: coefficientsPct,
	}, nil
}

func columnMeans(X [][]float64, p int) []float64 {
	sums := make([]float64, p)
	for _, row := range X {
		for j := 0; j < p && j < len(row); j++ {
			sums[j] += row[j]
		}
	}
	n := float64(len(X))
	for j := range sums {
		sums[j] /= n
	}
	return sums
}

func colMean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := 0.0
	for _, v := range xs {
		s += v
	}
	return s / float64(len(xs))
}
