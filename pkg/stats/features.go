package stats

import (
	"fmt"
	"math"
)

// ICE computes the Index of Concentration at the Extremes (Krieger et al., 2016).
// Returns a value in [-1.0, 1.0]. Nil if total is zero or inputs are nil.
func ICE(privileged, deprived, total *float64) *float64 {
	if privileged == nil || deprived == nil || total == nil || *total == 0 {
		return nil
	}
	v := (*privileged - *deprived) / *total
	if v > 1 {
		v = 1
	}
	if v < -1 {
		v = -1
	}
	return &v
}

// ICEIncomeRace computes the income-race ICE for a set of tracts.
func ICEIncomeRace(highIncomeWhite, lowIncomePOC, totalPop []*float64) ([]*float64, error) {
	n := len(totalPop)
	if len(highIncomeWhite) != n || len(lowIncomePOC) != n {
		return nil, fmt.Errorf("ICEIncomeRace: length mismatch")
	}
	result := make([]*float64, n)
	for i := 0; i < n; i++ {
		result[i] = ICE(highIncomeWhite[i], lowIncomePOC[i], totalPop[i])
	}
	return result, nil
}

// DissimilarityIndex computes D = 0.5 * Σ|bi/B - wi/W| (Massey & Denton, 1988).
// Returns [0.0, 1.0]: 0 = perfect integration, 1 = complete segregation.
func DissimilarityIndex(groupCount, referenceCount []*float64) (*float64, error) {
	n := len(groupCount)
	if len(referenceCount) != n {
		return nil, fmt.Errorf("DissimilarityIndex: length mismatch")
	}
	var totalGroup, totalRef float64
	for i := 0; i < n; i++ {
		if groupCount[i] != nil && referenceCount[i] != nil {
			totalGroup += *groupCount[i]
			totalRef += *referenceCount[i]
		}
	}
	if totalGroup == 0 || totalRef == 0 {
		return nil, nil
	}
	var sum float64
	for i := 0; i < n; i++ {
		if groupCount[i] != nil && referenceCount[i] != nil {
			sum += math.Abs(*groupCount[i]/totalGroup - *referenceCount[i]/totalRef)
		}
	}
	d := 0.5 * sum
	return &d, nil
}

// IsolationIndex computes P* = Σ(xi/X)*(xi/ti) (Lieberson, 1981).
func IsolationIndex(groupCount, totalPop []*float64) (*float64, error) {
	n := len(groupCount)
	if len(totalPop) != n {
		return nil, fmt.Errorf("IsolationIndex: length mismatch")
	}
	var totalGroup float64
	for i := 0; i < n; i++ {
		if groupCount[i] != nil {
			totalGroup += *groupCount[i]
		}
	}
	if totalGroup == 0 {
		return nil, nil
	}
	var sum float64
	for i := 0; i < n; i++ {
		if groupCount[i] != nil && totalPop[i] != nil && *totalPop[i] > 0 {
			sum += (*groupCount[i] / totalGroup) * (*groupCount[i] / *totalPop[i])
		}
	}
	return &sum, nil
}

// CoefficientOfVariation computes CV = SE/estimate where SE = MOE/1.645.
// Census Bureau thresholds: <0.15 high, 0.15-0.30 moderate, >=0.30 low reliability.
func CoefficientOfVariation(estimate, moe *float64) *float64 {
	if estimate == nil || moe == nil || *estimate == 0 {
		return nil
	}
	se := *moe / 1.645
	cv := math.Abs(se / *estimate)
	return &cv
}

// ReliabilityLevel returns Census Bureau reliability classification for a CV.
func ReliabilityLevel(cv *float64) string {
	if cv == nil {
		return ""
	}
	switch {
	case *cv < 0.15:
		return "high"
	case *cv < 0.30:
		return "moderate"
	default:
		return "low"
	}
}
