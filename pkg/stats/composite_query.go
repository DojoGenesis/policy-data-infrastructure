package stats

import (
	"fmt"
	"math"
	"sort"
)

// ComputeWeightedComposite computes a geometric-mean composite score from
// already-normalised (and guaranteed positive) values.
//
// values maps variable_id to its normalised value for a single geography.
// weights maps variable_id to its weight. Weights are normalised to sum to 1.0.
//
// Returns nil if no valid (non-nil) values are available from the intersection
// of values and weights. All values must be positive for the geometric mean.
func ComputeWeightedComposite(values map[string]*float64, weights map[string]float64) (*float64, error) {
	var logSum, weightSum float64

	for varID, w := range weights {
		v, ok := values[varID]
		if !ok || v == nil {
			continue
		}
		if *v <= 0 {
			return nil, fmt.Errorf("ComputeWeightedComposite: value for %q is %v, must be positive for geometric mean", varID, *v)
		}
		logSum += w * math.Log(*v)
		weightSum += w
	}

	if weightSum == 0 {
		return nil, nil
	}

	result := math.Exp(logSum / weightSum)
	return &result, nil
}

// CompositeInput holds per-geography values for composite computation.
type CompositeInput struct {
	GEOID  string
	Values map[string]*float64 // variable_id → normalised positive value
}

// CompositeScore is the composite result for a single geography.
type CompositeScore struct {
	GEOID       string   `json:"geoid"`
	Score       *float64 `json:"score"`
	ContribVars []string `json:"contributing_variables"`
	MissingVars []string `json:"missing_variables"`
}

// PerturbedScenario holds composite scores computed under one weight perturbation.
type PerturbedScenario struct {
	PerturbedVar string           `json:"perturbed_variable"`
	Direction    string           `json:"direction"` // "up" or "down"
	Perturbation float64          `json:"perturbation"`
	Scores       []CompositeScore `json:"scores"`
}

// SensitivityResult holds the full sensitivity-analysis output for a composite
// computation across multiple geographies.
type SensitivityResult struct {
	BaseScores   []CompositeScore    `json:"base_scores"`
	Perturbation float64             `json:"perturbation"`
	Scenarios    []PerturbedScenario `json:"scenarios"`
	Stability    map[string]float64  `json:"stability"` // geoid → fraction [0, 1]
}

// CompositeSensitivity runs sensitivity analysis on composite scores across
// multiple geographies. It perturbs each variable's weight by ±perturbation
// and measures ranking stability.
//
// geoidInputs provides per-geoid normalised positive values.
// weights maps variable_id to its base weight.
// perturbation is the fractional change (e.g. 0.20 = ±20 %).
func CompositeSensitivity(
	geoidInputs []CompositeInput,
	weights map[string]float64,
	perturbation float64,
) (*SensitivityResult, error) {
	if len(geoidInputs) == 0 {
		return nil, fmt.Errorf("CompositeSensitivity: no geoid inputs")
	}
	if len(weights) == 0 {
		return nil, fmt.Errorf("CompositeSensitivity: no weights")
	}
	if perturbation <= 0 || perturbation >= 1 {
		return nil, fmt.Errorf("CompositeSensitivity: perturbation must be in (0, 1), got %v", perturbation)
	}

	// Normalise base weights.
	baseWeights := normaliseWeights(weights)

	// Compute base scores.
	baseScores := make([]CompositeScore, len(geoidInputs))
	for i, inp := range geoidInputs {
		score, err := ComputeWeightedComposite(inp.Values, baseWeights)
		if err != nil {
			return nil, fmt.Errorf("CompositeSensitivity: geoid %q: %w", inp.GEOID, err)
		}
		cs := CompositeScore{GEOID: inp.GEOID, Score: score}
		for varID := range baseWeights {
			if v, ok := inp.Values[varID]; ok && v != nil {
				cs.ContribVars = append(cs.ContribVars, varID)
			} else {
				cs.MissingVars = append(cs.MissingVars, varID)
			}
		}
		baseScores[i] = cs
	}

	// Compute base ranks (higher score = rank 0).
	baseRanks := computeRanks(baseScores)

	result := &SensitivityResult{
		BaseScores:   baseScores,
		Perturbation: perturbation,
		Stability:    make(map[string]float64),
	}

	// Collect variable IDs in deterministic order.
	varIDs := make([]string, 0, len(baseWeights))
	for varID := range baseWeights {
		varIDs = append(varIDs, varID)
	}
	sort.Strings(varIDs)

	totalScenarios := 0
	rankHits := make(map[string]int)
	for _, inp := range geoidInputs {
		rankHits[inp.GEOID] = 0
	}

	for _, pertVar := range varIDs {
		for _, direction := range []string{"up", "down"} {
			pertWeights := make(map[string]float64, len(baseWeights))
			for varID, w := range baseWeights {
				pertWeights[varID] = w
			}

			baseW := baseWeights[pertVar]
			var newW float64
			if direction == "up" {
				newW = baseW * (1 + perturbation)
			} else {
				newW = baseW * (1 - perturbation)
			}

			// Proportionally adjust other weights so the total still sums to 1.
			delta := newW - baseW
			otherSum := 1.0 - baseW
			if otherSum > 0 {
				for varID := range pertWeights {
					if varID == pertVar {
						pertWeights[varID] = newW
					} else {
						pertWeights[varID] *= (1 - delta/otherSum)
					}
				}
			} else {
				pertWeights[pertVar] = 1.0
			}

			// Clamp negative weights to zero, then re-normalise.
			for varID, w := range pertWeights {
				if w < 0 {
					pertWeights[varID] = 0
				}
			}
			pertWeights = normaliseWeights(pertWeights)

			// Compute perturbed scores.
			pertScores := make([]CompositeScore, len(geoidInputs))
			for i, inp := range geoidInputs {
				score, err := ComputeWeightedComposite(inp.Values, pertWeights)
				if err != nil {
					return nil, fmt.Errorf("CompositeSensitivity: perturbed geoid %q: %w", inp.GEOID, err)
				}
				pertScores[i] = CompositeScore{
					GEOID:       inp.GEOID,
					Score:       score,
					ContribVars: baseScores[i].ContribVars,
					MissingVars: baseScores[i].MissingVars,
				}
			}

			result.Scenarios = append(result.Scenarios, PerturbedScenario{
				PerturbedVar: pertVar,
				Direction:    direction,
				Perturbation: perturbation,
				Scores:       pertScores,
			})
			totalScenarios++

			// Check rank preservation (allow ±1 position tolerance).
			pertRanks := computeRanks(pertScores)
			for _, inp := range geoidInputs {
				g := inp.GEOID
				if absInt(baseRanks[g]-pertRanks[g]) <= 1 {
					rankHits[g]++
				}
			}
		}
	}

	for geoid, hits := range rankHits {
		if totalScenarios > 0 {
			result.Stability[geoid] = float64(hits) / float64(totalScenarios)
		}
	}

	return result, nil
}

// normaliseWeights returns a copy of weights normalised to sum to 1.0.
func normaliseWeights(weights map[string]float64) map[string]float64 {
	var sum float64
	for _, w := range weights {
		sum += w
	}
	result := make(map[string]float64, len(weights))
	if sum == 0 {
		for k := range weights {
			result[k] = 1.0 / float64(len(weights))
		}
	} else {
		for k, w := range weights {
			result[k] = w / sum
		}
	}
	return result
}

// computeRanks returns a map from geoid to rank where 0 is the highest score.
func computeRanks(scores []CompositeScore) map[string]int {
	type indexed struct {
		geoid string
		score float64
	}
	var valid []indexed
	for _, cs := range scores {
		if cs.Score != nil {
			valid = append(valid, indexed{cs.GEOID, *cs.Score})
		}
	}
	sort.Slice(valid, func(i, j int) bool {
		return valid[i].score > valid[j].score // descending
	})
	ranks := make(map[string]int, len(scores))
	for i, v := range valid {
		ranks[v.geoid] = i
	}
	// Nil scores get the last rank.
	lastRank := len(valid)
	for _, cs := range scores {
		if cs.Score == nil {
			ranks[cs.GEOID] = lastRank
		}
	}
	return ranks
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
