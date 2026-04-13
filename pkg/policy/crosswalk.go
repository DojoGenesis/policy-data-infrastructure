package policy

import (
	"encoding/json"
	"fmt"
	"os"
)

// Crosswalk maps equity dimensions to the indicators that can evaluate them.
// The crosswalk definition is loaded from a JSON file (TOML parser not available in go.mod;
// use data/crosswalks/wi_equity_crosswalk.json as the canonical source).
type Crosswalk struct {
	Dimensions map[string]DimensionMapping `json:"dimensions"`
}

// DimensionMapping defines which indicators and analysis methods apply to an equity dimension.
type DimensionMapping struct {
	// Label is a human-readable name for the dimension.
	Label string `json:"label"`
	// Indicators lists PDI indicator variable_ids (e.g., "median_hh_income").
	Indicators []string `json:"indicators"`
	// Methods lists the analysis methods applicable to this dimension:
	// "descriptive", "correlation", "regression", "decomposition", "geographic_overlap".
	Methods []string `json:"methods"`
	// Priority is the Atlas analysis priority: "P1", "P2", or "P3".
	Priority string `json:"priority"`
	// PolicyCount is populated by EnrichWithCounts; not stored in the JSON file.
	PolicyCount int `json:"policy_count"`
	// Notes provides optional extension guidance.
	Notes string `json:"notes,omitempty"`
}

// LoadCrosswalkFromJSON loads a crosswalk definition from a JSON file.
func LoadCrosswalkFromJSON(path string) (*Crosswalk, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("crosswalk: open %s: %w", path, err)
	}
	defer f.Close()

	var cw Crosswalk
	if err := json.NewDecoder(f).Decode(&cw); err != nil {
		return nil, fmt.Errorf("crosswalk: decode %s: %w", path, err)
	}
	if cw.Dimensions == nil {
		return nil, fmt.Errorf("crosswalk: %s: missing 'dimensions' key", path)
	}
	return &cw, nil
}

// EnrichWithCounts populates PolicyCount for each DimensionMapping based on the
// provided policy set. This mutates the Crosswalk in place.
func (c *Crosswalk) EnrichWithCounts(policies []PolicyRecord) {
	// Reset counts first
	for key, dm := range c.Dimensions {
		dm.PolicyCount = 0
		c.Dimensions[key] = dm
	}
	for _, p := range policies {
		if dm, ok := c.Dimensions[p.EquityDimension]; ok {
			dm.PolicyCount++
			c.Dimensions[p.EquityDimension] = dm
		}
	}
}

// IndicatorsForPolicy returns the PDI indicator variable_ids relevant to a specific policy
// based on its equity dimension. Returns nil if the dimension is not mapped.
func (c *Crosswalk) IndicatorsForPolicy(p PolicyRecord) []string {
	dm, ok := c.Dimensions[p.EquityDimension]
	if !ok {
		return nil
	}
	return dm.Indicators
}

// MethodsForPolicy returns the analysis methods applicable to a specific policy
// based on its equity dimension. Returns nil if the dimension is not mapped.
func (c *Crosswalk) MethodsForPolicy(p PolicyRecord) []string {
	dm, ok := c.Dimensions[p.EquityDimension]
	if !ok {
		return nil
	}
	return dm.Methods
}

// DimensionForPolicy returns the full DimensionMapping for a policy's equity dimension.
// The second return value is false if the dimension is not found in the crosswalk.
func (c *Crosswalk) DimensionForPolicy(p PolicyRecord) (DimensionMapping, bool) {
	dm, ok := c.Dimensions[p.EquityDimension]
	return dm, ok
}
