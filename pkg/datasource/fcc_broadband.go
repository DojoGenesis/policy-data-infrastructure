package datasource

// FCCBroadbandSource fetches county-level fixed broadband deployment data
// from the FCC Form 477 dataset. The data includes the number of residential
// fixed broadband connections per 1,000 households and provider competition
// metrics.
//
// Source: https://www.fcc.gov/form-477-county-data-internet-access-services
//
// The FCC publishes Form 477 data semi-annually (June and December releases)
// as CSV downloads. No API key is required. The Go adapter currently delegates
// bulk ingestion to the Python helper script at ingest/fetch_fcc_broadband.py
// and returns a placeholder at the Go interface level to make the source
// visible in the pipeline while the Go-native parser is built.
//
// Variables produced:
//
//	fcc_broadband_access_pct    — Fixed residential broadband connections per
//	                               1,000 households (county-level)
//	fcc_multiple_providers_pct  — Percentage of census tracts with more than
//	                               one fixed broadband provider (county-level)
//
// Geographic level: county (5-digit GEOID). Tract-level data is available at
// the separate URL https://www.fcc.gov/form-477-census-tract-data-internet-access-services.

import (
	"context"
	"fmt"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// fccBroadbandVariables defines the schema produced by the FCC Broadband source.
var fccBroadbandVariables = []VariableDef{
	{
		ID:          "fcc_broadband_access_pct",
		Name:        "Broadband Access Rate",
		Description: "Fixed residential broadband Internet access connections per 1,000 households at the county level. Published semi-annually by FCC Form 477.",
		Unit:        "per_1000_households",
		Direction:   "higher_better",
	},
	{
		ID:          "fcc_multiple_providers_pct",
		Name:        "Multiple Broadband Providers",
		Description: "Percentage of census tracts within the county that have more than one fixed broadband provider. Higher values indicate greater provider competition.",
		Unit:        "percent",
		Direction:   "higher_better",
	},
}

// FCCBroadbandConfig configures a FCCBroadbandSource.
type FCCBroadbandConfig struct {
	// Year is the data vintage year (e.g. 2024). Defaults to 2024.
	Year int
}

// fccBroadbandSource implements DataSource for FCC Form 477 broadband data.
type fccBroadbandSource struct {
	cfg     FCCBroadbandConfig
	vintage string
}

// NewFCCBroadbandSource creates a FCCBroadbandSource from cfg.
func NewFCCBroadbandSource(cfg FCCBroadbandConfig) *fccBroadbandSource {
	if cfg.Year == 0 {
		cfg.Year = 2024
	}
	vintage := fmt.Sprintf("FCC-BROADBAND-%d", cfg.Year)
	return &fccBroadbandSource{cfg: cfg, vintage: vintage}
}

func (s *fccBroadbandSource) Name() string     { return "fcc-broadband" }
func (s *fccBroadbandSource) Category() string  { return "infrastructure" }
func (s *fccBroadbandSource) Vintage() string   { return s.vintage }

func (s *fccBroadbandSource) Schema() []VariableDef {
	out := make([]VariableDef, len(fccBroadbandVariables))
	copy(out, fccBroadbandVariables)
	return out
}

// FetchCounty returns a placeholder for Go-native fetch.
//
// The FCC Form 477 county-level CSV download is large (~50 MB for the full
// national file) and is handled by the Python ingest script at
// ingest/fetch_fcc_broadband.py. This method returns a nil slice with no error
// so that the pipeline does not abort when this source is included in a fetch
// command — the Python path is the recommended bulk ingestion method.
//
// TODO: Implement Go-native CSV parser for the FCC county-level download
// when the Go adapter path is preferred over the Python ingestion pipeline.
func (s *fccBroadbandSource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	// Python ingest path is the primary method for now.
	// Return empty so the pipeline doesn't block but also doesn't silently
	// load stale/nil data.
	return nil, nil
}

// FetchState returns a placeholder for Go-native fetch.
//
// Same rationale as FetchCounty. The Python ingest script
// (ingest/fetch_fcc_broadband.py) handles the full national CSV download and
// bulk load.
//
// TODO: Implement Go-native CSV parser for state-level aggregation.
func (s *fccBroadbandSource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	return nil, nil
}
