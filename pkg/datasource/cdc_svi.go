package datasource

// CDCSVISource fetches Social Vulnerability Index data from the CDC/ATSDR.
//
// The CDC SVI ranks census tracts and counties on 16 social factors grouped
// into four themes, producing an overall vulnerability percentile and four
// theme-level percentiles on a 0.0–1.0 scale (higher = more vulnerable).
//
// Data is distributed as CSV downloads (no API key required):
//
//	https://www.atsdr.cdc.gov/placeandhealth/svi/data_documentation_download.html
//
// Variables:
//
//	cdc_svi_overall           — Overall SVI percentile rank (0.0–1.0)
//	cdc_svi_socioeconomic     — Theme 1: Socioeconomic Status percentile
//	cdc_svi_household         — Theme 2: Household Characteristics percentile
//	cdc_svi_racial_ethnic     — Theme 3: Racial & Ethnic Minority Status percentile
//	cdc_svi_housing_transport — Theme 4: Housing Type & Transportation percentile
//
// This Go adapter provides the schema and identity to register the source in
// the pipeline. Bulk data ingest is handled by ingest/fetch_cdc_svi.py.

import (
	"context"
	"fmt"
	"net/http"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// cdcSVIVariables defines the schema produced by the CDC SVI source.
var cdcSVIVariables = []VariableDef{
	{
		ID:          "cdc_svi_overall",
		Name:        "CDC SVI — Overall Vulnerability",
		Description: "CDC/ATSDR Social Vulnerability Index overall percentile rank (0–1, higher = more vulnerable)",
		Unit:        "percentile",
		Direction:   "lower_better",
	},
	{
		ID:          "cdc_svi_socioeconomic",
		Name:        "CDC SVI — Theme 1: Socioeconomic Status",
		Description: "SVI socioeconomic theme: poverty, unemployment, income, education (percentile rank 0–1)",
		Unit:        "percentile",
		Direction:   "lower_better",
	},
	{
		ID:          "cdc_svi_household",
		Name:        "CDC SVI — Theme 2: Household Characteristics",
		Description: "SVI household theme: age 65+, age 17-, disability, single-parent, English proficiency (percentile rank 0–1)",
		Unit:        "percentile",
		Direction:   "lower_better",
	},
	{
		ID:          "cdc_svi_racial_ethnic",
		Name:        "CDC SVI — Theme 3: Racial & Ethnic Minority Status",
		Description: "SVI racial/ethnic minority theme: non-White, Hispanic/Latino, AI/AN, NHPI groups (percentile rank 0–1)",
		Unit:        "percentile",
		Direction:   "lower_better",
	},
	{
		ID:          "cdc_svi_housing_transport",
		Name:        "CDC SVI — Theme 4: Housing Type & Transportation",
		Description: "SVI housing/transport theme: multi-unit, mobile homes, crowding, no vehicle, group quarters (percentile rank 0–1)",
		Unit:        "percentile",
		Direction:   "lower_better",
	},
}

// CDCSVIConfig configures a CDCSVISource.
type CDCSVIConfig struct {
	// Year is the SVI release year (e.g. 2022).
	Year int
	// HTTPClient is used for any outbound requests. Defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// cdcSVISource implements DataSource for CDC SVI data.
type cdcSVISource struct {
	cfg     CDCSVIConfig
	vintage string
}

// NewCDCSVISource creates a CDCSVISource from cfg.
func NewCDCSVISource(cfg CDCSVIConfig) *cdcSVISource {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	vintage := "CDC-SVI"
	if cfg.Year > 0 {
		vintage = fmt.Sprintf("CDC-SVI-%d", cfg.Year)
	}
	return &cdcSVISource{cfg: cfg, vintage: vintage}
}

func (s *cdcSVISource) Name() string     { return "cdc-svi" }
func (s *cdcSVISource) Category() string { return "health" }
func (s *cdcSVISource) Vintage() string  { return s.vintage }

func (s *cdcSVISource) Schema() []VariableDef {
	out := make([]VariableDef, len(cdcSVIVariables))
	copy(out, cdcSVIVariables)
	return out
}

// FetchCounty is not implemented for CDC SVI via this Go adapter.
// Use the bulk CSV download at https://www.atsdr.cdc.gov/placeandhealth/svi/
// and the ingest/fetch_cdc_svi.py Python helper script instead.
func (s *cdcSVISource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	return nil, fmt.Errorf(
		"cdc-svi: FetchCounty not implemented (HTTP 501): " +
			"the CDC SVI data is distributed as bulk CSV downloads, not via a REST API. " +
			"Download the CSV from https://www.atsdr.cdc.gov/placeandhealth/svi/ " +
			"and use ingest/fetch_cdc_svi.py to load into the database",
	)
}

// FetchState is not implemented for CDC SVI via this Go adapter.
// Use the bulk CSV download at https://www.atsdr.cdc.gov/placeandhealth/svi/
// and the ingest/fetch_cdc_svi.py Python helper script instead.
func (s *cdcSVISource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	return nil, fmt.Errorf(
		"cdc-svi: FetchState not implemented (HTTP 501): " +
			"the CDC SVI data is distributed as bulk CSV downloads, not via a REST API. " +
			"Download the CSV from https://www.atsdr.cdc.gov/placeandhealth/svi/ " +
			"and use ingest/fetch_cdc_svi.py to load into the database",
	)
}