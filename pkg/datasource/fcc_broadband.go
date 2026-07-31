package datasource

// FCCBroadbandSource fetches county-level fixed broadband deployment data
// from the FCC Form 477 dataset. The data includes the number of residential
// fixed broadband connections per 1,000 households and provider competition
// metrics.
//
// Source: https://www.fcc.gov/form-477-county-data-internet-access-services
//
// The FCC publishes Form 477 data semi-annually (June and December releases)
// as bulk ZIP downloads (e.g. county_connections_200906_202506.zip). The
// program is not retired — data runs through June 2025 — but www.fcc.gov
// returns HTTP 403 to every programmatic request (it sits behind a WAF that
// requires a real browser session), and the Python helper script at
// ingest/fetch_fcc_broadband.py is not currently a working fallback either
// (its configured source URL is stale). FetchCounty and FetchState return an
// explicit "not implemented" error rather than a silent empty result, per
// this repo's rule of making gaps visible, not silent. Bulk ingestion is
// blocked on an operator decision (manual download, browser automation, or
// an alternate source such as the BDC API at broadbandmap.fcc.gov) — see
// TODO.md for the full writeup.
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
func (s *fccBroadbandSource) Category() string { return "infrastructure" }
func (s *fccBroadbandSource) Vintage() string  { return s.vintage }

func (s *fccBroadbandSource) Schema() []VariableDef {
	out := make([]VariableDef, len(fccBroadbandVariables))
	copy(out, fccBroadbandVariables)
	return out
}

// FetchCounty is not implemented for FCC Broadband via this Go adapter.
//
// FCC Form 477 county-level bulk data cannot be fetched automatically today:
// www.fcc.gov returns HTTP 403 to every programmatic request (verified from
// multiple IPs and browser User-Agents — it requires a real browser
// session), and the Python helper script at ingest/fetch_fcc_broadband.py is
// not a working fallback either (its configured source URL is stale). This
// is an operator decision, not a Go-native parser to build — see TODO.md.
func (s *fccBroadbandSource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	return nil, fmt.Errorf(
		"fcc-broadband: FetchCounty not implemented (HTTP 501): " +
			"FCC Form 477 bulk data is blocked from automated fetch — www.fcc.gov 403s all " +
			"programmatic requests behind its WAF, and ingest/fetch_fcc_broadband.py is not a " +
			"working fallback (stale source URL). Ingestion needs an operator decision; see TODO.md",
	)
}

// FetchState is not implemented for FCC Broadband via this Go adapter.
//
// Same rationale as FetchCounty: FCC Form 477 bulk data is blocked from
// automated fetch by www.fcc.gov's WAF, and the Python helper script is not
// currently a working fallback. See TODO.md for the blocker and options.
func (s *fccBroadbandSource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	return nil, fmt.Errorf(
		"fcc-broadband: FetchState not implemented (HTTP 501): " +
			"FCC Form 477 bulk data is blocked from automated fetch — www.fcc.gov 403s all " +
			"programmatic requests behind its WAF, and ingest/fetch_fcc_broadband.py is not a " +
			"working fallback (stale source URL). Ingestion needs an operator decision; see TODO.md",
	)
}
