package datasource

// EPAEJScreenSource fetches environmental justice indicators at the census
// tract level from the EPA EJScreen dataset.
//
// The EJScreen REST broker API is documented at:
//
//	https://ejscreen.epa.gov/mapper/ejscreenRESTbroker.aspx
//
// Variables available (EJScreen column names):
//
//	PM25        — PM2.5 concentration (annual mean, µg/m³)
//	OZONE       — Ozone summer seasonal average (ppb)
//	DSLPM       — Diesel PM (µg/m³)
//	CANCER      — Air toxics cancer risk (risk per million)
//	RESP        — Air toxics respiratory hazard index
//	PTRAF       — Traffic proximity and volume (vehicles/day / distance)
//	PNPL        — Proximity to NPL (Superfund) sites
//	PRMP        — Proximity to RMP (Risk Management Plan) facilities
//	PTSDF       — Proximity to TSDFs (hazardous waste treatment, storage, disposal)
//	PWDIS       — Wastewater discharge indicator (toxicity × flow / distance)
//	UST         — Underground storage tanks (count / distance)
//	LEAD_PAINT  — Pre-1960 housing (fraction of housing units built before 1960)
//
// Implementation note: The EJScreen REST broker requires geometry input
// (point or bounding box) and does not support bulk state-wide tract exports
// through its API. The recommended path for national-scale data is to download
// the annual EJScreen CSV from:
//
//	https://gaftp.epa.gov/EJSCREEN/
//
// and ingest it with the Python helper script at ingest/ejscreen_csv.py.
// This Go adapter therefore returns HTTP 501 for FetchCounty and FetchState
// to make the gap visible. Use the bulk download path for national fetches.

import (
	"context"
	"fmt"
	"net/http"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// ejscreenVariables defines the schema produced by the EJScreen source.
var ejscreenVariables = []VariableDef{
	{
		ID:          "epa_pm25",
		Name:        "PM2.5 Concentration",
		Description: "Annual mean PM2.5 concentration (µg/m³) from EPA modeling",
		Unit:        "rate",
		Direction:   "lower_better",
	},
	{
		ID:          "epa_ozone",
		Name:        "Ozone Seasonal Average",
		Description: "Summer seasonal average ozone concentration (ppb)",
		Unit:        "rate",
		Direction:   "lower_better",
	},
	{
		ID:          "epa_dslpm",
		Name:        "Diesel Particulate Matter",
		Description: "Diesel PM concentration from on-road and non-road sources (µg/m³)",
		Unit:        "rate",
		Direction:   "lower_better",
	},
	{
		ID:          "epa_cancer",
		Name:        "Air Toxics Cancer Risk",
		Description: "Lifetime cancer risk from inhalation of air toxics (risk per 1,000,000 people)",
		Unit:        "rate",
		Direction:   "lower_better",
	},
	{
		ID:          "epa_resp",
		Name:        "Air Toxics Respiratory Hazard Index",
		Description: "Ratio of exposure concentration to health-based reference concentration for respiratory effects",
		Unit:        "rate",
		Direction:   "lower_better",
	},
	{
		ID:          "epa_ptraf",
		Name:        "Traffic Proximity",
		Description: "Count of vehicles (AADT) at nearby roads, divided by distance to road (vehicles/day per meter)",
		Unit:        "rate",
		Direction:   "lower_better",
	},
	{
		ID:          "epa_pnpl",
		Name:        "Proximity to Superfund Sites",
		Description: "Count of proposed and listed NPL sites within 5 km, divided by distance (sites/km)",
		Unit:        "rate",
		Direction:   "lower_better",
	},
	{
		ID:          "epa_prmp",
		Name:        "Proximity to RMP Facilities",
		Description: "Count of RMP facilities within 5 km, divided by distance (facilities/km)",
		Unit:        "rate",
		Direction:   "lower_better",
	},
	{
		ID:          "epa_ptsdf",
		Name:        "Proximity to Hazardous Waste Facilities",
		Description: "Count of TSDFs within 5 km, divided by distance (facilities/km)",
		Unit:        "rate",
		Direction:   "lower_better",
	},
	{
		ID:          "epa_pwdis",
		Name:        "Wastewater Discharge Indicator",
		Description: "Toxicity-weighted concentration of wastewater discharges within 500 m (indicator score)",
		Unit:        "rate",
		Direction:   "lower_better",
	},
	{
		ID:          "epa_ust",
		Name:        "Underground Storage Tanks",
		Description: "Density of leaking underground storage tanks (count per km²)",
		Unit:        "rate",
		Direction:   "lower_better",
	},
	{
		ID:          "epa_lead_paint",
		Name:        "Pre-1960 Housing (Lead Paint Risk)",
		Description: "Fraction of housing units built before 1960 — proxy for lead paint exposure risk",
		Unit:        "percent",
		Direction:   "lower_better",
	},
}

// EPAEJScreenConfig configures an EPAEJScreenSource.
type EPAEJScreenConfig struct {
	// Year is the EJScreen data year (e.g. 2023).
	Year int
	// HTTPClient is used for all outbound requests. Defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// epaEJScreenSource implements DataSource for EPA EJScreen data.
type epaEJScreenSource struct {
	cfg     EPAEJScreenConfig
	vintage string
}

// NewEPAEJScreenSource creates an EPAEJScreenSource from cfg.
func NewEPAEJScreenSource(cfg EPAEJScreenConfig) *epaEJScreenSource {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	vintage := "EPA-EJScreen"
	if cfg.Year > 0 {
		vintage = fmt.Sprintf("EPA-EJScreen-%d", cfg.Year)
	}
	return &epaEJScreenSource{cfg: cfg, vintage: vintage}
}

func (s *epaEJScreenSource) Name() string     { return "epa-ejscreen" }
func (s *epaEJScreenSource) Category() string { return "environment" }
func (s *epaEJScreenSource) Vintage() string  { return s.vintage }

func (s *epaEJScreenSource) Schema() []VariableDef {
	out := make([]VariableDef, len(ejscreenVariables))
	copy(out, ejscreenVariables)
	return out
}

// FetchCounty is not implemented for EJScreen via the REST API.
// Use the bulk CSV download at https://gaftp.epa.gov/EJSCREEN/ and the
// ingest/ejscreen_csv.py helper script instead.
func (s *epaEJScreenSource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	return nil, fmt.Errorf(
		"epa-ejscreen: FetchCounty not implemented (HTTP 501): "+
			"the EJScreen REST API requires geometry input and does not support bulk tract export. "+
			"Download the annual CSV from https://gaftp.epa.gov/EJSCREEN/ and use ingest/ejscreen_csv.py",
	)
}

// FetchState is not implemented for EJScreen via the REST API.
// Use the bulk CSV download at https://gaftp.epa.gov/EJSCREEN/ and the
// ingest/ejscreen_csv.py helper script instead.
func (s *epaEJScreenSource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	return nil, fmt.Errorf(
		"epa-ejscreen: FetchState not implemented (HTTP 501): "+
			"the EJScreen REST API requires geometry input and does not support bulk tract export. "+
			"Download the annual CSV from https://gaftp.epa.gov/EJSCREEN/ and use ingest/ejscreen_csv.py",
	)
}
