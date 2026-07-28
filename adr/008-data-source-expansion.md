# ADR-008: Data Source Expansion — Crime, Broadband, and Social Vulnerability

**Status:** Accepted
**Date:** 2026-07-27
**Deciders:** Cruz Morales, Hermes Agent

## Context

PDI ingests 13 federal and state data sources covering demographics, health, housing, environment, economic, transit, food access, and education. Three critical policy-relevant domains were missing: crime/justice, digital infrastructure, and social vulnerability. Each of these is directly tied to equity dimensions in the candidate policy tracker and evidence card system.

The FBI NIBRS, FCC Broadband (Form 477), and CDC Social Vulnerability Index (SVI) are all publicly available at county and tract levels with no API key requirement for basic access (FBI requires a key for the API but offers CSV downloads).

## Decision

Add three new data sources following the established PDI adapter pattern:

### CDC SVI (Social Vulnerability Index)
- **Variables:** `cdc_svi_overall`, `cdc_svi_socioeconomic`, `cdc_svi_household`, `cdc_svi_racial_ethnic`, `cdc_svi_housing_transport`
- **Level:** county, tract
- **Vintage:** biennial (2022 available, 2024 pending)
- **Format:** CSV download from CDC/ATSDR
- **Direction:** `lower_is_better` (higher percentile = more vulnerable)
- **Unit:** percentile (0.0–1.0)

### FBI NIBRS (National Incident-Based Reporting System)
- **Variables:** `fbi_violent_crime_rate`, `fbi_property_crime_rate`
- **Level:** county (state also available)
- **Vintage:** annual (most recent available)
- **Format:** CSV download from FBI CDE; API key required for programmatic access (`FBI_CDE_API_KEY`)
- **Direction:** `lower_is_better`
- **Unit:** `rate_per_100k`

### FCC Broadband (Form 477)
- **Variables:** `fcc_broadband_access_pct`, `fcc_multiple_providers_pct`
- **Level:** county, tract
- **Vintage:** semi-annual (June and December)
- **Format:** CSV download from FCC.gov
- **Direction:** `higher_better`
- **Unit:** `percent` or `per_1000_households`

## Pattern

Each source follows the established PDI integration pattern:
1. `data/sources.toml` entry
2. `ingest/fetch_X.py` — standalone CLI with `--dry-run` → `--year` → `--load` flow
3. `pkg/datasource/X.go` — Go adapter implementing `DataSource` interface
4. `pkg/datasource/X_test.go` — minimum `TestNew` validating defaults
5. Registration in `cmd/pdi/fetch.go` and `cmd/pdi/pipeline.go`
6. Migration for `indicator_meta` entries (idempotent, `ON CONFLICT DO NOTHING`)

All three sources were built in parallel via agent dispatch (deleg_3fe9e1c5), producing 15 new files with 9 new indicator variables. Post-dispatch integration verified: `go build`, `go vet`, `go test ./... -short` all pass.

## Consequences

- **Positive:** Coverage expands from 42 to 51 variables across 16 sources — crime, broadband, and social vulnerability are directly relevant to 12+ policy positions in the Francesca Hong tracker
- **Positive:** All three sources are national-ready — CSV downloads include all US counties/tracts, not WI-only
- **Negative:** FBI API requires a key for programmatic access; the ingest script documents this in `--dry-run` output. CSV bulk download path exists but format may change between vintages
- **Negative:** CDC SVI release cadence (biennial) lags behind annual ACS data; users must be aware of vintage mismatch when comparing SVI to ACS indicators
- **Negative:** FCC Form 477 column names vary between vintages; the ingest script normalizes via a mapping table that must be maintained

## Migration Numbers

- 009: CDC SVI (`cdc_svi_meta`)
- 010: FBI NIBRS (`fbi_nibrs_meta`)
- 011: FCC Broadband (`fcc_broadband`)
