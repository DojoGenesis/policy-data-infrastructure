# ingest/Makefile — policy-data-infrastructure Python ingest scripts
#
# Required env vars:
#   STATE     — 2-digit state FIPS (e.g., 55 for Wisconsin)
#   COUNTY    — 3-digit county FIPS (e.g., 025 for Dane) [optional for county/state levels]
#   YEAR      — ACS/TIGER vintage year (e.g., 2023)
#   DATA_DIR  — path to atlas-format JSON files (for 'load' target)
#
# Optional env vars:
#   GEO_LEVEL   — tract (default) | block_group | county | state
#   TIGER_LEVELS — space-separated list: tract county (default: tract)
#   PDI_DATABASE_URL — postgres://... (default: postgres://pdi:pdi@localhost:5432/pdi)
#   CENSUS_API_KEY   — Census Bureau API key (optional; raises rate limit)
#
# Examples:
#   make install
#   make fetch-acs STATE=55 COUNTY=025 YEAR=2023
#   make fetch-tiger STATE=55 YEAR=2023 TIGER_LEVELS="tract county"
#   make fetch-all STATE=55 COUNTY=025 YEAR=2023
#   make load DATA_DIR=../CWD/atlas/data
#   make dry-run-acs STATE=55 COUNTY=025 YEAR=2023

STATE     ?= 55
COUNTY    ?= 025
YEAR      ?= 2023
GEO_LEVEL ?= tract
TIGER_LEVELS ?= tract
DATA_DIR  ?= ../CWD/atlas/data

.PHONY: install fetch-acs fetch-tiger fetch-all load dry-run-acs dry-run-tiger dry-run-load

install:
	pip install -r requirements.txt

## Fetch ACS data and write to PostgreSQL
fetch-acs:
	python fetch_acs.py --state $(STATE) --county $(COUNTY) --year $(YEAR) --geo-level $(GEO_LEVEL)

## Fetch TIGER boundaries and write to PostGIS
fetch-tiger:
	python fetch_tiger.py --state $(STATE) --year $(YEAR) --levels $(TIGER_LEVELS)

## Fetch TIGER first (geographies needed before indicators), then ACS
fetch-all: fetch-tiger fetch-acs

## Load existing atlas JSON files into PostgreSQL
load:
	python load_to_postgres.py --data-dir $(DATA_DIR)

## Dry runs (fetch/process but no DB write)
dry-run-acs:
	python fetch_acs.py --state $(STATE) --county $(COUNTY) --year $(YEAR) --geo-level $(GEO_LEVEL) --dry-run

dry-run-tiger:
	python fetch_tiger.py --state $(STATE) --year $(YEAR) --levels $(TIGER_LEVELS) --dry-run

dry-run-load:
	python load_to_postgres.py --data-dir $(DATA_DIR) --dry-run
