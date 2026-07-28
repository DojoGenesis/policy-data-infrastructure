#!/bin/bash
# deploy/seed.sh — Post-deploy seed: factor scores + LISA data on VPS.
#
# Run this AFTER each deploy to refresh analysis data in the database.
# It runs two analysis pipelines:
#   1. factor_analysis.py --load  — Exploratory Factor Analysis (SDOH factors)
#   2. spatial_lisa.py            — Local Moran's I spatial clustering
#
# Usage:
#   ./deploy/seed.sh              # run locally (DB must be reachable)
#   ./deploy/seed.sh --vps        # SCP files to dojo-gateway, run there
#
# The --vps path:
#   - SCPs analysis scripts + ingest/lib to /tmp/pdi-seed/ on the VPS
#   - Symlinks analysis/output/ from the deployed app (/opt/pdi/analysis/output/)
#     so the scripts can read generated data files (CSVs, GeoJSON)
#   - Runs both scripts via SSH with PDI_DATABASE_URL from the VPS env
#   - Cleans up /tmp/pdi-seed/ on completion
#
# Prerequisites (VPS):
#   - Python 3.11+ with: numpy, scikit-learn, factor-analyzer, psycopg,
#     geopandas, esda, libpysal
#   - PDI_DATABASE_URL set in the SSH environment
#   - SSH access to dojo-gateway

set -euo pipefail

VPS_HOST="${VPS_HOST:-dojo-gateway}"
REMOTE_DIR="/tmp/pdi-seed"
STATE_FIPS="55"
LISA_VARIABLES="poverty_rate,median_hh_income,pct_cost_burdened,uninsured_rate,pct_poc"

# ---------------------------------------------------------------------------
# Determine repo root (works whether run from repo root or deploy/ subdir)
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------------------------------------------------------------------------
# Files to SCP to the VPS (scripts + library code only).
# Generated data files (CSVs, GeoJSON) live in analysis/output/ which is
# .gitignored — they're produced by the ingest pipeline and already exist on
# the VPS at /opt/pdi/analysis/output/.  We symlink that directory on the
# VPS instead of shipping the data files across.
SCP_FILES=(
    "analysis/factor_analysis.py"
    "analysis/spatial_lisa.py"
    "ingest/lib/__init__.py"
    "ingest/lib/db.py"
)

# VPS app root (where the deployed app + generated data files live)
VPS_APP_ROOT="${VPS_APP_ROOT:-/opt/pdi}"

# ---------------------------------------------------------------------------
# Helper: print a section banner
# ---------------------------------------------------------------------------
banner() {
    echo
    echo "=============================================="
    echo "  $*"
    echo "=============================================="
}

# ---------------------------------------------------------------------------
# Helper: run factor analysis
# ---------------------------------------------------------------------------
run_factor_analysis() {
    local workdir="$1"
    banner "Step 1: Factor Analysis (EFA)"
    # NOTE: the script flag is --load, not --store.
    # --load writes factor scores to the factor_scores table.
    (cd "$workdir" && python3 analysis/factor_analysis.py --load)
}

# ---------------------------------------------------------------------------
# Helper: run spatial LISA
# ---------------------------------------------------------------------------
run_spatial_lisa() {
    local workdir="$1"
    banner "Step 2: Spatial LISA"
    (cd "$workdir" && python3 analysis/spatial_lisa.py \
        --state "$STATE_FIPS" \
        --variables "$LISA_VARIABLES")
}

# ---------------------------------------------------------------------------
# Helper: SCP script + lib files, symlink data directory on VPS
# ---------------------------------------------------------------------------
setup_vps_workspace() {
    echo "Creating remote directory structure..."
    ssh "$VPS_HOST" "rm -rf $REMOTE_DIR && mkdir -p $REMOTE_DIR/analysis $REMOTE_DIR/ingest/lib"

    echo "Copying scripts + library to $VPS_HOST:$REMOTE_DIR ..."
    for f in "${SCP_FILES[@]}"; do
        local_src="$REPO_ROOT/$f"
        if [[ -f "$local_src" ]]; then
            echo "  $f"
            scp -q "$local_src" "$VPS_HOST:$REMOTE_DIR/$f"
        else
            echo "  ERROR: missing local file: $local_src" >&2
            exit 1
        fi
    done

    echo "Linking data directory: $REMOTE_DIR/analysis/output -> $VPS_APP_ROOT/analysis/output"
    ssh "$VPS_HOST" "ln -s $VPS_APP_ROOT/analysis/output $REMOTE_DIR/analysis/output"
}

# ===========================================================================
# MAIN
# ===========================================================================

if [[ "${1:-}" == "--vps" ]]; then
    # -----------------------------------------------------------------------
    # VPS path: SCP → SSH → run → cleanup
    # -----------------------------------------------------------------------
    banner "Seeding via VPS ($VPS_HOST)"

    setup_vps_workspace

    echo "Running analysis on VPS..."
    run_factor_analysis "$REMOTE_DIR"
    run_spatial_lisa "$REMOTE_DIR"

    banner "Cleaning up $REMOTE_DIR on VPS"
    ssh "$VPS_HOST" "rm -rf $REMOTE_DIR"

    echo
    echo "Seed complete on $VPS_HOST."

else
    # -----------------------------------------------------------------------
    # Local path: run from repo root
    # -----------------------------------------------------------------------
    banner "Seeding locally"

    run_factor_analysis "$REPO_ROOT"
    run_spatial_lisa "$REPO_ROOT"

    echo
    echo "Seed complete (local)."
fi