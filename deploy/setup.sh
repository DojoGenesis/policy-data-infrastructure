#!/bin/bash
# NOTE: This Docker+nginx path does NOT feed api.policydatainfrastructure.com.
# Production runs via deploy/pdi.service (systemd unit -> pdi serve --port 8340,
# EnvironmentFile=/etc/pdi/env), installed directly on the VPS -- no Docker, no
# nginx. This script is an unused/alternate lane; treat it as such, not as a
# description of how the live API is actually deployed.
#
# One-time VPS setup script for policy-data-infrastructure
# Usage: ssh user@vps 'bash -s' < deploy/setup.sh
set -euo pipefail

# Install Docker if not present
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com | sh
fi

# Create app directory
mkdir -p /opt/pdi
cd /opt/pdi

# Create .env template
cat > .env.example <<'ENV'
PDI_DB_PASSWORD=changeme
PDI_PORT=8340
PDI_DB_PORT=5432
CENSUS_API_KEY=your_census_api_key
ENV

echo "Setup complete. Copy .env.example to .env and configure, then run: docker compose up -d"
