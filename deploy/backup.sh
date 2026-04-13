#!/bin/bash
# Backup PostgreSQL data
set -euo pipefail
BACKUP_DIR="/opt/pdi/backups"
mkdir -p "$BACKUP_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
docker compose exec -T postgres pg_dump -U pdi pdi | gzip > "$BACKUP_DIR/pdi_${TIMESTAMP}.sql.gz"
# Keep last 30 days
find "$BACKUP_DIR" -name "*.sql.gz" -mtime +30 -delete
echo "Backup saved: pdi_${TIMESTAMP}.sql.gz"
