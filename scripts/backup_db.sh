#!/usr/bin/env bash
# Postgres backup for 67chat: pg_dump inside the postgres container, gzip, retention.
#
# Cron example (daily at 03:15, keep 14 days), run from the project root:
#   15 3 * * * cd /opt/67chat && BACKUP_DIR=/var/backups/67chat RETENTION_DAYS=14 ./scripts/backup_db.sh >> /var/log/67chat-backup.log 2>&1
#
# Environment:
#   BACKUP_DIR      target directory            (default: ./data/backups)
#   RETENTION_DAYS  delete dumps older than N days (default: 14; 0 disables pruning)
#   PG_CONTAINER    postgres container name     (default: messenger67-postgres)
#   POSTGRES_USER / POSTGRES_DB  taken from the container environment when unset
#
# NOTE: uploaded media (./data/uploads) is NOT included; back it up separately
# (or use S3 storage). Restore:  gunzip -c FILE.sql.gz | docker exec -i messenger67-postgres psql -U messenger -d messenger
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-./data/backups}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"
PG_CONTAINER="${PG_CONTAINER:-messenger67-postgres}"

if ! [[ "$RETENTION_DAYS" =~ ^[0-9]+$ ]]; then
  echo "RETENTION_DAYS must be a non-negative integer" >&2
  exit 2
fi

mkdir -p "$BACKUP_DIR"
umask 077

stamp="$(date +%Y%m%d_%H%M%S)"
out="$BACKUP_DIR/messenger_${stamp}.sql.gz"
tmp="$out.partial"
trap 'rm -f "$tmp"' EXIT

# Resolve credentials inside the container so no secrets pass through this script.
docker exec "$PG_CONTAINER" sh -c 'pg_dump -U "${POSTGRES_USER:-messenger}" -d "${POSTGRES_DB:-messenger}" --no-owner --clean --if-exists' \
  | gzip -9 > "$tmp"

# Refuse to keep an empty/broken dump.
if ! gzip -t "$tmp" || [ "$(stat -c %s "$tmp" 2>/dev/null || wc -c < "$tmp")" -lt 200 ]; then
  echo "Backup failed: dump is empty or corrupt" >&2
  exit 1
fi
mv "$tmp" "$out"
trap - EXIT
echo "Backup written: $out ($(du -h "$out" | cut -f1))"

if [ "$RETENTION_DAYS" -gt 0 ]; then
  find "$BACKUP_DIR" -maxdepth 1 -type f -name 'messenger_*.sql.gz' -mtime +"$RETENTION_DAYS" -print -delete \
    | sed 's/^/Pruned: /'
fi
