#!/usr/bin/env bash
# Print the Tor hidden service (.onion) hostname, if Tor has generated it.
# The tor container mounts ./data/tor, so the hostname file lives there.
# Remember to add the origin to ORIGIN in .env (comma-separated) for CORS, e.g.
#   ORIGIN=https://67chat.ru,http://<hostname>
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
file="${TOR_HOSTNAME_FILE:-$root/data/tor/hostname}"

if [ ! -s "$file" ]; then
  echo "No onion hostname yet ($file not found)." >&2
  echo "Check: docker compose ps tor && docker compose logs tor" >&2
  exit 1
fi

host="$(tr -d '\r\n' < "$file")"
echo "Onion address: http://$host"
echo "CORS hint:     ORIGIN=\${ORIGIN},http://$host"
