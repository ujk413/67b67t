#!/usr/bin/env bash
# Publish a signed APK so installed apps pick it up automatically.
#
#   scripts/publish_apk.sh <apk> [notes]
#
# Writes /opt/messenger67/data/releases/latest.json + the APK. The API reads
# those files on the next /app/version request, so nothing restarts and no
# container is rebuilt. Run this on the server (or over ssh).
#
# The APK must be signed with the same key as the installed app: Android
# rejects an update signed by a different key. Check that here instead of
# letting every phone fail at install time.
set -euo pipefail

APK="${1:?usage: publish_apk.sh <apk> [notes]}"
NOTES="${2:-}"
RELEASE_DIR="${RELEASE_DIR:-/opt/messenger67/data/releases}"
VERSION_FILE="${VERSION_FILE:-mobile/app.json}"

[ -f "$APK" ] || { echo "APK not found: $APK" >&2; exit 1; }

# CI already knows the version from app.json and passes it in, so the server
# does not need a copy of the mobile project.
read_version() {
  if [ -n "${VERSION:-}" ] && [ -n "${VERSION_CODE:-}" ]; then
    if [ "$1" = "version" ]; then echo "$VERSION"; else echo "$VERSION_CODE"; fi
    return
  fi
  [ -f "$VERSION_FILE" ] || {
    echo "Set VERSION and VERSION_CODE, or point VERSION_FILE at mobile/app.json" >&2
    exit 1
  }
  python3 - "$VERSION_FILE" "$1" <<'PY'
import json, sys
config = json.load(open(sys.argv[1], encoding='utf-8'))['expo']
key = sys.argv[2]
value = config['version'] if key == 'version' else config['android'][key]
print(value)
PY
}

VERSION="$(read_version version)"
VERSION_CODE="$(read_version versionCode)"

case "$VERSION_CODE" in
  ''|*[!0-9]*) echo "versionCode must be a positive integer, got '$VERSION_CODE'" >&2; exit 1 ;;
esac

# Fail loudly if the installed app was signed with a different key.
if command -v apksigner >/dev/null 2>&1; then
  SIGNER="$(apksigner verify --print-certs "$APK" 2>/dev/null | grep -m1 'SHA-256 digest' || true)"
  echo "signer: ${SIGNER:-unknown}"
fi

SIZE="$(stat -c%s "$APK")"
SHA256="$(sha256sum "$APK" | cut -d' ' -f1)"
NAME="67chat-${VERSION}-${VERSION_CODE}.apk"

install -d -m 0755 "$RELEASE_DIR"
# Copy under a temporary name so a client never downloads a half-written file,
# then move it into place atomically.
cp "$APK" "$RELEASE_DIR/.${NAME}.tmp"
mv -f "$RELEASE_DIR/.${NAME}.tmp" "$RELEASE_DIR/$NAME"

python3 - "$RELEASE_DIR/latest.json" "$VERSION" "$VERSION_CODE" "$NAME" "$SIZE" "$SHA256" "$NOTES" <<'PY'
import datetime, json, os, sys
target, version, code, name, size, sha256, notes = sys.argv[1:8]
manifest = {
    'version': version,
    'versionCode': int(code),
    'file': name,
    'size': int(size),
    'sha256': sha256,
    'notes': notes,
    'publishedAt': datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z'),
}
tmp = target + '.tmp'
with open(tmp, 'w', encoding='utf-8') as handle:
    json.dump(manifest, handle, ensure_ascii=False, indent=2)
os.replace(tmp, target)
PY

# Keep only the builds the current manifest points at, so the disk does not
# fill up with 170 MB APKs.
find "$RELEASE_DIR" -maxdepth 1 -name '67chat-*.apk' ! -name "$NAME" -delete

echo "published $VERSION (versionCode $VERSION_CODE), $((SIZE / 1024 / 1024)) MB, sha256 ${SHA256:0:16}…"
echo "manifest: $RELEASE_DIR/latest.json"
