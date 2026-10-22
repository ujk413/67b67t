#!/usr/bin/env bash
# Keep the Android release signing material safe from `expo prebuild --clean`.
#
#   scripts/android_signing.sh backup    # android/ -> secrets/android-signing/
#   scripts/android_signing.sh restore   # secrets/android-signing/ -> android/
#   scripts/android_signing.sh check     # verify the APK key matches the store
#
# `expo prebuild --clean` deletes mobile/android/ entirely, which takes the
# release keystore and key.properties with it. The keystore cannot be
# regenerated: every installed app is pinned to its key, and an update signed
# with a different key is refused by Android. secrets/ is gitignored.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ANDROID="$ROOT/mobile/android"
BACKUP="$ROOT/secrets/android-signing"

case "${1:-}" in
  backup)
    install -d -m 0700 "$BACKUP"
    cp "$ANDROID/keystores/messenger67-release.keystore" "$BACKUP/"
    cp "$ANDROID/key.properties" "$BACKUP/"
    chmod 600 "$BACKUP"/*
    echo "backed up signing material to $BACKUP"
    ;;
  restore)
    [ -f "$BACKUP/messenger67-release.keystore" ] || {
      echo "no backup in $BACKUP — restore from CI secret ANDROID_KEYSTORE_BASE64" >&2
      exit 1
    }
    install -d -m 0755 "$ANDROID/keystores"
    cp "$BACKUP/messenger67-release.keystore" "$ANDROID/keystores/"
    cp "$BACKUP/key.properties" "$ANDROID/key.properties"
    chmod 600 "$ANDROID/keystores/messenger67-release.keystore" "$ANDROID/key.properties"
    echo "restored signing material into $ANDROID"
    ;;
  check)
    APK="${2:?usage: android_signing.sh check <apk>}"
    STORE="$ANDROID/keystores/messenger67-release.keystore"
    PASS="$(sed -n 's/^storePassword=//p' "$ANDROID/key.properties")"
    ALIAS="$(sed -n 's/^keyAlias=//p' "$ANDROID/key.properties")"
    SIGNER="$(command -v apksigner || true)"
    [ -n "$SIGNER" ] || SIGNER="${ANDROID_HOME:-$HOME/Android/Sdk}/build-tools/36.0.0/apksigner"
    APK_KEY="$("$SIGNER" verify --print-certs "$APK" | sed -n 's/.*SHA-256 digest: //p' | head -1)"
    STORE_KEY="$(keytool -list -v -keystore "$STORE" -storepass "$PASS" -alias "$ALIAS" \
      | sed -n 's/.*SHA256: //p' | head -1 | tr -d ':' | tr 'A-Z' 'a-z')"
    if [ "$APK_KEY" != "$STORE_KEY" ]; then
      echo "MISMATCH: APK is signed with $APK_KEY but the store holds $STORE_KEY" >&2
      echo "Android would refuse this update on every installed device." >&2
      exit 1
    fi
    echo "signing key matches: $APK_KEY"
    ;;
  *)
    echo "usage: $0 backup|restore|check <apk>" >&2
    exit 1
    ;;
esac
