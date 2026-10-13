#!/usr/bin/env sh
set -eu

BASE_URL="${1:-https://67chat.ru}"
USER_PATH="${2:-/user/Hitler}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

check_api_json() {
  path="$1"
  expected="$2"
  body="$tmpdir/body.txt"
  headers="$tmpdir/headers.txt"

  code="$(curl -ksS -o "$body" -D "$headers" -w "%{http_code}" "$BASE_URL$path")"
  content_type="$(awk 'BEGIN{IGNORECASE=1} /^content-type:/ {gsub("\r",""); print $2}' "$headers" | tail -n 1)"

  printf '%s -> status=%s content-type=%s\n' "$path" "$code" "$content_type"

  [ "$code" = "$expected" ] || {
    echo "FAIL: expected $expected for $path"
    exit 1
  }

  echo "$content_type" | grep -qi 'application/json' || {
    echo "FAIL: expected JSON response for $path"
    exit 1
  }

  if grep -qi '<!doctype html>' "$body"; then
    echo "FAIL: $path returned HTML"
    exit 1
  fi
}

check_frontend_html() {
  path="$1"
  body="$tmpdir/body.txt"
  headers="$tmpdir/headers.txt"

  code="$(curl -ksS -o "$body" -D "$headers" -w "%{http_code}" "$BASE_URL$path")"
  content_type="$(awk 'BEGIN{IGNORECASE=1} /^content-type:/ {gsub("\r",""); print $2}' "$headers" | tail -n 1)"

  printf '%s -> status=%s content-type=%s\n' "$path" "$code" "$content_type"

  [ "$code" = "200" ] || {
    echo "FAIL: expected 200 for $path"
    exit 1
  }

  grep -qi '<!doctype html>' "$body" || {
    echo "FAIL: expected SPA HTML for $path"
    exit 1
  }
}

check_static_js() {
  path="$1"
  body="$tmpdir/body.txt"
  headers="$tmpdir/headers.txt"

  code="$(curl -ksS -o "$body" -D "$headers" -w "%{http_code}" "$BASE_URL$path")"
  content_type="$(awk 'BEGIN{IGNORECASE=1} /^content-type:/ {gsub("\r",""); print $2}' "$headers" | tail -n 1)"

  printf '%s -> status=%s content-type=%s\n' "$path" "$code" "$content_type"

  [ "$code" = "200" ] || {
    echo "FAIL: expected 200 for $path"
    exit 1
  }

  echo "$content_type" | grep -Eqi 'javascript|ecmascript|text/plain' || {
    echo "FAIL: expected JS content-type for $path"
    exit 1
  }

  if grep -qi '<!doctype html>' "$body"; then
    echo "FAIL: $path returned HTML"
    exit 1
  fi
}

check_api_json "/api/health" "200"
check_api_json "/api/me" "401"
check_frontend_html "$USER_PATH"
check_static_js "/app.js"

echo "Smoke checks passed."
