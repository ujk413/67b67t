#!/bin/sh
set -eu

DOMAIN="${DOMAIN:-67chat.ru}"
EXPECTED_IP="${EXPECTED_IP:-141.105.68.207}"
PROJECT_DIR="${PROJECT_DIR:-/root/67b67tmess}"

cd "$PROJECT_DIR"

resolved="$(getent ahostsv4 "$DOMAIN" | awk '{print $1}' | sort -u | tr '\n' ' ')"
case " $resolved " in
  *" $EXPECTED_IP "*) ;;
  *)
    echo "DNS is not ready for $DOMAIN."
    echo "Expected: $EXPECTED_IP"
    echo "Resolved: ${resolved:-none}"
    exit 1
    ;;
esac

cp Caddyfile.67chat Caddyfile

if grep -q '^ORIGIN=' .env; then
  sed -i "s|^ORIGIN=.*|ORIGIN=https://$DOMAIN|" .env
else
  printf '\nORIGIN=https://%s\n' "$DOMAIN" >> .env
fi

if grep -q '^TURN_REALM=' .env; then
  sed -i "s|^TURN_REALM=.*|TURN_REALM=$DOMAIN|" .env
else
  printf 'TURN_REALM=%s\n' "$DOMAIN" >> .env
fi

if grep -q '^TURN_EXTERNAL_IP=' .env; then
  sed -i "s|^TURN_EXTERNAL_IP=.*|TURN_EXTERNAL_IP=$EXPECTED_IP|" .env
else
  printf 'TURN_EXTERNAL_IP=%s\n' "$EXPECTED_IP" >> .env
fi

docker compose up -d --force-recreate api caddy turn
sleep 5
docker compose ps
curl -fsS "https://$DOMAIN/api/health"
echo
echo "$DOMAIN is active."
