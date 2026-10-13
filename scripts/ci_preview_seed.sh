#!/usr/bin/env bash
# Seed a throwaway backend with users, chats and messages so the preview
# screenshots show a populated chat list instead of an empty state.
set -euo pipefail

B="${1:-http://localhost:3999}"
PASS='Passw0rd!long'
J=/tmp/seed_onyx.txt
J2=/tmp/seed_mira.txt
J3=/tmp/seed_dima.txt
rm -f "$J" "$J2" "$J3"

reg() { # reg <jar> <username>
  curl -s -c "$1" -H 'content-type: application/json' \
    -d "{\"username\":\"$2\",\"password\":\"$PASS\",\"captcha\":\"x\"}" \
    "$B/auth/register" > /dev/null
}

send() { # send <jar> <room_id> <text>
  curl -s -b "$1" -H 'content-type: application/json' \
    -d "$(python3 -c 'import json,sys;print(json.dumps({"type":"text","text":sys.argv[1],"meta":{"encrypted":False}}))' "$3")" \
    "$B/rooms/$2/messages" > /dev/null
}

room_id() { grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4; }

echo "→ регистрирую пользователей"
reg "$J"  onyx
reg "$J2" mira
reg "$J3" dima

echo "→ создаю чаты"
DM=$(curl -s -b "$J" -H 'content-type: application/json' \
      -d '{"kind":"dm","members":["mira"]}' "$B/rooms" | room_id)
GRP=$(curl -s -b "$J" -H 'content-type: application/json' \
      -d '{"kind":"group","title":"Роблокс пацаны","members":["mira","dima"]}' "$B/rooms" | room_id)
CH=$(curl -s -b "$J" -H 'content-type: application/json' \
      -d '{"kind":"channel","title":"Новости 67"}' "$B/rooms" | room_id)

echo "→ заливаю сообщения"
send "$J2" "$DM"  "привет, ты уже собрал апк?"
send "$J"  "$DM"  "да, пайплайн на гитхабе собирает"
send "$J2" "$DM"  "о, круто. а интерфейс переделывать будем?"
send "$J"  "$DM"  "да, с этого и начнём"

send "$J3" "$GRP" "го в роблокс вечером"
send "$J2" "$GRP" "я в деле"
send "$J"  "$GRP" "скинул новый билд в релизы"

send "$J"  "$CH"  "Мессенджер 67 — сборка обновлена"
send "$J"  "$CH"  "Теперь APK собирается автоматически на каждый пуш"

echo "→ готово: dm=$DM group=$GRP channel=$CH"
