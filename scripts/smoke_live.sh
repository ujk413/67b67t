#!/usr/bin/env bash
# Live smoke test against a locally running API (default http://localhost:3999).
# Usage: scripts/smoke_live.sh [base_url]
B="${1:-http://localhost:3999}"
J=/tmp/smoke_jar.txt; rm -f "$J"
pass=0; fail=0
check() { if [ "$2" = "$3" ]; then echo "ok   $1"; pass=$((pass+1)); else echo "FAIL $1 (got '$2', want '$3')"; fail=$((fail+1)); fi; }
code() { curl -s -o /tmp/smoke_body.txt -w '%{http_code}' "$@"; }
first_id() { grep -o '"id":"[^"]*"' /tmp/smoke_body.txt | head -1 | cut -d'"' -f4; }
U="smoke$RANDOM"

check register "$(code -c $J -H 'content-type: application/json' -d "{\"username\":\"$U\",\"password\":\"Passw0rd!long\",\"captcha\":\"x\"}" $B/auth/register)" 200
check me "$(code -b $J $B/me)" 200
check ice-servers "$(code -b $J $B/rtc/ice-servers)" 200
grep -q '"credential"' /tmp/smoke_body.txt && check ice-has-turn-cred ok ok || check ice-has-turn-cred missing ok
check ice-unauth "$(code $B/rtc/ice-servers)" 401

code -b $J -H 'content-type: application/json' -d '{"kind":"group","title":"smoke"}' $B/rooms >/dev/null
RID=$(first_id)
check room-created "$([ -n "$RID" ] && echo ok)" ok
check members-page "$(code -b $J "$B/rooms/$RID/members?limit=1&offset=0")" 200
check send-msg "$(code -b $J -H 'content-type: application/json' -d '{"type":"text","text":"hi","meta":{"encrypted":false}}' $B/rooms/$RID/messages)" 200
MID=$(first_id)
check edit-plain "$(code -b $J -X PUT -H 'content-type: application/json' -d '{"text":"hi2"}' $B/rooms/$RID/messages/$MID)" 200

# Big payloads go through files (command-line length limits), paths in native form for curl.
T="$(mktemp -d)"; TW="$(cygpath -m "$T" 2>/dev/null || echo "$T")"
{ printf '{"type":"text","encrypted_content":"{\\"a\\":\\"'; head -c 150000 /dev/zero | tr '\000' 'a'; printf '\\"}"}'; } > "$T/big.json"
check e2e-too-large "$(code -b $J -H 'content-type: application/json' --data-binary @"$TW/big.json" $B/rooms/$RID/messages)" 413

printf 'same-bytes' > "$T/f.txt"
up() { : > /tmp/smoke_body.txt; code -b $J -F "file=@$TW/f.txt;type=text/plain" "$B/media/upload?kind=file"; }
c1=$(up); id1=$(first_id)
c2=$(up); id2=$(first_id)
check upload-1 "$c1" 200
check upload-2 "$c2" 200
check upload-dedup "$([ -n "$id1" ] && [ "$id1" = "$id2" ] && echo ok)" ok
rm -rf "$T"

# Group E2E (Sender Keys) checks: a second member, the E2E flag, and the key inbox.
J2=/tmp/smoke_jar2.txt; J3=/tmp/smoke_jar3.txt; rm -f "$J2" "$J3"
code -c $J2 -H 'content-type: application/json' -d "{\"username\":\"${U}b\",\"password\":\"Passw0rd!long\",\"captcha\":\"x\"}" $B/auth/register >/dev/null
code -b $J2 $B/me >/dev/null; U2ID=$(first_id)
code -c $J3 -H 'content-type: application/json' -d "{\"username\":\"${U}c\",\"password\":\"Passw0rd!long\",\"captcha\":\"x\"}" $B/auth/register >/dev/null

G=$(code -b $J -H 'content-type: application/json' -d '{"kind":"group","title":"e2e"}' $B/rooms); GID=$(first_id)
check group-created "$([ -n "$GID" ] && echo ok)" ok
check add-member "$(code -b $J -H 'content-type: application/json' -d "{\"user_id\":\"$U2ID\"}" $B/rooms/$GID/members)" 200
check e2e-off-by-default "$(code -b $J $B/rooms/$GID/e2e)" 200
grep -q '"enabled":false' /tmp/smoke_body.txt && check e2e-off-by-default-value ok ok || check e2e-off-by-default-value on ok
check e2e-enable "$(code -b $J -X PUT -H 'content-type: application/json' -d '{"enabled":true}' $B/rooms/$GID/e2e)" 200
check e2e-by-member-forbidden "$(code -b $J2 -X PUT -H 'content-type: application/json' -d '{"enabled":false}' $B/rooms/$GID/e2e)" 403
check plaintext-rejected "$(code -b $J -H 'content-type: application/json' -d '{"type":"text","text":"hi","meta":{"encrypted":false}}' $B/rooms/$GID/messages)" 400
check ciphertext-accepted "$(code -b $J -H 'content-type: application/json' -d '{"type":"text","encrypted_content":"{\"v\":1,\"kid\":\"k1\",\"n\":0,\"ct\":\"AA==\",\"sig\":\"AA==\"}","meta":{"e2e_group":true,"v":1}}' $B/rooms/$GID/messages)" 200

check sender-key-post "$(code -b $J -H 'content-type: application/json' -d "{\"key_id\":\"k1\",\"recipients\":[{\"user_id\":\"$U2ID\",\"payload\":\"p1\"}]}" $B/rooms/$GID/sender-keys)" 200
check sender-key-post-outsider-rejected "$(code -b $J -H 'content-type: application/json' -d '{"key_id":"k1","recipients":[{"user_id":"00000000-0000-0000-0000-000000000000","payload":"p"}]}' $B/rooms/$GID/sender-keys)" 400
check sender-key-inbox-by-non-member "$(code -b $J3 $B/rooms/$GID/sender-keys)" 404
check sender-key-inbox "$(code -b $J2 $B/rooms/$GID/sender-keys)" 200
grep -q '"payload":"p1"' /tmp/smoke_body.txt && check sender-key-inbox-has-payload ok ok || check sender-key-inbox-has-payload missing ok
KID=$(first_id)
check sender-key-ack "$(code -b $J2 -X DELETE $B/rooms/$GID/sender-keys/$KID)" 200
check sender-key-inbox-empty-after-ack "$(code -b $J2 $B/rooms/$GID/sender-keys)" 200
grep -q '^\[\]$' /tmp/smoke_body.txt && check sender-key-inbox-empty ok ok || check sender-key-inbox-empty not-empty ok

check remove-member "$(code -b $J -X DELETE $B/rooms/$GID/members/$U2ID)" 200
check removed-member-inbox-denied "$(code -b $J2 $B/rooms/$GID/sender-keys)" 404
check removed-member-post-denied "$(code -b $J2 -H 'content-type: application/json' -d "{\"key_id\":\"k2\",\"recipients\":[{\"user_id\":\"$U2ID\",\"payload\":\"p\"}]}" $B/rooms/$GID/sender-keys)" 404

echo "pass=$pass fail=$fail"; [ "$fail" = 0 ]
