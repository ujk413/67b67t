#!/usr/bin/env bash
# Drive the freshly installed preview APK on a booted emulator: log in, then
# capture the screens we care about.
set -uo pipefail

OUT="${1:-preview-shots}"
PKG="ru.b67t.messenger.mobile"
USER_NAME="onyx"
PASSWORD="Passw0rd!long"
mkdir -p "$OUT"

log() { echo "  $*"; }

shot() {
  adb exec-out screencap -p > "$OUT/$1.png"
  log "снял $1.png ($(stat -c%s "$OUT/$1.png") байт)"
}

dump_ui() {
  adb shell uiautomator dump /sdcard/ui.xml >/dev/null 2>&1 || true
  adb pull /sdcard/ui.xml /tmp/ui.xml >/dev/null 2>&1 || true
}

# Centre of the first node whose text/content-desc contains $1.
center_of() {
  dump_ui
  [ -s /tmp/ui.xml ] || return 1
  python3 - "$1" <<'PY'
import re, sys, xml.etree.ElementTree as ET
want = sys.argv[1].lower()
try:
    root = ET.parse('/tmp/ui.xml').getroot()
except Exception:
    sys.exit(1)
for n in root.iter('node'):
    blob = f"{n.get('text') or ''}|{n.get('content-desc') or ''}".lower()
    if want in blob:
        nums = list(map(int, re.findall(r'\d+', n.get('bounds') or '')))
        if len(nums) == 4:
            x1, y1, x2, y2 = nums
            print((x1 + x2) // 2, (y1 + y2) // 2)
            sys.exit(0)
sys.exit(1)
PY
}

tap_at() { adb shell input tap $1; sleep 2; }

# The emulator software-renders the screen; under load system_server throws ANR
# dialogs, and the AOSP keyboard asks for contacts the moment it opens. Either
# one parks on top of the app and swallows every tap, so clear them first.
dismiss_dialogs() {
  local tries=0
  while [ $tries -lt 6 ]; do
    local handled=0
    if center_of "isn't responding" >/dev/null 2>&1; then
      log "ANR-диалог — закрываю"
      if center_of "Wait" > /tmp/dpos; then tap_at "$(cat /tmp/dpos)"; else adb shell input keyevent KEYCODE_BACK; sleep 2; fi
      handled=1
    elif center_of "has stopped" >/dev/null 2>&1; then
      log "диалог падения — закрываю"
      if center_of "Close app" > /tmp/dpos; then tap_at "$(cat /tmp/dpos)"; else adb shell input keyevent KEYCODE_BACK; sleep 2; fi
      handled=1
    elif center_of "to access your" >/dev/null 2>&1; then
      log "запрос разрешения — отклоняю"
      if center_of "DENY" > /tmp/dpos; then tap_at "$(cat /tmp/dpos)"; else adb shell input keyevent KEYCODE_BACK; sleep 2; fi
      handled=1
    fi
    [ $handled -eq 1 ] || return 0
    sleep 3
    tries=$((tries + 1))
  done
}

tap_text() {
  local coords
  dismiss_dialogs
  if coords=$(center_of "$1"); then
    log "тапаю «$1» по $coords"
    tap_at "$coords"
    return 0
  fi
  log "!! не нашёл «$1»"
  return 1
}

type_into() { # type_into <label> <text>
  tap_text "$1" || return 1
  dismiss_dialogs
  adb shell input text "$2"
  sleep 2
  dismiss_dialogs
}

# ── ждём, пока эмулятор реально догрузится ────────────────────────────────
echo "→ жду загрузку эмулятора"
adb wait-for-device
for _ in $(seq 1 90); do
  [ "$(adb shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')" = "1" ] && break
  sleep 5
done
log "boot_completed=$(adb shell getprop sys.boot_completed | tr -d '\r')"

echo "→ снижаю разрешение: software-рендер не тянет 1080x1920"
adb shell wm size 720x1280
adb shell wm density 320
sleep 15

# Keep the on-screen keyboard from popping a contacts permission dialog.
for ime in com.android.inputmethod.latin com.google.android.inputmethod.latin; do
  adb shell pm grant "$ime" android.permission.READ_CONTACTS >/dev/null 2>&1 || true
  adb shell cmd appops set "$ime" READ_CONTACTS deny >/dev/null 2>&1 || true
done

adb shell settings put global hide_error_dialogs 1
adb shell settings put global window_animation_scale 0
adb shell settings put global transition_animation_scale 0
adb shell settings put global animator_duration_scale 0
adb shell wm dismiss-keyguard || true

echo "→ даю системе успокоиться"
sleep 45
dismiss_dialogs

echo "→ запускаю приложение"
adb shell am force-stop "$PKG" || true
adb shell am start -n "$PKG/.MainActivity" >/dev/null
sleep 30
dismiss_dialogs

echo "→ экран входа"
shot 01-login

echo "→ логинюсь как $USER_NAME"
for attempt in 1 2 3; do
  log "попытка $attempt"
  type_into "Username" "$USER_NAME"
  type_into "Password" "$PASSWORD"
  adb shell input keyevent KEYCODE_BACK   # спрятать клавиатуру, иначе кнопка под ней
  sleep 3
  dismiss_dialogs
  tap_text "Login" || tap_text "Войти" || true
  sleep 25
  dismiss_dialogs
  center_of "Username" >/dev/null 2>&1 || { log "вошли"; break; }
  log "форма входа на месте"
done

echo "→ главный экран"
shot 02-main

echo "→ личка"
if coords=$(center_of "mira"); then
  tap_at "$coords"; sleep 12; dismiss_dialogs; shot 03-chat
  adb shell input keyevent KEYCODE_BACK; sleep 6
else
  log "!! чат mira не найден"
fi

echo "→ группа"
if coords=$(center_of "Роблокс"); then
  tap_at "$coords"; sleep 12; dismiss_dialogs; shot 04-group
  adb shell input keyevent KEYCODE_BACK; sleep 6
else
  log "!! группа не найдена"
fi

echo "→ профиль"
if coords=$(center_of "onyx"); then
  tap_at "$coords"; sleep 10; dismiss_dialogs; shot 05-profile
  adb shell input keyevent KEYCODE_BACK; sleep 4
else
  log "!! профиль не найден"
fi

# Keep the last UI dump around: it is what the script actually saw.
cp /tmp/ui.xml "$OUT/ui-last.xml" 2>/dev/null || true
adb logcat -d > "$OUT/logcat.txt" 2>/dev/null || true
echo "→ готово, файлы:"
ls -la "$OUT"
