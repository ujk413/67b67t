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

# The emulator software-renders the screen; under load system_server throws
# "isn't responding" dialogs that sit on top of everything. Clear them before
# every interaction, otherwise taps land on the dialog instead of the app.
dismiss_dialogs() {
  local tries=0
  while [ $tries -lt 4 ]; do
    if center_of "isn't responding" >/dev/null 2>&1 \
       || center_of "has stopped" >/dev/null 2>&1; then
      log "вижу системный диалог — закрываю"
      if center_of "Wait" > /tmp/waitpos; then
        adb shell input tap $(cat /tmp/waitpos)
      else
        adb shell input keyevent KEYCODE_BACK
      fi
      sleep 4
      tries=$((tries + 1))
    else
      return 0
    fi
  done
}

tap_text() {
  local coords
  dismiss_dialogs
  if coords=$(center_of "$1"); then
    log "тапаю «$1» по $coords"
    adb shell input tap $coords
    sleep 2
    return 0
  fi
  log "!! не нашёл «$1»"
  return 1
}

type_into() { # type_into <label> <text>
  tap_text "$1" || return 1
  adb shell input text "$2"
  sleep 2
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
type_into "Username" "$USER_NAME"
type_into "Password" "$PASSWORD"
tap_text "Login" || tap_text "Войти" || true
sleep 25
dismiss_dialogs

# If the login form is still there, the attempt did not go through.
if center_of "Username" >/dev/null 2>&1; then
  log "!! форма входа на месте — повторяю"
  type_into "Username" "$USER_NAME"
  type_into "Password" "$PASSWORD"
  tap_text "Login" || true
  sleep 25
  dismiss_dialogs
fi

echo "→ главный экран"
shot 02-main

echo "→ личка"
if coords=$(center_of "mira"); then
  adb shell input tap $coords
  sleep 12
  dismiss_dialogs
  shot 03-chat
  adb shell input keyevent KEYCODE_BACK
  sleep 6
else
  log "!! чат mira не найден"
fi

echo "→ группа"
if coords=$(center_of "Роблокс"); then
  adb shell input tap $coords
  sleep 12
  dismiss_dialogs
  shot 04-group
  adb shell input keyevent KEYCODE_BACK
  sleep 6
else
  log "!! группа не найдена"
fi

echo "→ профиль"
if coords=$(center_of "onyx"); then
  adb shell input tap $coords
  sleep 10
  dismiss_dialogs
  shot 05-profile
  adb shell input keyevent KEYCODE_BACK
  sleep 4
else
  log "!! профиль не найден"
fi

adb logcat -d > "$OUT/logcat.txt" 2>/dev/null || true
echo "→ готово, файлы:"
ls -la "$OUT"
