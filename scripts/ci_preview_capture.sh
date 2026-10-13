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

tap_text() {
  local coords
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
for _ in $(seq 1 60); do
  [ "$(adb shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')" = "1" ] && break
  sleep 5
done
log "boot_completed=$(adb shell getprop sys.boot_completed | tr -d '\r')"

# Без этого system_server вешает ANR-диалог поверх всего экрана.
adb shell settings put global hide_error_dialogs 1
adb shell settings put global window_animation_scale 0
adb shell settings put global transition_animation_scale 0
adb shell settings put global animator_duration_scale 0
adb shell wm dismiss-keyguard || true

echo "→ даю системе успокоиться"
sleep 30
adb shell input keyevent KEYCODE_BACK || true   # сбросить любой висящий диалог
sleep 2

echo "→ запускаю приложение"
adb shell am force-stop "$PKG" || true
adb shell am start -n "$PKG/.MainActivity" >/dev/null
sleep 25

echo "→ экран входа"
shot 01-login

echo "→ логинюсь как $USER_NAME"
type_into "Username" "$USER_NAME"
type_into "Password" "$PASSWORD"
tap_text "Login" || tap_text "Войти" || true
sleep 20

# Если всё ещё видим форму входа — логин не прошёл, пробуем ещё раз.
if center_of "Username" >/dev/null 2>&1; then
  log "!! форма входа на месте — повторяю попытку"
  type_into "Username" "$USER_NAME"
  type_into "Password" "$PASSWORD"
  tap_text "Login" || true
  sleep 20
fi

echo "→ главный экран"
shot 02-main

echo "→ открываю личку"
if coords=$(center_of "mira"); then
  adb shell input tap $coords
  sleep 10
  shot 03-chat
  adb shell input keyevent KEYCODE_BACK
  sleep 5
else
  log "!! чат mira не найден в списке"
fi

echo "→ группа"
if coords=$(center_of "Роблокс"); then
  adb shell input tap $coords
  sleep 10
  shot 04-group
  adb shell input keyevent KEYCODE_BACK
  sleep 5
else
  log "!! группа не найдена"
fi

echo "→ профиль"
if coords=$(center_of "onyx"); then
  adb shell input tap $coords
  sleep 8
  shot 05-profile
  adb shell input keyevent KEYCODE_BACK
  sleep 3
else
  log "!! профиль не найден"
fi

adb logcat -d > "$OUT/logcat.txt" 2>/dev/null || true
echo "→ готово, файлы:"
ls -la "$OUT"
