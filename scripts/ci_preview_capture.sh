#!/usr/bin/env bash
# Drive the freshly installed preview APK on a booted emulator: log in, then
# capture the screens we care about.
set -uo pipefail

OUT="${1:-preview-shots}"
PKG="ru.b67t.messenger.mobile"
USER_NAME="onyx"
PASSWORD="Passw0rd!long"
mkdir -p "$OUT"

shot() {
  adb exec-out screencap -p > "$OUT/$1.png"
  echo "  снял $1.png ($(stat -c%s "$OUT/$1.png") байт)"
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
    echo "  тапаю «$1» по $coords"
    adb shell input tap $coords
    sleep 1
    return 0
  fi
  echo "  !! не нашёл «$1»"
  return 1
}

type_into() { # type_into <label> <text>
  tap_text "$1" || return 1
  adb shell input text "$2"
  sleep 1
}

echo "→ запускаю приложение"
adb shell am force-stop "$PKG" || true
adb shell am start -n "$PKG/.MainActivity" >/dev/null
sleep 12

echo "→ экран входа"
shot 01-login

echo "→ логинюсь как $USER_NAME"
type_into "Username" "$USER_NAME"
type_into "Password" "$PASSWORD"
tap_text "Login" || tap_text "Войти" || true
sleep 12

echo "→ главный экран"
shot 02-main

echo "→ открываю первый чат"
if center_of "mira" > /tmp/c1; then
  adb shell input tap $(cat /tmp/c1)
  sleep 6
  shot 03-chat
  adb shell input keyevent KEYCODE_BACK
  sleep 3
else
  echo "  !! чат не найден в списке"
fi

echo "→ профиль"
if center_of "onyx" > /tmp/c2; then
  adb shell input tap $(cat /tmp/c2)
  sleep 5
  shot 04-profile
  adb shell input keyevent KEYCODE_BACK
  sleep 2
else
  echo "  !! профиль не найден"
fi

echo "→ готово, файлы:"
ls -la "$OUT"
