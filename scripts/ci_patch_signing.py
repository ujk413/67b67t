#!/usr/bin/env python3
"""Прописать релизную подпись в android/app/build.gradle.

Expo prebuild генерирует проект из шаблона React Native, а там релизная сборка
подписывается ОТЛАДОЧНЫМ ключом:

    signingConfigs {
        debug { storeFile file('debug.keystore') ... }
    }
    buildTypes {
        release {
            // Caution! In production, you need to generate your own keystore file.
            signingConfig signingConfigs.debug
        }
    }

Из-за этого восстановленный в CI keystore и key.properties ничего не дают:
Gradle их просто не читает, и APK уезжает подписанным как CN=Android Debug.

Скрипт добавляет release-конфигурацию, читающую key.properties, и переключает
на неё релизную сборку. Идемпотентен: повторный запуск ничего не портит.

Запускать из корня репозитория, после expo prebuild и до gradle assembleRelease.
"""

from __future__ import annotations

import pathlib
import sys

GRADLE = pathlib.Path("mobile/android/app/build.gradle")
KEY_PROPERTIES = pathlib.Path("mobile/android/key.properties")

LOADER = """
// --- релизная подпись (вставлено CI) ---
def keystorePropertiesFile = rootProject.file("../key.properties")
def keystoreProperties = new Properties()
if (keystorePropertiesFile.exists()) {
    keystoreProperties.load(new FileInputStream(keystorePropertiesFile))
}
// --- конец вставки ---
"""

RELEASE_SIGNING_CONFIG = """        release {
            if (keystorePropertiesFile.exists()) {
                storeFile file(keystoreProperties['storeFile'])
                storePassword keystoreProperties['storePassword']
                keyAlias keystoreProperties['keyAlias']
                keyPassword keystoreProperties['keyPassword']
            }
        }
"""


def find_block(text: str, open_brace: int) -> tuple[int, int]:
    """Вернуть (индекс '{', индекс парной '}') для блока, начинающегося с open_brace."""
    depth = 0
    for i in range(open_brace, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return open_brace, i
    raise ValueError("не нашёл закрывающую скобку — файл неожиданной формы")


def block_after(text: str, marker: str) -> tuple[int, int]:
    """Найти marker, затем его блок в фигурных скобках."""
    idx = text.index(marker)
    brace = text.index("{", idx + len(marker))
    return find_block(text, brace)


def main() -> int:
    if not GRADLE.exists():
        print(f"нет {GRADLE} — сначала выполни expo prebuild", file=sys.stderr)
        return 1

    text = GRADLE.read_text(encoding="utf-8")

    if "keystorePropertiesFile" in text:
        print("подпись уже прописана — ничего не меняю")
        return 0

    if not KEY_PROPERTIES.exists():
        print(f"нет {KEY_PROPERTIES} — нечего читать", file=sys.stderr)
        return 1

    # 1. Загрузчик key.properties — на верхнем уровне, до блока android {}.
    android_idx = text.index("\nandroid {")
    text = text[:android_idx] + LOADER + text[android_idx:]

    # 2. release-конфигурация внутрь signingConfigs {}.
    #    Вставляем в начало строки с закрывающей скобкой, иначе отступ
    #    складывается с уже имеющимся и блок уезжает вправо.
    open_b, close_b = block_after(text, "signingConfigs")
    line_start = text.rfind("\n", 0, close_b) + 1
    text = text[:line_start] + RELEASE_SIGNING_CONFIG + text[line_start:]

    # 3. Переключаем buildTypes.release с debug на release.
    open_b, close_b = block_after(text, "buildTypes")
    types_block = text[open_b:close_b]
    rel_open, rel_close = block_after(types_block, "release")

    before = types_block[:rel_open]
    rel_block = types_block[rel_open:rel_close]
    after = types_block[rel_close:]

    if "signingConfig signingConfigs.debug" not in rel_block:
        print("в release нет signingConfigs.debug — проверь файл руками", file=sys.stderr)
        return 1

    rel_block = rel_block.replace(
        "signingConfig signingConfigs.debug",
        "signingConfig signingConfigs.release",
        1,
    )
    text = text[:open_b] + before + rel_block + after + text[close_b:]

    GRADLE.write_text(text, encoding="utf-8")
    print(f"готово: {GRADLE} подписывает release ключом из key.properties")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
