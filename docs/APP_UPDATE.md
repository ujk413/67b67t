# Обновление приложения

Приложение само узнаёт о новой сборке и предлагает обновиться. Android всегда
показывает системный диалог установки — тихо поставить APK без root или
device-owner нельзя, это ограничение платформы.

## Как это работает

1. При запуске и при каждом возврате в приложение (не чаще раза в час) клиент
   запрашивает `GET /api/app/version`.
2. Сервер отвечает `204`, если релизов нет, либо отдаёт
   `{version, versionCode, path, size, sha256, notes}`.
3. Если `versionCode` больше установленного, вверху чата появляется полоса
   «Доступна версия N» с кнопкой «Обновить». Её можно отложить, если релиз не
   помечен `mandatory`.
4. По нажатию APK скачивается в кэш (`expo-file-system`, с прогрессом), размер
   сверяется с серверным, и файл передаётся системному установщику через
   `expo-intent-launcher`.
5. Android проверяет подпись: обновление принимается только если APK подписан
   тем же ключом, что и установленное приложение. Поэтому подмена файла на
   сервере не даёт установить чужой код — установка просто не пройдёт.

## Требования к сборке

- `versionCode` в `mobile/app.json` должен расти. Одинаковый или меньший номер
  Android не установит.
- Подпись — тем же ключом (`CN=67chat.ru`, SHA-256
  `1a218f85…9c9ab8`). Проверить:
  `scripts/android_signing.sh check <apk>`.
- Разрешение `REQUEST_INSTALL_PACKAGES` (уже в `app.json`). При первой установке
  Android спросит «Разрешить установку из этого источника?».

## Публикация

Автоматически: пуш в `main` → GitHub Actions собирает APK, заливает на сервер и
сам публикует (`scripts/publish_apk.sh`). Нужны секреты `DEPLOY_SSH_KEY`,
`DEPLOY_HOST`, `DEPLOY_USER`; без них шаг пропускается с предупреждением.

Вручную, на сервере:

```bash
cd /opt/messenger67
RELEASE_DIR=/opt/messenger67/data/releases VERSION=1.3.0 VERSION_CODE=11 \
  ./scripts/publish_apk.sh /tmp/app-release.apk 'Что нового'
```

Скрипт кладёт APK и `latest.json` в `data/releases/` (том `/app/releases` в
контейнере API), старый APK удаляет. Перезапуск не нужен: манифест
перечитывается на каждом запросе (кэш 2 секунды).

## Откат

Вернуть предыдущий `latest.json` + APK в `data/releases/`. Установленные клиенты
с более новым `versionCode` откат не примут — Android не понижает версию.

## Ключевой момент: не потерять keystore

`expo prebuild --clean` удаляет `mobile/android/` вместе с ключом, а ключ
восстановить нельзя. Перед prebuild:

```bash
scripts/android_signing.sh backup     # android/ -> secrets/android-signing/
npx expo prebuild --platform android --clean --no-install
scripts/android_signing.sh restore
```

`secrets/` в `.gitignore`; резервная копия ключа также лежит в CI-секрете
`ANDROID_KEYSTORE_BASE64`.
