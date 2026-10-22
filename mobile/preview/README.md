# Нативные скриншоты интерфейса

`index.tsx` показывает настоящие компоненты ChatListScreen и ChatScreen с вымышленными переписками. Это отдельная точка входа для скриншотов; обычная APK использует `mobile/index.ts` и полноценный App. В preview нет авторизации, обращения к серверу и настоящих сообщений.

Сначала собрать и сохранить обычную release APK. Для preview в отдельном PowerShell-процессе из `mobile/android`:

```powershell
$env:NODE_ENV='production'
$env:ENTRY_FILE='preview/index.tsx'
.\gradlew.bat assembleRelease --no-daemon --console=plain
```

`ENTRY_FILE` должен быть относительным к `mobile` (React Native Gradle plugin некорректно объединяет абсолютные пути на Windows). Preview использует тот же Android package; устанавливать только в тестовый эмулятор. Не распространять его как рабочий клиент.

После скриншотов убрать `ENTRY_FILE` и повторить обычную сборку. Снимки сохраняются в `builds/screenshots/`; они не доказывают работу сервера, push и отправки сообщений.
