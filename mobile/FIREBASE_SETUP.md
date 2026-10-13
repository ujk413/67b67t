# Firebase Setup Instructions

## Для Android

1. Создайте проект в Firebase Console: https://console.firebase.google.com/
2. Добавьте Android приложение с package name: `ru.b67t.messenger.mobile`
3. Скачайте `google-services.json`
4. Поместите файл в `mobile/google-services.json`

## Для iOS

1. В том же проекте Firebase добавьте iOS приложение
2. Скачайте `GoogleService-Info.plist`
3. Поместите файл в `mobile/GoogleService-Info.plist`

## Настройка Cloud Messaging

1. В Firebase Console → Project Settings → Cloud Messaging
2. Включите Cloud Messaging API (legacy) если нужно
3. Скопируйте Server Key для использования в API

## Установка зависимостей

```bash
cd mobile
npm install
npx expo prebuild
```

## Запуск

```bash
# Development build
npx expo run:android
# или
npx expo run:ios
```

**Примечание:** После добавления native модулей (Firebase, WebRTC) нужно использовать `expo run:android` вместо `expo start`, так как требуется Expo Dev Client.
