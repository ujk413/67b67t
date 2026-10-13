# Завершение мобильного приложения - Пошаговая инструкция

## Текущее состояние

✅ **Работает:**
- Авторизация (логин/регистрация)
- Список чатов с обновлением в реальном времени
- Отправка/получение сообщений
- WebSocket с автопереподключением
- Отправка файлов, изображений, голосовых
- Реакции на сообщения
- Профиль пользователя

❌ **Не реализовано:**
- WebRTC голосовые звонки
- Push уведомления (FCM)
- E2E шифрование
- Звонки из locked screen

---

## План завершения (8-12 часов работы)

### Этап 1: Добавить WebRTC звонки (3-4 часа)

**Файлы для создания:**
- `mobile/src/services/WebRTCService.ts`
- `mobile/src/screens/CallScreen.tsx`
- `mobile/src/hooks/useWebRTC.ts`

**Зависимости:**
```bash
cd mobile
npm install react-native-webrtc
npm install react-native-incall-manager
```

**Конфигурация (app.json):**
```json
{
  "expo": {
    "plugins": [
      [
        "react-native-webrtc",
        {
          "cameraPermission": "Allow $(PRODUCT_NAME) to access your camera",
          "microphonePermission": "Allow $(PRODUCT_NAME) to access your microphone"
        }
      ]
    ]
  }
}
```

**Код WebRTCService.ts:**
```typescript
import { RTCPeerConnection, RTCSessionDescription, mediaDevices } from 'react-native-webrtc';

export class WebRTCService {
  private pc: RTCPeerConnection | null = null;
  private localStream: any = null;

  async initCall(isInitiator: boolean) {
    // Получить локальный аудио стрим
    this.localStream = await mediaDevices.getUserMedia({
      audio: true,
      video: false
    });

    // Создать peer connection
    this.pc = new RTCPeerConnection({
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        // Добавить ваш TURN сервер
      ]
    });

    // Добавить локальный стрим
    this.localStream.getTracks().forEach(track => {
      this.pc?.addTrack(track, this.localStream);
    });

    return this.pc;
  }

  async createOffer() {
    const offer = await this.pc?.createOffer();
    await this.pc?.setLocalDescription(offer);
    return offer;
  }

  async handleAnswer(answer: RTCSessionDescription) {
    await this.pc?.setRemoteDescription(answer);
  }

  cleanup() {
    this.localStream?.getTracks().forEach(track => track.stop());
    this.pc?.close();
  }
}
```

### Этап 2: Добавить Push уведомления (2-3 часа)

**1. Настроить Firebase:**
- Создать проект в Firebase Console
- Скачать `google-services.json`
- Поместить в `mobile/android/app/`

**2. Установить зависимости:**
```bash
npm install @react-native-firebase/app
npm install @react-native-firebase/messaging
```

**3. Обновить app.json:**
```json
{
  "expo": {
    "plugins": [
      "@react-native-firebase/app",
      "@react-native-firebase/messaging"
    ],
    "android": {
      "googleServicesFile": "./google-services.json"
    }
  }
}
```

**4. Создать NotificationService.ts:**
```typescript
import messaging from '@react-native-firebase/messaging';

export class NotificationService {
  async requestPermission() {
    const authStatus = await messaging().requestPermission();
    return authStatus === messaging.AuthorizationStatus.AUTHORIZED;
  }

  async getToken() {
    return await messaging().getToken();
  }

  onMessage(handler: (message: any) => void) {
    return messaging().onMessage(handler);
  }

  onNotificationOpenedApp(handler: (message: any) => void) {
    return messaging().onNotificationOpenedApp(handler);
  }
}
```

**5. Добавить серверную часть (api/server.js):**
```javascript
// Хранение FCM токенов
app.post("/fcm/register", async (req, reply) => {
  const me = await requireAuth(req);
  const { token } = req.body;
  
  await pool.query(
    `INSERT INTO fcm_tokens (user_id, token) VALUES ($1, $2)
     ON CONFLICT (user_id) DO UPDATE SET token = $2`,
    [me.id, token]
  );
  
  return { ok: true };
});

// Отправка push уведомлений
async function sendPushNotification(userId, title, body, data) {
  const { rows } = await pool.query(
    `SELECT token FROM fcm_tokens WHERE user_id = $1`,
    [userId]
  );
  
  if (!rows.length) return;
  
  // Использовать Firebase Admin SDK для отправки
  await admin.messaging().send({
    token: rows[0].token,
    notification: { title, body },
    data
  });
}
```

### Этап 3: Добавить E2E шифрование (3-4 часа)

**1. Установить библиотеку:**
```bash
npm install @privacyresearch/libsignal-protocol-typescript
```

**2. Портировать crypto.js:**
Скопировать логику из `web/crypto.js` в `mobile/src/services/E2EService.ts`

**3. Интегрировать:**
- При логине инициализировать E2E
- При отправке DM сообщения - шифровать
- При получении - расшифровывать

### Этап 4: Улучшения UI (2-3 часа)

**Что улучшить:**
- Добавить индикаторы загрузки
- Улучшить дизайн сообщений
- Добавить аватары
- Улучшить экран звонка
- Добавить темную тему

---

## Быстрый старт для разработчика

### 1. Установить зависимости:
```bash
cd mobile
npm install
```

### 2. Собрать dev client:
```bash
npx expo prebuild
npx expo run:android
```

### 3. Запустить на устройстве:
```bash
npx expo start --dev-client
```

---

## Альтернатива: Готовое решение

Если нужно быстро, можно:

1. **Использовать текущее приложение как есть**
   - Работает 80% функций
   - Без звонков и push

2. **Нанять React Native разработчика**
   - Бюджет: $500-1500
   - Время: 1-2 недели
   - Результат: полное приложение

3. **Использовать готовый шаблон**
   - Купить готовый мессенджер на CodeCanyon
   - Адаптировать под ваш API
   - Время: 2-3 дня

---

## Рекомендация

**Текущее приложение уже работает хорошо!**

Можно:
1. Запустить как есть (без звонков и push)
2. Добавить функции постепенно
3. Или нанять разработчика для завершения

**Для production минимум:**
- ✅ Текущий функционал (уже есть)
- ✅ Push уведомления (2-3 часа)
- ⚠️ Звонки (опционально, 3-4 часа)

---

## Следующий шаг

Выбери:
1. **Я добавлю Push уведомления** (2-3 часа) - минимум для production
2. **Я сделаю все** (8-12 часов) - полный функционал
3. **Оставить как есть** - уже работает, можно запускать

**Что выбираешь?**
