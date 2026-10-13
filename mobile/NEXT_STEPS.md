# Следующие шаги для завершения мобильного приложения

## Что уже сделано (25%)

✅ Все зависимости добавлены в package.json  
✅ Конфигурация app.json (Firebase, permissions)  
✅ E2E шифрование (Signal Protocol) - src/crypto.ts  
✅ Push notifications (FCM) - src/notifications.ts  
✅ WebRTC звонки - src/webrtc.ts  
✅ API сервер поддерживает FCM  
✅ Миграция БД для fcm_token  

## Что нужно сделать

### 1. Установка и настройка (30 минут)

```bash
cd mobile
npm install
```

**Firebase Setup:**
1. Создать проект: https://console.firebase.google.com/
2. Добавить Android app: `ru.b67t.messenger.mobile`
3. Скачать `google-services.json` → `mobile/google-services.json`
4. Скопировать Server Key из Cloud Messaging
5. Добавить в `.env`: `FCM_SERVER_KEY=ваш_ключ`

**Prebuild:**
```bash
npx expo prebuild
```

### 2. Интеграция в App.tsx (2-3 часа)

Нужно добавить:

**A. WebSocket обработчики для звонков** (после строки 163):
```typescript
if (data.type === "call.offer") {
  // Показать входящий звонок
  setIncomingCall({
    roomId: data.room_id,
    callerId: data.caller_id,
    callerName: data.caller_name,
    isVideo: data.is_video
  });
}
if (data.type === "call.answer") {
  // Обработать ответ на звонок
  if (activeCall) {
    await activeCall.handleAnswer(data.answer);
  }
}
if (data.type === "call.ice_candidate") {
  // Добавить ICE candidate
  if (activeCall) {
    await activeCall.addIceCandidate(data.candidate);
  }
}
if (data.type === "call.end") {
  // Завершить звонок
  endCall();
}
```

**B. E2E шифрование сообщений** (в функции sendMessage):
```typescript
async function sendMessage() {
  if (!activeRoomId || !token || !composer.trim()) return;
  
  try {
    // Получить peer user ID для DM
    const room = rooms.find(r => r.id === activeRoomId);
    if (room?.kind === 'dm') {
      const peerId = activeRoomMembers.find(m => m.id !== me?.id)?.id;
      if (peerId) {
        // Зашифровать сообщение
        const encrypted = await e2e.encrypt(peerId, composer.trim());
        await api.sendMediaMessage(baseUrl, token, activeRoomId, {
          type: 'text',
          encrypted_content: JSON.stringify(encrypted),
          meta: { encrypted: true }
        });
        setComposer('');
        return;
      }
    }
  } catch (error) {
    console.error('E2E encryption failed:', error);
  }
  
  // Fallback to unencrypted
  await api.sendMessage(baseUrl, token, activeRoomId, composer.trim());
  setComposer('');
}
```

**C. Расшифровка входящих сообщений** (в WebSocket onmessage):
```typescript
if (data.type === "message.new") {
  const incoming = data.message as Message;
  
  // Расшифровать если зашифровано
  if (incoming.encrypted_content && incoming.sender_id !== me?.id) {
    try {
      const encrypted = JSON.parse(incoming.encrypted_content);
      const decrypted = await e2e.decrypt(incoming.sender_id, encrypted);
      incoming.text = decrypted;
      incoming._decrypted = true;
    } catch (error) {
      console.error('Failed to decrypt message:', error);
      incoming.text = '[Encrypted message]';
    }
  }
  
  // Остальной код...
}
```

**D. UI для звонков** (добавить компоненты):
```typescript
// Входящий звонок
{incomingCall && (
  <Modal visible transparent>
    <View style={styles.callModal}>
      <Text>{incomingCall.callerName} is calling...</Text>
      <Text>{incomingCall.isVideo ? 'Video Call' : 'Voice Call'}</Text>
      <View style={styles.callButtons}>
        <Pressable onPress={answerCall}>
          <Text>Answer</Text>
        </Pressable>
        <Pressable onPress={rejectCall}>
          <Text>Reject</Text>
        </Pressable>
      </View>
    </View>
  </Modal>
)}

// Активный звонок
{callState !== 'idle' && (
  <Modal visible>
    <View style={styles.callScreen}>
      {remoteStream && (
        <RTCView streamURL={remoteStream.toURL()} style={styles.remoteVideo} />
      )}
      {localStream && (
        <RTCView streamURL={localStream.toURL()} style={styles.localVideo} />
      )}
      <View style={styles.callControls}>
        <Pressable onPress={toggleAudio}>
          <Text>{isAudioEnabled ? '🔊' : '🔇'}</Text>
        </Pressable>
        {isVideoEnabled && (
          <Pressable onPress={toggleVideo}>
            <Text>📹</Text>
          </Pressable>
        )}
        <Pressable onPress={endCall}>
          <Text>❌</Text>
        </Pressable>
      </View>
    </View>
  </Modal>
)}
```

**E. Функции для звонков**:
```typescript
async function startCall(isVideo: boolean) {
  if (!activeRoomId || !token) return;
  
  const call = new WebRTCCall(
    { audio: true, video: isVideo },
    {
      onLocalStream: (stream) => setLocalStream(stream),
      onRemoteStream: (stream) => setRemoteStream(stream),
      onIceCandidate: (candidate) => {
        wsRef.current?.send(JSON.stringify({
          type: 'call.ice_candidate',
          room_id: activeRoomId,
          candidate
        }));
      },
      onConnectionStateChange: (state) => {
        if (state === 'connected') setCallState('connected');
        if (state === 'failed' || state === 'closed') endCall();
      }
    },
    true
  );
  
  await call.start();
  setActiveCall(call);
  setCallState('calling');
  
  const offer = call.getLocalDescription();
  wsRef.current?.send(JSON.stringify({
    type: 'call.offer',
    room_id: activeRoomId,
    offer,
    is_video: isVideo
  }));
}

async function answerCall() {
  if (!incomingCall) return;
  // Реализация ответа на звонок
}

function endCall() {
  activeCall?.end();
  setActiveCall(null);
  setLocalStream(null);
  setRemoteStream(null);
  setCallState('idle');
  setIncomingCall(null);
}
```

### 3. Запуск миграции БД

```bash
# На сервере
psql $DATABASE_URL < db/migrations/003_add_fcm_token.sql
```

### 4. Тестирование (1-2 часа)

```bash
# Запуск на Android
npx expo run:android

# Или на iOS
npx expo run:ios
```

**Что тестировать:**
- ✅ Логин/регистрация
- ✅ Список комнат
- ✅ Отправка/получение сообщений
- ✅ E2E шифрование (DM чаты)
- ✅ Push notifications (закрыть приложение, отправить сообщение)
- ✅ Голосовой звонок
- ✅ Видео звонок
- ✅ Загрузка медиа

## Полезные команды

```bash
# Логи Android
npx react-native log-android

# Логи iOS
npx react-native log-ios

# Очистка кеша
npx expo start -c

# Rebuild
rm -rf android ios && npx expo prebuild
```

## Troubleshooting

**Ошибка Firebase:**
- Проверить что google-services.json в корне mobile/
- Проверить package name в app.json и Firebase

**WebRTC не работает:**
- Проверить permissions в app.json
- Тестировать только на реальном устройстве (не эмулятор)

**Push notifications не приходят:**
- Проверить FCM_SERVER_KEY в .env
- Проверить что токен зарегистрирован: `SELECT fcm_token FROM users WHERE id='...'`

## Оценка времени

- Интеграция в App.tsx: 2-3 часа
- Тестирование и фиксы: 1-2 часа
- **Итого: 3-5 часов до полной готовности**

## Текущий прогресс: 25%

После завершения интеграции: **100%**
