# End-to-End шифрование (E2E)

## Статус: ✅ Полностью реализовано

Мессенджер использует **Signal Protocol** для E2E шифрования личных сообщений (DM).

## Как это работает

### Архитектура

```
┌─────────────┐                    ┌─────────────┐
│   Alice     │                    │     Bob     │
│             │                    │             │
│ ┌─────────┐ │                    │ ┌─────────┐ │
│ │ Private │ │                    │ │ Private │ │
│ │  Keys   │ │                    │ │  Keys   │ │
│ └─────────┘ │                    │ └─────────┘ │
│      │      │                    │      │      │
│      ▼      │                    │      ▼      │
│  Encrypt    │                    │  Decrypt    │
│      │      │                    │      │      │
└──────┼──────┘                    └──────┼──────┘
       │                                  │
       │    ┌──────────────────┐         │
       └───▶│     Server       │◀────────┘
            │                  │
            │ Хранит только:   │
            │ • Зашифрованный  │
            │   контент        │
            │ • Публичные      │
            │   ключи          │
            │                  │
            │ НЕ может:        │
            │ • Расшифровать   │
            │ • Прочитать      │
            └──────────────────┘
```

### Протокол Signal

Используется полная реализация Signal Protocol:
- **X3DH** (Extended Triple Diffie-Hellman) - для установки сессии
- **Double Ratchet** - для шифрования сообщений
- **Forward Secrecy** - компрометация ключа не раскрывает прошлые сообщения
- **Future Secrecy** - компрометация ключа не раскрывает будущие сообщения

### Что шифруется

✅ **Личные сообщения (DM)** - полное E2E шифрование
- Сервер хранит только `encrypted_content`
- Поле `text` в БД = `NULL`
- Расшифровка только на устройствах отправителя и получателя

⚠️ **Группы и каналы** - room-based шифрование (AES-GCM)
- Ключ комнаты хранится на сервере
- Сервер технически может расшифровать (но не делает этого)
- Для полного E2E в группах нужна реализация Sender Keys

## Технические детали

### Инициализация при логине

```javascript
// Автоматически вызывается после login/register
async function initE2E() {
  state.e2eManager = new E2EManager();
  
  // Генерация ключей
  const bundle = await state.e2eManager.init(userId, deviceId);
  
  // Загрузка на сервер
  await api("/e2e/keys", {
    method: "POST",
    body: JSON.stringify({
      identity_key: bundle.identityKey,
      signed_pre_key: bundle.signedPreKey,
      pre_keys: bundle.preKeys // 100 одноразовых ключей
    })
  });
  
  state.e2eReady = true;
}
```

### Отправка сообщения

```javascript
// Для DM чатов
if (currentRoom.kind === "dm" && state.e2eReady) {
  // 1. Получить ключи собеседника
  const bundle = await api(`/e2e/keys/${recipientId}`);
  
  // 2. Построить Signal сессию
  await state.e2eManager.buildSession(recipientId, deviceId, bundle);
  
  // 3. Зашифровать сообщение
  const encrypted = await state.e2eManager.encryptMessage(recipientId, deviceId, plaintext);
  
  // 4. Отправить на сервер
  ws.send({
    type: "message.send",
    message: {
      encrypted_content: JSON.stringify(encrypted),
      meta: { e2e: true }
    }
  });
}
```

### Получение сообщения

```javascript
// При получении сообщения через WebSocket
if (msg.encrypted_content && msg.meta?.e2e) {
  const ciphertext = JSON.parse(msg.encrypted_content);
  const plaintext = await state.e2eManager.decryptMessage(
    senderId, 
    deviceId, 
    ciphertext
  );
  // Показать расшифрованный текст
}
```

## Что хранится на сервере

### Таблица `messages`

```sql
CREATE TABLE messages (
  id uuid PRIMARY KEY,
  room_id uuid NOT NULL,
  sender_id uuid NOT NULL,
  type text NOT NULL,
  text text,                    -- NULL для E2E сообщений
  encrypted_content text,       -- Зашифрованный контент
  meta jsonb,                   -- { e2e: true, sender_device_id: '1' }
  created_at timestamptz
);
```

**Пример E2E сообщения в БД:**
```json
{
  "id": "123e4567-e89b-12d3-a456-426614174000",
  "text": null,
  "encrypted_content": "{\"type\":3,\"body\":\"base64...\",\"registrationId\":12345}",
  "meta": {"e2e": true, "sender_device_id": "1"}
}
```

### Таблицы ключей

```sql
-- Публичные identity ключи
CREATE TABLE user_identity_keys (
  user_id uuid PRIMARY KEY,
  public_key text NOT NULL
);

-- Устройства и signed pre-keys
CREATE TABLE user_devices (
  id uuid PRIMARY KEY,
  user_id uuid NOT NULL,
  device_id text NOT NULL,
  public_key text NOT NULL,
  signed_pre_key_id int NOT NULL,
  signed_pre_key_public text NOT NULL,
  signed_pre_key_signature text NOT NULL
);

-- Одноразовые pre-keys
CREATE TABLE user_prekeys (
  id uuid PRIMARY KEY,
  device_id uuid NOT NULL,
  key_id int NOT NULL,
  public_key text NOT NULL,
  used boolean DEFAULT false
);
```

## Безопасность

### ✅ Что защищено

1. **Сервер не может прочитать сообщения**
   - Приватные ключи хранятся только на клиенте
   - Сервер видит только зашифрованный blob

2. **Forward Secrecy**
   - Каждое сообщение использует новый ключ
   - Компрометация одного ключа не раскрывает историю

3. **Аутентификация отправителя**
   - Signed pre-keys подтверждают личность
   - Защита от MITM атак

4. **Защита от replay атак**
   - Double Ratchet обеспечивает уникальность ключей

### ⚠️ Ограничения

1. **Нет верификации ключей**
   - Отсутствует механизм Safety Numbers
   - Пользователи не могут проверить ключи собеседника
   - Возможна MITM атака со стороны сервера

2. **Метаданные не защищены**
   - Сервер знает: кто, кому, когда отправил
   - Размер сообщения виден
   - Время отправки известно

3. **Группы не имеют E2E**
   - Используется room-based шифрование
   - Сервер может расшифровать групповые сообщения

4. **Single device**
   - Пока поддержка только одного устройства
   - Нет синхронизации между устройствами

## Проверка работы E2E

### 1. Проверить инициализацию

Откройте консоль браузера после логина:
```javascript
console.log(state.e2eReady); // должно быть true
console.log(state.e2eManager); // должен быть объект
```

### 2. Проверить ключи на сервере

```bash
# Подключиться к БД
docker exec -it messenger67-postgres psql -U messenger

# Проверить identity keys
SELECT user_id, LEFT(public_key, 20) || '...' as key_preview 
FROM user_identity_keys;

# Проверить pre-keys
SELECT device_id, key_id, used 
FROM user_prekeys 
ORDER BY created_at DESC 
LIMIT 10;
```

### 3. Проверить сообщение в БД

```sql
-- Найти E2E сообщение
SELECT 
  id,
  sender_id,
  text,                    -- должно быть NULL
  encrypted_content IS NOT NULL as is_encrypted,
  meta->>'e2e' as is_e2e
FROM messages 
WHERE meta->>'e2e' = 'true'
ORDER BY created_at DESC 
LIMIT 5;
```

### 4. Проверить в Network Inspector

1. Откройте DevTools → Network → WS
2. Отправьте сообщение в DM
3. Найдите WebSocket frame с `message.send`
4. Проверьте payload:

```json
{
  "type": "message.send",
  "room_id": "...",
  "message": {
    "type": "text",
    "encrypted_content": "{\"type\":1,\"body\":\"CiUKI...\",\"registrationId\":12345}",
    "meta": {"e2e": true, "sender_device_id": "1"}
  }
}
```

## Сравнение с другими мессенджерами

| Функция | 67 Messenger | Signal | Telegram | WhatsApp |
|---------|--------------|--------|----------|----------|
| E2E для DM | ✅ | ✅ | ⚠️ Secret Chats | ✅ |
| E2E для групп | ❌ | ✅ | ❌ | ✅ |
| Forward Secrecy | ✅ | ✅ | ⚠️ Secret Chats | ✅ |
| Safety Numbers | ❌ | ✅ | ⚠️ Secret Chats | ✅ |
| Multi-device | ❌ | ✅ | ✅ | ✅ |
| Open Source | ✅ | ✅ | ⚠️ Client only | ❌ |

## Roadmap

### Планируется добавить:

1. **Safety Numbers** - верификация ключей собеседника
2. **Sender Keys** - E2E для групп
3. **Multi-device** - синхронизация между устройствами
4. **Sealed Sender** - скрытие метаданных отправителя
5. **Disappearing Messages** - самоуничтожающиеся сообщения

## FAQ

**Q: Может ли администратор сервера прочитать мои DM?**  
A: Нет, если у него нет доступа к вашему устройству. Приватные ключи хранятся только в браузере.

**Q: Что если я очищу cookies?**  
A: Потеряете доступ к старым сообщениям. Новые сообщения будут работать с новыми ключами.

**Q: Можно ли восстановить переписку?**  
A: Нет, это особенность E2E. Нет резервного копирования ключей.

**Q: Защищены ли групповые чаты?**  
A: Частично. Используется AES-GCM, но ключ хранится на сервере.

**Q: Можно ли использовать на нескольких устройствах?**  
A: Пока нет. Каждое устройство будет иметь свои ключи.

## Техническая документация

### Библиотека

Используется: `@privacyresearch/libsignal-protocol-typescript`
- Версия: 0.1.4
- CDN: `https://cdn.jsdelivr.net/npm/@privacyresearch/libsignal-protocol-typescript@0.1.4/dist/libsignal-protocol.min.js`

### Файлы

- `web/crypto.js` - реализация E2EManager и SignalProtocolStore
- `web/app.js` - интеграция E2E в приложение
- `api/server.js` - API endpoints для обмена ключами
- `db/init.sql` - схема БД для хранения ключей

### API Endpoints

**POST /e2e/keys** - загрузка ключей на сервер
```json
{
  "device_id": "1",
  "identity_key": "base64...",
  "signed_pre_key": {
    "keyId": 1,
    "publicKey": "base64...",
    "signature": "base64..."
  },
  "pre_keys": [
    {"keyId": 0, "publicKey": "base64..."},
    ...
  ]
}
```

**GET /e2e/keys/:userId** - получение ключей собеседника
```json
{
  "user_id": "uuid",
  "device_id": "1",
  "identity_key": "base64...",
  "signed_pre_key": {
    "keyId": 1,
    "publicKey": "base64...",
    "signature": "base64..."
  },
  "pre_key": {
    "keyId": 42,
    "publicKey": "base64..."
  }
}
```

## Заключение

Мессенджер 67 обеспечивает **настоящее E2E шифрование для личных сообщений** с использованием проверенного Signal Protocol. Сервер физически не может прочитать содержимое DM переписок.

Для максимальной безопасности рекомендуется:
1. Использовать HTTPS
2. Проверять сертификат сервера
3. Не использовать на публичных компьютерах
4. Регулярно обновлять приложение
