# Тест E2E шифрования

## Быстрая проверка

### 1. Запустить мессенджер

```bash
docker-compose up -d
```

### 2. Создать двух пользователей

Откройте два окна браузера (или обычное + инкогнито):

**Окно 1 - Alice:**
- Зарегистрируйтесь как `alice`
- Откройте консоль (F12) и проверьте:
```javascript
console.log(state.e2eReady); // должно быть true
```

**Окно 2 - Bob:**
- Зарегистрируйтесь как `bob`
- Проверьте `state.e2eReady === true`

### 3. Создать DM чат

**От Alice:**
- Нажмите "Новый чат"
- Найдите `bob`
- Отправьте сообщение: "Привет, это секретное сообщение!"

### 4. Проверить в базе данных

```bash
# Подключиться к PostgreSQL
docker exec -it messenger67-postgres psql -U messenger messenger

# Найти последнее сообщение
SELECT 
  id,
  sender_id,
  text,                           -- должно быть NULL!
  LEFT(encrypted_content, 50) as encrypted_preview,
  meta->>'e2e' as is_e2e
FROM messages 
ORDER BY created_at DESC 
LIMIT 1;
```

**Ожидаемый результат:**
```
 id   | sender_id | text |     encrypted_preview              | is_e2e
------+-----------+------+------------------------------------+--------
 ...  | ...       | NULL | {"type":1,"body":"CiUKIwoh...      | true
```

✅ **Если `text = NULL` и `is_e2e = true` - E2E работает!**

### 5. Проверить в Network Inspector

**В окне Alice:**
1. Откройте DevTools → Network → WS
2. Отправьте еще одно сообщение
3. Найдите WebSocket frame
4. Проверьте payload:

```json
{
  "type": "message.send",
  "message": {
    "type": "text",
    "encrypted_content": "{\"type\":3,\"body\":\"...\"}",
    "meta": {"e2e": true}
  }
}
```

✅ **Если видите `encrypted_content` вместо `text` - всё работает!**

### 6. Проверить расшифровку

**В окне Bob:**
- Должно отобразиться: "Привет, это секретное сообщение!"
- Если видите `[Не удалось расшифровать]` - проверьте консоль на ошибки

## Что проверяется

| Проверка | Что означает |
|----------|--------------|
| `state.e2eReady = true` | E2E инициализирован |
| `text = NULL` в БД | Сервер не хранит plaintext |
| `encrypted_content` не NULL | Зашифрованный контент сохранен |
| `meta.e2e = true` | Сообщение помечено как E2E |
| Bob видит текст | Расшифровка работает |

## Негативные тесты

### Тест 1: Сервер не может прочитать

```sql
-- Попытаться найти текст сообщения в БД
SELECT * FROM messages 
WHERE text LIKE '%секретное%';
-- Должно вернуть 0 строк!
```

### Тест 2: Без E2E нельзя отправить DM

1. Откройте консоль
2. Выполните:
```javascript
state.e2eReady = false;
```
3. Попробуйте отправить сообщение
4. Должна появиться ошибка: "E2E шифрование не инициализировано"

### Тест 3: Старые ключи не работают

1. Очистите localStorage в браузере Alice
2. Перезагрузите страницу и залогиньтесь
3. Попробуйте прочитать старые сообщения
4. Должно показать: `[Не удалось расшифровать]`

## Troubleshooting

### Ошибка: "E2E шифрование не инициализировано"

**Причина:** libsignal не загрузился

**Решение:**
```javascript
// Проверить в консоли
console.log(window.libsignal); // должен быть объект
console.log(window.E2EManager); // должен быть класс
```

Если `undefined` - проверьте:
1. Загружается ли `libsignal-protocol.min.js` в Network tab
2. Нет ли ошибок в консоли
3. Работает ли CDN jsdelivr

### Ошибка: "Не удалось зашифровать сообщение"

**Причина:** Нет ключей собеседника

**Решение:**
```javascript
// Проверить ключи Bob
const bundle = await api('/e2e/keys/bob_user_id');
console.log(bundle);
// Должен вернуть identity_key, signed_pre_key, pre_key
```

### Сообщения не расшифровываются

**Причина:** Сессия не установлена

**Решение:**
```javascript
// Проверить сессию
const session = await state.e2eManager.store.loadSession('bob_user_id.1');
console.log(session); // должен быть объект, не null
```

## Производительность

Измерьте время шифрования/расшифровки:

```javascript
// Шифрование
console.time('encrypt');
const encrypted = await encryptMessageE2E(recipientId, 'Test message');
console.timeEnd('encrypt');
// Обычно: 5-20ms

// Расшифровка
console.time('decrypt');
const plaintext = await decryptMessageE2E(senderId, encrypted);
console.timeEnd('decrypt');
// Обычно: 3-15ms
```

## Автоматический тест

Создайте файл `test_e2e.js`:

```javascript
async function testE2E() {
  console.log('🧪 Testing E2E encryption...');
  
  // 1. Check initialization
  if (!state.e2eReady) {
    console.error('❌ E2E not initialized');
    return;
  }
  console.log('✅ E2E initialized');
  
  // 2. Check manager
  if (!state.e2eManager) {
    console.error('❌ E2EManager not found');
    return;
  }
  console.log('✅ E2EManager exists');
  
  // 3. Test encryption
  const testText = 'Test message ' + Date.now();
  try {
    const encrypted = await state.e2eManager.encryptMessage('test_user', '1', testText);
    console.log('✅ Encryption works', encrypted);
  } catch (e) {
    console.error('❌ Encryption failed:', e);
    return;
  }
  
  console.log('🎉 All tests passed!');
}

// Запустить
testE2E();
```

## Заключение

Если все проверки прошли успешно:

✅ E2E шифрование работает  
✅ Сервер не может прочитать сообщения  
✅ Только отправитель и получатель видят plaintext  

**Мессенджер защищен на уровне Signal!**
