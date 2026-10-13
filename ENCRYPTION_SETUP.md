# Быстрая настройка полного шифрования

## Шаг 1: Генерация ключей

```bash
# Сгенерировать ключ шифрования метаданных (256 бит)
openssl rand -base64 32

# Сгенерировать пароли для БД и TURN
openssl rand -base64 32
openssl rand -base64 32
openssl rand -base64 32
```

## Шаг 2: Настройка .env

Скопируйте `.env.example` в `.env`:

```bash
cp .env.example .env
nano .env
```

Замените следующие значения:

```bash
# 1. Пароль PostgreSQL
POSTGRES_PASSWORD=ваш_сгенерированный_пароль_1
DATABASE_URL=postgres://messenger:ваш_сгенерированный_пароль_1@postgres:5432/messenger?sslmode=require

# 2. Ключ шифрования метаданных (КРИТИЧЕСКИ ВАЖНО!)
ENCRYPTION_KEY=ваш_сгенерированный_ключ_base64

# 3. TURN сервер
TURN_SECRET=ваш_сгенерированный_пароль_2
TURN_STATIC_PASSWORD=ваш_сгенерированный_пароль_3
TURN_EXTERNAL_IP=ваш_публичный_IP

# 4. Домен
ORIGIN=https://ваш-домен.com
```

## Шаг 3: Запуск

```bash
docker-compose up -d
```

## Шаг 4: Проверка

### 4.1. Проверить E2E шифрование

1. Создайте двух пользователей
2. Отправьте DM сообщение
3. Проверьте в БД:

```bash
docker exec -it messenger67-postgres psql -U messenger messenger

SELECT 
  text,                           -- должно быть NULL
  encrypted_content IS NOT NULL,  -- должно быть true
  meta->>'e2e'                    -- должно быть 'true'
FROM messages 
ORDER BY created_at DESC 
LIMIT 1;
```

**Ожидаемый результат:**
```
 text | ?column? | ?column?
------+----------+----------
 NULL | t        | true
```

✅ Если `text = NULL` - E2E работает!

### 4.2. Проверить шифрование метаданных

```bash
docker exec -it messenger67-postgres psql -U messenger messenger

SELECT username, LEFT(display_name, 50) 
FROM users 
WHERE display_name IS NOT NULL 
LIMIT 3;
```

**Ожидаемый результат:**
```
 username |                    left
----------+--------------------------------------------
 alice    | abc123...:def456...:ghi789...
 bob      | xyz789...:uvw456...:rst123...
```

✅ Если display_name содержит `:` и выглядит как base64 - шифрование работает!

### 4.3. Проверить WebRTC шифрование

1. Совершите звонок
2. Откройте консоль браузера (F12)
3. Выполните:

```javascript
const stats = await state.pc.getStats();
stats.forEach(report => {
  if (report.type === 'transport') {
    console.log('DTLS State:', report.dtlsState);
    console.log('SRTP Cipher:', report.srtpCipher);
  }
});
```

**Ожидаемый результат:**
```
DTLS State: connected
SRTP Cipher: AES_CM_128_HMAC_SHA1_80
```

✅ Если DTLS connected - звонки зашифрованы!

## Шаг 5: Backup ключа шифрования

⚠️ **КРИТИЧЕСКИ ВАЖНО!**

```bash
# Сохранить ключ в безопасное место
echo "ENCRYPTION_KEY=$(grep ENCRYPTION_KEY .env | cut -d= -f2)" > /secure/backup/encryption.key
chmod 600 /secure/backup/encryption.key

# Или используйте secrets manager
aws secretsmanager create-secret \
  --name messenger-encryption-key \
  --secret-string "$(grep ENCRYPTION_KEY .env | cut -d= -f2)"
```

**Потеря ключа = потеря доступа к метаданным!**

## Что защищено

| Данные | Метод | Сервер может прочитать? |
|--------|-------|-------------------------|
| DM сообщения | Signal Protocol | ❌ НЕТ |
| Медиафайлы (DM) | AES-256-GCM + E2E | ❌ НЕТ |
| Display names | AES-256-GCM | ⚠️ С ключом |
| Room titles | AES-256-GCM | ⚠️ С ключом |
| Звонки | DTLS-SRTP | ❌ НЕТ |
| Пароли | Argon2 | ❌ НЕТ |
| Групповые сообщения | AES-GCM | ✅ Да |

## Troubleshooting

### Ошибка: "ENCRYPTION_KEY not set"

**Решение:**
```bash
# Проверить .env
grep ENCRYPTION_KEY .env

# Если пусто - сгенерировать
openssl rand -base64 32

# Добавить в .env
echo "ENCRYPTION_KEY=ваш_ключ" >> .env

# Перезапустить
docker-compose restart api
```

### Ошибка: "Metadata decryption failed"

**Причина:** Изменился ключ шифрования

**Решение:**
1. Восстановить старый ключ из backup
2. Или пересоздать метаданные (потеря данных!)

### Сообщения не расшифровываются

**Причина:** E2E не инициализирован

**Решение:**
```javascript
// В консоли браузера
console.log(state.e2eReady); // должно быть true
console.log(window.libsignal); // должен быть объект

// Если false - перезагрузить страницу
location.reload();
```

## Безопасность

### ✅ Что делать

1. **Храните ключи в безопасности**
   - Используйте secrets manager
   - Ограничьте доступ к .env
   - Не коммитьте в git

2. **Регулярный backup**
   - Backup ключа шифрования
   - Backup базы данных
   - Тестируйте восстановление

3. **Мониторинг**
   - Логируйте доступ к БД
   - Отслеживайте неудачные попытки расшифровки
   - Алерты на подозрительную активность

4. **Обновления**
   - Регулярно обновляйте Docker образы
   - Следите за уязвимостями
   - Патчите систему

### ❌ Что НЕ делать

1. **Не используйте слабые ключи**
   - Минимум 256 бит
   - Генерируйте криптографически стойкие

2. **Не храните ключи в коде**
   - Только в .env или secrets manager
   - Никогда в git

3. **Не игнорируйте предупреждения**
   - "ENCRYPTION_KEY not set" - критично!
   - "Decryption failed" - проверьте ключ

4. **Не используйте один ключ везде**
   - Разные ключи для разных окружений
   - Dev ≠ Staging ≠ Production

## Производительность

### Ожидаемое влияние

- Отправка сообщений: +20-40ms
- Загрузка комнат: +10-20ms
- Обновление профиля: +5-10ms

### Оптимизация

Если производительность критична:

```javascript
// Кеширование расшифрованных данных
const cache = new Map();

function decryptCached(encrypted) {
  if (cache.has(encrypted)) return cache.get(encrypted);
  const decrypted = decrypt(encrypted);
  cache.set(encrypted, decrypted);
  return decrypted;
}
```

## Дополнительная информация

- **Полная документация:** [FULL_ENCRYPTION_REPORT.md](FULL_ENCRYPTION_REPORT.md)
- **E2E детали:** [E2E_ENCRYPTION.md](E2E_ENCRYPTION.md)
- **Тестирование:** [TEST_E2E.md](TEST_E2E.md)
- **База данных:** [DATABASE_ENCRYPTION.md](DATABASE_ENCRYPTION.md)

## Поддержка

При возникновении проблем:

1. Проверьте логи: `docker-compose logs -f api`
2. Проверьте консоль браузера (F12)
3. Проверьте .env файл
4. Создайте issue в репозитории

---

**Готово! Ваш мессенджер теперь защищен на уровне Signal.**
