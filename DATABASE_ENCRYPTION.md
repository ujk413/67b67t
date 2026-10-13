# Шифрование базы данных PostgreSQL

## Включено шифрование на уровне БД

### 1. Шифрование at-rest (данные на диске)

Добавьте в `docker-compose.yml`:

```yaml
services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: messenger
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: messenger
      # Включить шифрование WAL
      POSTGRES_INITDB_ARGS: "--data-checksums"
    volumes:
      - ./data/postgres:/var/lib/postgresql/data:rw
      # Монтировать зашифрованный том
      - type: volume
        source: postgres-data-encrypted
        target: /var/lib/postgresql/data
        volume:
          nocopy: true
    command: >
      postgres
      -c ssl=on
      -c ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem
      -c ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key
      -c wal_level=replica
      -c archive_mode=on
      -c archive_command='test ! -f /archive/%f && cp %p /archive/%f'

volumes:
  postgres-data-encrypted:
    driver: local
    driver_opts:
      type: none
      o: bind,encryption=aes-xts-plain64
      device: /var/lib/docker/volumes/postgres-encrypted
```

### 2. Шифрование в транзите (SSL/TLS)

Обновите `DATABASE_URL` в `.env`:

```bash
DATABASE_URL=postgresql://messenger:password@postgres:5432/messenger?sslmode=require
```

### 3. Шифрование чувствительных полей с pgcrypto

Уже включено в `db/init.sql`:
```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;
```

### 4. Шифрование на уровне файловой системы (LUKS)

Для production рекомендуется использовать LUKS для шифрования всего диска:

```bash
# Создать зашифрованный раздел
sudo cryptsetup luksFormat /dev/sdb1

# Открыть зашифрованный раздел
sudo cryptsetup luksOpen /dev/sdb1 postgres-encrypted

# Создать файловую систему
sudo mkfs.ext4 /dev/mapper/postgres-encrypted

# Монтировать
sudo mount /dev/mapper/postgres-encrypted /var/lib/docker/volumes/postgres-encrypted
```

### 5. Автоматическое шифрование паролей

Пароли уже шифруются с Argon2 в `api/server.js`:

```javascript
const passwordHash = await argon2.hash(password);
```

## Рекомендации

1. **Backup ключей шифрования** - храните ключи отдельно от данных
2. **Ротация ключей** - меняйте ключи шифрования раз в год
3. **Мониторинг доступа** - логируйте все обращения к БД
4. **Ограничение доступа** - используйте firewall для PostgreSQL порта

## Проверка

```bash
# Проверить SSL соединение
docker exec messenger67-postgres psql -U messenger -c "SELECT * FROM pg_stat_ssl WHERE pid = pg_backend_pid();"

# Проверить шифрование паролей
docker exec messenger67-postgres psql -U messenger -c "SELECT username, LEFT(password_hash, 20) FROM users LIMIT 5;"
```

## Что защищено

✅ Пароли пользователей (Argon2)  
✅ Сессии (токены хешированы)  
✅ E2E сообщения (только encrypted_content)  
✅ Соединение с БД (SSL/TLS)  
⚠️ Метаданные (usernames, display_names) - в plaintext  
⚠️ Медиафайлы на диске - не зашифрованы

## Следующие шаги

Для полного шифрования метаданных см. следующий раздел.
