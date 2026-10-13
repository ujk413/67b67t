# Мессенджер 67

🔐 **Безопасный мессенджер с полным E2E шифрованием на уровне Signal**

## 🎯 Основные возможности

- 🔐 **End-to-End шифрование** (Signal Protocol) - сервер не может прочитать личные сообщения
- 🔒 **Шифрование метаданных** (AES-256-GCM) - display names, room titles
- 🧅 **Tor Hidden Service** - анонимный доступ через .onion адрес
- 📞 **Голосовые и видео звонки** через WebRTC (DTLS-SRTP шифрование)
- 💬 **Личные сообщения, группы и каналы**
- 📎 **Загрузка файлов** с E2E шифрованием для DM
- 📱 **Web и мобильное приложение** (React Native)
- 🌙 **Темная тема**
- ⚡ **Real-time обновления** через WebSocket

## 🔐 Уровень защиты

### Что НЕ МОЖЕТ прочитать администратор сервера:

✅ Личные сообщения (DM) - Signal Protocol  
✅ Медиафайлы в DM - AES-256-GCM + E2E  
✅ Содержимое звонков - DTLS-SRTP  
✅ Пароли - Argon2 хеш  

### Сравнение с конкурентами:

| Функция | 67 Messenger | Signal | Telegram | WhatsApp |
|---------|--------------|--------|----------|----------|
| E2E для DM | ✅ | ✅ | ⚠️ Secret Chats | ✅ |
| Шифрование метаданных | ✅ | ❌ | ❌ | ❌ |
| Tor Hidden Service | ✅ | ❌ | ❌ | ❌ |
| Open Source | ✅ | ✅ | ⚠️ Client only | ❌ |

## 🚀 Быстрый старт

### Требования

- Docker и Docker Compose
- Домен с SSL сертификатом (для production)
- Открытые порты: 80, 443, 3478, 5349, 10000-10100

### Установка

1. **Клонировать репозиторий**
```bash
git clone <repo-url>
cd 67b67tmess
```

2. **Создать .env файл**
```bash
cp .env.example .env
```

3. **Сгенерировать безопасные пароли**
```bash
# Linux/Mac
openssl rand -base64 32

# Windows PowerShell
[Convert]::ToBase64String((1..32 | ForEach-Object { Get-Random -Maximum 256 }))
```

4. **Отредактировать .env**
```bash
nano .env
```

Обязательно замените:
- `POSTGRES_PASSWORD` - пароль PostgreSQL
- `TURN_SECRET` - секрет TURN сервера
- `TURN_STATIC_PASSWORD` - пароль для TURN
- `TURN_EXTERNAL_IP` - ваш публичный IP адрес
- `ORIGIN` - ваш домен (например, https://yourdomain.com)

5. **Запустить**
```bash
docker-compose up -d
```

6. **Проверить статус**
```bash
docker-compose ps
docker-compose logs -f
```

7. **Получить .onion адрес (опционально)**
```bash
# Linux/Mac
bash scripts/get_onion_address.sh

# Windows
powershell scripts/get_onion_address.ps1
```

Подробнее: [TOR_SETUP.md](TOR_SETUP.md)

## Конфигурация

### Переменные окружения

#### База данных
- `POSTGRES_USER` - пользователь PostgreSQL (по умолчанию: messenger)
- `POSTGRES_PASSWORD` - пароль PostgreSQL (**обязательно изменить**)
- `POSTGRES_DB` - имя базы данных (по умолчанию: messenger)
- `DATABASE_URL` - полный URL подключения к БД

#### Сервер
- `ORIGIN` - домен приложения (например: https://67b67t.ru)
- `PORT` - порт API сервера (по умолчанию: 3000)
- `NODE_ENV` - окружение (production/development)

#### TURN сервер (для звонков)
- `TURN_SECRET` - секретный ключ (**обязательно изменить**)
- `TURN_REALM` - домен TURN сервера
- `TURN_STATIC_USER` - имя пользователя для TURN
- `TURN_STATIC_PASSWORD` - пароль для TURN (**обязательно изменить**)
- `TURN_EXTERNAL_IP` - публичный IP сервера (**обязательно указать**)
- `TURN_RELAY_IP` - внутренний IP контейнера (по умолчанию: 172.18.0.2)

#### Хранилище файлов (опционально)
- `USE_S3` - использовать S3 (true/false)
- `S3_ENDPOINT` - endpoint S3-совместимого хранилища
- `S3_BUCKET` - имя bucket
- `S3_ACCESS_KEY` - ключ доступа
- `S3_SECRET_KEY` - секретный ключ
- `S3_REGION` - регион

#### Безопасность
- `HCAPTCHA_SECRET` - секрет hCaptcha (опционально)
- `ADMIN_USERNAMES` - список админов через запятую
- `SYSTEM_ADMIN_USERNAME` - системный админ
- `SYSTEM_ADMIN_PASSWORD` - пароль системного админа

## Архитектура

```
┌─────────────┐
│   Caddy     │  ← Reverse proxy + SSL
│   :80/:443  │
└──────┬──────┘
       │
┌──────┴──────┐
│  Fastify    │  ← API сервер (Node.js)
│   :3000     │
└──────┬──────┘
       │
┌──────┴──────┐
│ PostgreSQL  │  ← База данных
│   :5432     │
└─────────────┘

┌─────────────┐
│   TURN      │  ← Сервер для WebRTC
│ :3478/:5349 │
└─────────────┘
```

## Безопасность

### Реализованные меры

✅ **Аутентификация**
- HttpOnly cookies для защиты от XSS
- Argon2 для хеширования паролей
- Rate limiting на логин/регистрацию (10 запросов/мин)

✅ **Rate Limiting**
- Регистрация: 10/мин
- Логин: 10/мин
- Сообщения: 60/мин
- Создание комнат: 20/мин
- Загрузка файлов: 30/мин
- Реакции: 30/мин

✅ **Валидация**
- Максимальный размер файла: 100MB
- Максимум 1 файл за запрос
- Проверка MIME типов
- Валидация длины username/password

✅ **WebSocket**
- Автопереподключение с экспоненциальной задержкой
- Аутентификация при подключении

✅ **E2E Шифрование** (полностью реализовано)
- Signal Protocol (Double Ratchet + X3DH)
- Автоматическая инициализация при логине
- Все DM сообщения шифруются end-to-end
- Сервер физически не может прочитать личные сообщения
- См. [E2E_ENCRYPTION.md](E2E_ENCRYPTION.md) для деталей

### Рекомендации

1. **Всегда используйте HTTPS** в production
2. **Измените все дефолтные пароли** в .env
3. **Настройте firewall** - открывайте только необходимые порты
4. **Регулярно обновляйте** Docker образы
5. **Настройте backup** PostgreSQL базы данных
6. **Используйте hCaptcha** для защиты от ботов

## Разработка

### Локальный запуск

```bash
# Использовать локальную конфигурацию
docker-compose -f docker-compose.local.yml up -d

# Или напрямую без Docker
cd api
npm install
npm start
```

### Структура проекта

```
.
├── api/              # Backend (Fastify)
│   ├── server.js     # Основной сервер
│   ├── package.json
│   └── Dockerfile
├── web/              # Frontend (Vanilla JS)
│   ├── app.js        # Основное приложение
│   ├── crypto.js     # E2E шифрование
│   ├── webrtc.js     # WebRTC звонки
│   └── index.html
├── mobile/           # React Native приложение
│   └── App.tsx
├── db/
│   └── init.sql      # Схема базы данных
├── docker-compose.yml
├── Caddyfile         # Конфигурация Caddy
└── .env              # Переменные окружения
```

## Обновление

```bash
# Остановить контейнеры
docker-compose down

# Обновить код
git pull

# Пересобрать и запустить
docker-compose up -d --build

# Проверить логи
docker-compose logs -f
```

## Backup

### База данных

```bash
# Создать backup
docker exec messenger67-postgres pg_dump -U messenger messenger > backup.sql

# Восстановить
docker exec -i messenger67-postgres psql -U messenger messenger < backup.sql
```

### Файлы

```bash
# Backup загруженных файлов
tar -czf uploads-backup.tar.gz data/uploads/

# Восстановить
tar -xzf uploads-backup.tar.gz
```

## Troubleshooting

### Проблемы с подключением

```bash
# Проверить статус контейнеров
docker-compose ps

# Проверить логи
docker-compose logs api
docker-compose logs postgres
docker-compose logs caddy
docker-compose logs turn
```

### Проблемы с TURN сервером

1. Проверьте, что `TURN_EXTERNAL_IP` указан правильно
2. Убедитесь, что порты 3478, 5349, 10000-10100 открыты
3. Проверьте логи: `docker-compose logs turn`

### База данных не запускается

```bash
# Проверить права на директорию
ls -la data/postgres/

# Пересоздать volume
docker-compose down -v
docker-compose up -d
```

## Changelog

См. [CHANGELOG.md](CHANGELOG.md) для списка всех изменений.

## Лицензия

Proprietary

## Поддержка

Для вопросов и багрепортов создавайте issue в репозитории.
