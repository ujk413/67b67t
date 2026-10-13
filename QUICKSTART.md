# 🚀 Быстрый старт

## Запуск за 3 минуты

### 1. Отредактируйте .env

```bash
# Откройте файл
notepad .env

# Измените эти строки:
TURN_EXTERNAL_IP=ВАШ_ПУБЛИЧНЫЙ_IP    # Узнать: curl ifconfig.me
ORIGIN=http://localhost              # Или ваш домен
```

### 2. Запустите

**Windows (PowerShell):**
```powershell
.\test.ps1
```

**Linux/Mac:**
```bash
chmod +x test.sh
./test.sh
```

### 3. Откройте браузер

```
http://localhost
```

---

## Что проверяет тест

✅ Конфигурация (.env без дефолтных паролей)  
✅ Docker и Docker Compose  
✅ Все 4 контейнера запущены  
✅ API отвечает  
✅ PostgreSQL работает  
✅ E2E endpoints доступны  
✅ Rate limiting работает  
✅ TURN сервер запущен  
✅ Нет критических ошибок в логах  

---

## Ручное тестирование

### 1. Регистрация двух пользователей

1. Откройте http://localhost
2. Зарегистрируйте `user1` / `password123`
3. Откройте инкогнито окно
4. Зарегистрируйте `user2` / `password123`

### 2. Начните DM чат

1. В окне user1: найдите user2 через поиск
2. Начните чат
3. Отправьте сообщение "Hello!"

### 3. Проверьте шифрование

**В браузере (DevTools Console):**
```javascript
// Проверить что E2E работает
console.log(state.e2eReady); // должно быть true
```

**В базе данных:**
```bash
docker exec messenger67-postgres psql -U messenger -d messenger -c "SELECT encrypted_content, meta FROM messages ORDER BY created_at DESC LIMIT 1;"
```

Должно быть:
- `encrypted_content` содержит JSON с зашифрованными данными
- `meta` содержит `{"e2e": true}`
- `text` должен быть NULL

---

## Мониторинг

```bash
# Все логи
docker-compose logs -f

# Только API
docker-compose logs -f api

# Только ошибки
docker-compose logs | grep -i error
```

---

## Остановка

```bash
docker-compose down
```

---

## Troubleshooting

### Ошибка "Cannot connect to Docker"
```bash
# Запустите Docker Desktop (Windows/Mac)
# Или запустите Docker daemon (Linux)
sudo systemctl start docker
```

### Порт 80 занят
```bash
# Измените порты в docker-compose.yml
ports:
  - "8080:80"  # Вместо 80:80
  - "8443:443" # Вместо 443:443

# Откройте http://localhost:8080
```

### E2E не работает
```bash
# Проверьте что crypto.js загружается
# Откройте DevTools → Network → crypto.js (должен быть 200)

# Проверьте консоль на ошибки
# DevTools → Console
```

### База данных не запускается
```bash
# Удалите старые данные
docker-compose down -v
docker-compose up -d
```

---

## Что дальше?

📖 **Полная документация:** README.md  
🔒 **Безопасность:** SECURITY.md  
📝 **Все изменения:** CHANGELOG.md  
🔐 **Детали E2E:** E2E_INTEGRATION_REPORT.md  

---

**Важно:** E2E шифрование работает прозрачно в фоне.  
Пользователи не видят никаких упоминаний о шифровании - оно просто работает.
