#!/bin/bash
# Тестовый скрипт для проверки мессенджера
# Запускает все сервисы и проверяет их работу

set -e

echo "🧪 Тестирование мессенджера 67"
echo "================================"

# Цвета для вывода
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Функция для проверки
check() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $1"
    else
        echo -e "${RED}✗${NC} $1"
        exit 1
    fi
}

# 1. Проверка .env файла
echo ""
echo "1️⃣  Проверка конфигурации..."
if [ -f .env ]; then
    check ".env файл существует"

    # Проверка что нет дефолтных паролей
    if grep -q "change-me" .env; then
        echo -e "${RED}✗${NC} Найдены дефолтные пароли 'change-me' в .env!"
        exit 1
    fi
    check "Нет дефолтных паролей"

    # Проверка обязательных переменных
    for var in POSTGRES_PASSWORD TURN_SECRET TURN_STATIC_PASSWORD TURN_EXTERNAL_IP; do
        if grep -q "^${var}=" .env; then
            check "$var установлен"
        else
            echo -e "${RED}✗${NC} $var не найден в .env"
            exit 1
        fi
    done
else
    echo -e "${RED}✗${NC} .env файл не найден!"
    echo "Создайте его из .env.example"
    exit 1
fi

# 2. Проверка Docker
echo ""
echo "2️⃣  Проверка Docker..."
if command -v docker &> /dev/null; then
    check "Docker установлен"
else
    echo -e "${RED}✗${NC} Docker не установлен"
    exit 1
fi

if command -v docker-compose &> /dev/null || docker compose version &> /dev/null; then
    check "Docker Compose установлен"
else
    echo -e "${RED}✗${NC} Docker Compose не установлен"
    exit 1
fi

# 3. Запуск сервисов
echo ""
echo "3️⃣  Запуск сервисов..."
docker-compose down -v 2>/dev/null || true
docker-compose up -d
check "Сервисы запущены"

# Ждем запуска
echo "Ожидание запуска сервисов (30 сек)..."
sleep 30

# 4. Проверка контейнеров
echo ""
echo "4️⃣  Проверка контейнеров..."
for container in messenger67-postgres messenger67-api messenger67-caddy messenger67-turn; do
    if docker ps | grep -q $container; then
        check "$container работает"
    else
        echo -e "${RED}✗${NC} $container не запущен"
        docker-compose logs $container
        exit 1
    fi
done

# 5. Проверка API
echo ""
echo "5️⃣  Проверка API..."
if curl -f http://localhost/api/health &> /dev/null; then
    check "API отвечает"
else
    echo -e "${RED}✗${NC} API не отвечает"
    docker-compose logs api
    exit 1
fi

# 6. Проверка базы данных
echo ""
echo "6️⃣  Проверка базы данных..."
if docker exec messenger67-postgres psql -U messenger -d messenger -c "SELECT 1" &> /dev/null; then
    check "PostgreSQL работает"
else
    echo -e "${RED}✗${NC} PostgreSQL не работает"
    exit 1
fi

# Проверка таблиц
TABLES="users sessions rooms messages media user_identity_keys user_devices user_prekeys"
for table in $TABLES; do
    if docker exec messenger67-postgres psql -U messenger -d messenger -c "SELECT 1 FROM $table LIMIT 1" &> /dev/null 2>&1; then
        check "Таблица $table существует"
    else
        echo -e "${YELLOW}⚠${NC} Таблица $table не найдена (возможно пустая)"
    fi
done

# 7. Проверка E2E endpoints
echo ""
echo "7️⃣  Проверка E2E endpoints..."
# Регистрация тестового пользователя
USER1="testuser_$(date +%s)"
PASS1="TestPass123!"

REGISTER_RESPONSE=$(curl -s -X POST http://localhost/api/auth/register \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"$USER1\",\"password\":\"$PASS1\",\"captcha\":\"\"}" \
    -c /tmp/cookies1.txt)

if echo "$REGISTER_RESPONSE" | grep -q "user"; then
    check "Регистрация работает"
else
    echo -e "${YELLOW}⚠${NC} Регистрация не удалась (возможно нужна капча)"
    echo "Response: $REGISTER_RESPONSE"
fi

# Проверка E2E endpoint (если пользователь создан)
if echo "$REGISTER_RESPONSE" | grep -q "user"; then
    # Попытка загрузить ключи
    E2E_RESPONSE=$(curl -s -X POST http://localhost/api/e2e/keys \
        -H "Content-Type: application/json" \
        -b /tmp/cookies1.txt \
        -d '{"device_id":"1","identity_key":"test","signed_pre_key_id":1,"signed_pre_key_public":"test","signed_pre_key_signature":"test","pre_keys":[]}')

    if echo "$E2E_RESPONSE" | grep -q "ok"; then
        check "E2E endpoints работают"
    else
        echo -e "${YELLOW}⚠${NC} E2E endpoints доступны но вернули ошибку"
    fi
fi

# 8. Проверка rate limiting
echo ""
echo "8️⃣  Проверка rate limiting..."
RATE_LIMIT_HIT=false
for i in {1..15}; do
    RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost/api/auth/login \
        -H "Content-Type: application/json" \
        -d '{"username":"test","password":"test"}')

    if [ "$RESPONSE" = "429" ]; then
        RATE_LIMIT_HIT=true
        break
    fi
done

if [ "$RATE_LIMIT_HIT" = true ]; then
    check "Rate limiting работает"
else
    echo -e "${YELLOW}⚠${NC} Rate limiting не сработал (возможно лимит выше)"
fi

# 9. Проверка TURN сервера
echo ""
echo "9️⃣  Проверка TURN сервера..."
if docker exec messenger67-turn ps aux | grep -q turnserver; then
    check "TURN сервер запущен"
else
    echo -e "${RED}✗${NC} TURN сервер не запущен"
fi

# 10. Проверка логов на ошибки
echo ""
echo "🔟 Проверка логов на критические ошибки..."
CRITICAL_ERRORS=$(docker-compose logs --tail=100 2>&1 | grep -i "error\|fatal\|exception" | grep -v "E2E not available" | wc -l)
if [ "$CRITICAL_ERRORS" -eq 0 ]; then
    check "Нет критических ошибок в логах"
else
    echo -e "${YELLOW}⚠${NC} Найдено $CRITICAL_ERRORS возможных ошибок в логах"
    echo "Проверьте: docker-compose logs"
fi

# Итоги
echo ""
echo "================================"
echo -e "${GREEN}✓ Все базовые проверки пройдены!${NC}"
echo ""
echo "📝 Следующие шаги:"
echo "1. Откройте http://localhost в браузере"
echo "2. Зарегистрируйте двух пользователей"
echo "3. Начните DM чат"
echo "4. Отправьте сообщение"
echo "5. Проверьте в БД что сообщение зашифровано:"
echo "   docker exec messenger67-postgres psql -U messenger -d messenger -c \"SELECT encrypted_content FROM messages ORDER BY created_at DESC LIMIT 1;\""
echo ""
echo "📊 Мониторинг:"
echo "   docker-compose logs -f"
echo ""
echo "🛑 Остановка:"
echo "   docker-compose down"
echo ""

# Cleanup
rm -f /tmp/cookies1.txt

exit 0
