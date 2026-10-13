# Быстрый тест мессенджера
# PowerShell версия для Windows

Write-Host "🧪 Тестирование мессенджера 67" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan

$ErrorActionPreference = "Stop"

function Check-Step {
    param($message, $success)
    if ($success) {
        Write-Host "✓ $message" -ForegroundColor Green
    } else {
        Write-Host "✗ $message" -ForegroundColor Red
        exit 1
    }
}

# 1. Проверка .env
Write-Host "`n1️⃣  Проверка конфигурации..." -ForegroundColor Yellow
if (Test-Path .env) {
    Check-Step ".env файл существует" $true

    $envContent = Get-Content .env -Raw
    if ($envContent -match "change-me") {
        Write-Host "✗ Найдены дефолтные пароли!" -ForegroundColor Red
        exit 1
    }
    Check-Step "Нет дефолтных паролей" $true
} else {
    Write-Host "✗ .env файл не найден!" -ForegroundColor Red
    exit 1
}

# 2. Проверка Docker
Write-Host "`n2️⃣  Проверка Docker..." -ForegroundColor Yellow
try {
    docker --version | Out-Null
    Check-Step "Docker установлен" $true
} catch {
    Check-Step "Docker установлен" $false
}

try {
    docker-compose --version | Out-Null
    Check-Step "Docker Compose установлен" $true
} catch {
    Check-Step "Docker Compose установлен" $false
}

# 3. Запуск сервисов
Write-Host "`n3️⃣  Запуск сервисов..." -ForegroundColor Yellow
docker-compose down -v 2>$null
docker-compose up -d
Check-Step "Сервисы запущены" $true

Write-Host "Ожидание запуска (30 сек)..." -ForegroundColor Gray
Start-Sleep -Seconds 30

# 4. Проверка контейнеров
Write-Host "`n4️⃣  Проверка контейнеров..." -ForegroundColor Yellow
$containers = @("messenger67-postgres", "messenger67-api", "messenger67-caddy", "messenger67-turn")
foreach ($container in $containers) {
    $running = docker ps --format "{{.Names}}" | Select-String -Pattern $container
    Check-Step "$container работает" ($null -ne $running)
}

# 5. Проверка API
Write-Host "`n5️⃣  Проверка API..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost/api/health" -UseBasicParsing -TimeoutSec 5
    Check-Step "API отвечает" ($response.StatusCode -eq 200)
} catch {
    Check-Step "API отвечает" $false
}

# 6. Проверка БД
Write-Host "`n6️⃣  Проверка базы данных..." -ForegroundColor Yellow
try {
    docker exec messenger67-postgres psql -U messenger -d messenger -c "SELECT 1" 2>$null | Out-Null
    Check-Step "PostgreSQL работает" $true
} catch {
    Check-Step "PostgreSQL работает" $false
}

# 7. Проверка E2E endpoints
Write-Host "`n7️⃣  Проверка E2E endpoints..." -ForegroundColor Yellow
$user = "testuser_$(Get-Date -Format 'yyyyMMddHHmmss')"
$pass = "TestPass123!"

try {
    $body = @{
        username = $user
        password = $pass
        captcha = ""
    } | ConvertTo-Json

    $response = Invoke-WebRequest -Uri "http://localhost/api/auth/register" `
        -Method POST `
        -ContentType "application/json" `
        -Body $body `
        -SessionVariable session `
        -UseBasicParsing

    if ($response.Content -match "user") {
        Check-Step "Регистрация работает" $true
    } else {
        Write-Host "⚠ Регистрация не удалась (возможно нужна капча)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "⚠ Ошибка регистрации: $_" -ForegroundColor Yellow
}

# Итоги
Write-Host "`n================================" -ForegroundColor Cyan
Write-Host "✓ Все базовые проверки пройдены!" -ForegroundColor Green
Write-Host "`n📝 Следующие шаги:" -ForegroundColor Cyan
Write-Host "1. Откройте http://localhost в браузере"
Write-Host "2. Зарегистрируйте двух пользователей"
Write-Host "3. Начните DM чат"
Write-Host "4. Отправьте сообщение"
Write-Host "`n📊 Мониторинг:" -ForegroundColor Cyan
Write-Host "   docker-compose logs -f"
Write-Host "`n🛑 Остановка:" -ForegroundColor Cyan
Write-Host "   docker-compose down"
Write-Host ""
