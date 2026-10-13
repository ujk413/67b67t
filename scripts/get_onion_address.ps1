# Get Tor onion address
# This script retrieves the .onion address after Tor container starts

Write-Host "🧅 Получение .onion адреса..." -ForegroundColor Cyan
Write-Host ""

# Wait for Tor to generate the address
if (-not (Test-Path "data/tor/hostname")) {
    Write-Host "⏳ Ожидание создания .onion адреса..." -ForegroundColor Yellow
    Write-Host "   (это может занять 30-60 секунд)"
    Write-Host ""

    for ($i = 1; $i -le 60; $i++) {
        if (Test-Path "data/tor/hostname") {
            break
        }
        Start-Sleep -Seconds 1
        Write-Host "." -NoNewline
    }
    Write-Host ""
}

# Check if file exists
if (-not (Test-Path "data/tor/hostname")) {
    Write-Host "❌ Ошибка: файл data/tor/hostname не найден" -ForegroundColor Red
    Write-Host ""
    Write-Host "Проверьте:"
    Write-Host "  1. docker-compose ps tor  # Tor контейнер запущен?"
    Write-Host "  2. docker-compose logs tor  # Есть ли ошибки?"
    exit 1
}

# Read and display the address
$onionAddress = Get-Content "data/tor/hostname" -Raw
$onionAddress = $onionAddress.Trim()

Write-Host "✅ Ваш .onion адрес:" -ForegroundColor Green
Write-Host ""
Write-Host "   http://$onionAddress" -ForegroundColor White
Write-Host ""
Write-Host "📋 Инструкции:"
Write-Host "   1. Скачайте Tor Browser: https://www.torproject.org/download/"
Write-Host "   2. Откройте Tor Browser"
Write-Host "   3. Вставьте адрес: http://$onionAddress"
Write-Host "   4. Готово!"
Write-Host ""
Write-Host "💡 Совет: Сохраните этот адрес - он не изменится при перезапуске" -ForegroundColor Yellow
Write-Host ""
