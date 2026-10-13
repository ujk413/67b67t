#!/bin/bash

# Get Tor onion address
# This script retrieves the .onion address after Tor container starts

echo "🧅 Получение .onion адреса..."
echo ""

# Wait for Tor to generate the address
if [ ! -f "data/tor/hostname" ]; then
    echo "⏳ Ожидание создания .onion адреса..."
    echo "   (это может занять 30-60 секунд)"
    echo ""

    for i in {1..60}; do
        if [ -f "data/tor/hostname" ]; then
            break
        fi
        sleep 1
        echo -n "."
    done
    echo ""
fi

# Check if file exists
if [ ! -f "data/tor/hostname" ]; then
    echo "❌ Ошибка: файл data/tor/hostname не найден"
    echo ""
    echo "Проверьте:"
    echo "  1. docker-compose ps tor  # Tor контейнер запущен?"
    echo "  2. docker-compose logs tor  # Есть ли ошибки?"
    exit 1
fi

# Read and display the address
ONION_ADDRESS=$(cat data/tor/hostname)

echo "✅ Ваш .onion адрес:"
echo ""
echo "   http://$ONION_ADDRESS"
echo ""
echo "📋 Инструкции:"
echo "   1. Скачайте Tor Browser: https://www.torproject.org/download/"
echo "   2. Откройте Tor Browser"
echo "   3. Вставьте адрес: http://$ONION_ADDRESS"
echo "   4. Готово!"
echo ""
echo "💡 Совет: Сохраните этот адрес - он не изменится при перезапуске"
echo ""
