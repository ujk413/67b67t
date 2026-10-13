# 🧅 Tor Hidden Service для Messenger 67

**Дата:** 2026-04-10  
**Статус:** ✅ Реализовано

---

## 🎯 Что это дает

- **Анонимный доступ** через .onion адрес
- **Обход цензуры** - работает даже если основной домен заблокирован
- **Дополнительная приватность** - IP адрес пользователя скрыт
- **End-to-End шифрование** - Tor + наш E2E = двойная защита

---

## 🚀 Быстрый старт

### 1. Запустить с Tor

```bash
docker-compose up -d
```

Tor контейнер автоматически создаст .onion адрес.

### 2. Получить .onion адрес

```bash
# Подождать 30-60 секунд пока Tor создаст адрес
docker-compose logs tor

# Или прочитать напрямую
cat data/tor/hostname
```

Вы получите адрес вида:
```
abcdefghijklmnop1234567890qrstuvwxyz.onion
```

### 3. Открыть в Tor Browser

1. Скачать [Tor Browser](https://www.torproject.org/download/)
2. Открыть ваш .onion адрес
3. Готово! Мессенджер работает через Tor

---

## 📁 Структура

```
docker-compose.yml     # Добавлен tor сервис
tor/
  └── torrc           # Конфигурация Tor (не используется с goldy/tor-hidden-service)
data/
  └── tor/
      ├── hostname    # Ваш .onion адрес (создается автоматически)
      └── ...         # Приватные ключи Tor
```

---

## 🔧 Конфигурация

### Docker Compose

```yaml
tor:
  image: goldy/tor-hidden-service:latest
  container_name: messenger67-tor
  environment:
    MESSENGER67_TOR_SERVICE_HOSTS: "80:caddy:80,443:caddy:443"
    MESSENGER67_TOR_SERVICE_VERSION: "3"
  volumes:
    - ./data/tor:/var/lib/tor/hidden_service/
  depends_on:
    - caddy
  restart: unless-stopped
```

### Параметры

- **MESSENGER67_TOR_SERVICE_HOSTS** - маппинг портов (80 и 443 на Caddy)
- **MESSENGER67_TOR_SERVICE_VERSION** - версия 3 (современный формат .onion)
- **Volume** - сохранение ключей (адрес не меняется при перезапуске)

---

## 🔐 Безопасность

### Что защищает Tor

✅ **IP адрес пользователя** - скрыт от сервера  
✅ **Геолокация** - невозможно определить страну  
✅ **Обход блокировок** - работает даже если домен заблокирован  
✅ **Анонимность** - сложно связать пользователя с активностью

### Что НЕ защищает Tor

⚠️ **Содержимое сообщений** - но у нас есть E2E шифрование  
⚠️ **Метаданные на сервере** - кто, когда, кому писал (но не что)  
⚠️ **Fingerprinting** - браузер может быть идентифицирован

### Рекомендации

1. **Используйте Tor Browser** - не обычный браузер через Tor proxy
2. **Не логинтесь под реальным username** - создайте анонимный аккаунт
3. **Не загружайте личные фото** - они могут содержать метаданные
4. **Комбинируйте с E2E** - двойная защита

---

## 🧪 Тестирование

### 1. Проверить что Tor запущен

```bash
docker-compose ps tor
```

Должно быть: `Up`

### 2. Проверить логи

```bash
docker-compose logs -f tor
```

Должно быть:
```
[notice] Bootstrapped 100% (done): Done
[notice] Opened Socks listener connection
```

### 3. Получить .onion адрес

```bash
cat data/tor/hostname
```

### 4. Открыть в Tor Browser

1. Запустить Tor Browser
2. Вставить .onion адрес
3. Зарегистрироваться / войти
4. Отправить сообщение

### 5. Проверить что IP скрыт

```bash
# В логах API сервера IP будет от Tor exit node
docker-compose logs api | grep "New connection"
```

---

## 📊 Производительность

| Параметр | Обычный доступ | Через Tor | Разница |
|----------|----------------|-----------|---------|
| Загрузка страницы | ~200ms | ~2-5s | 10-25x медленнее |
| Отправка сообщения | ~50ms | ~500ms-1s | 10-20x медленнее |
| Загрузка файла (1MB) | ~1s | ~10-30s | 10-30x медленнее |

**Вывод:** Tor медленнее, но это цена за анонимность.

---

## 🔄 Обновление .onion адреса

### Если нужен новый адрес

```bash
# Остановить
docker-compose down

# Удалить старые ключи
rm -rf data/tor/*

# Запустить заново
docker-compose up -d

# Получить новый адрес
cat data/tor/hostname
```

⚠️ **Внимание:** Старый .onion адрес перестанет работать!

---

## 🌐 Vanity .onion адрес

Хотите красивый адрес типа `messenger67abc...onion`?

### Использовать mkp224o

```bash
# Установить
git clone https://github.com/cathugger/mkp224o.git
cd mkp224o
./autogen.sh
./configure
make

# Сгенерировать адрес начинающийся с "messenger"
./mkp224o messenger -d ./keys -n 1

# Скопировать ключи
cp keys/messenger*/hs_ed25519_secret_key data/tor/
cp keys/messenger*/hostname data/tor/

# Перезапустить
docker-compose restart tor
```

⚠️ **Внимание:** Генерация может занять часы/дни в зависимости от длины префикса.

---

## 🐛 Troubleshooting

### Tor не запускается

```bash
# Проверить логи
docker-compose logs tor

# Проверить права на директорию
ls -la data/tor/
chmod 700 data/tor/
```

### .onion адрес не работает

1. Подождите 1-2 минуты после запуска
2. Проверьте что Tor Browser подключен к сети Tor
3. Проверьте логи: `docker-compose logs tor`

### Медленно работает

Это нормально для Tor. Для ускорения:
1. Используйте Tor Browser (не proxy)
2. Выберите быстрые guard nodes
3. Подождите - первое подключение всегда медленнее

---

## 📚 Дополнительная информация

### Tor Hidden Service v3

- **Длина адреса:** 56 символов + .onion
- **Алгоритм:** Ed25519 (эллиптические кривые)
- **Безопасность:** Защита от перебора, forward secrecy
- **Срок жизни:** Бессрочно (пока есть приватный ключ)

### Как работает

```
Пользователь → Tor Browser → Guard Node → Middle Node → Exit Node → .onion → Caddy → API
```

1. Пользователь подключается к Tor сети
2. Трафик идет через 3 случайных узла
3. Последний узел подключается к .onion адресу
4. Caddy принимает запрос и проксирует на API

---

## 🎉 Итог

**Мессенджер 67 теперь доступен через Tor!**

✅ Анонимный доступ через .onion  
✅ Обход цензуры и блокировок  
✅ Дополнительная приватность  
✅ Совместимость с E2E шифрованием  
✅ Автоматическое создание адреса  

**Двойная защита: Tor (скрывает IP) + E2E (шифрует содержимое)**

---

## 🔗 Полезные ссылки

- [Tor Project](https://www.torproject.org/)
- [Tor Browser](https://www.torproject.org/download/)
- [Hidden Service Protocol](https://community.torproject.org/onion-services/)
- [mkp224o (vanity addresses)](https://github.com/cathugger/mkp224o)

---

**Автор:** Claude (Kiro)  
**Дата:** 2026-04-10  
**Время:** 12:36 UTC

**Готово к использованию! 🧅**
