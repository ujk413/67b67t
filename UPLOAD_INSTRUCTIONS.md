# 🚀 ИНСТРУКЦИЯ ПО РАЗВЕРТЫВАНИЮ

## Я не могу загрузить файлы напрямую, но вот простая инструкция:

---

## ВАРИАНТ 1: WinSCP (САМЫЙ ПРОСТОЙ) ⭐

### Шаг 1: Скачать WinSCP
https://winscp.net/eng/download.php

### Шаг 2: Подключиться
1. Запустите WinSCP
2. Нажмите "New Site"
3. Введите:
   - **File protocol:** SFTP
   - **Host name:** <SERVER_IP>
   - **Port number:** 22
   - **User name:** root
   - **Password:** <SERVER_PASSWORD>
4. Нажмите "Login"
5. Если появится предупреждение о ключе - нажмите "Yes"

### Шаг 3: Загрузить проект
1. В левой панели (ваш компьютер) перейдите в `C:\`
2. В правой панели (сервер) перейдите в `/root/`
3. Перетащите папку `67b67tmess` из левой панели в правую
4. Дождитесь завершения загрузки (займет 2-3 минуты)

### Шаг 4: Подключиться через PuTTY
1. Скачайте PuTTY: https://www.putty.org/
2. Запустите PuTTY
3. Введите:
   - **Host Name:** <SERVER_IP>
   - **Port:** 22
4. Нажмите "Open"
5. Если появится предупреждение - нажмите "Yes"
6. Введите:
   - **login as:** root
   - **password:** <SERVER_PASSWORD>

### Шаг 5: Запустить развертывание
В терминале PuTTY выполните:

```bash
cd /root/67b67tmess
chmod +x deploy.sh
./deploy.sh
```

Скрипт автоматически всё настроит! Займет 5-10 минут.

---

## ВАРИАНТ 2: FileZilla

### Шаг 1: Скачать FileZilla
https://filezilla-project.org/

### Шаг 2: Подключиться
1. Запустите FileZilla
2. Вверху введите:
   - **Host:** sftp://<SERVER_IP>
   - **Username:** root
   - **Password:** <SERVER_PASSWORD>
   - **Port:** 22
3. Нажмите "Quickconnect"

### Шаг 3: Загрузить проект
1. Слева найдите `C:\67b67tmess`
2. Справа перейдите в `/root/`
3. Перетащите папку `67b67tmess` слева направо
4. Дождитесь завершения

### Шаг 4: Запустить через PuTTY
(См. Вариант 1, Шаг 4-5)

---

## ВАРИАНТ 3: Использовать готовый архив

Я создал архив проекта: `C:\Users\ivano\AppData\Local\Temp\67b67tmess.tar.gz` (8.1 MB)

### Загрузить через WinSCP:
1. Подключитесь к серверу (см. Вариант 1)
2. Загрузите файл `C:\Users\ivano\AppData\Local\Temp\67b67tmess.tar.gz` в `/root/`
3. В PuTTY выполните:

```bash
cd /root
tar -xzf 67b67tmess.tar.gz
cd 67b67tmess
chmod +x deploy.sh
./deploy.sh
```

---

## ЧТО СДЕЛАЕТ deploy.sh:

1. ✅ Установит Docker и Docker Compose
2. ✅ Сгенерирует ключ шифрования
3. ✅ Настроит домен 67b67t.ru
4. ✅ Запустит миграции БД
5. ✅ Запустит все сервисы:
   - API (Node.js)
   - PostgreSQL
   - Caddy (reverse proxy + SSL)
   - Tor Hidden Service
6. ✅ Получит SSL сертификат (автоматически)
7. ✅ Создаст .onion адрес

---

## ПОСЛЕ РАЗВЕРТЫВАНИЯ:

### 1. Проверить статус:
```bash
docker-compose ps
```

### 2. Посмотреть логи:
```bash
docker-compose logs -f
```

### 3. Получить Tor адрес:
```bash
cat tor/hidden_service/hostname
```

### 4. Проверить API:
```bash
curl http://localhost/api/health
```

---

## НАСТРОЙКА DNS:

После развертывания настройте DNS:

1. Зайдите в панель управления DNS (где зарегистрирован домен)
2. Добавьте A запись:
   ```
   Тип: A
   Имя: @ (или оставьте пустым)
   Значение: <SERVER_IP>
   TTL: Auto (или 3600)
   ```
3. Сохраните
4. Подождите 5-10 минут

Caddy автоматически получит SSL сертификат от Let's Encrypt.

---

## ДОСТУП К МЕССЕНДЖЕРУ:

После настройки DNS:

- **Web:** https://67b67t.ru
- **Tor:** http://ваш-onion-адрес.onion
- **API:** https://67b67t.ru/api

---

## СБОРКА APK (ОПЦИОНАЛЬНО):

После развертывания web приложения:

```bash
cd /root/67b67tmess
chmod +x build_apk.sh
./build_apk.sh
```

APK будет в `/root/67b67tmess/builds/messenger67-latest.apk`

Скачайте через WinSCP.

---

## ПОЛЕЗНЫЕ КОМАНДЫ:

```bash
# Перезапустить сервисы
docker-compose restart

# Остановить сервисы
docker-compose down

# Посмотреть логи конкретного сервиса
docker-compose logs -f api
docker-compose logs -f postgres
docker-compose logs -f caddy
docker-compose logs -f tor

# Проверить использование ресурсов
docker stats

# Backup базы данных
docker-compose exec postgres pg_dump -U messenger messenger > backup.sql
```

---

## ЕСЛИ ЧТО-ТО НЕ РАБОТАЕТ:

### Проблема: Сервисы не запускаются
```bash
docker-compose logs
df -h  # проверить место на диске
free -h  # проверить память
```

### Проблема: SSL сертификат не получен
```bash
docker-compose logs caddy
nslookup 67b67t.ru  # должен показать <SERVER_IP>
```

### Проблема: Не могу подключиться к серверу
- Проверьте пароль: <SERVER_PASSWORD>
- Проверьте IP: <SERVER_IP>
- Попробуйте другой SSH клиент

---

## ИТОГО:

1. ✅ Скачайте WinSCP: https://winscp.net/
2. ✅ Подключитесь к <SERVER_IP> (root / <SERVER_PASSWORD>)
3. ✅ Загрузите папку `C:\67b67tmess` в `/root/`
4. ✅ Скачайте PuTTY: https://www.putty.org/
5. ✅ Подключитесь к <SERVER_IP> (root / <SERVER_PASSWORD>)
6. ✅ Выполните: `cd /root/67b67tmess && chmod +x deploy.sh && ./deploy.sh`
7. ✅ Настройте DNS: A запись 67b67t.ru → <SERVER_IP>
8. ✅ Готово! Откройте https://67b67t.ru

**Весь процесс займет 15-20 минут!** 🚀

---

**Если нужна помощь - смотрите:**
- DEPLOYMENT_GUIDE.md
- SERVER_DEPLOY.md
- READY_TO_DEPLOY.md
