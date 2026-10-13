# 🚀 ГОТОВО К РАЗВЕРТЫВАНИЮ!

**Сервер:** <SERVER_IP>  
**Домен:** 67b67t.ru  
**Пользователь:** root  
**Пароль:** <SERVER_PASSWORD>

---

## 📋 ЧТО НУЖНО СДЕЛАТЬ

### Вариант 1: Автоматическая загрузка (Windows)

```cmd
cd C:\67b67tmess
upload_and_deploy.cmd
```

Если pscp не установлен, используйте Вариант 2.

### Вариант 2: Ручная загрузка (Рекомендуется)

#### Шаг 1: Загрузить проект на сервер

**Используя WinSCP (Проще всего):**

1. Скачайте WinSCP: https://winscp.net/eng/download.php
2. Установите и запустите
3. Создайте новое подключение:
   - Протокол: SFTP
   - Хост: <SERVER_IP>
   - Порт: 22
   - Имя пользователя: root
   - Пароль: <SERVER_PASSWORD>
4. Нажмите "Войти"
5. Перейдите в `/root/`
6. Перетащите папку `67b67tmess` из `C:\` в `/root/`
7. Дождитесь завершения загрузки

**Или используя FileZilla:**

1. Скачайте FileZilla: https://filezilla-project.org/
2. Хост: sftp://<SERVER_IP>
3. Имя пользователя: root
4. Пароль: <SERVER_PASSWORD>
5. Порт: 22
6. Загрузите папку `67b67tmess` в `/root/`

#### Шаг 2: Подключиться к серверу

**Используя PuTTY (Windows):**

1. Скачайте PuTTY: https://www.putty.org/
2. Host Name: <SERVER_IP>
3. Port: 22
4. Connection type: SSH
5. Нажмите "Open"
6. Login as: root
7. Password: <SERVER_PASSWORD>

#### Шаг 3: Запустить развертывание

В терминале PuTTY выполните:

```bash
cd /root/67b67tmess
chmod +x deploy.sh
./deploy.sh
```

Скрипт автоматически:
- ✅ Установит Docker и Docker Compose
- ✅ Сгенерирует ключ шифрования
- ✅ Настроит домен 67b67t.ru
- ✅ Запустит миграции БД
- ✅ Запустит все сервисы (API, PostgreSQL, Caddy, Tor)
- ✅ Настроит SSL сертификат (автоматически через Caddy)
- ✅ Создаст Tor Hidden Service

#### Шаг 4: Проверить развертывание

```bash
# Проверить статус сервисов
docker-compose ps

# Посмотреть логи
docker-compose logs -f

# Проверить API
curl http://localhost/api/health

# Получить Tor адрес
cat tor/hidden_service/hostname
```

#### Шаг 5: Собрать APK (Опционально)

```bash
chmod +x build_apk.sh
./build_apk.sh
```

APK будет сохранен в `/root/67b67tmess/builds/messenger67-latest.apk`

**Скачать APK:**

Используя WinSCP:
1. Подключитесь к серверу
2. Перейдите в `/root/67b67tmess/builds/`
3. Скачайте `messenger67-latest.apk`

---

## 🌐 НАСТРОЙКА DNS

**ВАЖНО:** Направьте домен на сервер:

1. Зайдите в панель управления DNS (Cloudflare, GoDaddy и т.д.)
2. Добавьте A запись:
   ```
   Тип: A
   Имя: @
   Значение: <SERVER_IP>
   TTL: Auto
   ```
3. Подождите 5-10 минут для распространения DNS
4. Проверьте: `nslookup 67b67t.ru`

Caddy автоматически получит SSL сертификат после настройки DNS.

---

## ✅ ПРОВЕРКА ПОСЛЕ РАЗВЕРТЫВАНИЯ

### Web приложение
- [ ] Сервисы запущены: `docker-compose ps`
- [ ] API отвечает: `curl https://67b67t.ru/api/health`
- [ ] Web приложение доступно: https://67b67t.ru
- [ ] SSL сертификат получен (проверить в браузере)
- [ ] Можно зарегистрироваться
- [ ] Можно отправить сообщение
- [ ] E2E шифрование работает

### Tor Hidden Service
- [ ] Tor запущен: `docker-compose logs tor`
- [ ] Hostname сгенерирован: `cat tor/hidden_service/hostname`
- [ ] Доступен через Tor Browser

### База данных
- [ ] Миграции применены
- [ ] Можно создать пользователей
- [ ] Можно создать комнаты
- [ ] Данные сохраняются после перезапуска

---

## 📱 ДОСТУП К МЕССЕНДЖЕРУ

### Web приложение
- **HTTPS:** https://67b67t.ru
- **HTTP:** http://67b67t.ru (перенаправляет на HTTPS)

### Tor Hidden Service
```bash
# Получить onion адрес
ssh root@<SERVER_IP> "cat /root/67b67tmess/tor/hidden_service/hostname"

# Доступ через Tor Browser
http://ваш-onion-адрес.onion
```

### Мобильное приложение
1. Скачайте APK с сервера
2. Установите на Android устройство
3. Откройте приложение
4. Введите URL сервера: https://67b67t.ru/api
5. Зарегистрируйтесь/Войдите

---

## 🔧 ПОЛЕЗНЫЕ КОМАНДЫ

```bash
# Подключиться к серверу
ssh root@<SERVER_IP>

# Развернуть
cd /root/67b67tmess && ./deploy.sh

# Проверить статус
docker-compose ps

# Посмотреть логи
docker-compose logs -f

# Перезапустить
docker-compose restart

# Получить Tor адрес
cat tor/hidden_service/hostname

# Собрать APK
./build_apk.sh

# Скачать APK (с локальной машины)
# Используйте WinSCP или:
scp root@<SERVER_IP>:/root/67b67tmess/builds/messenger67-latest.apk .
```

---

## 📚 ДОКУМЕНТАЦИЯ

- **DEPLOYMENT_GUIDE.md** - Полное руководство по развертыванию
- **SERVER_DEPLOY.md** - Детали серверного развертывания
- **deploy.sh** - Скрипт автоматического развертывания
- **build_apk.sh** - Скрипт сборки APK
- **mobile/README.md** - Документация мобильного приложения
- **mobile/BUILD_INSTRUCTIONS.md** - Инструкции по сборке
- **mobile/FIREBASE_SETUP.md** - Настройка Firebase

---

## 🎯 БЫСТРЫЙ СТАРТ

### 1. Загрузите проект на сервер (WinSCP)
- Хост: <SERVER_IP>
- Пользователь: root
- Пароль: <SERVER_PASSWORD>
- Загрузите `C:\67b67tmess` в `/root/`

### 2. Подключитесь к серверу (PuTTY)
- Хост: <SERVER_IP>
- Пользователь: root
- Пароль: <SERVER_PASSWORD>

### 3. Запустите развертывание
```bash
cd /root/67b67tmess
chmod +x deploy.sh
./deploy.sh
```

### 4. Настройте DNS
- A запись: 67b67t.ru → <SERVER_IP>

### 5. Готово!
- Откройте: https://67b67t.ru
- Зарегистрируйтесь и начните использовать!

---

## 🆘 ПОДДЕРЖКА

Если что-то не работает:

1. **Проверьте логи:**
   ```bash
   docker-compose logs -f
   ```

2. **Проверьте статус:**
   ```bash
   docker-compose ps
   ```

3. **Перезапустите сервисы:**
   ```bash
   docker-compose restart
   ```

4. **Проверьте документацию:**
   - DEPLOYMENT_GUIDE.md
   - SERVER_DEPLOY.md

---

## 🎉 ВСЁ ГОТОВО!

Все файлы и скрипты созданы. Теперь просто:

1. **Загрузите проект на сервер** (WinSCP)
2. **Запустите deploy.sh** (PuTTY)
3. **Настройте DNS**
4. **Готово!**

**Ваш мессенджер будет доступен на https://67b67t.ru** 🚀

---

**Создано с помощью Claude Code**  
**Дата:** 2026-04-10  
**Готово к развертыванию!** ✅
