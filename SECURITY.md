# Инструкции по безопасному развертыванию

## ⚠️ ВАЖНО: Перед запуском в production

### 1. Генерация безопасных паролей

**НЕ используйте дефолтные пароли из .env.example!**

Сгенерируйте новые пароли:

```bash
# Linux/Mac
openssl rand -base64 32

# Windows PowerShell
[Convert]::ToBase64String((1..32 | ForEach-Object { Get-Random -Maximum 256 }))

# Python
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Замените в `.env`:
- `POSTGRES_PASSWORD`
- `TURN_SECRET`
- `TURN_STATIC_PASSWORD`
- `SYSTEM_ADMIN_PASSWORD` (если используется)

### 2. Настройка TURN сервера

Узнайте ваш публичный IP:
```bash
curl ifconfig.me
```

Укажите в `.env`:
```
TURN_EXTERNAL_IP=ВАШ_ПУБЛИЧНЫЙ_IP
```

### 3. Настройка домена

В `.env` укажите ваш домен:
```
ORIGIN=https://yourdomain.com
TURN_REALM=yourdomain.com
```

### 4. Firewall

Откройте необходимые порты:
```bash
# Ubuntu/Debian
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw allow 3478/tcp
sudo ufw allow 3478/udp
sudo ufw allow 5349/tcp
sudo ufw allow 5349/udp
sudo ufw allow 10000:10100/udp
sudo ufw enable

# CentOS/RHEL
sudo firewall-cmd --permanent --add-port=80/tcp
sudo firewall-cmd --permanent --add-port=443/tcp
sudo firewall-cmd --permanent --add-port=3478/tcp
sudo firewall-cmd --permanent --add-port=3478/udp
sudo firewall-cmd --permanent --add-port=5349/tcp
sudo firewall-cmd --permanent --add-port=5349/udp
sudo firewall-cmd --permanent --add-port=10000-10100/udp
sudo firewall-cmd --reload
```

### 5. SSL сертификаты

Caddy автоматически получит Let's Encrypt сертификаты, но убедитесь что:
- Домен указывает на ваш сервер (A запись в DNS)
- Порты 80 и 443 открыты
- Сервер доступен из интернета

### 6. Настройка hCaptcha (рекомендуется)

1. Зарегистрируйтесь на https://www.hcaptcha.com/
2. Создайте новый сайт
3. Получите Site Key и Secret Key
4. Добавьте в `.env`:
```
HCAPTCHA_SECRET=ваш_секретный_ключ
```
5. Добавьте Site Key в `web/index.html`

### 7. Backup стратегия

Настройте автоматический backup:

```bash
# Создайте скрипт /root/backup.sh
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/root/backups"
mkdir -p $BACKUP_DIR

# Backup базы данных
docker exec messenger67-postgres pg_dump -U messenger messenger | gzip > $BACKUP_DIR/db_$DATE.sql.gz

# Backup файлов
tar -czf $BACKUP_DIR/uploads_$DATE.tar.gz /path/to/67b67tmess/data/uploads/

# Удалить старые backup (старше 7 дней)
find $BACKUP_DIR -name "*.gz" -mtime +7 -delete

# Сделайте исполняемым
chmod +x /root/backup.sh

# Добавьте в crontab (каждый день в 3:00)
crontab -e
0 3 * * * /root/backup.sh
```

### 8. Мониторинг

Настройте мониторинг доступности:

```bash
# Простой health check скрипт
#!/bin/bash
if ! curl -f http://localhost/api/health > /dev/null 2>&1; then
    echo "API is down!" | mail -s "Alert: API Down" admin@yourdomain.com
    docker-compose restart api
fi
```

### 9. Логирование

Настройте ротацию логов:

```bash
# /etc/docker/daemon.json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}

# Перезапустите Docker
sudo systemctl restart docker
```

### 10. Обновления

Регулярно обновляйте систему:

```bash
# Обновление системы
sudo apt update && sudo apt upgrade -y

# Обновление Docker образов
cd /path/to/67b67tmess
docker-compose pull
docker-compose up -d --build

# Проверка
docker-compose ps
docker-compose logs -f
```

## Чеклист перед запуском

- [ ] Сгенерированы уникальные пароли для всех сервисов
- [ ] Указан правильный `TURN_EXTERNAL_IP`
- [ ] Настроен домен и DNS записи
- [ ] Открыты необходимые порты в firewall
- [ ] Настроен hCaptcha (опционально)
- [ ] Настроен автоматический backup
- [ ] Настроен мониторинг
- [ ] Настроена ротация логов
- [ ] Проверена доступность через HTTPS
- [ ] Протестированы звонки (WebRTC)

## Проверка безопасности после установки

```bash
# 1. Проверить, что пароли не дефолтные
grep -i "change-me" .env
# Не должно быть результатов!

# 2. Проверить SSL
curl -I https://yourdomain.com
# Должен быть статус 200 и HTTPS

# 3. Проверить rate limiting
# Попробуйте отправить 20 запросов на /auth/login
# Должен вернуть 429 Too Many Requests

# 4. Проверить TURN сервер
# Используйте https://webrtc.github.io/samples/src/content/peerconnection/trickle-ice/
# Должны появиться relay кандидаты

# 5. Проверить размер файлов
# Попробуйте загрузить файл > 100MB
# Должна быть ошибка
```

## Что делать при взломе

1. **Немедленно остановите сервисы**
```bash
docker-compose down
```

2. **Смените все пароли**
```bash
# Сгенерируйте новые пароли
openssl rand -base64 32

# Обновите .env
nano .env

# Обновите пароль в БД
docker-compose up -d postgres
docker exec -it messenger67-postgres psql -U messenger
ALTER USER messenger WITH PASSWORD 'новый_пароль';
```

3. **Проверьте логи**
```bash
docker-compose logs > incident_logs.txt
```

4. **Восстановите из backup**
```bash
# Если данные скомпрометированы
docker-compose down -v
# Восстановите из последнего чистого backup
```

5. **Обновите систему**
```bash
sudo apt update && sudo apt upgrade -y
docker-compose pull
docker-compose up -d --build
```

## Контакты для сообщения о проблемах безопасности

Если вы обнаружили уязвимость, пожалуйста, сообщите об этом приватно.

**НЕ создавайте публичные issue для проблем безопасности!**
