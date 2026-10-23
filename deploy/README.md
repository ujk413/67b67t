# Новый отдельный сервер Ubuntu

Это запуск с пустой базой. Основная копия исходников: `C:\67b67tmess\67b67t`.
Компактный интерфейс с обеими темами реализован в Android- и веб-клиенте.
Архив включает текущий API и веб-клиент, без старой базы, вложений и секретов.

## Подготовка

1. Установить Docker Engine и Compose plugin из официального apt-репозитория:
   https://docs.docker.com/engine/install/ubuntu/ (Ubuntu 26.04 LTS поддерживается).
2. На рабочем компьютере: `python scripts/prepare_server_bundle.py`.
3. Передать `builds/messenger67-server.tar.gz` и файл `.sha256` на сервер.
4. Проверить `sha256sum -c messenger67-server.tar.gz.sha256`, распаковать в `/opt/messenger67`.
5. Убедиться, что DNS A домена указывает на сервер. Старую AAAA-запись убрать, если
   новый сервер не обслуживает указанный IPv6. Домен в текущем APK: `67chat.ru`.

## Первый запуск

```bash
cd /opt/messenger67
sudo python3 scripts/init_server.py --domain 67chat.ru --ip PUBLIC_IPV4
sudo python3 scripts/configure_web_push.py
sudo docker compose -f docker-compose.server.yml config --quiet
sudo docker compose -f docker-compose.server.yml up -d --build --wait --wait-timeout 180
sudo docker compose -f docker-compose.server.yml ps
curl --fail --silent --show-error https://67chat.ru/api/health
```

`init_server.py` создаёт свежие пароли и ключ шифрования в `.env` с правами 0600.
Он откажется перезаписывать существующий `.env` или запускаться поверх непустой базы.
Вложения принадлежат UID 1000, под которым работает API. Базовая SQL-схема применяется
при инициализации PostgreSQL; API применяет свои дополнения при запуске.
Старые `deploy.sh` и `docker-compose.prod.yml` здесь не используются.

Если сервер за NAT, передать `--relay-ip` с адресом сетевого интерфейса сервера.
TURN использует host network Ubuntu и адрес хоста, вместо фиксированного IP Docker.
Здесь настроен TURN на 3478 UDP/TCP, TLS TURN на 5349 не включён.
HTTPS сайта и защищённый WebSocket предоставляет Caddy на 443.

Открытые входящие порты в firewall провайдера: SSH (обычно 22/TCP), 80/TCP,
443/TCP (+443/UDP для HTTP/3), 3478/TCP+UDP и 10000–10100/UDP.
PostgreSQL не опубликован; API доступен на хосте только через `127.0.0.1:3000`.
Публикуемые Docker-порты обходят обычные правила UFW:
https://docs.docker.com/engine/network/packet-filtering-firewalls/.

## Проверка после запуска

Автоматическая проверка с двумя временными аккаунтами и удалением их данных:

```bash
sudo docker exec -i messenger67-api node --input-type=module < scripts/live_server_check.mjs
```

- Действующий HTTPS-сертификат, сайт и JS загружаются без ошибок.
- `/api/health` возвращает JSON, `/api/me` без сессии — 401.
- Два новых аккаунта: регистрация, вход, сообщения туда и обратно через WebSocket.
- В APK: история, повтор отправки, фото, голосовое сообщение; проверить звонок
  между разными сетями и наличие relay-кандидата. Открытый порт не доказывает работу звонка.
- Проверить перезапуск контейнеров и сохранение истории и вложений.

Веб-уведомления используют Web Push/VAPID. `configure_web_push.py` добавляет ключи
в серверный `.env`; повторный запуск сохраняет существующую пару. После добавления
ключей пересоздать API. Пользователь включает уведомления в настройках сайта и
разрешает их в браузере. Там же есть проверочное уведомление. Передаётся только
«Новое сообщение», без текста переписки; нажатие открывает нужный чат. Отключённые
и открытые активные чаты учитываются. Работу конкретного браузера проверять на
устройстве: наличие серверных ключей не подтверждает доставку через push-провайдера.

Android push использует отдельный Firebase-аккаунт с ролью FCM API Admin.
В `.env` установить `FIREBASE_SECRETS_DIR=/etc/messenger67` и
`GOOGLE_APPLICATION_CREDENTIALS=/run/secrets/firebase-service-account.json`.
Каталог секретов монтируется API только для чтения; ключ должен быть доступен
UID 1000. Клиентский `google-services.json` используется только при сборке APK.
Подробности и права файлов: `mobile/FIREBASE_SETUP.md` в исходном проекте.

## Данные и обновления

Не удалять `data/` и `.env`. В резервные копии входят база, `data/uploads`, `.env`.
`scripts/backup_db.sh` сохраняет только PostgreSQL. Нужна отдельная копия вложений
и зашифрованная копия секретов вне сервера. Для согласованной полной копии остановить
API на время дампа и копирования файлов, затем запустить снова.

Обновление кода: передать новый архив, проверить хеш, распаковать в тот же каталог,
затем `docker compose -f docker-compose.server.yml up -d --build --wait`.
Архив не содержит `.env` и `data/`, но резервную копию перед обновлением всё равно сделать.
Сервер 2 vCPU/4 ГБ: следить за RAM, диском и одновременными обработками FFmpeg.
Логи контейнеров ограничены тремя файлами по 10 МБ на контейнер.

### После включения групповых звонков

При наличии LIVEKIT_* в существующем .env обновлять stack с overlay: `docker compose -f docker-compose.server.yml -f docker-compose.calls.yml up -d --build --wait`. Он сохраняет собственную SFU и её настройки API. Открыты только7881TCP/7882UDP для медиа; signaling7880 доступен исключительно внутри Compose через проверяющий API-шлюз. Групповой звонок ограничен шестью участниками и не обещает E2E медиапотоков. Проверенный image и сценарии: `docs/plans/call-features-integration.md` в исходниках. Переключатель LIVEKIT_ENABLE_GROUP_CALLS=false отключает выдачу новых конференций без изменения остальных данных.
