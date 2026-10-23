# Push нескольких аккаунтов

Backend хранит native-подписки по `(user_id, installation_id)`, browser-подписки по `(user_id, endpoint)`. Один FCM token / browser endpoint может принадлежать нескольким аккаунтам на устройстве; регистрация следующего не переносит существующую запись. Один аккаунт в одной установке сохраняет последнюю session_id. DELETE требует тот же user_id и session_id: поздний выход старого сеанса не удаляет новую регистрацию или другой аккаунт.

## Клиентский контракт

- Native `PUT /me/push-devices/:installationId` сохраняет прежние поля token, active_room_id, quick_reply. Новый клиент добавляет installation_secret, installation_epoch, resume_push по контракту `account-push-policy.js`. Старый APK без этих полей работает, пока установка не поставлена на защищённую паузу.
- Native FCM payload содержит recipient_id и recipientId с UUID получателя. Оба поля назначает сервер после входных data; notification tag включает UUID аккаунта. Приватный текст/имя отправителя в notification body не добавляются.
- Browser `PUT /me/web-push` принимает прежнюю subscription и active_room_id; для каждого сохранённого аккаунта требуется отдельный authenticated запрос. Payload содержит recipientId/recipient_id, roomId/messageId; URL `/?account_id=<UUID>&room=<UUID>` и tag различают аккаунты. Service Worker должен выбирать соответствующий аккаунт до навигации в комнату.
- Переключение активного аккаунта сохраняет подписки остальных; только явный logout/removal отзывает свой сеанс. На смену FCM token клиент повторно регистрирует все сохранённые аккаунты. Неактивным аккаунтам передаёт active_room_id=null.

## Пауза установки

Регистрация выполняется через withPushRegistration в одной транзакции с fence `account_push_installations`. Новый epoch после device-wide disable делает старые запросы регистрации недействительными; явное resume требует текущий secret+epoch. Реализация/маршруты политики принадлежат `account-push-policy.js/.sql`.

Native delivery использует withPushDelivery с той же блокировкой установки, повторно проверяет живой owning session, точный token, членство/настройки комнаты и удерживает fence до завершения Firebase send. Disable дожидается текущей отправки, затем отмечает паузу и удаляет подписки всех аккаунтов установки. SDK ограничивает отдельный HTTP attempt 15 секундами и имеет ограниченные повторы; медленный провайдер может задержать подтверждение отключения. Promise.race не используется: его timeout оставил бы настоящий сетевой запрос работать после снятия fence. Уже принятые провайдером сообщения отозвать нельзя.

Expired-token cleanup ограничена аккаунтом+установкой+точным token. Browser cleanup дополнительно сравнивает snapshot subscription, чтобы поздний 410 не удалил новые ключи/чужой аккаунт. Sender соединяет session_id с user_id, пропускает истёкший/отозванный сеанс. Удаление самого сеанса удаляет его подписку через FK, оставляя остальные аккаунты установки.

## Миграции и проверка

`api/mobile-schema.sql` обновляет native legacy PRIMARY KEY без удаления корректных строк; `webPushSchema` делает то же для browser endpoint. Они вызываются при старте API и продублированы в db/init.sql / manual migration. Повторное применение проверено настоящим SQL (PGlite). Новые policy-файлы необходимо включить в server bundle.

`node --test tests/mobile-api.test.mjs tests/native-push.test.mjs tests/web-push.test.mjs tests/account-push-policy.test.mjs tests/feature-schema.test.mjs` проверяет старые регистрации, составные подписки, разных получателей на одном токене, session/logout isolation, late provider response, epoch pause/resume и выбранную до паузы отправку. UI/SW маршрутизация нескольких browser cookies и native регистрация всех аккаунтов проверяются их отдельными клиентскими тестами. Ничего не опубликовано.
