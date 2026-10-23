# Приватность и аккаунты: подключение и границы этапа 6

Сервер, веб и Android подключены к основным entrypoint. Production, secrets, пользовательские базы и deployment этим исполнителем не менялись. Этот документ описывает текущие контракты, а не обещание физически проверенного релиза.

## Сервер

api/server.js вызывает applyAccountMigrations(pool) с обязательной ошибкой старта при сбое и installAccountIntegration(app,deps). SQL account-features.sql, account-integration.sql и account-push-policy.sql идемпотентен; scripts/sync_feature_schema.py переносит определения в db/init.sql и db/migrations/20261023_remaining_features.sql. JS/SQL включены в server bundle. После изменения SQL повторить schema sync и tests/feature-schema.test.mjs.

Обязательные deps: pool, requireAuth, стабильный securityKey Buffer32, verifyPassword(storedHash,password), issueSession(user,request,reply), revokeUserSessions, purgeAccountData. Интеграция передаёт настоящие callbacks, а не заглушки. securityKey выводится отдельным HMAC-контекстом из существующего master key и не логируется. Прямой installAccountFeatures без purge/revoke не разрешает удаление.

Contacts принадлежат аккаунту, не телефонной книге. account.canWrite(sender,recipient) ограничивает создание/отправку/изменение DM и звонки. Privacy preSerialization обрабатывает authenticated HTTP; per-socket сериализованный фильтр обрабатывает WS, presence, derived online, last_seen и взаимные read/listened receipts. requireSession проверяет актуальность сеанса на каждом входящем WS frame. Эти фильтры не заменяют проверки membership/ownership в самих routes.

## Пароль, второй фактор и сеансы

После проверки пароля, ДО session mint:

    const gate=await account.authGate(user,{code:body.totp||body.code,backup_code:body.backup_code,device_key:body.device_key,device_name,ip});
    if(!gate.allowed)return reply.code(202).send({error:'account_challenge_required',challenge:gate.challenge});
    return issueSession(user,req,reply);

challenge = {id,secret,kind,expires_at}; secret хранится только в памяти экрана, это не token сеанса. POST /auth/account-challenge/complete {id,secret,code} либо backup_code возвращает обычный cookie/bearer ответ. 403 bad_factor — неверный/повторный фактор, 409 device_approval_required — разрешить вход на действующем устройстве через POST /me/account/challenges/:id/approve и повторить completion. Успешно проверенный фактор сохраняется в challenge, ожидание approval не расходует failed-factor attempts. Challenge одноразовый, срок две минуты. TOTP SHA1/30s, ±1 step, replay counter; пять ошибок блокируют фактор на пять минут. Backup codes хранятся только хешами, расходуются под SQL row lock. TOTP по умолчанию выключен. Secondary username merge также проверяет фактор второго аккаунта.

Один createSessionIssuer повторно проверяет ban/login_disabled/merged под row lock перед сохранением token hash. Все revoke/logout/ban/merge/delete пути закрывают соответствующие WS и call leases; общие сеансы других устройств не закрываются при отзыве одного. Login audit и new-device notification адресованы только владельцу.

Веб createAuthFlow действительно ждёт completeWebAccountChallenge и prepareUser до setUser/синхронизации. Android AuthChallenge и restoreSession используют bearer transport. Ни пароль аккаунта, ни пароль файла не сохраняются и не подменяют друг друга.

## Доказательство владения Signal-устройством

GET /me/account/capabilities объявляет device_proof, backup_staging, secret_device_proof_required. Новый клиент запрашивает POST /e2e/device-proof {device_id,identity_key}; ответ {id,user_id,device_id,challenge,expires_at}. Точный подписываемый UTF8 challenge содержит шесть строк:

    67chat-device-proof-v1
    user UUID
    authenticated session UUID
    device_id
    SHA256(exact Base64 identity_key string), hex
    server random nonce

Клиент подписывает Signal identity private key, публикует POST /e2e/keys со стандартным bundle и device_proof:{id,signature}. challenge одноразовый, двухминутный, SQL привязывает user/session/device/key hash. Сервер проверяет настоящую Signal XEdDSA подпись: Montgomery→Edwards с sign bit из signature[63]; @noble/curves1.9.7 strict verification. Проверки включают реальные Java Curve25519.calculateSignature и browser WASM signatures, изменения message/key/signature, replay, другой session, expiry и попытку чтения/ACK чужого device mailbox.

Публикация bundle, prekeys и binding выполняется в одной транзакции с session row lock и advisory lock user/device. Неуспешный prekey write откатывает всё. Старые установленные клиенты могут публиковать bundle без proof, но не получают proof_verified_at и доступа к секретному mailbox. Не связанный сеанс не может заменить ключ уже существующего device. Новый вход, даже при полном prekey pool, заново доказывает владение. Mobile api.uploadE2EKeys покрывает также group callers; веб init и replenishment используют один proof publisher.

## QR

POST /auth/account-handoff возвращает id, secret для claim и qr с отдельным approval. QR содержит только {v:1,type:'67chat-handoff',id,approval}; session token и claim secret туда не входят. Действующее устройство вызывает POST /me/account/handoff/:id/approve {approval}. Новое устройство проверяет POST /auth/account-handoff/claim {id,secret}; 409 approval_pending, успех — обычный session response. Срок две минуты, одноразовое SQL consumption. Android камера/сканирование и веб локальный QR renderer + polling подключены; нет внешнего QR-сервиса. Локально vendored qrcode1.5.4/dijkstrajs с MIT notices. Отмена во время запроса дожидается результата и отзывает поздно созданный cookie сеанс.

## Безопасное восстановление резервной копии

Общий envelope messenger67-e2e-backup v1: PBKDF2-SHA256310000 (import210000..5000000), AES256-GCM, salt16/iv12, AAD format|v1|iterations, binary tags __b64. Password/ciphertext/account binding проверяются до записи. Native adapter валидирует private/public пары и signed prekey signatures; web staging делает то же настоящим Curve25519 wrapper. Неизвестные native_sessions/mobile_key_state сохраняются как foreign state, а не притворяются исполняемыми JS ratchets.

Android:

- signalPrepareBackupImportAsync(user,jsonKeys) возвращает JSON {id,expires_at,bundle}; только memory stage, обычный Signal временно приостановлен.
- signalSignBackupProofAsync(user,id,challenge) подписывает staged identity.
- signalCommitBackupImportAsync(user,id) атомарно заменяет encrypted vault после сохранения rollback journal; рабочее состояние сравнивается со staging snapshot.
- signalFinishBackupImportAsync(user,id,true|false) завершает или откатывает; signalRecoverBackupImportAsync(user,commit) восстанавливает после process death.
- Прямой signalImportBackupKeysAsync оставлен в API совместимости, но отвечает ошибкой: использовать staging, не silent overwrite.

restoreAccountBackup сначала валидирует repository key state, сохраняет исходное состояние в SQLCipher, получает новый unbound bearer session с отдельным account password/TOTP, выбирает новый numeric device ID, проходит server preflight, подписывает/публикует staged bundle, и только затем коммитит native vault + repository.replaceKeyState (exact transactional replacement). before durable commit любая ошибка откатывает native/repo/credentials и удаляет staged server device/session. После durable committed marker сетевой finalize повторяется при старте и не мешает офлайн запуску. SecureStore marker 67chat.restore.pending.v1 содержит токены; account metadata в AsyncStorage токенов не содержит.

Точный AccountNativeHooks контракт:

    restoreSession({password,totp?,backup_code?}) => Promise<{user,token}>
    onRestoredSession({user,token,deviceId}) => Promise<void> // ТОЛЬКО persistence, без bootstrap
    onRestoreFinished() => Promise<void> // переключение refs/state и bootstrap ПОСЛЕ durable commit

Root hooks уже подключены в App.tsx. Startup recovery выполняется до открытия account repository в React, WS и bootstrap. Repository exportKeyState/replaceKeyState включают group:/e2e: sender/receiver/room keys, исключают plaintext caches/messages; native adapter сохраняет foreign поля для последующего экспорта. Web↔Android Signal sessions не преобразуются: прежняя DM-история может быть недоступна. Native Java sessions восстанавливаются только на native, JS sessions только на JS. Ключи групп разных реализаций не объявляются автоматически переведёнными по одному наличию envelope.

Веб: account-restore.js создаёт E2EManager с memory backend, проверяет пары/подписи и account ownership. Пользователь явно выходит и входит с account password/TOTP до bootstrap. Fresh cookie session + новый device проходят preflight/proof/publication. Один IDB transaction заменяет только sig:signal:user:* / sig:gk:user:* и записывает committed marker sig:restore:user. До этой транзакции старые ключи не меняются; prepared crash marker удаляет staged device/session, committed marker восстанавливает правильный device ID до bootstrap. Ошибка finalize после commit сохраняет рабочие новые ключи и marker для повтора. Старые destructive phrase/direct import UI пути отключены. Веб экспорт дополняет глобальные group keys и foreign archive state.

Server routes: POST /me/account/backup/preflight {device_id,identity_key} требует новый unbound session и новый device; /backup/abort {restore_id} удаляет только проверенный staged device/session своего аккаунта; /backup/finalize очищает marker. Старый рабочий session не может молча заменить identity binding.

## Удаление

Пароль и TOTP (когда включён) обязательны. В одной транзакции purge удаляет ancillary personal rows по FK, private key material/device/prekeys, contacts/blocks/memberships/sessions/subscriptions/drafts/folders/schedules, очищает own message text/ciphertext/meta/media pointers и делает disabled tombstone. Shared rooms/messages других участников сохраняются; владение room передаётся живому участнику. Edit history удаляется ПОСЛЕ tombstone UPDATE повторно, потому что реальный conversation_capture_edit trigger может сохранить прежний ciphertext при UPDATE. Отдельный тест запускает настоящий trigger.

Неразделяемые owned media и previews удаляются durable job после commit; ошибки повторяются. Вложения, на которые ссылаются сообщения/аватары других участников, остаются как их shared content. Нельзя обещать удалить копии у получателей.

Android onDeleted останавливает очередь, eraseDecryptedMedia отменяет/дренирует загрузку и native decrypt, signalClearAccountAsync стирает per-account vault, memory stages, native restore journal и сохраняет permanent erased UUID guard против поздних native вызовов. JS SecureStore restore marker этого аккаунта удаляется, SQLCipher/media/registry token очищаются root hooks. Общий AndroidKeystore master alias не удаляется: он используется другими аккаунтами; per-account encrypted записи удаляются. Нельзя обещать zeroization неизменяемых объектов Java heap. Web root erase hooks удаляют account cache, Signal/group/backup/restore records и блокируют stale storage handles.

## Реальный SOCKS/Tor транспорт Android

SocksPolicy/AccountTransport устанавливает persisted SOCKS-only ProxySelector в MainApplication.attachBaseContext ДО content providers/RN/Expo clients. RN HTTP/WS/Fresco получают explicit selector и DNS guard через OkHttpClientProvider. Установленные Expo FileSystem/Video используют default OkHttp selector, APK downloader — URLConnection. SOCKS destination остаётся unresolved: имя уходит в SOCKS5 CONNECT; в protected списке нет DIRECT fallback. Binary/multipart/uploads не проходят через текстовый JS bridge. Proxy endpoint — numeric IPv4 либо ::1; proxy DNS/authentication пока не поддерживаются. Tor mode требует локальный 127.0.0.1/::1 SOCKS (например Orbot); встроенного Tor daemon нет.

Native policy — authority, AsyncStorage только metadata. Configure проверяет policy и ставит процессную паузу push до первого await: новые connect/register/retain не могут восстановить push во время переключения. После дренирования регистраций dedicated native OkHttp client через НОВЫЙ SOCKS маршрут вызывает POST /auth/account-push-disable {installation_id,secret}. Секрет установки — random32 bytes в SecureStore, на сервере только SHA256. Endpoint увеличивает epoch, ставит paused и удаляет ВСЕ account subscriptions установки. Регистрация требует текущий installation_secret/installation_epoch и explicit resume_push:true; legacy APK разрешён только пока paused=false. Регистрация, доставка и disable используют одну SQL installation lock; disable ждёт уже начатый provider send, stale выбранные subscriptions повторно проверяются перед отправкой. Уже принятый Google push отозвать нельзя. Пауза снимается только при неуспешном переключении, успешное завершается перезапуском.

Глобальная проверка выполняется в AccountRegistry даже до входа: nonsecret server origins и capability сохраняются после выхода. Для legacy установки без capability policy native сначала требует успешный owner claim через POST /me/account/push-installation. Если legacy rows существуют, а действующего owning token больше нет, включение защиты отклоняется до входа владельца. Если строк нет, public disable безопасно создаёт paused fence. Сбой SOCKS/сервера не меняет policy, восстанавливает прежние уведомления и сохраняет действующие account очередь/WS/calls. Прямой HTTP для включения защиты не требуется. После успешного отзыва hook останавливает очередь/WS/calls; native сохраняет policy, блокирует новые routes и отменяет tracked RN requests. Отдельный transport_restart процесс завершает прежний процесс и запускает приложение с новой policy, закрывая также pooled Expo connections. JS reload недостаточен. Outbox/credentials сохраняются; возврат к direct доступен офлайн на экране до входа.

FCM Google Play находится в другом системном процессе и не маршрутизируется этим app proxy. В protected mode отключены app Firebase receiver/service, auto-init и notification delegation; native callbacks отбрасываются, JS не запрашивает/регистрирует token. Настройка не претендует на маршрутизацию остальных приложений/Google Play. Звонки/WebRTC/LiveKit/демонстрация запрещены native+JS guards ДО capture/подключения; UDP через SOCKS не реализован. При недоступном SOCKS сеть приложения отвечает ошибкой, прямого fallback нет.

withAccountTransport plugin сохраняет ранний Application hook, restart Activity и manifest flags при обычном prebuild. Не запускать prebuild --clean без signing backup. Физические Android tests маршрута всех стеков/перезапуска, screenshots/recent-app preview и реальный Orbot остаются обязательной release-проверкой; JVM и compile не заменяют её. Browser page не может управлять/проверять прокси ОС: веб UI честно указывает browser-level настройку.

## Секретные чаты и несколько аккаунтов

Секретный API — expiring bounded ciphertext mailbox, отдельно от room/cloud history. Membership + proven session device обязательны. TTL30..86400, до100 pending envelopes64КБ, ACK/expiry удаляют ciphertext; plaintext bodies запрещены. Web/Android используют общий `web/secret-vault.js`: X25519/HKDF/AESGCM device state, ручная сверка fingerprint, replay/expiry checks и история сохраняются только внутри зашифрованного account-local store, отдельно от экспорта ключей. Background закрывает экран и удаляет plaintext drafts/поля, но не уничтожает ещё действующую локальную историю; повторный вход в secret screen восстанавливает ту же пару после проверки сервером. Новый device_id не может восстановить старый чат. Состояние ключа истекает через TTL после последнего создания/подтверждения/сообщения; каждое сообщение имеет собственный TTL. Таймер/следующее открытие/запуск аккаунта очищают истёкшее состояние. Explicit logout/account deletion очищают secret vault; account switch только блокирует экран. Android использует установленный Expo ScreenCapture, вызванный до показа истории; физическая проверка снимков ещё нужна. Браузерный экран подключён и явно предупреждает, что запретить screenshots/recording невозможно. Это не Double Ratchet и не per-message forward secrecy. Экспорт запрещён. Для восстановления локальной истории нужно соединение: авторизация exact device проверяется до показа, иначе невозможно подтвердить, что собеседник не удалил чат. Подробнее: `docs/SECRET_CHATS.md`.

AccountRegistry сериализует close→open, хранит tokens только SecureStore. Root переключение закрывает старые WS/queues/repository/caches, новые ответы проверяются token/generation. Текущий APK поддерживает аккаунты одного фиксированного сервера; namespace helper включает origin+UUID. Native rotation регистрирует FCM token для всех сохранённых аккаунтов, inactive active_room_id=null; switch деактивирует foreground suppression прежнего аккаунта, сохраняя подписку, logout удаляет свою. Incoming recipient управляет suppression/quiet prefs и явным переключением на tap/quick reply. Native системный answer другого аккаунта сначала требует подтверждённого переключения и API проверки владельца; snapshot запрашивается только его token, late responses после switch игнорируются, app lock запрещает autoanswer. Старый pending без recipient не запускает запрос/захват; текущий WS звонок остаётся доступен обычным способом.

Browser multi-account использует отдельные HttpOnly account cookies; JS хранит только выбранный UUID вкладки. X-Account-Id и account_id query выбирают соответствующий cookie, Bearer имеет прежний приоритет. List/select/remove проверяют действительность own session; никакие bearer tokens в browser registry не записываются. Общий endpoint push хранит подписку каждого аккаунта независимо, recipient/tag/tap различают аккаунты. См. docs/plans/push-accounts-integration.md.

## Проверки

Целевые Node suites: account-features, account-integration, account-device-proof, account-restore, web-account-restore, web-account-auth, auth-flow, mobile-data, call-features, web-syntax/web-handlers. Они используют Fastify/PGlite, настоящий SQL trigger, actual SQLite, Java/browser Curve signatures, реальный IndexedDB backend, backup failure/recovery и capture guards.

Native: gradlew :67chat-room-key:testDebugUnitTest :app:compileDebugKotlin. Одиннадцать JVM тестов: BackupBridge3 (реальные libsignal keys/signatures/foreign/native/group/room roundtrip), KDF2, SOCKS6 (OkHttp + global URLConnection remote DNS, binary upload128КБ, unavailable proxy не касается direct target, blocked/invalid policy, strict capability revocation, legacy owner claim через SOCKS). Полный app Kotlin compile прошёл. Дополнительно account-push-policy, account-transport, mobile-push и native-call-account проверяют actual SQL fence, logged-out preflight, pause→new connect race, rotation/recipient suppression и подтверждённое call account routing. TypeScript tsc --noEmit проходит. Физические device/browser проверки выполняются отдельно; source/JVM проверки не означают production release этапа6.
