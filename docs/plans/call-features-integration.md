# Этап 5: интеграция звонков

Реализованы P2P-история с постоянными событиями в переписке, единая занятость P2P/SFU между устройствами пользователя, второй входящий, восстановление ICE, демонстрация экрана и настоящий LiveKit-клиент. Проверка Android↔Android↔браузер с реальными сетями и шесть одновременно подключённых участников остаётся отдельной физической проверкой. SFU по умолчанию отключён: LIVEKIT_ENABLE_GROUP_CALLS=false. До внедрения и проверки политики отзыва уже выданных токенов не включать его в обычный выпуск. Наличие настроек не означает доступность SFU: join возвращает 503 при отключённой функции/отсутствии конфигурации/доступа к серверу.

## Сервер

1. Импортировать `installCallFeatures`, `createLiveKitService`, `callFeaturesMigration` из `api/call-features.js`. Последний экспорт — **строка SQL**, а не функция. Добавить SQL в `runMigrations()` и те же определения таблиц в `db/init.sql`. Миграция идемпотентна; создаёт `call_history` и `group_call_leases`.
2. До прослушивания порта:

```js
const callFeatures = installCallFeatures(app, {
  pool, requireAuth, sendToUser, broadcastToRoom,
  notifyRoomListUpdateForRoom,
  sfu: createLiveKitService(),
  // Необязательно: получить полную обычную форму message.new.
  // getMessage: async id => (await pool.query(fullMessageSQL,[id])).rows[0],
});
const relayCall = installCalls(app, {
  pool, requireAuth, sendToUser, sendToSession, sendToUserExceptSession,
  sendToUserExceptEndpoint, push: sendPushNotification,
  history: callFeatures.history,
  canCall: (from,to) => account.canWrite(from,to),
});
callFeatures.setRelay(relayCall); // Обязательно для общей занятости P2P/SFU.
await callFeatures.history.recover();
```

`installCalls` по-прежнему синхронно возвращает вызываемую async-функцию. Добавлены методы `relayCall.revokeUser(userId,{exceptSessionId?,onlySessionId?})`, `relayCall.revokeSession(userId,sessionId)`, `relayCall.isBusy(userId)`. Отзыв одной сессии вызывает `revokeSession`, отзыв остальных — `revokeUser` с исключением оставляемой сессии, бан/удаление — полный `revokeUser`; выход соседнего устройства не завершает разговор владельца. SFU-аренда содержит session_id без FK: удаление sessions не должно уничтожать запись раньше отключения медиа; удаление участника группы вызывает `callFeatures.revokeMembership(roomId,userId)` **до** удаления строки/пользователя. Это прекращает медиа через LiveKit RemoveParticipant и освобождает аренду.

WS-шлюз должен пропускать дополнительно `call.waiting.accept`, `call.waiting.reject`, `call.renegotiate`, `call.renegotiate.answer`, `call.resume`. Для каждого сокета сервер генерирует свой `callEndpointId` и вызывает relay(msg,me,{id:socket.callEndpointId,send:payload=>socket.send(JSON.stringify(payload))}). Идентификатор берётся только из серверного контекста, не из сообщения. `me.sessionId` обязателен. `sendToSession(user,session,payload)` и `sendToUserExceptSession(user,session,payload)` обязательны при установке; `sendToUserExceptEndpoint(user,endpointId,payload)` исключает только выигравший сокет, а не все вкладки одного cookie. Поздний сигнал другого устройства/вкладки не может прекратить или изменить выбранный звонок. Приватность проверяется и на последующих сигналах. Сигналы SDP/ICE не записываются в БД. Вызовы сериализованы в одном API-процессе; несколько API-реплик без общей сигнализации не поддерживаются.

`history.start` записывает попытку до доставки offer; `answer` фиксирует ответ; `save` одной SQL-командой фиксирует окончание и вставляет ровно одно `messages(type='call',id=call.id)` с разрешёнными полями статуса/времени/участников. Затем отправляются `room.call` участникам и обычный `message.new` комнате. При перезапуске незавершённые звонки становятся `missed`/`interrupted` и получают такие же сообщения. Если отправитель уже вышел из комнаты, личная история сохраняется, новое сообщение от его имени не создаётся. Клиент должен распознавать `type='call'` как системное событие, не посылать его в дешифратор текста.

Статусы: `completed`, `missed` с reason=`timeout`, `rejected`, `busy`, `cancelled`, `interrupted`. Завершённый разговор сохраняет `completed` и причину окончания (`replaced`, `session_revoked`, и т.д.).

API:

- `GET /calls/history?room_id=&limit=40&before=&before_id=` — только собственные вызовы, составной курсор, до 100 строк.
- `POST /rooms/:id/calls/join` — членство в группе, блокировка занятости, ограничение LiveKit комнаты до 6, токен только этой комнаты/identity на 90 секунд. 409=`busy`; 503=`group_calls_unavailable`.
- `POST /rooms/:id/calls/heartbeat` с `{lease_id}` из join — каждые 30 секунд; продлевает аренду до 120 секунд только действующему участнику.
- `POST /rooms/:id/calls/leave` с `{lease_id}` — завершить SFU-участие. После обрыва/смерти процесса аренда истекает, серверный watchdog отключает участника. Идентификатор аренды не позволяет запоздалому старому устройству продлить/прекратить новый звонок другого устройства.

После долгого createRoom сервер повторно проверяет членство, текущую аренду и сессию, удерживая строки аренды/участника/комнаты/сессии до завершения подписи токена. Отозванное за время ожидания членство не получает токен. Просроченная аренда сначала удаляет прежнего SFU-участника из прежней комнаты, затем выдаёт новую.

Пользовательская аренда запрещает второй SFU-вход даже в ту же комнату с другого устройства. При P2P-входящем во время SFU абонент получает `call.waiting`; действие accept сначала отключает SFU. Неподтверждённый второй входящий заканчивается через 45 секунд. Существующий подписанный LiveKit-токен остаётся пригодным до истечения 90 секунд: RemoveParticipant прекращает текущую сессию, но полная моментальная отмена уже выданного токена требует отдельной политики LiveKit/webhook. Не обещать мгновенный отзыв токена на уровне SFU.

## Android

Закреплённые зависимости после проверки нативной сборки: `@livekit/react-native@3.0.0`, `@livekit/react-native-webrtc@144.2.0`, `livekit-client@2.20.1`, `@livekit/react-native-expo-plugin@1.1.0`, `@config-plugins/react-native-webrtc@14.0.0`. Версия 15 последнего плагина требует Expo 56 и несовместима с Expo 55. Используется **один** поставщик WebRTC: `@livekit/react-native-webrtc@144.2.0` для P2P, диагностики и SFU. Оригинальный `react-native-webrtc@124` удалён. Префиксных C++/ObjC-символов LiveKit 3 недостаточно для совместного Android-autolinking двух Java NativeModule-поставщиков: это приводит к одинаковому имени WebRTCModule. `react-dom@19.2.0` согласован с React; npm peer dependencies должны проходить без force/legacy-peer-deps.

Порядок плагинов:

```js
['@livekit/react-native-expo-plugin', {
  android: {audioType:'communication', enableScreenShareService:true}
}],
'@config-plugins/react-native-webrtc'
```

Нужны разрешения `FOREGROUND_SERVICE`, `FOREGROUND_SERVICE_MEDIA_PROJECTION`; служба MediaProjection и разрешение пользователя обязательны для реального Android-экрана. `mobile/src/features/livekitBootstrap.ts` вызывает `registerGlobals()` до импортов клиента. Экран `CallFeatures` использует общий `FeatureContext` и режимы `calls`/`call-history`/`group-call`; его можно показывать для любого из этих режимов. `GroupCall` действительно подключается, показывает видео/участников, публикует микрофон/камеру/экран и освобождает AudioSession.

Существующий P2P `WebRTCCall`:

```ts
onRenegotiation: offer => send({type:'call.renegotiate',room_id,offer}),
onScreenShareChange: enabled => setSharing(enabled),
```

`call.renegotiate` → `await active.handleOffer(data.offer)`, ответ `call.renegotiate.answer`; этот ответ → `active.handleAnswer(data.answer)`. `expo-network.addNetworkStateListener` вызывает `active.restartIce()` при смене сети; удалить listener на окончании/выходе. Только инициатор формирует ICE-restart, чтобы обе стороны не создавали offer одновременно. `call.waiting` держать отдельно от текущего вызова: «Завершить текущий и ответить» отправляет `call.waiting.accept`, «Отклонить» — `call.waiting.reject`; настоящий offer приходит после подтверждения сервером. Сервер отправляет `call.accepted` **только проигравшим endpoint** (phase selected/answered). На нём всегда локально освободить соответствующий peer/таймеры и убрать входящий/принятый UI **без отправки hangup**: локальная камера могла уже открыться до проигрыша гонки. Победитель получает `call.answered`. `call.created`/`call.answered` содержат секретный `resume_token`, а адресный offer после waiting.accept также может содержать его. Хранить только в памяти текущего звонка; после нового WS auth.ok отправить `call.resume` с call_id/room_id/resume_token. Сервер сверяет прежнюю authenticated session и capability, переводит владение на новый сокет и отвечает `call.resumed`; другой участник получает `call.peer_reconnected`. После успешного resume инициатор может перезапустить ICE. Обычный call_id не даёт права забрать звонок между вкладками. Серверный onClose вызывает relay.endpointClosed(socket.callEndpointId): закрытие только владеющего сокета запускает 30-секундный период восстановления. Resume отменяет этот срок; иначе звонок завершается network_error, освобождая занятость. Старый APK без resume-протокола продолжает работать до переподключения WS, после чего вызов завершится по этому сроку; новый APK нужен для сохранения звонка при WS reconnect. Кнопки экрана вызывают `startScreenShare/stopScreenShare`; в аудио-P2P без video sender выдаётся явная ошибка, в SFU аудио/экран независимы.

Захват запрашивает echo cancellation/noise suppression/AGC, реальный эффект зависит от аудиодвижка/устройства. Публичный STUN fallback в новом нативном P2P-коде удалён: использовать выданные своим `/rtc/ice-servers` адреса. После завершения очищаются tracks, обработчики, отложенные ICE и restart timers; поздний результат разрешения камеры не воскрешает звонок.

## Веб

`openCallFeature(mode,context)` из `web/call-features.js`, где `context={request,room,me,openRoom,report?}` с обычным относительным API-клиентом, открывает историю и настоящий групповой звонок. История создаёт ссылки на исходные комнаты. SDK загружается локально из `/vendor/livekit-client.esm.mjs`; скопировать файл из установленного `mobile/node_modules/livekit-client/dist/` и лицензию. Добавить `.mjs` в расширения bundle-скрипта либо сохранить ESM как `.js` и изменить URL. Внешние CDN не используются.

`enhancePeerCall(peer,{isInitiator,sendOffer,onError})` подключается к существующему `SecureWebRTC` после `initCall()`. `sendOffer` отправляет `call.renegotiate`. Методы `startScreenShare`, `stopScreenShare`, `restart`, `dispose`; dispose вызывать перед `peer.hangup()`. Удалить старый автоматический `onconnectionstatechange -> restartIce()` у этого peer, чтобы не отправлять два restart-offer. Входящий renegotiate: `handleOffer` + `createAnswer`, затем сигнал `call.renegotiate.answer`; ответ передать `handleAnswer`.

Call-only hooks уже интегрированы в server.js, App.tsx/startConnection.ts и web/app.js. Нативный запуск/приём проверяет поколение после каждой асинхронной границы; запуск/остановка аудиорежима выполняются общей очередью, чтобы поздний старый start не оставил активный аудиорежим или не остановил новый звонок. Браузер защищает поколения SDK join, PC/capture и обработчиков; поздний старый capture освобождает только собственные потоки.

После интеграции web-shell обновить версии index.html и CACHE_NAME вместе, добавить новые серверные файлы/compose overlay и SDK в список поставки.

## Собственный SFU

Опциональный `docker-compose.calls.yml` подключается к основному server-compose. По умолчанию overlay оставляет LIVEKIT_ENABLE_GROUP_CALLS=false. Нужны закреплённый проверенный `LIVEKIT_IMAGE`, отдельные `LIVEKIT_API_KEY` и `LIVEKIT_API_SECRET`; существующий `.env` не заменять.

Публичный signaling идёт **только** через проверяющий сеанс/членство/lease шлюз API (`wss://<SITE_DOMAIN>/api/livekit`). Внутренний LiveKit API `http://livekit:7880` доступен только приватной Compose-сети; не публиковать 7880 через ports, Caddy или отдельный домен. Прямой reverse_proxy на 7880 обходит отзыв JWT и запрещён. Клиентский identity включает UUID пользователя и уникальный lease_id; токен старого lease шлюз не принимает. Отзыв закрывает signaling до RemoveParticipant; при недоступном управляющем API запись revoking сохраняется для повторного отключения.

Открываются только медиапорты 7881/TCP, 7882/UDP. LiveKit сам не отзывает ранее подписанные JWT на self-hosted установке: защита требует обязательного шлюза без обходного signaling-маршрута. Флаг выпуска остаётся выключенным до отдельной проверки шлюза, отзыва активного медиа, Android↔браузер и нагрузки шесть участников. SFU не является групповым E2E: клиент прямо сообщает, что собственный сервер обрабатывает потоки.

Источники совместимости: [официальный Expo quickstart](https://docs.livekit.io/home/quickstarts/expo), [SDK package.json](https://github.com/livekit/client-sdk-react-native/blob/main/package.json), [префиксные сборки v3](https://github.com/livekit/client-sdk-react-native/releases), [порты LiveKit](https://docs.livekit.io/transport/self-hosting/ports-firewall/), [screen share](https://docs.livekit.io/transport/media/screenshare/). Версии Expo-плагинов проверены по npm peerDependencies, не взяты из примера latest.

## Проверка

`node --test tests/calls.test.mjs tests/call-features.test.mjs`; `cd api && npm test`; `cd mobile && npm test`; `cd mobile && npx tsc --noEmit`. PGlite проверяет настоящие запросы SQL, одно постоянное событие на звонок, приватность истории, восстановление после перезапуска, SFU-членство/занятость. Тесты исполняют нативный TS и браузерный SDK-контроллер с подменой только недоступной в Node медиа/сетевой границы. Нужны отдельные физические проверки микрофона/эха, отзыва Android MediaProjection, Wi-Fi↔LTE, двух устройств, восстановления из фона и нагрузки шесть участников. Ничего не публикует APK/сервер автоматически.


## Медиаресурсы и аудиосессия

`mediaLifecycle.ts` отменяет задачи конкретного аккаунта; `cancelDecryptedMedia(userId)` вызывается при выходе/переключении, `eraseDecryptedMedia(userId)` — перед удалением ключей и локальной БД. Расшифровщик пишет во временный файл, публикует verified-кэш только после успешного native completion и проверяет поколение после каждого await. Удаление дожидается native decrypt/record/copy и чистит собственные каталоги; поздняя операция не создаёт plaintext заново.

`playbackOwnership.ts` объединяет voice/video/PiP; Expo Video playingChange охватывает nativeControls, звонок останавливает всех владельцев и текущую запись. Viewer transition останавливает предыдущий ролик; поздняя загрузка voice не отбирает воспроизведение. audioSession сериализует смену режима и не позволяет recorder cleanup сбросить активный call mode. Android file action использует ACTION_VIEW с content URI и временным правом чтения; отдельная кнопка «Поделиться» сохраняет share-сценарий. Viewer generation проверяется до внешнего запуска и сохранения в галерею.

`node --test tests/media-lifecycle.test.mjs tests/audio-session.test.mjs` проверяет отмену после permission/copy/native capture/decrypt, очистку аккаунта, late voice ownership, PiP/native control events и viewer-close до ACTION_VIEW. API/SDK/native I/O подменены только на границе недоступной в Node платформы. Kotlin metadata retriever читает фактический готовый файл: display width/height, duration_ms, rotation, orientation=1 для нормализованного layout. Физические EXIF6/8 и rotated-video fixtures, настоящий Android chooser/PiP и аппаратное эхо остаются отдельной проверкой устройства, не результатом Node тестов.

## Проверка шлюза и собственной SFU 23.10.2026

На изолированной БД/Compose проверен LiveKit1.13.10, image `livekit/livekit-server@sha256:89ca9ce0791e18d6e03bf6e2c49ceb27e1c754eae8bd50c989386c1a5963791a`. Шесть настоящих SDK-клиентов RTC-node1.2.1 отправляли синтетический звук и каждый принимал пять потоков, суммарно51521 декодированный аудиокадр в финальном прогоне. SFU отклонила седьмого. Отзыв аренды удалил участника/медиа, повторное соединение с ещё не истёкшим JWT было отвергнуто шлюзом. В выборке при шести участниках SFU:12.39% CPU/58.82MiB, API24.27%/50.5MiB; это один короткий синтетический аудиопрогон, не гарантия ёмкости для шести камер/показа экрана.

Реальный SDK использует также Bearer-заголовок при WS handshake: шлюз теперь передаёт только проверенный токен upstream, что покрыто регрессией. Защитный шлюз обязателен; более раннее утверждение о пригодности отозванного токена относится к прямому LiveKit без шлюза, который публично не открывается. При авто-reconnect SDK может ещё показывать переподключение; сервер при этом уже прекращает медиа и не выдаёт доступ.

Для production использовать оба compose-файла: `docker compose -f docker-compose.server.yml -f docker-compose.calls.yml up -d --build --wait`. В существующий .env добавлять только отсутствующие LIVEKIT_IMAGE/API_KEY/API_SECRET и явный LIVEKIT_ENABLE_GROUP_CALLS; старые ENCRYPTION_KEY/пароли/FCM не заменять. Медиапорты7881TCP/7882UDP;7880 остаётся приватным. При отказе вернуть флаг false и пересоздать API, существующие P2P-звонки сохраняются. Физическая проверка новой APK, Wi-Fi↔LTE, Bluetooth/эхо и MediaProjection не выполнена: пользователь отменил USB-приёмку.
