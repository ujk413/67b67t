# Интеграция переписки и сообществ

Основная реализация: `api/conversation-features.js`, повторяемые миграции `api/conversation-features.sql`, веб `web/conversation-features.js`, Android `mobile/src/features/ConversationFeatures.tsx`. Модуль не изменяет существующие отправку, комментарии, E2E и папки альтернативными маршрутами.

## Подключение API

После базовых миграций (должны существовать `messages.read_by`, `client_message_id`, `rooms.pinned_message_id`, `e2e_enabled`, `hidden_messages`, `room_preferences`, `group_sender_keys`) и до `listen`:

```js
import {installConversationFeatures} from './conversation-features.js';
const conversations = await installConversationFeatures(app, {
  pool, requireAuth, isRoomMember, getRoomRole, broadcastToRoom,
  notifyRoomListUpdate, evictRoomUser,
  getMessage: id => getMobileMessage(pool, id, decryptMetadata),
});
let conversationWorkerBusy = false;
const conversationTimer = setInterval(async () => {
  if (conversationWorkerBusy) return;
  conversationWorkerBusy = true;
  try { await conversations.processJobs(); }
  catch (error) { app.log.error({code:error.code}, 'conversation jobs failed'); }
  finally { conversationWorkerBusy = false; }
}, 15000);
conversationTimer.unref();
app.addHook('onClose', async () => clearInterval(conversationTimer));
```

`getMessage` необязателен: с ним WS содержит привычный нормализованный объект. Без него публикация возвращает поля `messages` и пустые реакции. Вызовы worker из нескольких API-процессов безопасны: задачи захватываются `FOR UPDATE SKIP LOCKED`; INSERT и отметка `sent` фиксируются одной транзакцией. WS после COMMIT не является долговременной доставкой — пропущенное событие восстанавливается обычной синхронизацией. Напоминания не отправляются собеседнику: статус `due` виден владельцу в списке; `notifyRoomListUpdate` сообщает об обновлении. Системный push напоминаний требует отдельного подключения планировщика к существующему push-сервису.

В `db/init.sql` включить тот же SQL после базовой схемы; installer повторно применяет идемпотентный файл на существующей базе. Добавить оба новых API-файла в серверный bundle.

### Действующие пути отправки/правки

Триггеры БД покрывают HTTP, WS и комментарии без дублирования мессенджера:

- `conversation_message_guard`: сериализует отправки одной комнаты; медленный режим для участников (owner/admin освобождены), повтор одинакового шифротекста/текста+метаданных+media_id в течение 10 секунд. Хранится MD5 содержимого, не его копия. Повтор с тем же `sender_id/client_message_id` пропускается до существующего `ON CONFLICT`. Разные E2E ciphertext одного текста невозможно распознать на сервере.
- `conversation_edit_history`: сохраняет только прежний `encrypted_content` или прежний legacy AES ciphertext с минимальными `encrypted/e2ee_v/iv`; открытый текст в историю не попадает. Новая версия сохраняется обычным edit-маршрутом.
- `conversation_members_epoch` и `conversation_e2e_epoch`: изменение состава или E2E увеличивает `conversation_epoch`. Устаревшие отложенные сообщения переходят в `needs_reencrypt` и требуют повторного клиентского шифрования. После удаления участника надо сохранить существующее `room.members_changed`/ротацию sender keys, для batch/public join модуль уже отправляет это событие.
- `conversation_legacy_pin_sync`: прежний `/pin` добавляет закрепление в общую таблицу; legacy unpin удаляет только текущую закреплённую запись. Новый DELETE выбирает оставшееся последнее закрепление для legacy-поля.
- автоудаление комнаты `auto_delete_seconds`: применяется только к новым сообщениям, отсчитывается от отправки. Индивидуальное `meta.self_destruct_seconds` (0–604800) начинается однократно с первого обновления `read_by`, содержащего чужого читателя. До чтения индивидуальный таймер не запущен; при сочетании политик действует более ранний срок. Поля: `self_destruct_seconds`, `self_destruct_started_at`, `expires_at`.

В общем обработчике ошибок SQL `P0001` + message `slow_mode`/`duplicate_spam` отображать как HTTP 429 (WS — такое же машинное поле `error`). Ошибки `not_member` — 403; `bad_self_destruct_seconds` — 400. Не показывать SQL/параметры в клиентском ответе.

Удаления worker выполняются через обычный DELETE. Существующий триггер `messages` → `message_changes` обязан записывать `delete`, включая TTL: это удаляет устаревшие записи из постоянных клиентских кешей. WS `message.delete` модуль рассылает самостоятельно. Если journal установлен в другом installer, он должен быть подключён до запуска worker.

### Маршруты и форматы

Все приватные маршруты проверяют авторизацию и членство. UUID и ограниченные числовые/текстовые значения валидируются; hidden_messages исключаются из закреплений, истории и date anchors.

| Маршрут | Формат |
| --- | --- |
| `GET /rooms/:id/pins` | Массив сообщений с `pinned_at/pinned_by` |
| `PUT,DELETE /rooms/:id/pins/:messageId` | 200 `{ok:true}`, группы/каналы только owner/admin |
| `GET /rooms/:id/draft` | `{revision,encrypted_content,updated_at?}`; отсутствие = revision 0 |
| `PUT /rooms/:id/draft` | `{revision,encrypted_content:string|null}`; CAS увеличивает revision; 409 `{error:'draft_conflict',current,incoming}` сохраняет обе зашифрованные версии |
| `GET /rooms/:id/anchor?date=<ISO>` | `{message_id,created_at}`; первая доступная запись начиная с даты, либо null |
| `GET /rooms/:id/messages/:messageId/history` | До 100 прежних зашифрованных версий с id/sender_id/room_id/created_at |
| `POST /me/read-all` | Все доступные входящие сообщения, сброс `marked_unread`, события чтения |
| `GET,PUT /rooms/:id/conversation-settings` | `slow_mode_seconds:0..3600`, `auto_delete_seconds:0/3600/86400/604800/2592000`, `public:boolean`, `public_title`, `slug`, `description`; запись owner/admin, dm запрещён |
| `GET,POST /rooms/:id/scheduled` | POST `{send_at:ISO,kind:'message'|'reminder',encrypted_content}`; GET только своих задач, включая due/cancelled/needs_reencrypt |
| `DELETE /rooms/:id/scheduled/:scheduleId` | Отмена только своей задачи |
| `POST /rooms/:id/members/batch` | `{action:'kick'|'admin'|'member',user_ids:[uuid...]}` до 100; отдельный результат каждого, owner/self защищены, admin не меняет админов |
| `GET /communities?q=...` | Только явно опубликованные группы/каналы: id/kind/title/slug/description/member_count, до 50 |
| `GET /communities/:slug` | Только публичный preview, никаких сообщений или списка участников |
| `POST /communities/:slug/join` | Идемпотентное вступление, `{ok:true,room_id}` |

Draft/schedule — opaque encrypted payload, лимит 256 KiB; поле `text` в запросе запрещено. Клиент **обязан** вызвать действующий encoder и проверить наличие шифрования до запроса. Формат сохранения `{v:1,body:<обычное зашифрованное исходящее сообщение>}` сохраняет legacy AES и group sender-key кодировки. `unpackConversationBody` возвращает только разрешённые ciphertext/meta поля для обычного INSERT; `wrapEncryptedBody` веба запрещает plaintext fallback. Сервер не способен криптографически доказать, что произвольная строка — шифротекст.

## Минимальные правки существующих members/folders

В существующем `GET /rooms/:id/members` выполнять поиск по расшифрованному `display_name`: в базе оно может быть шифротекстом. Нельзя делать ILIKE по этому полю и затем ожидать совпадения с введённым именем. Root реализует поиск/пагинацию после безопасной расшифровки разрешённых членством записей; X-Total-Count должен отражать отфильтрованный набор. `username` не зашифрован, но комбинированный поиск должен сохранять одинаковую семантику COUNT/страницы.

В существующем `organization.js` (маршруты не дублировать):

```js
import {listConversationFolders,reorderConversationFolders} from './conversation-features.js';
// existing GET /me/folders handler:
return listConversationFolders(pool, me.id);
// new action in the same folder installer, before parameterized PUT:
app.put('/me/folders/order', async req => {
  const me = await requireAuth(req);
  const result = await reorderConversationFolders(pool, me.id, req.body?.items);
  await notifyRoomListUpdate(me.id);
  return result;
});
```

Порядок отправляется как `{items:[{id,revision},...]}` с полным списком папок; конфликт 409 требует повторной загрузки. У существующего UPDATE title/room_ids добавить `revision=revision+1,updated_at=now()`, при записи проверять переданный revision. Создание/изменение/удаление должны вызывать `notifyRoomListUpdate(me.id)`; GET отдаёт `position,revision,updated_at,unread_count`. Счётчики считают сообщения, не hidden_messages и не сообщения чужих комнат. Вставка новой папки position = `coalesce(max(position),-1)+1` для её владельца.

## UI и контексты

Обе темы используют существующие токены; веб строится DOM API/textContent. Экранные режимы: `conversation,pins,drafts,scheduled,history,members,settings,communities,channels` плюс Android `comments` (существующие `/posts/.../comments`). В `conversation` доступны переход по дате/прочитать чат/прочитать все и навигация. История и закрепление допускают ID сообщения; связать с контекстным меню сообщений для удобства.

Веб `openConversationFeature(mode,context)`: `request(path,{method,body})` принимает объект и сам JSON-кодирует, ошибки содержат `status`; `me,room,rooms`, `openRoom(id,messageId?)`, `refresh()`, `decode(message)`, **`buildText(roomId,text)`** возвращает исходящее тело без отправки; `report(error)` и `local?.get(key)` необязательны. Не передавать напрямую старый `api`, который ожидает JSON-строку: адаптер должен вызывать `api(path,{method,body:body===undefined?undefined:JSON.stringify(body)})`. В конфликтах черновика серверная версия показана отдельно, текст редактора остаётся; пользователь выбирает/объединяет и повторно сохраняет с новой revision.

Android `ConversationFeatures({mode,context})`: базовый `FeatureContext` плюс `buildText(roomId,text):Promise<Record<string,unknown>>`, использующий действующую mobile-криптографию. `decode` получает синтетическое обычное сообщение. `onOpen` открывает chat/message, `onMode` переключает дочерний экран. История не получает медиа/plaintext, comments используют существующую канальную модель (не E2E-группы).

Ограничения, которые не скрыты: модули не вставляют кнопки в чужие entrypoints; root должен подключить экраны и worker. Автосинхронизация редактора при наборе и уведомление напоминания в ОС требуют подключения к существующему composer/push. Модуль предоставляет ручную синхронизацию с разрешением конфликта и серверное долговременное расписание. Физическая проверка двух устройств и системных уведомлений не заменяется тестами PGlite.

Проверка: `node --test tests/conversation-features.test.mjs`; `cd mobile && npx tsc --noEmit`. Не запускать destructive `test.sh/test.ps1`.

## Исправления по результатам независимого review

- Batch kick требует `deps.evictRoomUser(roomId,userId)`: await вызывается под блокировкой комнаты **до DELETE и COMMIT**, затем производится fanout. Без hook возвращается 503, при исключении транзакция откатывается. Hook обязан удалить все подписки пользователя/устройств из room fanout и не делать самостоятельных COMMIT.
- Расписание проверяет актуальный `e2e_enabled` и `meta.e2e_group` при создании и при публикации. Sender-key envelope проверяется по форме/версиям/base64/размерам/итерации; подпись криптографически проверяют получатели с их ключами. Legacy AES запрещён для обязательного группового E2E; устаревшая/неправильная сохранённая задача получает `needs_reencrypt`. Канальная публикация вставляется с `is_post=true`, сохраняя существующие комментарии.
- `next_attempt_at`, `attempts`, `last_error` сохраняют повторные попытки. Slow mode/duplicate backoff начинается с 15 секунд и растёт до часа; другие due-задачи продолжают обработку. Неожиданные ошибки сохраняются как `publication_failed`, получают задержку минуту, затем worker сообщает AggregateError после попытки обработать независимые задачи. В БД не записывается исходная SQL-ошибка/параметры. GET scheduled отдаёт поля повторных попыток.
- Публикация сообщества требует явного `public_title` (1–120 символов) вместе с `public:true`, slug и description. `rooms.title` никогда не попадает в публичную выдачу и поиск; выдача использует только новый открытый opt-in `public_title`. SQL не умеет расшифровать старые room titles: уже существующие public_slug без public_title скрыты до повторного подтверждения публикации владельцем. Поле public_title показывается в обеих формах настроек и очищается при снятии публикации.
- Выбранный в панели черновик сохраняется в локальный формат реального composer: web `local.set('draft:'+roomId,text)`, mobile `repository.set('draft:'+roomId,{...prior,text})` с сохранением reply. Необязательный контекстный `setDraft(roomId,text)` обновляет активный composer; root должен предоставить hook, а автоматическую CAS-синхронизацию composer подключает отдельно. Помощники экспортируются и тестируются; выбор серверной версии и ручное сохранение вызывают эти помощники.

## Подключённый веб (заменяет прежнее ограничение об entrypoints)

`web/app.js` и `web/views.js` теперь подключают экраны и стандартные адаптеры JSON/FormData, шифрования/дешифрования, account-scoped local store и composer. Меню переписки содержит multiple pins, черновики, расписание, date/unread anchors, read/all-read, participants/E2E/settings, export; меню чатов содержит hidden list, global search и folder order. Кнопка дополнительных сообщений открывает expressive modes, форматирование и индивидуальное исчезновение после чтения. Папки показывают серверные unread_count, редактор передаёт revision, порядок использует полный CAS-список.

Автоматические веб-черновики используют общий `web/draft-sync.js` (`DraftSync`), отдельный debounce на комнату и защищённый JSON `{format:'67chat-draft-v1',draft:{text,reply}}` внутри обычного шифротекста. Старые простые текстовые черновики читаются совместимо. Initial open, online/foreground и rooms.update запускают сверку. Конфликт не заменяет локальный ввод: диалог предлагает локальную, серверную или объединённую версию. Координатор повторно сверяет локальный ввод после сети/дешифрования. При logout таймеры/координатор останавливаются; manual panels распаковывают JSON и сохраняют reply.

Пересылка позволяет выбрать несколько получателей и комментарий, показывает результат каждого чата; повтор использует стабильные client_message_id и пропускает уже поставленные в очередь пары. Ключ encrypted attachment перепаковывается для получателя; media captions проходят обычную очередь. Shift-click/контекстный диапазон выделяют только явно загруженные сообщения. Необходимо загрузить предыдущую историю для более широкого диапазона; UI не заявляет, что выбрана вся незагруженная переписка.

Глобальный поиск явно читает все authorized history pages, дешифрует на клиенте, сохраняет локальный кеш, позволяет остановить и сообщает число проверенных записей. Hidden chats и message tombstones исключены. Общий `fetchAllRoomMessages` обнаруживает stalled cursor/слишком большой обход и сообщает ошибку вместо молчаливого обрезания. Результаты открывают точный message anchor.

`web/message-format.js/.d.ts` — общий plaintext tokenizer для веба и native: text/code/codeblock/quote/bold/italic/strike/spoiler/link/mention. HTML formatter экранирует текст; URL разрешены только http/https без credentials. Native читает токены, не HTML. Web IME Enter не отправляет сообщение до завершения композиции.

Expressive controls скрыты из обычных пузырей, но входят в reducers; poll/location renderer получает полную авторизованную историю, а не только текущие 300 сообщений. Сообщения обрабатываются после дешифрования; превью используют expressiveSummary. Account logout останавливает live location и очищает in-memory event history. Account settings связаны с `openAccountFeature`; резервное копирование использует реальные E2E export/import hooks. Динамический `openCallTools` подключает отдельный call-features UI.

Canonical links `https://67chat.ru/chat/{room}/message/{message}`, `/chat/{room}`, `/profile/{username}`, `/invite/{token}` используют общий deep-links parser; остаются pending до авторизации и работают с popstate. Legacy `?room&msg` и `/user/{username}` сохранены. Invite join использует действующий endpoint и его `{id}` ответ.

Проверки доказывают API/модули/ключевые реальные callbacks (IME, маршрутизация до/после auth, частичный retry пересылки); физическая браузерная/двухустройственная проверка ещё нужна. ОС-push напоминаний, account-store/Signal очистка при удалении аккаунта и полноценные account switching/secret-chat hooks относятся к root/account integration, не подменены фиктивными кнопками.
