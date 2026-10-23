# Execution ledger — plan: docs/plans/2026-10-23-remaining-features.md

Base: 58c0c880d8de1848e88a82c391b42ac3c1fda14b. Branch: codex/remaining-features.

Ruling: work in a new branch of the clean primary checkout rather than a second checkout — preserves the previous PR and uses installed native dependencies/signing assets without copying secrets; concurrent edits would require reconciliation.

Pre-flight: media queue, encryption metadata, sync and account isolation are shared between stages 1–3; preserve stable client_message_id and capture the originating account/room before asynchronous work. New message types must use encryption before upload; never put plaintext secrets in metadata.

Baseline: immediately before implementation, 56 API/web tests and 81 mobile tests passed; typecheck passed. No production or phone changes in this run.

## Stage 1 — published; physical acceptance waived by user

- APK download currently deletes the previous file; use durable byte-range chunks and a persisted release identity instead of opaque native resume data, so process death is recoverable.
- SHA-256 must be streaming/on a native worker; avoid loading a 65 MB APK into the JS heap.

Implemented: durable range download + SHA-256 on a native worker; release identity changes reset the file; hash verification gates installer retries; reconnect retries transient downloads. Android media moves atomically from draft to persistent outbox with a stable ID, encrypted-file checkpoint, cancellation and cache protection. Browser stores the encrypted upload blob in its account outbox and uploads after reconnect. Added API/authenticated-WS/actual-TURN-relay diagnostic screens and a distinct synchronization status.

Verification: 56 API/web tests, 85 mobile tests, 4 real HTTP/JVM downloader tests, 2 diagnostic tests passed. Kotlin/Java release compilation succeeded (`:67chat-room-key:compileReleaseKotlin :67chat-room-key:compileReleaseJavaWithJavac`). Expo Android Hermes export succeeded with the shared browser diagnostic module. Physical upgrade/data-preservation acceptance and production publication are still pending; do not mark the stage fully accepted.

2026-10-23 release: isolated commit `d09b318` built as 1.3.5 / versionCode16 in `C:/67b67tmess/r`. Fresh checks: 58 API/web, 89 mobile, typecheck; Gradle assembleRelease succeeded. APK certificate SHA-256 remains `1a218f859a412f623104d83aaaad0a44abdad3ed665060ac8085902891dc9ab8`. APK SHA-256 `a3dee5f986369a99ad1a3df3c3e57b1d164a62f7afca6b5c9a56172f03827239`, 64,467,094 bytes. Server bundle and signed APK published to production; live smoke tests passed registration/login/cookie/bearer/WS/idempotency/history/media/TURN/pin/read/private hide. Public manifest16 and HTTP206 ranges at0/1MiB verified. Previous code, DB and release preserved on server in `data/backups/stage1_20261023_093519`. User cancelled USB acceptance and explicitly requested publication through auto-update. No device installation or physical resume/data-preservation claim.

The paragraph above supersedes the earlier pending-release statement. Installed applications remain old until each user accepts the offered update. Later stages below are SOURCE ONLY.

Ruling: switch later independent feature modules to subagent-driven development — the approved multi-stage scope is much larger than an inline pass; split file ownership avoids concurrent edits. Root retains central server/App/Workspace integration and all release/device work. Cost: more isolated review contexts, lower implementation bottleneck.

## Stage 2 — in progress

Added real native recording meter sampling, protected media-details envelope, waveform normalization, listening-receipt API/migrations, client receipt persistence and circle gesture/preview component. These changes are not yet fully integrated/tested.

## Delegated work (not complete until reviewed/integrated)

- conversation_features: new standalone API/SQL and web/mobile panels for stages 3–4. No edits to central entrypoints.
- account_features: standalone account/privacy services and panels for stage 6. No central auth/App edits.
- expressive_features: standalone typed-message services and panels for stage 7 plus settings quality where self-contained. No central entrypoint edits.

## Integration update — later stages, unpublished

- Central API/web/Android feature entrypoints now wired. API test command expanded to include feature suites; 198 tests passed before the latest reviewer fixes. Subsequent changes require a fresh full run.
- Stages3–4: multi-pins, encrypted CAS drafts/conflict UI, date anchors/read-all, shared safe formatting, forwarding IDs/ranges, channels/public communities/member management and scheduling/TTL migrations. Root additionally fixed scheduled recipient-privacy recheck, durable reminder notification hook, rescheduling endpoint and history reaction N+1 (actual page test12 queries →1). Remaining UX/acceptance work is not automatically complete.
- Stage6 security review fixed copied-public-key session impersonation with signed session-bound proof, post-delete edit-history trigger leakage, and destructive backup replacement. Native staged restore/proof/erasure bridges and browser challenge/QR/staged restore added. Native transport and complete browser account isolation remain work in progress.
- Stages7–8: protected payloads/catalog/polls/location/export, native incoming share/widget, quality preferences/quiet hours/traffic/wallpapers/crash reports. Reviewer fixes notification tap/RemoteInput account routing, partial-share retry IDs, private sticker protected file delivery, full poll-event pagination and preservation of SW shell during media-cache cleanup. Full product RU/EN translation remains unfinished (quality panel currently translated).
- Stage2 review: working waveform/receipts/circle gestures/photo/video processing/gallery and PiP primitives; fixes in progress for recorder cancellation, account plaintext cache erasure, stale viewer, bounded browser Blob URL cache and media metadata cancellation. Physical recording/PiP checks not performed.
- Stage5: persistent call history, exact endpoint ownership/resume, browser/native reconnection/capture guards and optional LiveKit SDK integration. Group SFU is default OFF pending authenticated signaling gateway/revocation, deployment and load/real-media acceptance.
- Fresh and manual DB schemas generated from startup feature SQL by `scripts/sync_feature_schema.py`; actual fresh-schema/idempotent PGlite test passes. Regenerate after subsequent module SQL changes.
- No later-stage server bundle or APK has been published. Main branch working tree contains ongoing integrated work; do not equate source changes with installed functionality.

## Source checkpoint after auto-update publication — 23 October 2026

User explicitly waived reconnecting the phone and requested publication of the ready build through auto-update. That request is fulfilled by the isolated stage1 release above. A fresh public check still returns version1.3.5/code16, matching SHA-256 and size; an offset1MiB range returns HTTP206 with the requested1024bytes. No later-stage changes have been added to that APK.

Later source fixes reviewed at this checkpoint:

- Browser accounts use per-account HttpOnly cookies and a per-tab nonsecret selector; HTTP, WebSocket, media and queued uploads retain the originating account. Push taps include recipient and message, cold start selects that account. Disabling one account preserves the shared browser endpoint and other accounts. Late401 responses cannot log out another account.
- Feature adapters and Signal/key initialization now fence asynchronous work by account AND session generation. Logout clears volatile room/group secrets, stops voice/call capture, and invalidates late initialization. Account deletion retains owner-specific erasure after server session revocation; detached SFU cleanup can release only its original account/session lease.
- Native/web push subscriptions retain several accounts on one installation/endpoint. A device capability with an epoch fences registration/delivery during protected-transport transitions. SOCKS preflight revokes all installation accounts before persistence; no direct fallback. Native call/notification routing validates the recipient before account switching and capture.
- Voice cancellation, late recorder preparation, media cache/account erasure, external-file opening and playback ownership reviewed. Browser voice playback cannot win a stale source race or resume after reset.
- Existing scheduled messages now expose time changes; pins expose a list and selection from a message without entering raw IDs. Draft empty-state text and localized forwarding labels no longer control user-data or delivery state.
- Shared explicit RU/EN UI catalog, formatted dates, native notification/widget labels and content-preservation tests added. Static Android shortcuts/system CallStyle labels follow the Android language; that limitation is documented.

Verification evidence: latest complete API/web run270/270 (`builds/current-api-tests.log`), followed by18 focused browser/UI/crypto/i18n regression tests and7 transport/call-account tests after the final source edits; latest complete mobile run172/172 and TypeScript clean (`builds/current-mobile-tests.log`). Native owners compiled the RoomKey, application and ExpressiveIntegration Kotlin targets;11 JVM backup/KDF/SOCKS tests passed. `git diff --check` and repository safety tests passed. These checks validate source scenarios, not a newly installed main-tree APK.

Remaining work before claiming the whole eight-stage plan complete includes: a fresh signed main-tree APK/native integration build and staged server startup/acceptance; optional SFU deployment, revocation/load/media acceptance before enabling group calls; browser secret-chat UI/local encrypted history completion; self-hosted map data; background location and process-death share-confirmation recovery; large-history event/performance checks and streaming export beyond the documented20MiB limit; remaining device/accessibility checks. Local Docker engine could not start, so no new full-stack staging acceptance was claimed. Existing production remains on the separately verified stage1 deployment.

## Integrated release candidate 1.3.6 / code17

This section supersedes earlier source TODO notes, without turning unit tests into physical acceptance. Secret web/native screens and device-scoped encrypted vault completed; independent scoped fix review clean. Own OSM-derived overview now ships in API bundle, license/attribution included; street detail is explicitly unavailable. Incoming Android grants are copied into an encrypted Keystore journal with recipient-stable confirmation and resume; native module Kotlin+Java compile passed. Incremental event history and300-message native window preserve the reading anchor and unseen state. Native account activation review exposed A→B→A saved-token reuse; monotonic activation fencing and repository pinning added with37 focused regressions/typecheck green, rereview pending.

Staging is isolated at /opt/67chat-staging-20261023 with separate DB/credentials/ports, production untouched. Fresh PostgreSQL16 startup/migrations succeed. Real HTTP+WS acceptance passed registration, cookie/bearer multiaccount selection, encrypted text/history/idempotency, read/pins/date, encrypted CAS draft conflict and scheduling edit/cancel. Browser restored login, decrypted history and sent a new message through UI in both themes. SFU actual SDK surfaced missing Authorization forwarding (regression RED→GREEN); six LiveKit1.13.10/RTC-node1.2.1 clients each subscribed5 and decoded56,006 audioframes. Seventh rejected, revocation removed active participant/stopped incoming media, old signed token reconnect denied. This does not prove real microphone, Android screen capture or Wi-Fi/cellular behavior.

Host benchmark:1000 real chat-row renders46.38ms,10000-event firstscan10.68ms/100pages, next event0.053ms/0fetches, native bounded-window4.79ms. Full API/web312pass before latest focused adapters/activation changes; mobile final suite pending. Physical phone acceptance is waived by user. Source/API/web/APK publication will be recorded only after actual deployment.

Ruling: bounded inline export remains20MiB and live location stops in background; approved plan requires optional attachments and a limited-time broadcast but does not require unbounded streaming export/background tracking. Limits are visible and documented in USER_GUIDE. Own OSM overview satisfies local coordinate rendering; street tiles remain a stated product limitation. Cost if broader behavior was expected: follow-up export/map/background implementation.

## Фактический выпуск1.3.6/code17 — 23.10.2026

Этот раздел заменяет прежние отметки SOURCE ONLY/pending для этапов2–8. Интегрированные функции из RELEASE_1_3_6 опубликованы на сервере/сайте и в подписанном APK. Финальное ревью и узкая повторная проверка приняты; API/web349/349, mobile249/249, TS чистый; repository2/2, deployment rollback4/4. Реальные шесть внешних RTC-клиентов, отзыв медиа/доступа, веб в обеих темах и переключение аккаунтов проверены. Рабочий сервер прошёл live_server_check, все133 хеша пакета совпали.

APK1.3.6/code17 опубликован18:36:24UTC,67,862,178байт,SHA256085e7957232656d37616f1ea22f983e7d7c79c89d224e9e1027b6abb84377e44; исходники181f00c, прежняя подпись сохранена. Публичные манифест,206 диапазоны0/1MiB и полный поток проверены. Backup: /opt/messenger67/data/backups/release136_20261023_183517.

Не заявляем физическую установку, аппаратные звонки/PiP/TalkBack/разрешения/переход сети и upgrade16→17 на телефоне: пользователь отменил USB-приёмку. Обзорная карта без улиц,20MiB inline-экспорта, остановка live location в фоне и ограничения секретных чатов/прокси описаны в RELEASE_1_3_6 и USER_GUIDE. Это сохраняющиеся границы, а не скрытые завершённые проверки.
