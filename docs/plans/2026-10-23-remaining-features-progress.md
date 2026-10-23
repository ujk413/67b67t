# Execution ledger — plan: docs/plans/2026-10-23-remaining-features.md

Base: 58c0c880d8de1848e88a82c391b42ac3c1fda14b. Branch: codex/remaining-features.

Ruling: work in a new branch of the clean primary checkout rather than a second checkout — preserves the previous PR and uses installed native dependencies/signing assets without copying secrets; concurrent edits would require reconciliation.

Pre-flight: media queue, encryption metadata, sync and account isolation are shared between stages 1–3; preserve stable client_message_id and capture the originating account/room before asynchronous work. New message types must use encryption before upload; never put plaintext secrets in metadata.

Baseline: immediately before implementation, 56 API/web tests and 81 mobile tests passed; typecheck passed. No production or phone changes in this run.

## Stage 1 — in progress

- APK download currently deletes the previous file; use durable byte-range chunks and a persisted release identity instead of opaque native resume data, so process death is recoverable.
- SHA-256 must be streaming/on a native worker; avoid loading a 65 MB APK into the JS heap.

Implemented: durable range download + SHA-256 on a native worker; release identity changes reset the file; hash verification gates installer retries; reconnect retries transient downloads. Android media moves atomically from draft to persistent outbox with a stable ID, encrypted-file checkpoint, cancellation and cache protection. Browser stores the encrypted upload blob in its account outbox and uploads after reconnect. Added API/authenticated-WS/actual-TURN-relay diagnostic screens and a distinct synchronization status.

Verification: 56 API/web tests, 85 mobile tests, 4 real HTTP/JVM downloader tests, 2 diagnostic tests passed. Kotlin/Java release compilation succeeded (`:67chat-room-key:compileReleaseKotlin :67chat-room-key:compileReleaseJavaWithJavac`). Expo Android Hermes export succeeded with the shared browser diagnostic module. Physical upgrade/data-preservation acceptance and production publication are still pending; do not mark the stage fully accepted.
