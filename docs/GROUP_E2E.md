# Group E2E (Sender Keys)

Opt-in end-to-end encryption for **group** chats (not DMs, not channels). Enabled per group by an owner or admin.

## How it works

- **Enable/disable:** `PUT /rooms/:roomId/e2e` with `{"enabled": true|false}`. Read with `GET /rooms/:roomId/e2e`. Members get a `room.e2e` WebSocket event.
- **Sender key:** each member has one symmetric chain per group (HMAC-SHA256 ratchet) and an ECDSA P-256 signing key. Every message uses a fresh AES-GCM key derived from the chain; the AAD binds room, sender, key id and iteration; the envelope is signed.
- **Distribution:** the sender's chain state is sent to each other member as a message encrypted with the existing pairwise Signal session. The server stores these as opaque payloads (`POST /rooms/:roomId/sender-keys`, `GET` for your inbox, `DELETE /rooms/:roomId/sender-keys/:id` to acknowledge).
- **Enforcement:** while a group has E2E on, the server rejects plaintext text messages (`400 e2e_required`) on both the HTTP and WebSocket send paths.
- **Rotation:** on departure, online members re-key at once. Before sending, a sender also compares its recipient list with the current members. If someone who had the key has left, the sender creates a new key and distributes it to everyone. If someone new joined, the current chain (not the genesis) goes only to them, so they cannot read earlier messages.

## Threat model: what this protects

- The server cannot read message text in an E2E group. It only stores ciphertext and opaque key distributions.
- Tampered ciphertext, or a message altered to look like another room, sender or iteration, is rejected (signature and AAD checks).
- A member who joined later cannot read earlier messages from a sender.

## Limits (read before relying on it)

- **Messages sent before a removal stay readable by the removed member.** They already held the key. Nothing can revoke past access.
- **Removal re-key is immediate only for online members.** When someone leaves or is removed, the server broadcasts `room.members_changed`, and online members generate and distribute a new sender key at once. An offline sender does not need to rotate in advance: it cannot post while offline, and on its next send it fetches the current member list and rotates before encrypting if a former recipient is gone.
- **Metadata is visible to the server:** who writes, when, room membership, message sizes.
- **Attachments and media are not encrypted** by this mode.
- **No per-message forward secrecy on the device.** The chain state is stored in IndexedDB, the same trust boundary as the identity key.
- **Mobile app cannot read or write E2E groups.** It shows a placeholder for them and refuses to send into them. Use the web client.
- **Per-device:** sender keys belong to a browser profile. A new device without a backup does not get group keys, so older group messages show "ключ отправителя недоступен". The passphrase key backup (`exportKeys`/`importKeys`) includes group keys, so restoring from it brings them back.
- **Editing and deleting** E2E messages is not supported (editing is blocked in the web client).
- **Size cap:** at most 500 members per E2E group. Each distribution payload is capped at 16 KB, and a sender is limited to 60 distributions per hour.

## Endpoints

| Method | Path | Who | Notes |
|---|---|---|---|
| GET | `/rooms/:roomId/e2e` | member | `{enabled}` |
| PUT | `/rooms/:roomId/e2e` | owner/admin | group rooms only |
| POST | `/rooms/:roomId/sender-keys` | member | `{key_id, recipients:[{user_id,payload}]}`; recipients must be members |
| GET | `/rooms/:roomId/sender-keys` | member | pending distributions addressed to you |
| DELETE | `/rooms/:roomId/sender-keys/:id` | recipient | acknowledge |

Removing a member, or a member leaving, deletes their pending distributions for that room and broadcasts `room.members_changed`.

## Verification

- `node --test tests/groupcrypto.test.mjs`: 13 tests covering round-trip, out-of-order delivery, replay refusal, tamper detection, forged far-future envelopes not advancing the chain, delta cap, late joiners, rotation, removal-driven re-keying (`planSend`), re-added members, and malformed distributions.
- `scripts/smoke_live.sh` against a running API and Postgres: 32 checks, including enable/disable permissions, plaintext rejection, the key inbox round trip, member removal and non-member rejection.
- Not exercised: two real browsers exchanging keys through the UI, group key backup/restore, and the mobile typing indicator on a device. A two-browser run was started (two accounts registered, group created) but did not reach message exchange.
