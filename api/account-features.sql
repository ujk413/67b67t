-- Account/privacy services: safe to execute at every boot and on fresh DBs.
CREATE TABLE IF NOT EXISTS account_contacts (
 user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 contact_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 name text NOT NULL DEFAULT '', created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(user_id,contact_id), CHECK(user_id<>contact_id));
CREATE TABLE IF NOT EXISTS account_privacy (
 user_id uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 last_seen text NOT NULL DEFAULT 'everyone' CHECK(last_seen IN('everyone','contacts','nobody')),
 write text NOT NULL DEFAULT 'everyone' CHECK(write IN('everyone','contacts','nobody')),
 read_receipts boolean NOT NULL DEFAULT true);
CREATE TABLE IF NOT EXISTS account_security (
 user_id uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 secret_sealed text, pending_sealed text, pending_until timestamptz,
 enabled boolean NOT NULL DEFAULT false, approval_required boolean NOT NULL DEFAULT false,
 last_step bigint NOT NULL DEFAULT -1, failures int NOT NULL DEFAULT 0,
 locked_until timestamptz, backup_hashes jsonb NOT NULL DEFAULT '[]');
CREATE TABLE IF NOT EXISTS account_login_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 device_key text, device_name text, ip text, outcome text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS account_login_audit_user ON account_login_audit(user_id,created_at DESC);
CREATE TABLE IF NOT EXISTS account_trusted_devices (
 user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE, device_hash text NOT NULL,
 name text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(user_id,device_hash));
CREATE TABLE IF NOT EXISTS account_auth_challenges (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), user_id uuid REFERENCES users(id) ON DELETE CASCADE,
 secret_hash text NOT NULL, approval_hash text, kind text NOT NULL CHECK(kind IN('factor','device','handoff')),
 device_hash text, device_name text, approved boolean NOT NULL DEFAULT false,
 factor_verified boolean NOT NULL DEFAULT false, failures int NOT NULL DEFAULT 0,
 expires_at timestamptz NOT NULL DEFAULT now()+interval '2 minutes');
CREATE INDEX IF NOT EXISTS account_challenge_expiry ON account_auth_challenges(expires_at);
CREATE TABLE IF NOT EXISTS account_secret_chats (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), initiator uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 recipient uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE, initiator_device text NOT NULL,
 recipient_device text NOT NULL, ttl int NOT NULL CHECK(ttl BETWEEN 30 AND 86400),
 created_at timestamptz NOT NULL DEFAULT now(), closed_at timestamptz, CHECK(initiator<>recipient));
CREATE TABLE IF NOT EXISTS account_secret_mailbox (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), chat_id uuid NOT NULL REFERENCES account_secret_chats(id) ON DELETE CASCADE,
 sender_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE, nonce text NOT NULL,
 ciphertext text NOT NULL CHECK(octet_length(ciphertext) BETWEEN 24 AND 65536),
 created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz NOT NULL,
 UNIQUE(chat_id,sender_id,nonce));
CREATE INDEX IF NOT EXISTS account_secret_mailbox_expiry ON account_secret_mailbox(expires_at);
