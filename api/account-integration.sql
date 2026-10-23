-- Critical session/device ownership binding. New sessions are unbound until key publication.
CREATE TABLE IF NOT EXISTS account_session_devices (
 session_id uuid PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
 user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 device_id text NOT NULL, public_key_hash text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(user_id,device_id) REFERENCES user_devices(user_id,device_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS account_session_devices_user ON account_session_devices(user_id);
CREATE TABLE IF NOT EXISTS account_media_purge_jobs (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), media_id uuid NOT NULL,
 local_path text,thumb_path text,s3_path text,created_at timestamptz NOT NULL DEFAULT now(),
 attempts int NOT NULL DEFAULT 0, next_attempt_at timestamptz NOT NULL DEFAULT now(),
 last_error text, UNIQUE(media_id)
);

ALTER TABLE account_session_devices ADD COLUMN IF NOT EXISTS proof_verified_at timestamptz;
CREATE TABLE IF NOT EXISTS account_device_proofs(
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),session_id uuid NOT NULL REFERENCES sessions(id)ON DELETE CASCADE,
 user_id uuid NOT NULL REFERENCES users(id)ON DELETE CASCADE,device_id text NOT NULL,public_key_hash text NOT NULL,
 challenge text NOT NULL,expires_at timestamptz NOT NULL DEFAULT now()+interval '2 minutes'
);
CREATE INDEX IF NOT EXISTS account_device_proofs_expiry ON account_device_proofs(expires_at);
CREATE TABLE IF NOT EXISTS account_backup_restores(
 session_id uuid PRIMARY KEY REFERENCES sessions(id)ON DELETE CASCADE,
 user_id uuid NOT NULL REFERENCES users(id)ON DELETE CASCADE,
 device_id text NOT NULL,public_key_hash text NOT NULL,created_at timestamptz NOT NULL DEFAULT now()
);
