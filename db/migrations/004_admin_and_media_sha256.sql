-- Admin flag (in addition to ADMIN_USERNAMES) and upload de-duplication hash.
-- Idempotent; the same statements also run from runMigrations() in api/server.js.
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin boolean NOT NULL DEFAULT false;

ALTER TABLE media ADD COLUMN IF NOT EXISTS sha256 text;
CREATE INDEX IF NOT EXISTS media_owner_sha256_idx ON media(owner_id, sha256) WHERE sha256 IS NOT NULL;

-- Grant admin from SQL:  UPDATE users SET is_admin = true WHERE username = 'alice';
