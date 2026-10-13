-- Group E2E (Sender Keys): opt-in flag per group and pending sender-key distributions.
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS e2e_enabled boolean NOT NULL DEFAULT false;
CREATE TABLE IF NOT EXISTS group_sender_keys (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  room_id uuid NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
  from_user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  to_user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  key_id text NOT NULL,
  payload text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS group_sender_keys_inbox_idx ON group_sender_keys(room_id, to_user_id, created_at);
