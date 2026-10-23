CREATE TABLE IF NOT EXISTS call_history (
 id uuid PRIMARY KEY, room_id uuid NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
 caller_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 callee_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 is_video boolean NOT NULL DEFAULT false,
 started_at timestamptz NOT NULL, answered_at timestamptz, ended_at timestamptz,
 duration_ms bigint NOT NULL DEFAULT 0,
 status text NOT NULL DEFAULT 'ringing' CHECK(status IN ('ringing','active','completed','missed','rejected','busy','cancelled','interrupted')),
 reason text
);
CREATE INDEX IF NOT EXISTS call_history_caller_time ON call_history(caller_id,started_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS call_history_callee_time ON call_history(callee_id,started_at DESC,id DESC);
CREATE TABLE IF NOT EXISTS group_call_leases (
 user_id uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 room_id uuid NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
 expires_at timestamptz NOT NULL
);
ALTER TABLE group_call_leases ADD COLUMN IF NOT EXISTS lease_id uuid NOT NULL DEFAULT gen_random_uuid();
-- Deliberately no session FK: deleting a session must not erase the SFU lease
-- before the revocation hook has disconnected its participant.
ALTER TABLE group_call_leases ADD COLUMN IF NOT EXISTS session_id uuid;
ALTER TABLE group_call_leases ADD COLUMN IF NOT EXISTS revoking_at timestamptz;
