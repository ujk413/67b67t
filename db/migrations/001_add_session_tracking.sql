-- Add session tracking fields for device management
-- Run this migration to add device tracking to sessions table

ALTER TABLE sessions
ADD COLUMN IF NOT EXISTS device_name text,
ADD COLUMN IF NOT EXISTS device_type text,
ADD COLUMN IF NOT EXISTS ip_address inet,
ADD COLUMN IF NOT EXISTS user_agent text,
ADD COLUMN IF NOT EXISTS last_activity timestamptz DEFAULT now();

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS sessions_user_last_activity_idx ON sessions(user_id, last_activity DESC);

-- Add comment
COMMENT ON COLUMN sessions.device_name IS 'User-friendly device name (e.g., "Chrome on Windows")';
COMMENT ON COLUMN sessions.device_type IS 'Device type: desktop, mobile, tablet';
COMMENT ON COLUMN sessions.ip_address IS 'IP address of the session';
COMMENT ON COLUMN sessions.user_agent IS 'Browser user agent string';
COMMENT ON COLUMN sessions.last_activity IS 'Last activity timestamp for this session';
