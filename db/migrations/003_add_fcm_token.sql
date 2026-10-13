-- Add FCM token support for mobile push notifications
-- Run this migration to add FCM token storage

-- Add fcm_token to users table
ALTER TABLE users
ADD COLUMN IF NOT EXISTS fcm_token text;

-- Add index for faster queries
CREATE INDEX IF NOT EXISTS users_fcm_token_idx ON users(fcm_token) WHERE fcm_token IS NOT NULL;

-- Add comment
COMMENT ON COLUMN users.fcm_token IS 'Firebase Cloud Messaging token for push notifications';
