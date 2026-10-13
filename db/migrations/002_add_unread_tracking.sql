-- Add unread message tracking
-- Run this migration to add unread counter support

-- Add last_read_message_id to room_members to track what user has read
ALTER TABLE room_members
ADD COLUMN IF NOT EXISTS last_read_message_id uuid REFERENCES messages(id) ON DELETE SET NULL;

-- Add index for faster queries
CREATE INDEX IF NOT EXISTS room_members_last_read_idx ON room_members(user_id, last_read_message_id);

-- Add comment
COMMENT ON COLUMN room_members.last_read_message_id IS 'Last message ID that user has read in this room';
