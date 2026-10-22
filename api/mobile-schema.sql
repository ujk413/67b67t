ALTER TABLE messages ADD COLUMN IF NOT EXISTS client_message_id text;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS edited_at timestamptz;
CREATE UNIQUE INDEX IF NOT EXISTS messages_client_id ON messages(sender_id,client_message_id);
CREATE INDEX IF NOT EXISTS messages_room_order ON messages(room_id,created_at,id);
CREATE TABLE IF NOT EXISTS room_preferences (
  user_id uuid REFERENCES users(id) ON DELETE CASCADE,
  room_id uuid REFERENCES rooms(id) ON DELETE CASCADE,
  pinned boolean NOT NULL DEFAULT false, muted boolean NOT NULL DEFAULT false,
  archived boolean NOT NULL DEFAULT false, hidden boolean NOT NULL DEFAULT false,
  marked_unread boolean NOT NULL DEFAULT false, PRIMARY KEY(user_id,room_id)
);
CREATE TABLE IF NOT EXISTS push_devices (
  installation_id text PRIMARY KEY, user_id uuid REFERENCES users(id) ON DELETE CASCADE,
  session_id uuid REFERENCES sessions(id) ON DELETE CASCADE,
  token text NOT NULL, active_room_id uuid, updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS room_invites (
  token text PRIMARY KEY, room_id uuid REFERENCES rooms(id) ON DELETE CASCADE,
  created_by uuid REFERENCES users(id) ON DELETE CASCADE, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS message_changes (
  seq bigserial PRIMARY KEY, room_id uuid NOT NULL, message_id uuid NOT NULL,
  operation text NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS message_changes_room_seq ON message_changes(room_id,seq);
CREATE OR REPLACE FUNCTION track_message_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(670024);
  IF TG_OP='DELETE' THEN
    INSERT INTO message_changes(room_id,message_id,operation) VALUES(OLD.room_id,OLD.id,'delete');
    RETURN OLD;
  END IF;
  INSERT INTO message_changes(room_id,message_id,operation) VALUES(NEW.room_id,NEW.id,'upsert');
  IF TG_OP='INSERT' THEN
    UPDATE room_preferences SET hidden=false WHERE room_id=NEW.room_id;
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS messages_sync ON messages;
CREATE TRIGGER messages_sync AFTER INSERT OR UPDATE OR DELETE ON messages
  FOR EACH ROW EXECUTE FUNCTION track_message_change();
