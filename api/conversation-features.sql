-- Shared by fresh installs and boot migrations. No extensions required.
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS conversation_epoch bigint NOT NULL DEFAULT 0;
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS slow_mode_seconds integer NOT NULL DEFAULT 0;
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS auto_delete_seconds integer NOT NULL DEFAULT 0;
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS public_slug text;
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS public_title text;
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS public_description text NOT NULL DEFAULT '';
CREATE UNIQUE INDEX IF NOT EXISTS rooms_public_slug ON rooms(public_slug) WHERE public_slug IS NOT NULL;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS expires_at timestamptz;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS self_destruct_seconds integer NOT NULL DEFAULT 0;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS self_destruct_started_at timestamptz;
CREATE INDEX IF NOT EXISTS messages_expiry ON messages(expires_at) WHERE expires_at IS NOT NULL;
CREATE TABLE IF NOT EXISTS room_pins(room_id uuid NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,message_id uuid NOT NULL REFERENCES messages(id) ON DELETE CASCADE,pinned_by uuid REFERENCES users(id) ON DELETE SET NULL,created_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(room_id,message_id));
INSERT INTO room_pins(room_id,message_id) SELECT r.id,r.pinned_message_id FROM rooms r JOIN messages m ON m.id=r.pinned_message_id AND m.room_id=r.id ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS encrypted_drafts(user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,room_id uuid NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,revision integer NOT NULL DEFAULT 0,encrypted_content text,updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(user_id,room_id));
CREATE TABLE IF NOT EXISTS message_edit_history(id bigserial PRIMARY KEY,message_id uuid NOT NULL REFERENCES messages(id) ON DELETE CASCADE,encrypted_content text NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS message_edit_history_message ON message_edit_history(message_id,id);
CREATE TABLE IF NOT EXISTS scheduled_messages(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),room_id uuid NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,encrypted_content text NOT NULL,send_at timestamptz NOT NULL,kind text NOT NULL DEFAULT 'message' CHECK(kind IN ('message','reminder')),epoch bigint NOT NULL,status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','sent','due','cancelled','needs_reencrypt')),message_id uuid REFERENCES messages(id) ON DELETE SET NULL,created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS scheduled_messages_due ON scheduled_messages(send_at) WHERE status='pending';
ALTER TABLE scheduled_messages ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE scheduled_messages ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0;
ALTER TABLE scheduled_messages ADD COLUMN IF NOT EXISTS last_error text;
CREATE INDEX IF NOT EXISTS scheduled_messages_retry ON scheduled_messages(next_attempt_at,send_at) WHERE status='pending';
CREATE TABLE IF NOT EXISTS conversation_send_limits(room_id uuid NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,last_sent_at timestamptz NOT NULL,last_hash text NOT NULL,PRIMARY KEY(room_id,user_id));
CREATE TABLE IF NOT EXISTS chat_folders(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,title text NOT NULL,room_ids jsonb NOT NULL DEFAULT '[]',created_at timestamptz NOT NULL DEFAULT now());
ALTER TABLE chat_folders ADD COLUMN IF NOT EXISTS position integer NOT NULL DEFAULT 0;
ALTER TABLE chat_folders ADD COLUMN IF NOT EXISTS revision integer NOT NULL DEFAULT 0;
ALTER TABLE chat_folders ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
CREATE OR REPLACE FUNCTION conversation_epoch_changed() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='DELETE' THEN UPDATE rooms SET conversation_epoch=conversation_epoch+1 WHERE id=OLD.room_id; RETURN OLD; END IF;
 UPDATE rooms SET conversation_epoch=conversation_epoch+1 WHERE id=NEW.room_id; RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS conversation_members_epoch ON room_members;
CREATE TRIGGER conversation_members_epoch AFTER INSERT OR DELETE ON room_members FOR EACH ROW EXECUTE FUNCTION conversation_epoch_changed();
CREATE OR REPLACE FUNCTION conversation_e2e_changed() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.e2e_enabled IS DISTINCT FROM OLD.e2e_enabled THEN NEW.conversation_epoch=OLD.conversation_epoch+1; END IF; RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS conversation_e2e_epoch ON rooms;
CREATE TRIGGER conversation_e2e_epoch BEFORE UPDATE OF e2e_enabled ON rooms FOR EACH ROW EXECUTE FUNCTION conversation_e2e_changed();
CREATE OR REPLACE FUNCTION conversation_capture_edit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF OLD.encrypted_content IS NOT NULL AND OLD.encrypted_content IS DISTINCT FROM NEW.encrypted_content THEN
 INSERT INTO message_edit_history(message_id,encrypted_content) VALUES(OLD.id,jsonb_build_object('v',1,'body',jsonb_build_object('encrypted_content',OLD.encrypted_content,'meta',jsonb_build_object('e2e_group',OLD.meta->'e2e_group','v',OLD.meta->'v')))::text);
 ELSIF OLD.encrypted_content IS NULL AND OLD.meta->>'encrypted'='true' AND (OLD.text IS DISTINCT FROM NEW.text OR OLD.meta->>'iv' IS DISTINCT FROM NEW.meta->>'iv') THEN
 INSERT INTO message_edit_history(message_id,encrypted_content) VALUES(OLD.id,jsonb_build_object('v',1,'body',jsonb_build_object('text',OLD.text,'meta',jsonb_build_object('encrypted',true,'e2ee_v',OLD.meta->'e2ee_v','iv',OLD.meta->'iv')))::text);
 END IF; RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS conversation_edit_history ON messages;
CREATE TRIGGER conversation_edit_history BEFORE UPDATE OF encrypted_content,text,meta ON messages FOR EACH ROW EXECUTE FUNCTION conversation_capture_edit();
CREATE OR REPLACE FUNCTION conversation_start_self_destruct() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.self_destruct_seconds>0 AND OLD.self_destruct_started_at IS NULL AND EXISTS(SELECT 1 FROM jsonb_array_elements_text(COALESCE(NEW.read_by,'[]'::jsonb)) AS reader(value) WHERE reader.value<>NEW.sender_id::text) THEN
 NEW.self_destruct_started_at=clock_timestamp(); NEW.expires_at=LEAST(NEW.expires_at,NEW.self_destruct_started_at+make_interval(secs=>NEW.self_destruct_seconds));
 END IF; RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS conversation_read_expiry ON messages;
CREATE TRIGGER conversation_read_expiry BEFORE UPDATE OF read_by ON messages FOR EACH ROW EXECUTE FUNCTION conversation_start_self_destruct();
CREATE OR REPLACE FUNCTION conversation_legacy_pin() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.pinned_message_id IS DISTINCT FROM OLD.pinned_message_id AND NEW.pinned_message_id IS NOT NULL THEN
 INSERT INTO room_pins(room_id,message_id) SELECT NEW.id,NEW.pinned_message_id WHERE EXISTS(SELECT 1 FROM messages WHERE id=NEW.pinned_message_id AND room_id=NEW.id) ON CONFLICT DO NOTHING;
 ELSIF NEW.pinned_message_id IS NULL AND OLD.pinned_message_id IS NOT NULL THEN DELETE FROM room_pins WHERE room_id=NEW.id AND message_id=OLD.pinned_message_id;
 END IF; RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS conversation_legacy_pin_sync ON rooms;
CREATE TRIGGER conversation_legacy_pin_sync AFTER UPDATE OF pinned_message_id ON rooms FOR EACH ROW EXECUTE FUNCTION conversation_legacy_pin();
CREATE OR REPLACE FUNCTION conversation_guard_send() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE settings rooms%ROWTYPE; previous conversation_send_limits%ROWTYPE; actor_role text; fingerprint text;
BEGIN
 IF NEW.client_message_id IS NOT NULL AND EXISTS(SELECT 1 FROM messages WHERE sender_id=NEW.sender_id AND client_message_id=NEW.client_message_id) THEN RETURN NEW; END IF;
 SELECT * INTO settings FROM rooms WHERE id=NEW.room_id FOR UPDATE;
 SELECT role INTO actor_role FROM room_members WHERE room_id=NEW.room_id AND user_id=NEW.sender_id;
 IF actor_role IS NULL THEN RAISE EXCEPTION 'not_member' USING ERRCODE='42501'; END IF;
 IF NEW.type<>'call' THEN
 fingerprint=md5(COALESCE(NEW.encrypted_content,NEW.text,'') || ':' || COALESCE(NEW.meta::text,'') || ':' || COALESCE(to_jsonb(NEW)->>'media_id',''));
 SELECT * INTO previous FROM conversation_send_limits WHERE room_id=NEW.room_id AND user_id=NEW.sender_id;
 IF actor_role NOT IN ('owner','admin') AND previous.last_sent_at + make_interval(secs=>settings.slow_mode_seconds)>clock_timestamp() THEN RAISE EXCEPTION 'slow_mode' USING ERRCODE='P0001'; END IF;
 IF previous.last_hash=fingerprint AND previous.last_sent_at + interval '10 seconds'>clock_timestamp() THEN RAISE EXCEPTION 'duplicate_spam' USING ERRCODE='P0001'; END IF;
 INSERT INTO conversation_send_limits(room_id,user_id,last_sent_at,last_hash) VALUES(NEW.room_id,NEW.sender_id,clock_timestamp(),fingerprint) ON CONFLICT(room_id,user_id) DO UPDATE SET last_sent_at=EXCLUDED.last_sent_at,last_hash=EXCLUDED.last_hash;
 END IF;
 IF settings.auto_delete_seconds>0 THEN NEW.expires_at=clock_timestamp()+make_interval(secs=>settings.auto_delete_seconds); END IF;
 IF NEW.meta ? 'self_destruct_seconds' THEN
 IF NEW.meta->>'self_destruct_seconds' !~ '^[0-9]{1,6}$' THEN RAISE EXCEPTION 'bad_self_destruct_seconds'; END IF;
 NEW.self_destruct_seconds=(NEW.meta->>'self_destruct_seconds')::integer;
 END IF;
 IF NEW.self_destruct_seconds<0 OR NEW.self_destruct_seconds>604800 THEN RAISE EXCEPTION 'bad_self_destruct_seconds'; END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS conversation_message_guard ON messages;
CREATE TRIGGER conversation_message_guard BEFORE INSERT ON messages FOR EACH ROW EXECUTE FUNCTION conversation_guard_send();
