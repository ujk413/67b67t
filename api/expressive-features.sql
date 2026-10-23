-- Apply to fresh databases and as an idempotent startup migration.
-- Polls, answers, votes and coordinates intentionally have NO server tables.
CREATE TABLE IF NOT EXISTS expressive_packs (
 id uuid PRIMARY KEY, owner_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 title text NOT NULL CHECK (length(title) BETWEEN 1 AND 100),
 tags text[] NOT NULL DEFAULT '{}', visibility text NOT NULL DEFAULT 'private' CHECK (visibility IN ('private','public')),
 created_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz
);
CREATE INDEX IF NOT EXISTS expressive_packs_owner ON expressive_packs(owner_id) WHERE deleted_at IS NULL;
CREATE TABLE IF NOT EXISTS expressive_installed_packs (
 user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 pack_id uuid NOT NULL REFERENCES expressive_packs(id) ON DELETE CASCADE,
 added_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(user_id,pack_id)
);
CREATE TABLE IF NOT EXISTS expressive_catalog (
 id uuid PRIMARY KEY, pack_id uuid NOT NULL REFERENCES expressive_packs(id) ON DELETE CASCADE,
 media_id uuid NOT NULL REFERENCES media(id) ON DELETE CASCADE,
 kind text NOT NULL CHECK (kind IN ('sticker','gif')), label text NOT NULL DEFAULT '', tags text[] NOT NULL DEFAULT '{}',
 created_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz,
 UNIQUE(pack_id,media_id)
);
CREATE INDEX IF NOT EXISTS expressive_catalog_media ON expressive_catalog(media_id) WHERE deleted_at IS NULL;
CREATE TABLE IF NOT EXISTS expressive_recent (
 user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 item_id uuid NOT NULL REFERENCES expressive_catalog(id) ON DELETE CASCADE,
 used_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(user_id,item_id)
);
CREATE TABLE IF NOT EXISTS expressive_dice (
 id uuid PRIMARY KEY, room_id uuid NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
 sender_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 value smallint NOT NULL CHECK (value BETWEEN 1 AND 6), created_at timestamptz NOT NULL DEFAULT now(),
 message_id uuid UNIQUE REFERENCES messages(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS expressive_dice_recent ON expressive_dice(sender_id,created_at DESC);
-- Certificates cannot be rerolled after issuance. Only binding to a message is mutable.
CREATE OR REPLACE FUNCTION expressive_dice_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.id<>OLD.id OR NEW.room_id<>OLD.room_id OR NEW.sender_id<>OLD.sender_id OR NEW.value<>OLD.value OR NEW.created_at<>OLD.created_at THEN
  RAISE EXCEPTION 'immutable_dice';
 END IF;
 IF OLD.message_id IS NOT NULL AND NEW.message_id IS DISTINCT FROM OLD.message_id THEN
  RAISE EXCEPTION 'immutable_dice_binding';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS expressive_dice_immutable_trigger ON expressive_dice;
CREATE TRIGGER expressive_dice_immutable_trigger BEFORE UPDATE ON expressive_dice FOR EACH ROW EXECUTE FUNCTION expressive_dice_immutable();
CREATE OR REPLACE FUNCTION expressive_dice_message_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM expressive_dice WHERE message_id=OLD.id) AND
   (NEW.text IS DISTINCT FROM OLD.text OR NEW.encrypted_content IS DISTINCT FROM OLD.encrypted_content OR NEW.meta IS DISTINCT FROM OLD.meta OR NEW.room_id<>OLD.room_id OR NEW.sender_id<>OLD.sender_id) THEN
  RAISE EXCEPTION 'immutable_dice_message';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS expressive_dice_message_trigger ON messages;
CREATE TRIGGER expressive_dice_message_trigger BEFORE UPDATE ON messages FOR EACH ROW EXECUTE FUNCTION expressive_dice_message_immutable();
