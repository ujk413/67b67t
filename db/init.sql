-- 1. Подготовка расширений
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS citext;

-- 2. Таблица пользователей
CREATE TABLE IF NOT EXISTS users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  username citext NOT NULL UNIQUE,
  display_name text,
  avatar_id uuid,
  password_hash text NOT NULL,
  profile_tagline text,
  merged_into_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
  login_disabled_at timestamptz,
  last_seen timestamptz,
  banned boolean DEFAULT false,
  fcm_token text,
  is_admin boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE users ADD COLUMN IF NOT EXISTS banned boolean DEFAULT false;
ALTER TABLE users ADD COLUMN IF NOT EXISTS fcm_token text;
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin boolean NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS users_fcm_token_idx ON users(fcm_token) WHERE fcm_token IS NOT NULL;
CREATE INDEX IF NOT EXISTS users_banned_idx ON users(banned) WHERE banned = true;

CREATE TABLE IF NOT EXISTS user_public_names (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name citext NOT NULL UNIQUE,
  kind text NOT NULL CHECK (kind IN ('primary_tag', 'alias')),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS user_public_names_user_idx ON user_public_names(user_id);
CREATE UNIQUE INDEX IF NOT EXISTS user_public_names_one_primary_tag_idx ON user_public_names(user_id) WHERE kind = 'primary_tag';

-- 3. Таблица сессий (для авторизации)
CREATE TABLE IF NOT EXISTS sessions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token_hash text NOT NULL,
  expires_at timestamptz NOT NULL,
  device_name text,
  device_type text,
  ip_address inet,
  user_agent text,
  last_activity timestamptz DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sessions_token_hash_idx ON sessions(token_hash);
CREATE INDEX IF NOT EXISTS sessions_user_id_idx ON sessions(user_id);
CREATE INDEX IF NOT EXISTS sessions_user_last_activity_idx ON sessions(user_id, last_activity DESC);

-- 4. Таблица комнат
CREATE TABLE IF NOT EXISTS rooms (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  kind text NOT NULL CHECK (kind IN ('dm', 'group', 'channel')),
  title text,
  avatar_id uuid,
  created_by uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- 5. Участники комнат
CREATE TABLE IF NOT EXISTS room_members (
  room_id uuid NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  role text NOT NULL DEFAULT 'member',
  joined_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (room_id, user_id)
);

CREATE INDEX IF NOT EXISTS room_members_user_idx ON room_members(user_id);

CREATE TABLE IF NOT EXISTS room_secrets (
  room_id uuid PRIMARY KEY REFERENCES rooms(id) ON DELETE CASCADE,
  secret text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS user_blocks (
  blocker_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  blocked_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (blocker_id, blocked_id),
  CHECK (blocker_id <> blocked_id)
);

CREATE INDEX IF NOT EXISTS user_blocks_blocked_idx ON user_blocks(blocked_id);

-- 6. Теги комнат (для поиска)
CREATE TABLE IF NOT EXISTS room_tags (
  room_id uuid NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
  tag text NOT NULL,
  PRIMARY KEY (room_id, tag)
);

-- 7. Медиафайлы (картинки, файлы, голосовые, видео-кружки, аватарки)
CREATE TABLE IF NOT EXISTS media (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  owner_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  kind text NOT NULL CHECK (kind IN ('image', 'file', 'voice', 'video_note', 'avatar')),
  mime text NOT NULL,
  size_bytes bigint NOT NULL,
  path text NOT NULL,
  thumb_path text,
  duration_sec int,
  width int,
  height int,
  sha256 text,
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE media ADD COLUMN IF NOT EXISTS sha256 text;
CREATE INDEX IF NOT EXISTS media_owner_sha256_idx ON media(owner_id, sha256) WHERE sha256 IS NOT NULL;

-- 8. Сообщения
-- Включает поддержку типов: текст, медиа, голос, видео-кружки и системное уведомление о звонке
CREATE TABLE IF NOT EXISTS messages (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  room_id uuid NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
  sender_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  type text NOT NULL DEFAULT 'text' CHECK (type IN ('text', 'media', 'voice', 'video_note', 'call')),
  text text,
  media_id uuid REFERENCES media(id) ON DELETE SET NULL,
  encrypted_content text, -- Зашифрованный контент для E2E
  meta jsonb NOT NULL DEFAULT '{}'::jsonb, -- IV, sender_device_id, recipient_device_ids для E2EE
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS messages_room_idx ON messages(room_id);
CREATE INDEX IF NOT EXISTS messages_created_at_idx ON messages(created_at);

-- Reactions: rows disappear together with their message / user (ON DELETE CASCADE).
CREATE TABLE IF NOT EXISTS reactions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  message_id uuid REFERENCES messages(id) ON DELETE CASCADE,
  user_id uuid REFERENCES users(id) ON DELETE CASCADE,
  emoji text NOT NULL,
  created_at timestamptz DEFAULT now(),
  CONSTRAINT reactions_one_per_user UNIQUE (message_id, user_id)
);

-- 9. Рейтинги пользователей (если использовались в твоей логике)
CREATE TABLE IF NOT EXISTS user_ratings (
  target_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  author_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  rating int NOT NULL CHECK (rating >= 1 AND rating <= 5),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (target_id, author_id)
);

-- 10. E2E Encryption: Identity Keys (долгосрочные ключи пользователя)
CREATE TABLE IF NOT EXISTS user_identity_keys (
  user_id uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  public_key text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- 11. E2E Encryption: Device Keys (ключи устройств для мультидевайс)
CREATE TABLE IF NOT EXISTS user_devices (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  device_id text NOT NULL,
  public_key text NOT NULL,
  signed_pre_key_id int NOT NULL,
  signed_pre_key_public text NOT NULL,
  signed_pre_key_signature text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(user_id, device_id)
);

CREATE INDEX IF NOT EXISTS user_devices_user_idx ON user_devices(user_id);

-- 12. E2E Encryption: One-Time Pre-Keys (одноразовые ключи для X3DH)
CREATE TABLE IF NOT EXISTS user_prekeys (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  device_id uuid NOT NULL REFERENCES user_devices(id) ON DELETE CASCADE,
  key_id int NOT NULL,
  public_key text NOT NULL,
  used boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(device_id, key_id)
);

CREATE INDEX IF NOT EXISTS user_prekeys_device_idx ON user_prekeys(device_id);
CREATE INDEX IF NOT EXISTS user_prekeys_unused_idx ON user_prekeys(device_id, used) WHERE used = false;

-- 13. E2E Encryption: Session States (состояния Double Ratchet сессий)
CREATE TABLE IF NOT EXISTS e2e_sessions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  room_id uuid NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
  sender_device_id uuid NOT NULL REFERENCES user_devices(id) ON DELETE CASCADE,
  receiver_device_id uuid NOT NULL REFERENCES user_devices(id) ON DELETE CASCADE,
  session_state jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(room_id, sender_device_id, receiver_device_id)
);

CREATE INDEX IF NOT EXISTS e2e_sessions_room_idx ON e2e_sessions(room_id);

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

CREATE TABLE IF NOT EXISTS hidden_messages (user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE, message_id uuid NOT NULL REFERENCES messages(id) ON DELETE CASCADE, created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(user_id,message_id));
CREATE INDEX IF NOT EXISTS hidden_messages_user_idx ON hidden_messages(user_id);
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS pinned_message_id uuid REFERENCES messages(id) ON DELETE SET NULL;
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS pinned_at timestamptz;
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS pinned_by uuid REFERENCES users(id) ON DELETE SET NULL;
