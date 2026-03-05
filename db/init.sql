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
  last_seen timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- 3. Таблица сессий (для авторизации)
CREATE TABLE IF NOT EXISTS sessions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token_hash text NOT NULL,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sessions_token_hash_idx ON sessions(token_hash);
CREATE INDEX IF NOT EXISTS sessions_user_id_idx ON sessions(user_id);

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
  created_at timestamptz NOT NULL DEFAULT now()
);

-- 8. Сообщения
-- Включает поддержку типов: текст, медиа, голос, видео-кружки и системное уведомление о звонке
CREATE TABLE IF NOT EXISTS messages (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  room_id uuid NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
  sender_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  type text NOT NULL DEFAULT 'text' CHECK (type IN ('text', 'media', 'voice', 'video_note', 'call')),
  text text,
  media_id uuid REFERENCES media(id) ON DELETE SET NULL,
  meta jsonb NOT NULL DEFAULT '{}'::jsonb, -- Здесь хранятся IV для E2EE и другие данные
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS messages_room_idx ON messages(room_id);
CREATE INDEX IF NOT EXISTS messages_created_at_idx ON messages(created_at);

-- 9. Рейтинги пользователей (если использовались в твоей логике)
CREATE TABLE IF NOT EXISTS user_ratings (
  target_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  author_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  rating int NOT NULL CHECK (rating >= 1 AND rating <= 5),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (target_id, author_id)
);