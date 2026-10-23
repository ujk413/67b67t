CREATE TABLE IF NOT EXISTS quality_preferences (
  user_id uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  language text NOT NULL DEFAULT 'ru' CHECK(language IN ('ru','en')),
  quiet_enabled boolean NOT NULL DEFAULT false,
  quiet_start text NOT NULL DEFAULT '22:00',
  quiet_end text NOT NULL DEFAULT '08:00',
  time_zone text NOT NULL DEFAULT 'Europe/Moscow',
  group_notifications boolean NOT NULL DEFAULT true,
  download_policy jsonb NOT NULL DEFAULT '{"wifi":{"image":true,"video":false,"voice":true,"file":false},"mobile":{"image":false,"video":false,"voice":false,"file":false}}'::jsonb,
  cache_max_mb integer NOT NULL DEFAULT 256 CHECK(cache_max_mb BETWEEN 64 AND 2048),
  wallpaper text NOT NULL DEFAULT 'plain' CHECK(wallpaper IN ('plain','dots','mesh')),
  accent text NOT NULL DEFAULT 'emerald' CHECK(accent IN ('emerald','blue','violet')),
  crash_consent boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS quality_crash_reports (
  id uuid PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  platform text NOT NULL,
  code text NOT NULL,
  component text NOT NULL,
  app_version text NOT NULL,
  occurred_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS quality_crash_user_created ON quality_crash_reports(user_id,created_at DESC);
ALTER TABLE IF EXISTS push_devices ADD COLUMN IF NOT EXISTS quick_reply boolean NOT NULL DEFAULT false;
