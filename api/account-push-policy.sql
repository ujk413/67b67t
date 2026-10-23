-- Installation capability is device scoped, deliberately independent of accounts.
CREATE TABLE IF NOT EXISTS account_push_installations (
  installation_id text PRIMARY KEY,
  secret_hash text NOT NULL,
  epoch integer NOT NULL DEFAULT 1 CHECK(epoch>0),
  paused boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);
