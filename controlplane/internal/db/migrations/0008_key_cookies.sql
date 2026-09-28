-- Per-key cookie jar, filled by the client that can actually pass the provider's check.
-- Kept in its own table so the secret never rides along with the key row that admin
-- and ingest endpoints marshal wholesale.
CREATE TABLE IF NOT EXISTS key_cookies (
    key_id      uuid PRIMARY KEY REFERENCES keys(id) ON DELETE CASCADE,
    cookies_enc bytea NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_key_cookies_updated_at ON key_cookies (updated_at);
