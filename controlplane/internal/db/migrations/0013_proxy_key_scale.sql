-- Scale for the outbound proxy keys.
--
-- A public subscription list runs to thousands of links, and the panel has to take them in
-- one go: the link is sealed, so a plain list of links cannot be compared against what is
-- already stored, and importing the same list twice filled the table with copies.
-- The hash of the link makes an import idempotent and lets the same link serve another
-- carrier instead of becoming a second row.
--
-- The state columns record what the node found when it last used the key: which provider
-- it could reach, how long it took, and what the refusal was. A key list is mostly dead
-- links, so without this the panel cannot say which ones are worth keeping.
ALTER TABLE proxy_keys ADD COLUMN IF NOT EXISTS link_hash bytea;
ALTER TABLE proxy_keys ADD COLUMN IF NOT EXISTS state text NOT NULL DEFAULT 'unknown';
ALTER TABLE proxy_keys ADD COLUMN IF NOT EXISTS state_checked_at timestamptz;
ALTER TABLE proxy_keys ADD COLUMN IF NOT EXISTS state_latency_ms integer;
ALTER TABLE proxy_keys ADD COLUMN IF NOT EXISTS state_error text;

CREATE UNIQUE INDEX IF NOT EXISTS proxy_keys_link_hash_idx ON proxy_keys (link_hash) WHERE link_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS proxy_keys_state_idx ON proxy_keys (state, state_checked_at);
