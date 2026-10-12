-- A Cloudflare WARP account the panel registered on the admin's behalf. A key that points at one has
-- its exit node dial the internet through WARP instead of straight from the node's own address.
CREATE TABLE IF NOT EXISTS warp_profiles (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name            text NOT NULL DEFAULT '',
    device_id       text NOT NULL DEFAULT '',
    private_key_enc bytea NOT NULL,
    peer_public_key text NOT NULL,
    endpoint        text NOT NULL,
    address_v4      text NOT NULL,
    address_v6      text NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE keys ADD COLUMN IF NOT EXISTS warp_profile_id uuid REFERENCES warp_profiles (id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS keys_warp_profile_idx ON keys (warp_profile_id) WHERE warp_profile_id IS NOT NULL;
