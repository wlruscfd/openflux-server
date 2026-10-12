-- One WARP identity must never be used from two nodes: Cloudflare keeps a single endpoint per WireGuard
-- key and follows whichever address handshook last, so two nodes sharing a key take turns losing every
-- packet. A key's warp_profile_id now only says "send me through WARP"; the node uses the profile bound to it.
ALTER TABLE warp_profiles ADD COLUMN IF NOT EXISTS node_id uuid REFERENCES nodes (id) ON DELETE CASCADE;

CREATE UNIQUE INDEX IF NOT EXISTS warp_profiles_node_idx ON warp_profiles (node_id) WHERE node_id IS NOT NULL;
