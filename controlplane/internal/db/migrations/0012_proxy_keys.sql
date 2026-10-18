-- Outbound proxy keys: a carrier connection the exit node opens to its provider through someone else's address.
-- The panel stores the subscription link the admin pasted; the node parses it and balances across the keys
-- assigned to that carrier, because a provider blocks a node's own address long before it blocks a proxy's.
CREATE TABLE IF NOT EXISTS proxy_keys (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name       text NOT NULL DEFAULT '',
    link_enc   bytea NOT NULL,
    kind       text NOT NULL DEFAULT '',
    server     text NOT NULL DEFAULT '',
    port       integer NOT NULL DEFAULT 0,
    priority   integer NOT NULL DEFAULT 1,
    enabled    boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS proxy_key_carriers (
    proxy_key_id uuid NOT NULL REFERENCES proxy_keys (id) ON DELETE CASCADE,
    carrier      text NOT NULL,
    PRIMARY KEY (proxy_key_id, carrier)
);

CREATE INDEX IF NOT EXISTS proxy_key_carriers_by_carrier_idx ON proxy_key_carriers (carrier);

-- Which key a node put on which proxy, so a restart keeps the address the provider's check was issued for.
CREATE TABLE IF NOT EXISTS proxy_assignments (
    node_id      uuid NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    subject      text NOT NULL,
    proxy_key_id uuid NOT NULL REFERENCES proxy_keys (id) ON DELETE CASCADE,
    assigned_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (node_id, subject)
);