# OpenFlux Control Plane

**English** | [Русский](README.ru.md)

Full HTTP API reference: [API.md](API.md) ([Русский](API.ru.md)).

Multi-tenant key/token management, traffic accounting, and node coordination for OpenFlux exit
nodes. This is a separate Go module and a separately deployed service — it does not carry or
relay any tunneled traffic itself; that still flows directly between client and exit node over
the transport (Yandex Docs, etc.), exactly as before. The control plane only handles control
traffic: issuing/validating keys, batched traffic-usage reports, and node coordination.

## Configuration (environment variables)

| Variable                          | Required | Description                                                        |
|------------------------------------|----------|----------------------------------------------------------------------|
| `CONTROLPLANE_DATABASE_URL`        | yes      | Postgres connection string (`postgres://user:pass@host:5432/db`)   |
| `CONTROLPLANE_TOKEN_PEPPER`        | yes      | Secret appended before hashing every token; rotating it invalidates all existing tokens |
| `CONTROLPLANE_ADMIN_TOKEN`         | yes      | Bootstrap bearer token for the `/v1/admin/*` endpoints              |
| `CONTROLPLANE_LISTEN_ADDR`         | no       | Default `:8080`                                                     |
| `CONTROLPLANE_RATE_LIMIT_RPS`      | no       | Per-IP requests/sec allowed on `/v1/resolve`, default `1`           |
| `CONTROLPLANE_PUBLIC_URL`          | no       | How clients reach this server (e.g. `https://your-domain-or-ip`, no path/trailing slash) — set by `deploy/install.sh` automatically. Without it, key-creation responses omit `deep_link` (see API.md) since a link with no working `control_url` isn't useful |

Migrations in `internal/db/migrations` run automatically on startup (tracked in a
`schema_migrations` table, safe to run repeatedly).

## Build & run

```bash
go build -o controlplane ./cmd/controlplane
CONTROLPLANE_DATABASE_URL="postgres://openflux:secret@localhost:5432/openflux?sslmode=disable" \
CONTROLPLANE_TOKEN_PEPPER="$(openssl rand -hex 32)" \
CONTROLPLANE_ADMIN_TOKEN="$(openssl rand -hex 32)" \
./controlplane
```

## Admin panel

Two interchangeable frontends are served — pick one, or both:

- **Embedded fallback (Go, always works):** a self-contained HTML page
  (`internal/api/web/admin.html`, no build step, no external assets) is embedded into the
  binary and served at `/admin/`, in Russian. Paste `CONTROLPLANE_ADMIN_TOKEN` once (kept in the
  browser's `localStorage`), then a "Keys" tab covers create/list/filter/enable/disable/delete,
  and a "Settings" tab covers the registered exit node (status, heartbeat, rotate its token) plus
  ingest-token management.
- **Full dashboard (SvelteKit + Bun, recommended):** `controlplane/web` — SvelteKit 5, Tailwind,
  SSR + client hydration, RU/EN, dark/light theme, QR deep links on key creation, a dashboard with
  stat cards and a traffic chart, server load card, node/key/ingest management. Ships with a
  production entry (`bun server.js`, `controlplane/web/README.md`) that also proxies `/v1/*` to
  this Go process for http-mode deployments without Nginx; in the normal Nginx setup use the split
  described below so Go never goes through Bun.

Nginx (recommended): `/v1/*` → this Go service on `:8080`, `/admin/*` → Bun on `:3000`. Deploy
fragments: `controlplane/web/deploy/` (systemd unit, Nginx location, env template).

## Concepts

- **Node** — one exit-node process/machine. Has its own bearer token, polls for the keys assigned
  to it, and reports usage.
- **Key** — one end-user's access token. Bound to a `transport` + `doc_url` (e.g. a specific
  Yandex Docs link), has an optional `traffic_limit_bytes`, and can be enabled/disabled. Created
  either directly by an admin or in bulk via an ingest token.
- **Ingest token** — a scoped credential handed to a third-party app/script that generates keys
  (and their backing Yandex Docs) and registers them here.

New keys are best-effort auto-assigned to whichever active node currently has spare capacity
(`max_keys`), so you scale by registering more nodes, not by growing one process.

## Cascades (two-hop exit)

A key normally exits straight from its assigned node. It can instead exit through a *second*
node instead: client → Yandex Docs (disguised) → entry node → fast encrypted UDP link → final-exit
node → real internet. Only the client-facing hop needs to look like something else - the two nodes
are both yours, so the link between them is plain (if still encrypted) UDP instead of paying Yandex's
disguise overhead a second time. Typical use: an entry node inside a heavily-filtered network,
final-exiting through a node elsewhere.

To set one up:
1. Register both nodes normally (same flow either way - nothing marks a node as "entry" or
   "final-exit" ahead of time, that's decided per key below).
2. On the **Nodes** page, set the final-exit node's **public address** (its own IP or hostname, no
   port) - a node only ever calls out to this controlplane, so there's no other way to learn it.
3. On the **Keys** page, open a key's cascade control and pick that node as its final exit. This
   allocates a dedicated UDP port for the link (a small 41000-41999 range, separate from raw mode's
   own per-key port allocation) and both nodes pick it up on their next poll.

### Cascade through Cloudflare WARP

On the same cascade control, pick **Cloudflare WARP**. The key's exit node then reaches the internet
through a WireGuard link to Cloudflare WARP, so the sites it visits see a WARP address instead of the
node's own. Nothing needs preparing: the first time you enable it the panel registers an anonymous
WARP account with Cloudflare (this server must reach `api.cloudflareclient.com`), keeps the private key
sealed like a key token and hands the config to the node on its next poll. Every key shares that one
account unless you register more (`POST /v1/admin/warp`, `PATCH /v1/admin/keys/{id}/warp` with
`{"enabled": true, "profile_id": "..."}`).

A key uses either a final-exit node or WARP, not both. If the WARP link cannot come up the key stays
down rather than leaking out of the node's own address. TCP only, like every other exit.

Not supported yet: combining a cascade with `e2e_encryption` on the same key (the final-exit node
would need to own the E2E unwrap instead of the entry node, and that boundary isn't wired up) - the
entry node refuses to start that key rather than getting it wrong. Usage accounting is unaffected
either way: it's still measured on the entry node's client-facing side, so nothing double-counts.

## API walkthrough

```bash
BASE=http://localhost:8080
ADMIN=$CONTROLPLANE_ADMIN_TOKEN

# 1. Register an exit node, get its token
NODE=$(curl -s -X POST $BASE/v1/admin/nodes \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"name":"node-eu-1","max_keys":300}')
echo "$NODE"
NODE_TOKEN=$(echo "$NODE" | jq -r .token)
NODE_ID=$(echo "$NODE" | jq -r .id)

# 2. Create an ingest token for a third-party key-generation script
INGEST=$(curl -s -X POST $BASE/v1/admin/ingest-tokens \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"label":"key-gen-bot"}')
INGEST_TOKEN=$(echo "$INGEST" | jq -r .token)

# 3. That third-party app registers a new key (its own Yandex Docs link)
curl -s -X POST $BASE/v1/ingest/keys \
  -H "Authorization: Bearer $INGEST_TOKEN" -H 'Content-Type: application/json' \
  -d '{"label":"user-42","doc_url":"https://docs.yandex.ru/docs/edit?url=...","traffic_limit_bytes":10737418240}'

# 3b. List or revoke ingest tokens later (e.g. one leaked)
curl -s $BASE/v1/admin/ingest-tokens -H "Authorization: Bearer $ADMIN"
curl -s -X POST $BASE/v1/admin/ingest-tokens/<id>/disable -H "Authorization: Bearer $ADMIN"

# 4. The exit node polls for its assigned keys
curl -s $BASE/v1/nodes/keys -H "Authorization: Bearer $NODE_TOKEN"

# 5. The exit node reports batched usage
curl -s -X POST $BASE/v1/nodes/$NODE_ID/usage \
  -H "Authorization: Bearer $NODE_TOKEN" -H 'Content-Type: application/json' \
  -d '{"deltas":[{"key_id":"<key-id>","bytes_sent_delta":1048576,"bytes_received_delta":2097152}]}'

# 6. A client resolves its own key to connection details
curl -s -X POST $BASE/v1/resolve -H "Authorization: Bearer <client key token>"
```

## Scaling notes

- The service is stateless besides Postgres, so run multiple replicas behind a load balancer.
- `/v1/resolve` is the only endpoint an untrusted client token hits directly; it's rate-limited
  per IP (in-memory token bucket — resets per replica, so treat it as a brake on casual abuse, not
  a hard guarantee).
- Usage is reported in batches by each node on an interval, not per packet, so write volume scales
  with node count, not user count. A single batch is capped at 1000 deltas server-side
  (`handleNodeUsage`) — the node itself chunks into multiple requests above that, so this only
  matters if you're changing the cap on both ends together.
- How many keys one exit node can serve depends on its `--mode` (see the main README): `proxy` has
  no per-node cap beyond what the box's own resources allow; `raw` reserves a disjoint port range
  per key to avoid cross-talk on the one shared raw socket/IP, which caps a single raw-mode node at
  roughly 252 concurrent keys (`portRangeSize`/`portRangeMax` in `nodeagent/orchestrator.go`) —
  assign more nodes rather than expecting one raw-mode node to grow past that.
- A raw-mode node's raw sockets are opened once per process and shared by every worker on it, not
  one pair per key - see `tunnel/rawsocket_linux.go`'s `rawSocketCore` - so CPU spent on the return
  path scales with real traffic, not with how many keys happen to be assigned to that node.
