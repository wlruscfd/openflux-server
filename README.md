# OpenFlux Server

**English** | [Русский](README.ru.md)

A fork of [p1neappleXpress/OpenFlux](https://github.com/p1neappleXpress/OpenFlux). Network stack
research tool: TCP tunnel with pluggable transports, plus a multi-user control plane and gomobile
bindings for the [Android app](https://github.com/wlruscfd/openflux-app).

Sibling repos: [openflux-app](https://github.com/wlruscfd/openflux-app) (the Android client) and
[openflux-deploy](https://github.com/wlruscfd/openflux-deploy) (rolls this repo's `controlplane`
out onto a VPS).

## Overview
```
Client (SOCKS5) --> Transport --> Exit Node --> Internet
```

TCP packets are sent via Transport. Available transports (`--transport`):
1. `yandex`/`yandex_multistream`/`volga` - send packets via Yandex Docs cursor messages;
2. `oneme` (Max) - sends packets via WebRTC DataChannel (desktop client/exit-node only - the
   Android app doesn't support it; see `mobile/mobile.go`'s package comment for why);
3. `cupsonline` - sends packets via cups.online's collaborative interview-room cursor sync
   (desktop client/exit-node only, ported from upstream);
4. `mailru` - sends packets via Mail.ru Docs cursor messages, the same coauthoring-protocol
   family as `yandex` (desktop client/exit-node only, ported from upstream).

Client side runs a SOCKS5 proxy, exit node decapsulates and forwards packets to destination point.

## Requirements
1. Golang v. 1.26.3+ - for building the desktop client / exit-node binary (universal-bypass-tool);
2. Android NDK v.27.0.12077973+ - for building the `.aar` the Android app embeds (`./build_android_aar.sh`);
3. XCode v. 26.6+ - for building the iOS client binary;
4. A Linux VPS/VDS for the exit node and, if you want the multi-user control plane, for `controlplane` too (see [openflux-deploy](https://github.com/wlruscfd/openflux-deploy)).

## Structure

```
main.go
transport/
├── transport.go      # Transport interface
├── yandex/           # Yandex Docs backend (also Volga, yandex_multistream)
├── oneme/            # MAX Messenger backend (desktop only)
├── cupsonline/       # cups.online backend (desktop only, ported from upstream)
└── mailru/           # Mail.ru Docs backend (desktop only, ported from upstream)
tunnel/
├── tunnel.go         # TCP tunnel core
├── endpoint.go       # Virtual NIC
└── rawsocket.go      # Raw socket (exit node)
socks5/                # SOCKS5 server (desktop client)
gateway/               # TUN-based transparent proxy (Android client, via VpnService)
mobile/                # gomobile bind entry point consumed by openflux-app
nodeagent/             # Exit-node orchestrator for managed (controlplane) mode
controlplane/          # Multi-user key/traffic/token service + admin panel - separate Go
│                      # module, see controlplane/README.md
network/               # Checksums, packet parsing
utils/                 # Debug logging
```

## Build (desktop client / exit-node binary)

```bash
go mod tidy
go build -o universal-bypass-tool .
```

## Build for Android
See [openflux-app](https://github.com/wlruscfd/openflux-app)'s README - `./build_android_aar.sh`
here builds `mobile/` into an `.aar` via `gomobile bind` for that repo to embed.

## Build for iOS (client binary)
```bash
export XCODE_PATH="<your Xcode.app path>" # optional, defaults to /Applications/Xcode.app
./build_ios.sh
```

## Usage

### Setting up an exit node

If Yandex serves a CAPTCHA instead of the doc (`yandex`/`yandex_multistream` transport only), the
exit node tries to clear it unattended with a shared, lazily-started headless Chrome/Chromium
(`transport/yandex/captchasolver.go`) - many CAPTCHAs turn out to be a JS/behavioral check a real
browser passes on its own within seconds. This is best-effort: it needs `chromium` (or
`google-chrome`) on `PATH` (`deploy/install.sh` installs it, non-fatally, when you opt to run a
node), and a genuine interactive puzzle just times out and falls back to the normal
cooldown-and-retry - there's no way around that one without a human or a paid solving service.

The exit node reaches the real internet in one of two modes (`--mode`):

- **raw** (default) — gvisor forwards raw IP packets through a real raw socket (needs root)
  and its own NAT/forwarding. Carries any IP protocol the client sends - this is how general
  UDP relay (not just DNS) currently works - but the kernel has no socket for these
  gvisor-terminated connections and sends a real RST on every reply unless suppressed; see
  below. Only tested on Linux.
- **proxy** — each TCP flow is terminated locally in gvisor and re-originated with a plain
  `net.Dial` to the real destination. No root, no raw socket, no RST-drop rule needed at all.
  TCP only: a UDP packet gets gvisor's own default port-unreachable response instead of being
  relayed (which incidentally makes QUIC-preferring apps fall back to TCP fast instead of
  stalling). Works on Linux, Windows, macOS.

Raw mode's kernel-generated RSTs must be suppressed, but **scoped**, not host-wide - a blanket
`-j DROP` on all outbound RSTs makes every closed port on the box answer with silence (a port
scanner sees "filtered" instead of "closed") and stops the host resetting any of its own other
connections. Neither `-m owner --uid-owner` nor `-s <ip>` can scope this correctly: the
kernel-generated RSTs have no owning socket, and the raw socket sends its own legitimate RSTs
from that same IP too - either match drops both, silently EPERM'ing our own connection resets
and leaving real peers thinking a torn-down connection is still open. The raw socket marks its
own packets (`SO_MARK`, see `tunnel/rawsocket_linux.go`) specifically so the rule can tell them
apart:

```bash
# raw mode (default):
sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -m mark ! --mark 0x2547 -j DROP
sudo ./universal-bypass-tool --exit-node --url "YOUR_YANDEX_DOC_URL" --debug

# proxy mode - no root, no iptables rule, but no general UDP relay either:
./universal-bypass-tool --exit-node --mode proxy --url "YOUR_YANDEX_DOC_URL" --debug
```

1. Only the legacy Yandex document editor is supported (toggle this from the interface).

### Setting up a desktop client

```bash
./universal-bypass-tool --client --url "YOUR_YANDEX_DOC_URL" --socks5 :1080 --debug
```

Then set up a SOCKS5 proxy in your browser at localhost:1080.

## Flags

| Flag          | Default             | Description                |
|---------------|---------------------|----------------------------|
| `--client`    |                     | Run as client              |
| `--exit-node` |                     | Run as exit node           |
| `--socks5`    | `:1080`             | SOCKS5 listen address      |
| `--url`       | `https://localhost` | Document URL (Yandex Docs) |
| `--maxToken`  | ``                  | Auth token (Max)           |
| `--maxUid`    | ``                  | User ID (Max)              |
| `--debug`     | `false`             | Enable verbose logging     |
| `--transport` | `yandex`            | Select transport backend (`yandex`, `volga`, `oneme`, `yandex_multistream`, `cupsonline`, `mailru`) |
| `--managed`      | `false` | Exit node only: fetch active keys from a controlplane instance instead of a single `--url` |
| `--control-url`  | ``      | Managed mode: base URL of the `openflux-control` service |
| `--node-token`   | ``      | Managed mode: this node's bearer token from controlplane |
| `--mode`         | `raw`   | Exit node only: `raw` (needs root, general UDP relay) or `proxy` (no root, TCP only) |
| `--local-ip`     | ``      | Raw mode only: exit node egress IP, for a box with more than one |
| `--port-range-size` | `96` | Managed raw mode only: outbound ports reserved per concurrent key - lower fits more keys on this node (`~65000/size`), higher tolerates one key opening more simultaneous connections at once (e.g. Telegram loading media) before new ones start failing |
| `--captcha-solve-mode` | `headless_browser` | Exit node only (`yandex`/`yandex_multistream`): `headless_browser` tries a shared headless Chrome/Chromium automatically when Yandex serves a CAPTCHA (needs it on `PATH` - `deploy/install.sh` asks and installs it), or `off` to just wait out the normal cooldown-and-retry |
| `--codec`        | `legacy` | Wire codec for `--transport volga`/`oneme`/`cupsonline`/`mailru`: `legacy` (per-packet LZ4, unchanged) or `batched` (coalesce bursts into one zstd-compressed message per transport send - see below). Both ends must agree. Ignored for `yandex`/`yandex_multistream` - see below. |

Ported from upstream [p1neappleXpress/OpenFlux](https://github.com/p1neappleXpress/OpenFlux):
`batched` coalesces a burst of outgoing tunnel packets (plus a short linger
window to catch stragglers - both tunable via `OPENFLUX_BATCH_BYTES` /
`OPENFLUX_BATCH_COUNT` / `OPENFLUX_BATCH_LINGER_MS` env vars) into a single
zstd-compressed message per transport send, instead of one message per
packet. Applies to `volga`/`oneme`/`cupsonline`/`mailru` only; a client and
exit node must run the same `--codec` for these - they can't decode each
other's frames otherwise.

The `yandex`/`yandex_multistream` transports don't use `--codec` at all -
they already coalesce internally, and auto-negotiate whole-batch zstd
compression with the peer instead of compressing each packet before
batching: once a peer's keepalive proves it understands the newer format,
several raw packets are framed together and zstd-compressed as one unit
rather than LZ4-compressed one at a time before being batched - strictly
better compression (it can exploit redundancy between packets in the batch,
not just within one) at no compatibility cost. An old client or exit node
that's never seen this feature is unaffected: the bytes it sends and
receives are untouched, and a new peer talking to it just keeps using the
older per-packet format it always used, indefinitely if that peer never
upgrades. Nothing to configure - this is automatic and safe to roll out to
only one side of a deployment at a time.

This applies with `e2e_encryption` on too, not just plain keys: instead of
encrypting each compressed packet independently and then batching the
ciphertexts (what the traditional wrapping does, and what this falls back to
until negotiated), a whole batch of raw packets is compressed and THEN
encrypted as one sealed unit once the peer proves (via a second,
separate capability check) it does encrypted self-compression for this key -
plaintext compression markers never leave the process either way, and a peer
still on the traditional wrapping decrypts and decompresses the fallback
format exactly as before.

## Multi-user deployments (controlplane)

For running many keys/users behind a fleet of exit nodes — auth tokens, per-key traffic
accounting, enabling/disabling keys, a web admin panel, and an ingestion API for third-party key
generators — see [controlplane/README.md](controlplane/README.md). Exit nodes opt into this with
`--exit-node --managed --control-url ... --node-token ...`; the plain single-`--url` flow above
still works unchanged for manual/one-off use. To actually roll `controlplane` out onto a VPS
(Postgres, systemd, Nginx, Let's Encrypt), see
[openflux-deploy](https://github.com/wlruscfd/openflux-deploy).

## Implementing custom transports

You are free to implement the `Transport` interface from `transport/transport.go` and register
your custom transport in `main.go`'s switch block.

## License

This project is licensed under the **GNU General Public License v3.0 or later**.
See [LICENSE](LICENSE) for the full text.

Third-party licenses are listed in [NOTICE](NOTICE).

## Disclaimer

Educational use only. Test on your own machines and networks.
