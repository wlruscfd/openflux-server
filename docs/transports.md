# Transports

A transport is the carrier that moves tunnel traffic between client and
exit. OpenFlux ships several native (Go) transports, plus a scripted
engine for writing more without touching Go at all - see
[scripted-transports.md](scripted-transports.md).

## Native transports

| Type | Carrier | Notes |
|------|---------|-------|
| `yandex` | Yandex.Docs (WebSocket) | Default. Solves Yandex's proof-of-work captcha automatically. |
| `vyandex` | Yandex Volga (HTTP relay + WS) | Can use a Netscape `cookies.txt` with a logged-in session (`--yandex-cookies-file`). |
| `boards` | Yandex Board (WebSocket) | |
| `oneme` | MAX/OneMe (WebRTC DataChannel) | Needs `--maxToken` / `--maxUid`; cannot be shared via a link (the token belongs to one account). |
| `avito` | code.avito.ru (ShareDB rooms) | The exit creates the rooms with an Avito account (`NODEAGENT_AVITO_COOKIES`, or `NODEAGENT_AVITO_COOKIES_FILE`, a raw `name=value; ...` cookie string) and reports the base64 room list as the key's doc URL. Rooms per key shrink as the node serves more avito keys (16 rooms in total are shared out, at most 4 per direction and key). Room creation and connects follow `OPENFLUX_HOST_RATE` (default `code.avito.ru=4` per second). |
| `cupsonline` | Cups.online (Centrifugo rooms) | The exit prints a base64 room list; pass it to the client via `--url`, or use it as a key's doc URL. Meant for self-hosting: a room carries roughly 50-100 KB/s, and cups.online refuses new rooms from an address that creates many (HTTP 403), so create the list once and reuse it (`OPENFLUX_CUPS_ROOMS` sets how many rooms). |
| `mailru` | Mail.ru Docs (WebSocket) | Accepts a bare weblink (`AbCdEfGh1/IjKlMnOp2`) or a full `https://cloud.mail.ru/public/...` URL. |
| `telemost` | Yandex Telemost (WebRTC video via the SFU) | Pass the conference link `https://telemost.yandex.ru/j/...`. Runs five lanes by default, about 16 Mbit/s each (`OPENFLUX_TELEMOST_LANES=N\|auto`, `OPENFLUX_TELEMOST_MAX_LANES`); publishes at 30 fps while idle (`TELEMOST_IDLE_FPS`). Roughly 3% of a core per lane, so keep clients per key low. No end-to-end encryption layer yet. |
| `direct` | Plain TCP to the exit | Sessions only; needs the exit's port reachable from the client. |
| `script` | Signed JS (goja) transport | Sessions only (needs `--encryption-key-file`); see [scripted-transports.md](scripted-transports.md). |

```bash
# Yandex Volga (HTTP relay + WS)
./openflux --role=exit --mode=l3 --transport=vyandex --url="..." --debug

# MAX / OneMe (WebRTC DataChannel)
./openflux --role=exit --mode=l3 --transport=oneme \
    --maxToken="..." --maxUid="..." --debug

# Cups.online (Centrifugo rooms)
./openflux --role=exit --mode=l3 --transport=cupsonline --debug
# prints a base64 room list; pass it to the client via --url

# Yandex Board (WS)
./openflux --role=exit --mode=l3 --transport=boards --url="..." --debug

# Mail.ru Docs (WS)
./openflux --role=exit --mode=l3 --transport=mailru \
    --url="YOUR_MAILRU_PUBLIC_LINK" --debug
```

## Codec

By default every transport uses the batched + zstd codec
(`transport/batched.go` + `transport/framing.go`): it coalesces many tunnel
packets into a single transport message, for fewer channel messages and
higher throughput. `--codec=legacy` reverts to the old per-packet LZ4 codec
(for A/B comparison against older clients):

```bash
./openflux --role=client --codec=legacy ...
```

Batched and legacy codecs are wire-incompatible as a *requirement* - but a
peer running the Session (see [sessions.md](sessions.md)) decodes either
one it receives and follows what the other side actually sends, so
`--codec` there is a preference, not a hard requirement.

## Captchas

- **Proof-of-work captcha** (`showcaptchafast`) is solved by the transport
  itself, nothing to do.
- **SmartCaptcha or a login wall on the client's own transport**: with
  `--ipc-socket=PATH` the core asks the app (`CookiesRequest`), the app opens
  the page in a browser view and answers with the cookies (`CookiesOffer`);
  the transport applies them and reconnects.
- **The same on the exit**: the exit reports it to the client as a control
  message over any transport that still works (for example `direct` while the
  document is the one stuck). The client passes it to the app as a
  `CookiesRequest` with `remote: true` and `proxy`: a local HTTP proxy whose
  connections leave through the tunnel and the exit, so the check is passed
  from the exit's address. The app answers with `remote: true` and the exit
  applies the cookies. The proxy's TCP stack shares the tunnel address and uses
  local ports 12000-12999.
- In practice a real browser coming from the exit's address is usually let
  straight through to the document (the captcha targets the transport's HTTP
  client), so loading the page and sending its cookies is typically enough.
- Cookies are persisted in `--cookie-store` (default
  `./cookies-<transport>.json`) and reused after restarts. Under systemd with
  `ProtectSystem=strict`, point it at a writable directory.
- A scripted transport reaches the same pipeline via `raise("captchaRequired", ...)`
  / `raise("needsSetup", ...)` - see [scripted-transports.md](scripted-transports.md).

Wire-level details of how a captcha/login signal rides the Session's control
channel: [../PROTOCOL_NEGOTIATION.md](../PROTOCOL_NEGOTIATION.md).

## Implementing a native transport

Implement the `Transport` interface from `transport/transport.go` and
register your transport in `transport_factory.go` (sessions, `--transports`)
and in the `--transport` switch in `main.go` (single-transport mode); see
`transport/mailru/` for a complete example. The batched codec
(`BatchedTransport`) wraps any transport, so a new backend gets batching for
free. To take part in captcha handling, also implement
`transport.ErrorNotifier` and `transport.CookieExchanger`.

If a native Go implementation is more than you need - no new wire format,
just moving bytes over an HTTP/WebSocket API - writing a
[scripted transport](scripted-transports.md) instead needs no Go toolchain
and no rebuild of the core or the apps to ship.
