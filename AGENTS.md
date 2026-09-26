# AGENTS.md

## Code style

- **No comments in code.** The owner does not want them. Name things clearly and let the
  code speak for itself; if something is genuinely surprising, prefer fixing or renaming it
  over explaining it in a comment.
- Keep commit messages descriptive - that is where the reasoning belongs.

## Commands

- Build: `go build ./...` (root module), `go build ./...` inside `controlplane/` (separate module).
- Tests: `go test ./transport/... ./tunnel/... ./nodeagent/...`; controlplane is its own module.
- `tunnel/TestExitModeProxyRelaysRealTCP` fails on hosts whose first non-loopback IPv4 is an
  APIPA address (169.254.x.x) because it cannot be bound. Environmental, not a regression.

## Live tests

`transport/mts` has opt-in tests against a real MTS Link Boards share link. They skip unless
env vars are set, and the skip message names the variable:

- `MTS_LIVE_BOARD` - board share URL, required by all of them
- `MTS_LIVE_RATE`, `MTS_LIVE_SECONDS` - sustained-rate sweep
- `MTS_LIVE_GUESTS` - guest count for the shared-board test
- `MTS_LIVE_IDLE_SECONDS` - idle stability window
- `MTS_LIVE_DEBUG` - transport debug logging
