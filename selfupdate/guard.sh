#!/bin/sh
# Runs before every start of the node (ExecStartPre). It does not trust the node binary: after an update the
# new binary has a probation period, and a binary that crashes, hangs or never becomes healthy is replaced by
# the previous one here, so even a core that cannot start at all is rolled back by the next restart.
BIN="${1:-/opt/openflux/bin/universal-bypass-tool}"
DIR=$(dirname "$BIN")
STATE="$DIR/update.state"
MAX_STARTS=3
BAD_FOR=43200

[ -f "$STATE" ] || exit 0

get() { sed -n "s/^$1=//p" "$STATE" | head -n1; }

PHASE=$(get PHASE)
[ "$PHASE" = "probation" ] || exit 0

VERSION=$(get VERSION)
PREV_VERSION=$(get PREV_VERSION)
STARTS=$(get STARTS)
DEADLINE=$(get DEADLINE)
BAD_VERSION=$(get BAD_VERSION)
BAD_UNTIL=$(get BAD_UNTIL)
STARTS=${STARTS:-0}
NOW=$(date +%s)

write_state() {
    printf 'PHASE=%s\nVERSION=%s\nPREV_VERSION=%s\nSTARTS=%s\nDEADLINE=%s\nBAD_VERSION=%s\nBAD_UNTIL=%s\n' \
        "$1" "$VERSION" "$PREV_VERSION" "$2" "$DEADLINE" "$3" "$4" > "$STATE.tmp" && mv -f "$STATE.tmp" "$STATE"
}

STARTS=$((STARTS + 1))

if [ "$STARTS" -gt "$MAX_STARTS" ] || { [ -n "$DEADLINE" ] && [ "$NOW" -gt "$DEADLINE" ]; }; then
    if [ -f "$BIN.prev" ] && cp -f "$BIN.prev" "$BIN.rollback" && chmod 755 "$BIN.rollback" && mv -f "$BIN.rollback" "$BIN"; then
        write_state "" 0 "$VERSION" $((NOW + BAD_FOR))
        logger -t openflux-guard "update to $VERSION did not become healthy, rolled back to $PREV_VERSION" 2>/dev/null
        echo "openflux-guard: update to $VERSION did not become healthy, rolled back to $PREV_VERSION"
    else
        write_state "" 0 "$BAD_VERSION" "${BAD_UNTIL:-0}"
        echo "openflux-guard: no previous binary to roll back to"
    fi
else
    write_state "probation" "$STARTS" "$BAD_VERSION" "${BAD_UNTIL:-0}"
fi
exit 0
