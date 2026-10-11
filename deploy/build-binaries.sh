#!/bin/sh
# Builds the two Linux binaries the server runs:
#   controlplane          -> /opt/openflux/bin/controlplane
#   openflux-node         -> /opt/openflux/bin/universal-bypass-tool
#
# The managed nodeagent lives in ./cmd/openflux-node, NOT in the repository root:
# the root package is the v2 interactive CLI, which no longer accepts --managed,
# --node-token or --control-url and exits 2 on those flags. Building "." here
# produces a binary the nodeagent unit cannot start.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUT=${1:-"$ROOT/dist"}

mkdir -p "$OUT"

build() {
    echo "building $2 -> $OUT/$2"
    (cd "$ROOT/$1" && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o "$OUT/$2" "./$3")
}

build controlplane controlplane cmd/controlplane
build .          universal-bypass-tool cmd/openflux-node

ls -la "$OUT"