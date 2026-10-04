#!/bin/bash
set -e

# Builds the Go core (bridge/, the fork's facade; upstream's own bridge lives in mobile/) into an .aar via `gomobile bind`. Usage: ./build_android_aar.sh [output-libs-dir] (defaults to ../app/app/libs).

: "${ANDROID_HOME:?Set ANDROID_HOME to your Android SDK path}"
: "${ANDROID_NDK_HOME:?Set ANDROID_NDK_HOME to your Android NDK path (e.g. \$ANDROID_HOME/ndk/<version>)}"

OUTPUT_DIR="${1:-../app/app/libs}"
OUTPUT_AAR="$OUTPUT_DIR/openflux.aar"

mkdir -p "$OUTPUT_DIR"

if ! command -v gomobile >/dev/null 2>&1; then
    echo "gomobile not found on PATH - installing it (one-time setup)..."
    go install golang.org/x/mobile/cmd/gomobile@latest
fi

# gomobile bind needs golang.org/x/mobile as a tool dependency; no-op if already present.
go get -tool golang.org/x/mobile/cmd/gobind

echo "Building $OUTPUT_AAR (androidapi 26, arm64/arm)..."
# -checklinkname=0 works around Go 1.23+ rejecting anet's //go:linkname hook; -s -w strips symbols here since Gradle's own strip step can't touch a gomobile-produced .so.
gomobile bind -target=android/arm64,android/arm -androidapi 26 -ldflags="-checklinkname=0 -s -w" -o "$OUTPUT_AAR" ./bridge

echo "Build successful: $OUTPUT_AAR"
