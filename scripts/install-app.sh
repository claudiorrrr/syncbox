#!/usr/bin/env bash
#
# Build the macOS app and install it into /Applications without the
# Gatekeeper "right-click → Open" dance.
#
# Why this works: that prompt only appears when macOS has tagged the app with
# the `com.apple.quarantine` extended attribute. That tag is added by the
# *transfer* — a browser download, AirDrop, or unzipping a downloaded .zip —
# never by a local build or a local `cp`. So building and copying on the same
# Mac means the app is never quarantined and never needs `xattr`.
#
# The `xattr -dr` below is a belt-and-suspenders clear, in case the destination
# carried the flag from an earlier downloaded copy.
#
# Usage:  bun run reinstall      (or: ./scripts/install-app.sh)
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Building…"
bun run tauri build

APP="target/release/bundle/macos/syncbox.app"
DEST="/Applications/syncbox.app"

if [ ! -d "$APP" ]; then
  echo "build output not found at $APP" >&2
  exit 1
fi

# Quit any running copy so replacing the bundle doesn't fail.
osascript -e 'quit app "syncbox"' >/dev/null 2>&1 || true
killall syncbox >/dev/null 2>&1 || true
sleep 1

rm -rf "$DEST"
cp -R "$APP" "$DEST"
xattr -dr com.apple.quarantine "$DEST" 2>/dev/null || true

echo "Installed: $DEST"
echo "Launch from /Applications — no right-click needed."
