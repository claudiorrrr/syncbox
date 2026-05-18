#!/usr/bin/env bash
#
# Build, sign, notarize, and publish a syncbox release to GitHub.
#
# One command produces and uploads three assets:
#   * syncbox-macos-arm64.app.zip      — signed + notarized app, for fresh
#                                        downloads (opens with no Gatekeeper
#                                        prompt)
#   * syncbox-macos-arm64.app.tar.gz   — the Tauri updater artifact, plus its
#     (+ .sig)                           detached minisign signature
#   * latest.json                      — the manifest the in-app updater polls
#
# Existing installs poll the `releases/latest/download/latest.json` URL on
# launch and self-update; first-timers grab the .app.zip.
#
# Credentials live in scripts/release-env.sh (gitignored) — copy
# scripts/release-env.sh.example and fill it in once.
#
# Usage:  ./scripts/release.sh v0.2.0
set -euo pipefail

cd "$(dirname "$0")/.."

TAG="${1:-}"
if [ -z "$TAG" ]; then
  echo "usage: $0 <tag>   e.g. $0 v0.2.0" >&2
  exit 1
fi

ENV_FILE="scripts/release-env.sh"
if [ ! -f "$ENV_FILE" ]; then
  echo "missing $ENV_FILE — copy ${ENV_FILE}.example and fill in the creds" >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$ENV_FILE"

REPO="claudiorrrr/syncbox"
BUNDLE="target/release/bundle/macos"
APP="$BUNDLE/syncbox.app"
VERSION="${TAG#v}"

echo "Building signed + notarized release $TAG…"
bun run tauri build

[ -d "$APP" ] || { echo "build output missing: $APP" >&2; exit 1; }

# Refuse to publish anything that doesn't actually verify.
echo "Verifying signature + notarization…"
codesign --verify --strict "$APP"
xcrun stapler validate "$APP"

# Tauri names the updater artifact after productName; glob rather than guess.
TARBALL="$(ls "$BUNDLE"/*.app.tar.gz 2>/dev/null | head -1)"
[ -n "$TARBALL" ] && [ -f "$TARBALL" ] || {
  echo "updater artifact (*.app.tar.gz) missing — is createUpdaterArtifacts set?" >&2
  exit 1
}
[ -f "$TARBALL.sig" ] || { echo "updater signature ${TARBALL}.sig missing" >&2; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Asset 1 — plain app zip for first-time downloads.
ditto -c -k --keepParent "$APP" "$WORK/syncbox-macos-arm64.app.zip"

# Asset 2 — the updater tarball.
cp "$TARBALL" "$WORK/syncbox-macos-arm64.app.tar.gz"

# Asset 3 — latest.json manifest. darwin-aarch64 is the updater's platform key
# for Apple Silicon; the signature is the detached .sig content verbatim.
SIG="$(cat "$TARBALL.sig")"
PUB_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cat > "$WORK/latest.json" <<EOF
{
  "version": "$VERSION",
  "pub_date": "$PUB_DATE",
  "platforms": {
    "darwin-aarch64": {
      "signature": "$SIG",
      "url": "https://github.com/$REPO/releases/download/$TAG/syncbox-macos-arm64.app.tar.gz"
    }
  }
}
EOF

# Publish. Create the release on first run, then upload (clobbering on re-run).
if ! gh release view "$TAG" -R "$REPO" >/dev/null 2>&1; then
  gh release create "$TAG" -R "$REPO" --title "syncbox $TAG" --generate-notes
fi
gh release upload "$TAG" -R "$REPO" --clobber \
  "$WORK/syncbox-macos-arm64.app.zip" \
  "$WORK/syncbox-macos-arm64.app.tar.gz" \
  "$WORK/latest.json"

echo
echo "Published $TAG"
echo "  app.zip + app.tar.gz + latest.json uploaded to $REPO"
echo "  installs on a build older than $VERSION will self-update on next launch"
