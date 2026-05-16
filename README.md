# syncbox

Dropbox-style folder sync for your own machines. Pick a folder, pair with a
6-character code, and it stays in sync — peer-to-peer, no cloud account.

A macOS menu-bar app, plus a headless `syncbox` CLI for Linux servers.

## What it does

- Watches one folder per device; adds, edits, and deletes propagate in seconds.
- Direct device-to-device transfer (same Wi-Fi = fast, no internet hop).
- Conflicts keep both copies — the loser becomes
  `name.conflict-<host>-<timestamp>.ext`.
- Deletes propagate everywhere. Exception: a file edited *after* the delete
  wins and comes back (last-write-wins).
- File content is streamed, so large files don't blow up memory.

## How it works

Each file is one entry in an [iroh-docs](https://iroh.computer) CRDT document
(`key = relative/path`, content addressed by BLAKE3). iroh handles the
networking — QUIC transport, NAT hole-punching, encryption — and iroh-blobs
moves the content. syncbox mirrors the document to disk and back.

Built on **Rust**, **Tauri 2** (menu-bar app), the **iroh** stack (P2P + sync),
**notify** (file watching), and **tokio**. Pairing uses a tiny **Cloudflare
Worker**.

## Layout

```
crates/syncbox-core/   sync engine (shared)
crates/syncbox-cli/    headless CLI
src-tauri/             macOS menu-bar app
src/                   app window UI (HTML/CSS/JS)
pair-server/           Cloudflare Worker — 6-char pairing codes
```

## Build

Needs [Rust](https://rustup.rs). The GUI also needs [Bun](https://bun.sh).

Optionally bake in your pairing-server host (file is gitignored):

```bash
echo "https://pair.<your-domain>" > pair-server.txt
```

**macOS app:**

```bash
bun install
bun run tauri build       # → target/release/bundle/macos/
```

Unsigned — on first launch, right-click the app → **Open** → **Open**.

**CLI:**

```bash
cargo build --release -p syncbox-cli      # → target/release/syncbox
```

## Pair two devices

GUI: **Choose folder**, click **Get code** on one Mac, type it into **Use
code** on the other.

CLI:

```bash
syncbox init ~/Sync       # both devices
syncbox pair              # device A — prints a code
syncbox join ABC-123      # device B
syncbox run               # both — or via systemd, see crates/syncbox-cli/syncbox.service
```

## Pairing — how the code works

The 6-character code is a short-lived swap, not a connection:

1. Device A uploads its iroh ticket (public connection info) to the pairing
   server and gets back a 6-char code with a 5-minute TTL.
2. Device B redeems the code for the ticket; the server then forgets it —
   single use.
3. B connects to A directly over iroh (direct → hole-punch → relay). From
   here the pairing server is out of the loop.

The server only ever holds the ticket — public metadata, never file content.

## Self-hosting

No central server for your data. Two optional pieces, both yours to run.

**Pairing server** — `pair-server/` is a Cloudflare Worker:

```bash
cd pair-server
bun install
bunx wrangler login
bunx wrangler kv namespace create PAIR    # paste the id into wrangler.toml
# set the routes pattern in wrangler.toml to pair.<your-domain>
bunx wrangler deploy
```

Point devices at it — resolved highest-priority-first: the `SYNCBOX_PAIR_SERVER`
env var, the app's **Advanced → Pair server URL** field, or the compiled-in
default from `pair-server.txt`. It is two endpoints (`POST /pair`,
`GET /pair/<code>`), so you can reimplement it in anything — see
`pair-server/README.md`.

**Relay** (optional) — iroh uses n0's public relays for NAT traversal; they
see connection metadata, never content. Run your own
[`iroh-relay`](https://docs.rs/iroh-relay) to avoid that.

## Config & data

Per device, under `~/Library/Application Support/dev.syncbox/` (override with
the `SYNCBOX_DATA_DIR` env var):

- `config.json` — folder, device name, ticket, known peers
- `iroh/` — secret key, blob store, docs database

## Limitations

Lightly tested — keep backups of anything important.

- **Pre-release dependency** — pinned to `iroh 1.0.0-rc.0`; upgrades may
  need code changes.
- **No delta sync** — editing one byte of a 1 GB file re-sends the gigabyte.
- **No atomic snapshots** — app bundles (`.alfredpreferences`, SQLite+WAL)
  can sync half-updated. Individual files write atomically; bundles don't.
- **Deletes are real** — no trash, no undo. Recover from a backup.
- **Last-write-wins** — simultaneous edits don't merge; clock skew can pick
  the wrong winner.
- **Empty folders don't sync** — only files are entries.
- **One folder, one peer set** — no multi-folder or selective sync.
- **Unsigned app**, no auto-updates.
- **Blob store is not encrypted at rest** — on-wire traffic is encrypted, the
  local `iroh/` directory is not.

## Develop

```bash
bun run tauri dev                       # GUI
cargo run -p syncbox-cli -- status      # CLI
```

## License

MIT. See LICENSE.
