# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Peer-to-peer folder sync (Dropbox-style) for a user's own machines. A macOS
menu-bar app plus a headless `syncbox` CLI for servers. No cloud account — devices
pair with a 6-char code and transfer directly over iroh.

## Commands

```bash
# GUI (macOS) — needs bun
bun install
bun run tauri dev                       # run with hot reload
bun run tauri build                     # → target/release/bundle/macos/

# CLI
cargo build --release -p syncbox-cli    # → target/release/syncbox
cargo run -p syncbox-cli -- status      # init/pair/join/run/status/list/remove

# Tests
cargo test -p syncbox-core              # syncbox-core currently has none

cargo fmt && cargo clippy --workspace

# Pair server (Cloudflare Worker)
cd pair-server && bunx wrangler deploy
```

The CLI uses `RUST_LOG` (e.g. `RUST_LOG=debug`) for log verbosity; defaults to `info`.

## Architecture

Cargo workspace, three members:

- **`crates/syncbox-core`** — the sync engine. UI- and platform-agnostic. Both
  front-ends depend on it.
- **`crates/syncbox-cli`** — headless client (`main.rs`, clap subcommands).
- **`src-tauri`** — macOS menu-bar GUI. `src/` holds the plain HTML/CSS/JS window.
- `pair-server/` — Cloudflare Worker, single `worker.ts`, swaps codes for tickets.

### How sync works

Each file is **one entry in an iroh-docs CRDT document**: `key = relative/path`,
content addressed by BLAKE3 via iroh-blobs. iroh-docs syncs entries *and*
auto-downloads their content to every peer. syncbox only mirrors the doc to disk
and back — it never drives the blob downloader itself.

The engine is `sync::run(SyncState, shutdown)` — a `tokio::select!` loop in
`crates/syncbox-core/src/sync.rs`. Both front-ends construct a `SyncState`
(the bag of runtime handles) and call it. Two directions:

- **local → doc**: `notify` watcher (debounced) → `handle_local_change` →
  `upload_file` (`doc.import_file`, streamed). `scan_local` does the same on
  startup.
- **doc → local**: doc `LiveEvent`s → `handle_event` schedules a debounced
  `reconcile_remote`, which walks `single_latest_per_key` and calls
  `write_entry_to_disk` / `apply_remote_delete`.

Key invariants — preserve these when editing `sync.rs`:

- **Subscribe to doc events before the first reconcile.** iroh-docs starts
  syncing the moment the doc opens; subscribing late drops events and files
  silently never land.
- **Echo guard** (`EchoGuard`): after writing/deleting a file to apply a remote
  change, the path is marked so the watcher recognises its own footprint and
  doesn't bounce the change back.
- **Conflicts are last-write-wins by timestamp.** The newer file wins; the
  older copy is overwritten and discarded — no conflict copies are kept.
- **Deletes are tombstones** (empty entry). `reconcile_remote` acts only on the
  CRDT's winning entry, so a stale tombstone can't delete a newer edit and a
  stale edit can't resurrect a deleted file. Do *not* re-add mtime comparison in
  `apply_remote_delete` — a synced file's mtime is its local write time, not the
  content's logical age (that bug resurrected deleted files).
- **Empty folders** have no doc entry (only files do), so they can't sync on
  their own. `scan_local` keeps a `.syncbox-keep` marker file in any subfolder
  with no real content (`sync_keep_marker`); it is an ordinary synced file, and
  it's removed once the folder gains real content.

`peer.rs` — `Node` bundles the long-lived iroh handles (`Endpoint`, `FsStore`,
`Docs`, `Gossip`, `Router`). Keep `Router`/`gossip` alive or the QUIC server stops.

### Pairing

6-char code is a short-lived dead-drop, not a connection. Device A POSTs its
iroh `DocTicket` to the pair server → gets a code (5-min TTL, single use).
Device B redeems the code → gets the ticket → connects directly over iroh.
The server only ever holds the ticket (public metadata, never file content).

Pair-server URL resolution order: `SYNCBOX_PAIR_SERVER` env var → config.json /
GUI Advanced field → compiled-in default. The default is baked from
`pair-server.txt` (repo root, gitignored) by `crates/syncbox-core/build.rs`.

### Config & data

Per device under `~/Library/Application Support/dev.syncbox/` — override with
`SYNCBOX_DATA_DIR` (lets several isolated instances run on one machine for
testing). `config.json` + `iroh/` (secret key, blob store, docs DB).

`config.json` holds a `folders` list — each `FolderConfig` is one synced
folder (local `path` + its own doc `namespace_id`/`doc_ticket`). A device can
sync several; each folder is an independent iroh-docs namespace over the one
shared `Node`. Both front-ends run one `sync::run` loop per folder: the CLI's
`run` over every folder, the GUI via a `FolderRuntime` per folder in
`src-tauri`'s `Inner` (index-aligned with `config.folders`; the window shows
one at a time and switches). `config::load()` migrates pre-0.3.1 single-folder
configs (top-level `folder`/`doc_ticket`/`namespace_id`) into the list on
first load.

## Gotchas

- **iroh is pinned to pre-release `1.0.0-rc.0`** (iroh-docs 0.99, iroh-blobs
  0.101, iroh-gossip 0.99 in `Cargo.toml [workspace.dependencies]`). APIs shift
  between rc releases — bumping any iroh crate likely needs code changes.
- Tauri commands are the `cmd_*` fns in `src-tauri/src/lib.rs`, registered in the
  `generate_handler!` list; the JS calls them via `invoke`.
- Release profile is `panic = "abort"` + LTO — no unwinding, no catch_unwind.
