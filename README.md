# syncbox

A simple, Dropbox-style folder sync for macOS.

Pick one folder. Pair two Macs with a short 6-character code. Files stay in
sync between them automatically. No Dropbox, no Nextcloud, no Syncthing.

## What it does

- Watches one folder on each device.
- When a file is added, changed, or deleted, the change shows up on every
  paired device within a few seconds.
- Transfers go directly between your Macs when possible (same Wi-Fi = fast,
  no internet round-trip).
- Conflicts (same file edited on two devices at once) keep both copies — the
  loser is renamed `name.conflict-<device>-<timestamp>.ext`, never destroyed.
- Deleting a file on one device deletes it on every device. (Exception: if
  another device edited the file *after* the delete, the edit wins and the
  file comes back — last-write-wins.)

## Architecture

```
   Mac A                                          Mac B
┌──────────────────────┐                ┌──────────────────────┐
│  syncbox (menu bar)  │                │  syncbox (menu bar)  │
│                      │                │                      │
│  watch folder        │                │  watch folder        │
│      │               │                │       │              │
│      ▼               │   encrypted    │       ▼              │
│  file ─▶ doc entry   │◀═══ QUIC ══════▶│  doc entry ─▶ file   │
│      │               │  direct / relay │       │              │
│      ▼               │                │       ▼              │
│  manifest (CRDT) ◀───┼─── converges ──┼──▶ manifest (CRDT)    │
└──────────────────────┘                └──────────────────────┘
```

Each file is one doc entry: `key = relative/path`, `value = file bytes`.
The value is content-addressed (BLAKE3). iroh-docs syncs the entry list
*and* downloads the content to every peer automatically; syncbox just
mirrors the doc onto disk and back.

### Source layout

```
syncbox/
├── src-tauri/src/
│   ├── lib.rs              Tauri app, tray icon, UI commands
│   ├── peer.rs             iroh endpoint + protocol wiring
│   ├── sync.rs             watch → publish / receive → write
│   ├── conflict.rs         last-write-wins conflict naming
│   ├── ignore_patterns.rs  .syncboxignore handling
│   └── config.rs           persisted settings
├── src/                    menu bar window UI (HTML/CSS/JS)
└── pair-server/            Cloudflare Worker for 6-char pairing codes
```

## Tech stack — and why

| Piece | What it does | Why this and not something else |
|-------|--------------|----------------------------------|
| **Rust** | The whole client core. | iroh is Rust-native; one static binary; no GC pauses on a background daemon. |
| **Tauri 2** | App shell — menu bar app, native webview UI. | ~6 MB bundle vs ~100 MB for Electron. Rust backend with no IPC bridge to a separate process. First-class macOS tray support. |
| **iroh** | QUIC transport, NAT traversal, peer discovery. | Hole-punching across home routers is genuinely hard. iroh ships it — STUN/relay/direct — so we don't write a NAT-traversal stack. QUIC means encrypted + multiplexed by default. |
| **iroh-docs** | The manifest: a CRDT key-value document. | Two devices editing offline must converge without a server refereeing. A CRDT does that mathematically. Range-based set reconciliation makes a 10k-file sync cheap. |
| **iroh-blobs** | File content storage + transfer. | Content-addressed by BLAKE3 → identical files dedupe for free, and transfers are verifiable and resumable. |
| **iroh-gossip** | Propagates doc changes between peers. | iroh-docs needs a broadcast layer; gossip is the one it's built for. |
| **notify** + **notify-debouncer-full** | Filesystem watching. | Wraps macOS FSEvents. The debouncer collapses editor "write storms" (vim/VS Code save = many events) into one. |
| **ignore** | `.syncboxignore` matching. | Same crate ripgrep uses — battle-tested gitignore semantics, so users already know the syntax. |
| **tokio** | Async runtime. | iroh is built on it; no second runtime. |
| **Cloudflare Worker + KV** | Pairing rendezvous (see below). | A 6-char code needs a tiny always-on dead-drop. A Worker is ~50 lines, free tier, nothing to maintain. |

The headline bet: **iroh does the hard networking**. Without it this project
would need its own relay protocol, NAT traversal, content hashing, and a
sync/merge algorithm. iroh collapses all of that into a dependency.

## How the handshake works

Pairing has two halves: a **rendezvous** (swapping a tiny code for connection
info) and the **iroh connection** itself. The example below uses
`pair.DOMAIN.COM` as the rendezvous host.

### 1. Rendezvous — turning a long ticket into a 6-char code

When Mac A clicks **Get code**:

1. A creates (or reuses) an iroh *document* and produces a **DocTicket** — a
   ~200-character string containing the document's capability (permission to
   read/write the namespace) plus A's node id, relay URL, and direct
   addresses.
2. A `POST`s that ticket to `https://pair.DOMAIN.COM/pair`.
3. The Worker stores the ticket in Cloudflare KV under a random 6-character
   code (`ABC-123`), with a **5-minute TTL**, and returns the code.
4. A shows the code. You read it to whoever is on Mac B.

When Mac B enters the code under **Use code**:

5. B `GET`s `https://pair.DOMAIN.COM/pair/ABC-123`.
6. The Worker returns the ticket **and deletes the KV entry** — the code is
   single-use. A second lookup returns `404`.

```
Mac A ──POST /pair {ticket}──▶  pair.DOMAIN.COM  ──"ABC-123"──▶  Mac A
                                  (KV, 5-min TTL)
Mac B ──GET  /pair/ABC-123──▶   pair.DOMAIN.COM  ──{ticket}───▶  Mac B
                                  (entry deleted)
```

The rendezvous server only ever sees the **ticket** — public connection
metadata. It never sees a single byte of your files, and after the code is
redeemed it holds nothing.

### 2. The iroh connection — actually moving data

7. B calls `docs.import(ticket)`: it joins A's document namespace and starts
   syncing with the node addresses baked into the ticket.
8. iroh dials A. It tries, in order: a **direct** connection (instant on the
   same Wi-Fi), **hole-punching** through both NATs, and finally a **relay**
   (a public n0 server, or your own — see below) if direct fails.
9. The connection is QUIC, encrypted end-to-end with each node's keypair —
   the relay, if used, only forwards ciphertext.
10. Once connected, iroh-docs runs range reconciliation: the two manifests
    exchange just the differences. iroh-blobs then transfers any file content
    the receiving side is missing. syncbox writes those files to disk.
11. A `peer connected` line appears in each app's status; the tray icon goes
    green.

From step 8 on, `pair.DOMAIN.COM` is no longer involved — it was only the
introduction. Re-pairs and reconnects happen peer-to-peer.

## Install

Requires [Rust](https://rustup.rs), [Bun](https://bun.sh), and macOS.

### 1. Deploy the pairing server (once)

```bash
cd pair-server
bun install
bunx wrangler login
bunx wrangler kv namespace create PAIR     # paste the printed id into wrangler.toml
bunx wrangler deploy
```

Before deploying, edit `wrangler.toml` — set the `routes` pattern to your own
`pair.<domain>`. The host is **not** baked into the app; you point each Mac at
it afterwards (see step 3). The deploy + domain setup is covered in detail
under **Self-hosting**.

### 2. Build the app

Optionally, bake your pair-server host in as the default so your builds work
without any per-device setup — write the URL into a gitignored file:

```bash
echo "https://pair.<your-domain>" > syncbox/src-tauri/pair-server.txt
```

`build.rs` reads that file and compiles the URL in. Without the file, builds
fall back to a `pair.example.com` placeholder. The file is in `.gitignore`,
so your domain never lands in the repo.

```bash
cd syncbox
bun install
bun run tauri build
```

The app lands at `src-tauri/target/release/bundle/macos/syncbox.app`.
Copy it (or the `.zip` beside it) to each Mac and drag it to `/Applications`.

> Unsigned build: on first launch macOS Gatekeeper blocks it.
> Right-click the app → **Open** → **Open**.

### 3. Pair two Macs

On each Mac, after launching syncbox (icon in the menu bar):

1. Make sure it points at your pair server. If you baked it in via
   `pair-server.txt` (step 2) there's nothing to do. Otherwise set the
   `SYNCBOX_PAIR_SERVER` environment variable, or open the window →
   **Advanced → Pair server URL**, paste `https://pair.<your-domain>`, **Save**.
2. **Choose folder** to sync.
3. On the first Mac: click **Get code**, read the 6 characters aloud.
4. On the second Mac: type the code into **Use code**.

Drop a file into the folder on either Mac. The tray icon shows state:
gray ring (not set up) → blue disc (syncing) → green check (in sync).

## Self-hosting

syncbox has no central server for your data — only the tiny rendezvous
Worker, and optionally a relay. Both are yours to run.

### The pairing server

`pair-server/` is a Cloudflare Worker. To host it on your own domain:

1. Put your zone (`DOMAIN.COM`) in a Cloudflare account.
2. Edit `pair-server/wrangler.toml` — set the `routes` pattern to
   `pair.DOMAIN.COM` (`custom_domain = true` makes wrangler create the DNS
   record and TLS certificate for you).
3. `bunx wrangler kv namespace create PAIR`, paste the id into `wrangler.toml`.
4. `bunx wrangler deploy`.
5. Point each Mac at it. The host is resolved at runtime, highest priority
   first:
   1. the `SYNCBOX_PAIR_SERVER` environment variable,
   2. the **Advanced → Pair server URL** field (saved in `config.json`),
   3. the compiled-in default — whatever was in `src-tauri/pair-server.txt`
      at build time, or a harmless `pair.example.com` placeholder if that
      file was absent.

   So you have three independent ways to set it: bake it into your own
   builds (`pair-server.txt`), export an env var, or type it in the app.
   The repo itself never contains a real host.

The endpoints are trivial — `POST /pair` and `GET /pair/<code>` — so you can
also reimplement it in anything (a small axum binary, a Flask app) and point
the app at that URL instead. See `pair-server/README.md` for the contract.

### Your own relay (optional)

By default iroh uses **n0's public relay network** for NAT traversal when a
direct connection can't be made. That means a third party (n0) can see
*connection metadata* — node ids and addresses — though never file content.

To remove that dependency, run your own [`iroh-relay`](https://docs.rs/iroh-relay)
— it's a single binary, and a small VPS or an always-on home machine with a
public address is enough. Point the client's endpoint config at it instead of
the n0 preset. Without *any* relay, syncbox still works for peers on the same
network (direct + mDNS) but cross-internet sync behind two NATs will fail.

### Where your data lives

Nowhere but your Macs. Per device, under
`~/Library/Application Support/dev.syncbox/`:

- `config.json` — folder path, device name, doc ticket, known peers
- `iroh/` — the endpoint's secret key, the blob store, the docs database

## Pitfalls & limitations

Read this before trusting it with anything important.

- **MVP, lightly tested.** Treat it as a power tool, not a backup. Keep a
  copy of anything you can't afford to lose.
- **Pre-release dependency.** It's pinned to `iroh 1.0.0-rc.0`. The API
  still moves; upgrades may need code changes.
- **Whole-file transfer.** Editing one byte of a 1 GB file re-uploads the
  whole gigabyte. There's no delta/block-level sync yet. Fine for documents,
  photos, code; bad for huge files edited in place (video projects, VM disks,
  databases).
- **No atomic snapshots.** Sync is file-by-file. An app-managed *bundle*
  (e.g. `Alfred.alfredpreferences`, some `.app`s, SQLite-with-WAL) can be
  captured half-old/half-new if it's written while syncing — the same reason
  Nextcloud/Dropbox corrupt such bundles. Per-file writes are atomic
  (temp + rename), so individual files are never half-written, but the
  *bundle as a whole* has no consistency guarantee.
- **Conflict sidecars.** A conflict creates a `.conflict-…` copy **in the
  synced folder**. Inside an app bundle that extra file can confuse the app.
- **Deletes are real and have no trash.** Deleting a file removes it on every
  device immediately. There is no undo — recover from a backup if you delete
  something by mistake.
- **Last-write-wins.** Simultaneous edits don't merge — newest timestamp
  wins, the other becomes a `.conflict` copy. Clock skew between Macs can
  pick the "wrong" winner.
- **Empty folders don't sync.** Only files are doc entries; a directory with
  no files in it has nothing to represent.
- **No file locking.** syncbox won't notice an app holding a file open.
- **One folder, one peer set.** No multi-folder or selective sync yet.
- **Unsigned app.** No code signing / notarization — Gatekeeper warns on
  first launch, and there are no automatic updates.
- **Blob store is not encrypted at rest.** Content on the wire is encrypted;
  the local `iroh/` directory is plain.

## Develop

```bash
cd syncbox
bun install
bun run tauri dev
```

## License

MIT. See LICENSE.
