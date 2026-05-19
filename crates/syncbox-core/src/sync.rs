//! The sync engine.
//!
//! Model — deliberately simple so iroh does the heavy lifting:
//!
//! * The user picks one folder. Its absolute path is the *root*.
//! * Each file is one doc entry: `key = relative/path`, content addressed
//!   by iroh-blobs. iroh-docs syncs the entry *and* downloads its content
//!   to every peer automatically — we never run the blob downloader.
//! * Local edits → `doc.import_file`; remote edits land via `ContentReady`
//!   and we mirror the doc onto disk. Content is streamed both ways, so
//!   memory use stays constant regardless of file size.
//! * Conflict policy is last-write-wins. If a local file is newer than the
//!   incoming entry, the incoming copy is kept as
//!   `<name>.conflict-<host>-<ts>.<ext>` — nothing is destroyed.
//! * Deletes are tombstones (`doc.del`). An incoming tombstone removes the
//!   local file, unless the local copy was edited after the delete was
//!   issued — then the edit wins and is re-published.

use crate::{conflict, ignore_patterns::IgnoreSet, peer::Node};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use futures_lite::StreamExt;
use iroh::EndpointAddr;
use iroh_blobs::{
    api::blobs::{ExportMode, ImportMode},
    Hash,
};
use iroh_docs::{api::Doc, engine::LiveEvent, store::Query, AuthorId, ContentStatus, Entry};
use notify::{EventKind, RecursiveMode};
use notify_debouncer_full::new_debouncer;
use serde::Serialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tokio::time::Instant;

const DEBOUNCE_MS: u64 = 250;
/// After a content event, wait this long for more before reconciling, so a
/// burst of incoming files collapses into a single disk-reconcile pass.
const RECONCILE_DEBOUNCE_MS: u64 = 400;
/// Safety-net interval: re-run a full reconcile + local scan this often, so a
/// change a dropped watcher or doc event missed still converges in bounded
/// time. Both passes are idempotent and content-hash gated.
const SWEEP_SECS: u64 = 30;

/// Key prefix for reserved entries that carry a device's display name. A real
/// file key is a relative path, which can never start with a NUL byte, so
/// these never collide with synced files.
const NAME_KEY_PREFIX: &[u8] = b"\x00name/";

/// What happened to a path because of a remote action. Stored briefly in the
/// echo guard so the file watcher can recognise its own footprint and avoid
/// bouncing the change back across the network.
#[derive(Debug, Clone)]
pub enum EchoMark {
    /// BLAKE3 of the bytes we just wrote.
    Wrote(Hash),
    /// We just deleted (or moved aside) the path.
    Deleted,
}

pub type EchoGuard = Arc<Mutex<HashMap<PathBuf, EchoMark>>>;

#[derive(Debug, Clone, Serialize)]
pub struct PeerEntry {
    pub online: bool,
    pub last_seen_unix: u64,
}

/// Shared live view of peers we've seen via gossip neighbor events. Keyed by
/// hex-encoded EndpointId. Updated by the sync event loop, consumed by the
/// UI status command.
pub type PeerMap = Arc<Mutex<HashMap<String, PeerEntry>>>;

/// Shared map of endpoint-id (hex) → friendly device name, populated from the
/// reserved name entries peers publish into the doc.
pub type NameMap = Arc<Mutex<HashMap<String, String>>>;

/// Rolling transfer statistics surfaced to the UI.
#[derive(Debug, Clone, Default, Serialize)]
pub struct TransferStats {
    /// Total bytes received from peers since the sync task started.
    pub down_total: u64,
    /// Total bytes published to the doc since the sync task started.
    pub up_total: u64,
    /// Smoothed current receive rate, bytes/second.
    pub down_rate: f64,
    /// Wall-clock of the last rate update, unix milliseconds.
    pub last_update_ms: u64,
    /// Wall-clock of the last byte moved in *either* direction, unix ms.
    /// The tray reads this to animate the "syncing" icon.
    pub last_activity_ms: u64,
    /// Doc entries whose blob content is still downloading from a peer. The
    /// network transfer itself runs inside iroh-blobs, invisible to us — this
    /// counter is how the tray knows a download is in flight. Bumped by a
    /// remote insert whose content isn't local yet, drawn down by
    /// ContentReady events, and zeroed when a sync run drains its queue.
    pub pending_downloads: u32,
}

pub type StatsHandle = Arc<Mutex<TransferStats>>;

/// A short human-readable line describing the most recent sync activity,
/// shown verbatim in the UI ("waiting for peer", "received notes.md", …).
pub type StatusLine = Arc<Mutex<String>>;

/// A bounded ring buffer of recent activity lines, surfaced in the debug
/// panel. Newest entries are pushed to the back.
pub type LogHandle = Arc<Mutex<std::collections::VecDeque<String>>>;

const LOG_CAP: usize = 200;

/// Append a timestamped line to the debug log.
pub async fn log_line(log: &LogHandle, msg: impl AsRef<str>) {
    let secs = now_unix();
    let (h, m, s) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    let line = format!("{h:02}:{m:02}:{s:02}  {}", msg.as_ref());
    let mut buf = log.lock().await;
    buf.push_back(line);
    while buf.len() > LOG_CAP {
        buf.pop_front();
    }
}

#[derive(Clone)]
pub struct SyncState {
    pub node: Arc<Node>,
    pub doc: Doc,
    pub author: AuthorId,
    pub root: PathBuf,
    pub host: String,
    pub echo: EchoGuard,
    pub peers: PeerMap,
    /// Sink for fresh peer addresses; the front-end (GUI or CLI) persists
    /// them to config so the next restart can call `doc.start_sync` directly
    /// instead of waiting on discovery.
    pub addr_sink: tokio::sync::mpsc::UnboundedSender<EndpointAddr>,
    /// Gitignore-style filter loaded from `.syncboxignore` + builtins.
    pub ignores: Arc<IgnoreSet>,
    /// If set, this device receives changes but never propagates its own.
    pub read_only: bool,
    /// Endpoint IDs (hex form, matches `PublicKey::to_string()`) we refuse
    /// to apply changes from. Used by the "revoke device" feature.
    pub blocked: Arc<Mutex<std::collections::HashSet<String>>>,
    /// Counter of in-flight transfers. The tray reads it to show the
    /// "syncing" icon state.
    pub active: Arc<std::sync::atomic::AtomicU32>,
    /// Rolling throughput, surfaced to the UI.
    pub stats: StatsHandle,
    /// Human-readable status line, surfaced to the UI.
    pub status: StatusLine,
    /// Debug log ring buffer, surfaced in the debug panel.
    pub log: LogHandle,
    /// Endpoint-id (hex) → friendly device name, learned from the reserved
    /// name entries peers publish into the doc.
    pub names: NameMap,
}

/// Update the one-line status *and* append it to the debug log.
async fn set_status(state: &SyncState, msg: impl Into<String>) {
    let msg = msg.into();
    tracing::info!(status = %msg, "sync status");
    *state.status.lock().await = msg.clone();
    log_line(&state.log, msg).await;
}

/// Append to the debug log only (used for detail/errors that shouldn't
/// replace the headline status).
async fn note(state: &SyncState, msg: impl AsRef<str>) {
    tracing::debug!(note = %msg.as_ref());
    log_line(&state.log, msg).await;
}

pub async fn run(state: SyncState, shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
    tracing::info!(root = %state.root.display(), "starting sync");
    set_status(&state, "starting…").await;

    // Subscribe BEFORE doing anything else. iroh-docs starts reconciling with
    // peers as soon as the doc is open; if we subscribed later we would miss
    // the InsertRemote / ContentReady events for everything that synced in
    // the gap, and those files would silently never land on disk.
    let mut events = state
        .doc
        .subscribe()
        .await
        .context("subscribe to doc events")?;

    // Pull whatever the doc already knows onto disk, and push whatever is on
    // disk into the doc. Either direction may already be partly done.
    if let Err(e) = reconcile_remote(&state).await {
        tracing::warn!(error = ?e, "initial remote reconcile failed");
    }
    if let Err(e) = scan_local(&state).await {
        tracing::warn!(error = ?e, "initial local scan failed");
    }
    if let Err(e) = publish_device_name(&state).await {
        tracing::warn!(error = ?e, "publish device name failed");
    }
    set_status(&state, "watching for changes").await;

    // File watcher channel (debounced).
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<PathBuf>>();
    let _watcher = spawn_watcher(state.root.clone(), tx)?;

    let mut shutdown = shutdown;
    // When set, a disk reconcile is due at this instant (debounced).
    let mut reconcile_at: Option<Instant> = None;

    // Safety-net sweep: every SWEEP_SECS, re-run a full reconcile + local
    // scan so a change the file watcher or a doc event dropped still
    // converges within a bounded time. The first tick fires immediately —
    // consume it, the startup reconcile + scan above already covered it.
    let mut sweep = tokio::time::interval(Duration::from_secs(SWEEP_SECS));
    sweep.tick().await;

    loop {
        let recon_timer = async {
            match reconcile_at {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            biased;

            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!("sync shutdown requested");
                    break;
                }
            }

            Some(paths) = rx.recv() => {
                for p in paths {
                    if let Err(e) = handle_local_change(&state, &p, false).await {
                        tracing::warn!(path = %p.display(), error = ?e, "local change failed");
                    }
                }
            }

            Some(ev) = events.next() => {
                let Ok(ev) = ev else { continue };
                if handle_event(&state, ev).await {
                    // A content event landed — schedule a debounced reconcile.
                    reconcile_at = Some(
                        Instant::now() + Duration::from_millis(RECONCILE_DEBOUNCE_MS),
                    );
                }
            }

            _ = recon_timer => {
                reconcile_at = None;
                if let Err(e) = reconcile_remote(&state).await {
                    tracing::warn!(error = ?e, "reconcile failed");
                }
            }

            _ = sweep.tick() => {
                if let Err(e) = reconcile_remote(&state).await {
                    tracing::warn!(error = ?e, "periodic reconcile failed");
                }
                if let Err(e) = scan_local(&state).await {
                    tracing::warn!(error = ?e, "periodic scan failed");
                }
            }
        }
    }

    Ok(())
}

fn spawn_watcher(
    root: PathBuf,
    tx: tokio::sync::mpsc::UnboundedSender<Vec<PathBuf>>,
) -> Result<
    notify_debouncer_full::Debouncer<
        notify::RecommendedWatcher,
        notify_debouncer_full::RecommendedCache,
    >,
> {
    let mut debouncer = new_debouncer(
        Duration::from_millis(DEBOUNCE_MS),
        None,
        move |res: notify_debouncer_full::DebounceEventResult| {
            let Ok(events) = res else { return };
            let mut paths: Vec<PathBuf> = Vec::new();
            for ev in events {
                if !matches!(
                    ev.event.kind,
                    EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
                ) {
                    continue;
                }
                for p in &ev.event.paths {
                    paths.push(p.clone());
                }
            }
            if !paths.is_empty() {
                let _ = tx.send(paths);
            }
        },
    )?;
    debouncer.watch(&root, RecursiveMode::Recursive)?;
    Ok(debouncer)
}

// ---------- local → doc ----------

async fn handle_local_change(state: &SyncState, path: &Path, from_scan: bool) -> Result<()> {
    if state.read_only {
        return Ok(());
    }
    let rel = match path.strip_prefix(&state.root) {
        Ok(r) => r.to_path_buf(),
        Err(_) => return Ok(()),
    };
    if should_skip(&rel, &state.ignores, path.is_dir()) {
        return Ok(());
    }
    let key = rel_to_key(&rel);

    if !path.exists() {
        // Path gone. If we removed it ourselves applying a remote tombstone,
        // swallow the event. Otherwise propagate our own tombstone.
        {
            let mut guard = state.echo.lock().await;
            if matches!(guard.get(path), Some(EchoMark::Deleted)) {
                guard.remove(path);
                return Ok(());
            }
        }
        let removed = delete_from_doc(state, &key).await;
        if removed > 0 {
            tracing::info!(path = %rel.display(), removed, "propagated local delete");
            set_status(state, format!("deleted {}", rel.display())).await;
        }
        return Ok(());
    }

    let meta = tokio::fs::metadata(path).await?;
    if meta.is_dir() {
        // A directory was created or moved into the folder (e.g. dragged in
        // via Finder). The watcher often reports only the directory, not the
        // files inside it — walk the subtree and publish every file.
        upload_subtree(state, path, from_scan).await?;
        return Ok(());
    }
    if !meta.is_file() {
        return Ok(());
    }
    upload_file(state, path, from_scan).await
}

/// Tombstone a removed path in the doc. A removed file tombstones its own
/// key; a removed directory tombstones every file key beneath it.
///
/// We `doc.del` each *exact* key rather than relying on `doc.del`'s prefix
/// form, because iroh-docs' prefix delete only clears entries authored by
/// *this* device (`remove_prefix_filtered` is author-scoped). Files an earlier
/// session — or the other peer — published live under a different author and
/// would survive a directory-level delete, resurrecting the folder. An
/// exact-key tombstone, by contrast, wins by timestamp across every author.
async fn delete_from_doc(state: &SyncState, key: &Bytes) -> usize {
    let kb: &[u8] = key.as_ref();
    let mut victims: Vec<Bytes> = Vec::new();

    // Every non-empty doc key at or below this path. key_prefix also matches
    // sibling names ("X" matches "Xyz"), so keep only the exact path and real
    // children ("X/...").
    match state
        .doc
        .get_many(Query::single_latest_per_key().key_prefix(key.clone()))
        .await
    {
        Ok(stream) => {
            tokio::pin!(stream);
            while let Some(entry) = stream.next().await {
                let Ok(entry) = entry else { continue };
                if entry.is_empty() {
                    continue;
                }
                let k = entry.key();
                if k == kb || (k.len() > kb.len() && k.starts_with(kb) && k[kb.len()] == b'/') {
                    victims.push(Bytes::copy_from_slice(k));
                }
            }
        }
        Err(e) => tracing::warn!(error = ?e, "enumerate keys to delete failed"),
    }
    // Always tombstone the path itself too. For a directory this is the
    // folder-level tombstone the startup scan checks before re-publishing a
    // child (see `under_deleted_dir`); for a file it covers an entry the
    // enumeration missed.
    if !victims.iter().any(|v| v == key) {
        victims.push(key.clone());
    }

    let mut removed = 0;
    for k in victims {
        match state.doc.del(state.author, k).await {
            Ok(_) => removed += 1,
            Err(e) => {
                tracing::warn!(error = ?e, "doc.del failed");
                note(state, format!("error deleting: {e}")).await;
            }
        }
    }
    removed
}

/// True if any ancestor directory of `key` carries a tombstone — i.e. a parent
/// folder was deleted. Lets the startup scan tell a file whose folder is gone
/// from a genuinely new one, even when the child has no doc entry of its own.
async fn under_deleted_dir(doc: &Doc, key: &[u8]) -> bool {
    let Ok(s) = std::str::from_utf8(key) else {
        return false;
    };
    let mut idx = 0;
    while let Some(pos) = s[idx..].find('/') {
        let cut = idx + pos;
        let ancestor = &s[..cut];
        if let Ok(Some(e)) = doc
            .get_one(
                Query::single_latest_per_key()
                    .key_exact(ancestor.as_bytes())
                    .include_empty(),
            )
            .await
        {
            if e.is_empty() {
                return true;
            }
        }
        idx = cut + 1;
    }
    false
}

/// Publish a single file to the doc. Skips ignored paths, echoes of our own
/// writes, and content already present in the doc. The file is streamed into
/// the blob store, so memory use is constant regardless of file size.
async fn upload_file(state: &SyncState, path: &Path, from_scan: bool) -> Result<()> {
    let rel = match path.strip_prefix(&state.root) {
        Ok(r) => r.to_path_buf(),
        Err(_) => return Ok(()),
    };
    if should_skip(&rel, &state.ignores, false) {
        return Ok(());
    }
    let key = rel_to_key(&rel);

    // Hash the file with constant memory to decide whether it's worth
    // publishing. This is a separate read from the import below, but it lets
    // us skip the (more expensive) import entirely for echoes and no-ops.
    let want = hash_file(path).await?;

    // Echo guard: if the file already matches what we wrote applying a
    // remote change, this event is our own footprint — skip.
    {
        let guard = state.echo.lock().await;
        if let Some(EchoMark::Wrote(known)) = guard.get(path) {
            if *known == want {
                return Ok(());
            }
        }
    }

    // Look up the current winning doc entry for this key, tombstone included.
    if let Ok(Some(entry)) = state
        .doc
        .get_one(
            Query::single_latest_per_key()
                .key_exact(key.clone())
                .include_empty(),
        )
        .await
    {
        if entry.is_empty() {
            // The doc says this path was deleted. During the startup scan a
            // file still on disk is a leftover the pending reconcile is about
            // to remove; re-publishing it would out-timestamp the tombstone
            // and resurrect the file on every peer. Skip it. A live watcher
            // event means the user re-created the file — fall through.
            if from_scan {
                return Ok(());
            }
        } else if entry.content_hash() == want {
            // Already in the doc with the same content — nothing to do.
            return Ok(());
        }
    }

    // A directory delete tombstones the folder key and clears its children
    // from the doc, so a child left on disk has no entry of its own and the
    // check above sees nothing. During the startup scan, treat a file whose
    // folder was deleted as deleted too — don't resurrect the folder.
    if from_scan && under_deleted_dir(&state.doc, key.as_ref()).await {
        return Ok(());
    }

    let _busy = ActiveGuard::new(&state.active);
    // import_file streams the file into the blob store and sets the doc
    // entry in one step. ImportMode::Copy (never TryReference): the blob
    // store must own its bytes, since this file stays live and mutable.
    let outcome = state
        .doc
        .import_file(&state.node.store, state.author, key, path, ImportMode::Copy)
        .await
        .context("import_file into doc")?
        .await
        .context("import_file into doc")?;

    {
        let mut s = state.stats.lock().await;
        s.up_total = s.up_total.saturating_add(outcome.size);
        s.last_activity_ms = now_unix_ms();
    }
    tracing::info!(path = %rel.display(), bytes = outcome.size, "published local change");
    set_status(state, format!("sent {}", rel.display())).await;
    Ok(())
}

/// Walk a directory subtree and publish every file inside it.
async fn upload_subtree(state: &SyncState, dir: &Path, from_scan: bool) -> Result<()> {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        if let Ok(rel) = d.strip_prefix(&state.root) {
            if !rel.as_os_str().is_empty() && state.ignores.is_ignored(rel, true) {
                continue;
            }
        }
        let mut rd = match tokio::fs::read_dir(&d).await {
            Ok(rd) => rd,
            Err(_) => continue,
        };
        while let Some(ent) = rd.next_entry().await? {
            let p = ent.path();
            let ft = ent.file_type().await?;
            if ft.is_dir() {
                stack.push(p);
            } else if ft.is_file() {
                if let Err(e) = upload_file(state, &p, from_scan).await {
                    tracing::warn!(path = %p.display(), error = ?e, "subtree upload failed");
                }
            }
        }
    }
    Ok(())
}

/// Walk the folder and publish anything the doc doesn't already have.
async fn scan_local(state: &SyncState) -> Result<()> {
    if state.read_only {
        return Ok(());
    }
    let mut stack = vec![state.root.clone()];
    while let Some(dir) = stack.pop() {
        if let Ok(rel) = dir.strip_prefix(&state.root) {
            if !rel.as_os_str().is_empty() && state.ignores.is_ignored(rel, true) {
                continue;
            }
        }
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(_) => continue,
        };
        while let Some(ent) = rd.next_entry().await? {
            let p = ent.path();
            let ft = ent.file_type().await?;
            if ft.is_dir() {
                stack.push(p);
            } else if ft.is_file() {
                if let Err(e) = handle_local_change(state, &p, true).await {
                    tracing::warn!(path = %p.display(), error = ?e, "scan upload failed");
                }
            }
        }
    }
    Ok(())
}

// ---------- doc → local ----------

/// True if the event was content-related and a reconcile should be scheduled.
async fn handle_event(state: &SyncState, ev: LiveEvent) -> bool {
    match ev {
        LiveEvent::InsertRemote {
            from,
            entry,
            content_status,
        } => {
            {
                let blocked = state.blocked.lock().await;
                if blocked.contains(&from.to_string()) {
                    tracing::debug!(from = %from, "ignoring change from blocked peer");
                    return false;
                }
            }
            // A non-empty entry whose blob isn't here yet means iroh-blobs has
            // a download queued — count it so the tray shows the syncing icon
            // until the matching ContentReady lands.
            if !entry.is_empty() && content_status != ContentStatus::Complete {
                let mut s = state.stats.lock().await;
                s.pending_downloads = s.pending_downloads.saturating_add(1);
                s.last_activity_ms = now_unix_ms();
            }
            // Content entry or tombstone — let the debounced reconcile apply
            // it. reconcile_remote walks single_latest_per_key, so it always
            // acts on the CRDT's winning entry: a stale tombstone can't delete
            // a newer edit, nor a stale edit resurrect a deleted file.
            true
        }
        LiveEvent::ContentReady { .. } => {
            let mut s = state.stats.lock().await;
            s.pending_downloads = s.pending_downloads.saturating_sub(1);
            s.last_activity_ms = now_unix_ms();
            true
        }
        LiveEvent::PendingContentReady => {
            // The sync run drained its download queue: every queued blob has
            // completed or failed. Clear any count left standing by drift.
            state.stats.lock().await.pending_downloads = 0;
            true
        }
        LiveEvent::SyncFinished(_) => true,
        LiveEvent::NeighborUp(pk) => {
            {
                let mut peers = state.peers.lock().await;
                peers.insert(
                    pk.to_string(),
                    PeerEntry {
                        online: true,
                        last_seen_unix: now_unix(),
                    },
                );
            }
            set_status(state, "peer connected").await;
            if let Some(info) = state.node.endpoint.remote_info(pk).await {
                let addr =
                    EndpointAddr::from_parts(info.id(), info.into_addrs().map(|a| a.into_addr()));
                let _ = state.addr_sink.send(addr);
            }
            // A newly-connected peer may already have entries — reconcile.
            true
        }
        LiveEvent::NeighborDown(pk) => {
            let mut peers = state.peers.lock().await;
            if let Some(e) = peers.get_mut(&pk.to_string()) {
                e.online = false;
                e.last_seen_unix = now_unix();
            }
            false
        }
        LiveEvent::InsertLocal { .. } => false,
    }
}

/// Mirror the doc onto disk: for every entry whose content is already local,
/// write the file if disk doesn't already match. Idempotent — safe to run
/// repeatedly. Entries whose content hasn't downloaded yet are skipped; a
/// later ContentReady will trigger another pass.
async fn reconcile_remote(state: &SyncState) -> Result<()> {
    // include_empty() is load-bearing: a tombstone is an empty entry, and
    // single_latest_per_key drops empty entries unless asked to keep them.
    // Without it, remote deletes never reach apply_remote_delete below and
    // deleted files linger forever on the receiving device.
    let stream = state
        .doc
        .get_many(Query::single_latest_per_key().include_empty())
        .await
        .context("doc.get_many")?;
    tokio::pin!(stream);

    let mut wrote = 0u32;
    while let Some(entry) = stream.next().await {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(error = ?e, "bad entry in reconcile");
                continue;
            }
        };
        // Reserved name entries are device metadata, not files — record the
        // name and never mirror them to disk.
        if entry.key().starts_with(NAME_KEY_PREFIX) {
            record_device_name(state, &entry).await;
            continue;
        }
        if entry.is_empty() {
            // Tombstone — make sure the file is gone locally.
            if let Err(e) = apply_remote_delete(state, entry.key()).await {
                tracing::warn!(error = ?e, "reconcile delete failed");
            }
            continue;
        }
        match write_entry_to_disk(state, &entry).await {
            Ok(true) => wrote += 1,
            Ok(false) => {}
            Err(e) => tracing::warn!(error = ?e, "reconcile write failed"),
        }
    }
    if wrote > 0 {
        set_status(state, format!("received {wrote} file(s)")).await;
    }
    Ok(())
}

/// Publish a device's display name into the doc under its reserved key, so
/// peers show a friendly name instead of a hex id. Idempotent: skips the write
/// when the doc already holds this exact name, to avoid churn. Also records
/// the name in the shared name map. Called at startup and whenever the user
/// renames the device in the GUI.
pub async fn publish_name(
    doc: &Doc,
    author: AuthorId,
    node: &Node,
    names: &NameMap,
    name: &str,
) -> Result<()> {
    let id = node.endpoint.id().to_string();
    let mut key = NAME_KEY_PREFIX.to_vec();
    key.extend_from_slice(id.as_bytes());
    let key = Bytes::from(key);

    if let Ok(Some(entry)) = doc.get_one(Query::key_exact(key.clone())).await {
        if !entry.is_empty() {
            if let Ok(cur) = node.store.blobs().get_bytes(entry.content_hash()).await {
                if cur.as_ref() == name.as_bytes() {
                    names.lock().await.insert(id, name.to_string());
                    return Ok(());
                }
            }
        }
    }
    doc.set_bytes(author, key, Bytes::from(name.as_bytes().to_vec()))
        .await
        .context("publish device name")?;
    names.lock().await.insert(id, name.to_string());
    Ok(())
}

/// Publish this running engine's configured display name. Thin wrapper over
/// [`publish_name`] for the startup call.
async fn publish_device_name(state: &SyncState) -> Result<()> {
    publish_name(
        &state.doc,
        state.author,
        &state.node,
        &state.names,
        &state.host,
    )
    .await
}

/// Record the device name carried by a reserved name entry into the shared
/// name map. A not-yet-downloaded blob is ignored — a later reconcile retries.
async fn record_device_name(state: &SyncState, entry: &Entry) {
    let Some(id) = entry
        .key()
        .strip_prefix(NAME_KEY_PREFIX)
        .and_then(|b| std::str::from_utf8(b).ok())
    else {
        return;
    };
    if entry.is_empty() {
        state.names.lock().await.remove(id);
        return;
    }
    if let Ok(bytes) = state
        .node
        .store
        .blobs()
        .get_bytes(entry.content_hash())
        .await
    {
        if let Ok(name) = std::str::from_utf8(&bytes) {
            state
                .names
                .lock()
                .await
                .insert(id.to_string(), name.to_string());
        }
    }
}

/// Write one doc entry to disk. Returns Ok(true) if a file was written,
/// Ok(false) if nothing needed doing (content not local yet, or disk already
/// matches). The blob is streamed to disk, so memory use is constant
/// regardless of file size.
async fn write_entry_to_disk(state: &SyncState, entry: &Entry) -> Result<bool> {
    let rel = key_to_rel(entry.key())?;
    if should_skip(&rel, &state.ignores, false) {
        return Ok(false);
    }
    let abs = state.root.join(&rel);
    let content_hash = entry.content_hash();

    // Content must already be local. iroh-docs downloads it on its own; if
    // it isn't here yet, bail and let a later ContentReady retry.
    if !state
        .node
        .store
        .blobs()
        .has(content_hash)
        .await
        .unwrap_or(false)
    {
        return Ok(false);
    }

    // Disk already matches the doc? Nothing to do. Hashed with constant
    // memory; errors (e.g. file missing) just fall through to the write.
    if let Ok(local) = hash_file(&abs).await {
        if local == content_hash {
            return Ok(false);
        }
    }

    let _busy = ActiveGuard::new(&state.active);

    // Last-write-wins. entry timestamps are unix microseconds.
    let entry_ms = entry.timestamp() / 1000;
    let local_mtime = match tokio::fs::metadata(&abs).await {
        Ok(m) => Some(mtime_unix_ms(&m)),
        Err(_) => None,
    };
    let write_to = match local_mtime {
        None => abs.clone(),
        Some(local_ms) if local_ms <= entry_ms => abs.clone(),
        // Local file is newer than the incoming entry — keep both. The
        // incoming copy is set aside under a conflict name.
        Some(_) => conflict::conflict_path(&abs, &state.host),
    };

    if let Some(parent) = write_to.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    // Atomic write: stream the blob into a temp file, then rename. Clear any
    // stale temp file left by a previous interrupted write first.
    let tmp = with_partial_ext(&write_to);
    tokio::fs::remove_file(&tmp).await.ok();
    let size = state
        .doc
        .export_file(&state.node.store, entry.clone(), &tmp, ExportMode::Copy)
        .await
        .context("export_file from doc")?
        .await
        .context("export_file from doc")?;
    tokio::fs::rename(&tmp, &write_to)
        .await
        .context("rename temp")?;

    {
        let mut guard = state.echo.lock().await;
        guard.insert(write_to.clone(), EchoMark::Wrote(content_hash));
    }
    {
        let mut s = state.stats.lock().await;
        let n = size;
        let now = now_unix_ms();
        let dt = (now.saturating_sub(s.last_update_ms)) as f64 / 1000.0;
        if dt > 0.0 && s.last_update_ms != 0 {
            let inst = n as f64 / dt;
            s.down_rate = if s.down_rate == 0.0 {
                inst
            } else {
                0.6 * s.down_rate + 0.4 * inst
            };
        }
        s.down_total = s.down_total.saturating_add(n);
        s.last_update_ms = now;
        s.last_activity_ms = now;
    }
    tracing::info!(path = %write_to.display(), "wrote remote file");
    Ok(true)
}

/// React to a tombstone: a delete on one device removes the file everywhere.
///
/// This runs only when the tombstone is the latest entry for its key — i.e.
/// iroh-docs' CRDT has already decided the delete wins over any concurrent
/// edit. We trust that decision and remove the file. We do *not* compare
/// mtimes to second-guess it: a received file's mtime is its local write
/// time, not the content's logical age, so that comparison wrongly judged
/// freshly-synced files "newer than the delete" and resurrected them.
async fn apply_remote_delete(state: &SyncState, key: &[u8]) -> Result<()> {
    let rel = key_to_rel(key)?;
    if should_skip(&rel, &state.ignores, false) {
        return Ok(());
    }
    let abs = state.root.join(&rel);

    match tokio::fs::metadata(&abs).await {
        Ok(m) if m.is_file() => {}
        _ => return Ok(()), // already gone, or not a regular file
    }

    // Mark the path so the watcher swallows the Remove event our own
    // deletion is about to produce (otherwise we'd re-propagate the delete).
    {
        let mut guard = state.echo.lock().await;
        guard.insert(abs.clone(), EchoMark::Deleted);
    }
    tokio::fs::remove_file(&abs).await?;
    prune_empty_dirs(&state.root, &abs).await;
    tracing::info!(path = %rel.display(), "applied remote delete");
    set_status(state, format!("removed {}", rel.display())).await;
    Ok(())
}

/// After removing `removed`, delete any parent directories it left empty,
/// climbing toward `root`. Stops at `root` (never removed) or the first
/// directory that still holds something. Without this a remotely-deleted
/// folder lingers as an empty husk on the receiving device.
async fn prune_empty_dirs(root: &Path, removed: &Path) {
    let mut dir = removed.parent();
    while let Some(d) = dir {
        if d == root || !d.starts_with(root) {
            break;
        }
        if !remove_dir_if_empty(d).await {
            break;
        }
        dir = d.parent();
    }
}

/// Remove `dir` if it holds nothing the user would miss — i.e. it's empty, or
/// the only things left are OS-generated metadata files (`.DS_Store` and
/// friends). Returns true if the directory was removed.
///
/// Plain `remove_dir` fails on a `.DS_Store`-only folder: macOS drops one into
/// any directory opened in Finder, so a folder that looks empty to the user
/// isn't empty to the filesystem. We never delete a real file the user keeps
/// here, even one sync ignores — only known OS junk.
async fn remove_dir_if_empty(dir: &Path) -> bool {
    if tokio::fs::remove_dir(dir).await.is_ok() {
        return true;
    }
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => return false,
    };
    let mut junk = Vec::new();
    loop {
        match rd.next_entry().await {
            Ok(Some(ent)) => {
                if is_os_junk(&ent.file_name()) {
                    junk.push(ent.path());
                } else {
                    return false; // a real entry — leave the directory alone
                }
            }
            Ok(None) => break,
            Err(_) => return false,
        }
    }
    for f in junk {
        if tokio::fs::remove_file(&f).await.is_err() {
            return false;
        }
    }
    tokio::fs::remove_dir(dir).await.is_ok()
}

/// macOS and Windows scatter these metadata files into folders. They're never
/// synced and the user never created them, so a directory holding only these
/// is safe to remove.
fn is_os_junk(name: &std::ffi::OsStr) -> bool {
    let n = name.to_string_lossy();
    n == ".DS_Store" || n == ".localized" || n == "Thumbs.db" || n.starts_with("._")
}

// ---------- helpers ----------

fn with_partial_ext(p: &Path) -> PathBuf {
    let mut name = p
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "tmp".into());
    name.push_str(".syncbox-partial");
    p.with_file_name(name)
}

/// BLAKE3-hash a file with constant memory (chunked read). iroh-blobs hashes
/// a raw blob the same way, so the result compares directly against doc entry
/// content hashes.
async fn hash_file(path: &Path) -> Result<Hash> {
    use tokio::io::AsyncReadExt;
    let mut f = tokio::fs::File::open(path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(Hash::from_bytes(*hasher.finalize().as_bytes()))
}

/// RAII bump of the in-flight transfer counter; the tray reads it to show
/// the "syncing" icon state.
struct ActiveGuard<'a> {
    counter: &'a std::sync::atomic::AtomicU32,
}

impl<'a> ActiveGuard<'a> {
    fn new(counter: &'a std::sync::atomic::AtomicU32) -> Self {
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for ActiveGuard<'_> {
    fn drop(&mut self) {
        self.counter
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

fn should_skip(rel: &Path, ignores: &IgnoreSet, is_dir: bool) -> bool {
    if rel.as_os_str().is_empty() {
        return true;
    }
    if rel.components().any(|c| {
        c.as_os_str()
            .to_string_lossy()
            .ends_with(".syncbox-partial")
    }) {
        return true;
    }
    ignores.is_ignored(rel, is_dir)
}

fn rel_to_key(rel: &Path) -> Bytes {
    let s = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/");
    Bytes::from(s.into_bytes())
}

fn key_to_rel(key: &[u8]) -> Result<PathBuf> {
    let s = std::str::from_utf8(key).context("key not utf8")?;
    let mut p = PathBuf::new();
    for seg in s.split('/') {
        if seg.is_empty() || seg == ".." {
            bail!("invalid key segment");
        }
        p.push(seg);
    }
    Ok(p)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn mtime_unix_ms(m: &std::fs::Metadata) -> u64 {
    m.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
        .unwrap_or_else(now_unix_ms)
}
