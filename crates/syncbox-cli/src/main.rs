//! syncbox — headless command-line client.
//!
//! Same sync engine as the macOS GUI (`syncbox-core`), no window. Built for
//! Linux servers and headless machines: pair once, then run `syncbox run`
//! under systemd.
//!
//! A device can sync several folders at once. Each folder is one iroh-docs
//! namespace; `syncbox run` drives a sync loop for every folder over one
//! shared iroh endpoint.
//!
//! Typical flow:
//!   device A:  syncbox init ~/Sync && syncbox pair       -> prints a code
//!   device B:  syncbox join ABC-123 ~/Sync
//!   both:      syncbox run                               (or via systemd)

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use futures_lite::StreamExt;
use iroh::EndpointAddr;
use iroh_docs::{api::Doc, store::Query, DocTicket, NamespaceId};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{atomic::AtomicU32, Arc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use syncbox_core::{
    config,
    ignore_patterns::IgnoreSet,
    pair,
    peer::{self, Node},
    sync::{self, SyncState},
};
use tokio::sync::{mpsc, watch, Mutex};

/// GC loop tick for `syncbox gc` — short, but long enough that docs is up and
/// the protect callback can answer before the first sweep.
const GC_FORCED_INTERVAL: Duration = Duration::from_secs(5);
/// How often `syncbox gc` re-measures the store.
const GC_POLL: Duration = Duration::from_secs(1);
/// Steady readings needed before the sweep is called done.
const GC_SETTLE_POLLS: u32 = 4;
/// Hard stop, however large the store.
const GC_TIMEOUT: Duration = Duration::from_secs(900);
/// Where iroh-blobs keeps blobs too big to inline into `blobs.db`.
const BLOB_DATA_DIR: &str = "data";
/// How long to let the endpoint wind down before `syncbox gc` quits anyway.
const GC_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

#[derive(Parser)]
#[command(
    name = "syncbox",
    version,
    about = "Folder sync over iroh — headless client"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Start syncing a local folder (creates it if missing).
    Init {
        /// Path to the folder.
        folder: PathBuf,
    },
    /// Create a pairing code other devices can use to join one of your folders.
    Pair {
        /// Share read-only: joiners receive changes but can't push their own.
        #[arg(long)]
        read_only: bool,
        /// Which folder to share, by path. Optional when only one is synced.
        folder: Option<PathBuf>,
    },
    /// Join a folder shared from another device: redeem its code into a path.
    Join {
        /// The 6-character code, e.g. ABC-123.
        code: String,
        /// Local folder to sync the shared content into (created if missing).
        folder: PathBuf,
    },
    /// Run the sync engine for every folder in the foreground (use under systemd).
    Run,
    /// Show configuration and pairing state for every folder.
    Status,
    /// List the synced folders.
    List,
    /// Stop syncing a folder. Local files are left in place.
    Remove {
        /// Path of the folder to drop.
        folder: PathBuf,
    },
    /// Dump every entry in a folder's doc (diagnostics).
    Dump {
        /// Which folder, by path. Optional when only one is synced.
        folder: Option<PathBuf>,
    },
    /// Sweep blob-store copies of files that no folder needs any more.
    ///
    /// The engine does this every 30 minutes anyway; this forces one now.
    /// Stop `syncbox run` (and quit the app) first — the store is single-writer.
    Gc,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Init { folder } => do_init(folder).await,
        Cmd::Pair { read_only, folder } => do_pair(read_only, folder).await,
        Cmd::Join { code, folder } => do_join(code, folder).await,
        Cmd::Run => {
            init_tracing();
            run_sync().await
        }
        Cmd::Status => do_status().await,
        Cmd::List => do_list().await,
        Cmd::Remove { folder } => do_remove(folder).await,
        Cmd::Dump { folder } => do_dump(folder).await,
        Cmd::Gc => do_gc().await,
    }
}

// ---------- subcommands ----------

async fn do_init(folder: PathBuf) -> Result<()> {
    std::fs::create_dir_all(&folder)
        .with_context(|| format!("create folder {}", folder.display()))?;
    let folder = folder
        .canonicalize()
        .context("resolve the folder's absolute path")?;
    let mut cfg = config::load().await?;
    if cfg
        .folders
        .iter()
        .any(|f| f.path.as_deref() == Some(folder.as_path()))
    {
        println!("Already syncing: {}", folder.display());
        return Ok(());
    }
    cfg.folders
        .push(config::FolderConfig::for_path(folder.clone()));
    config::save(&cfg).await?;
    println!("Now syncing: {}", folder.display());
    println!("Next:  syncbox pair   (to add another device)");
    Ok(())
}

async fn do_pair(read_only: bool, folder: Option<PathBuf>) -> Result<()> {
    let mut cfg = config::load().await?;
    let idx = resolve_folder(&cfg, folder.as_deref())?;
    let node = spawn_node().await?;

    // Reuse the folder's existing doc if it has one; otherwise create it.
    let doc = match open_doc(&node, &mut cfg.folders[idx]).await? {
        Some(d) => d,
        None => node.docs.create().await.context("create doc")?,
    };
    let (mode, opts) = peer::share_opts(read_only);
    let ticket = doc.share(mode, opts).await.context("share doc")?;
    let ticket_str = ticket.to_string();

    cfg.folders[idx].namespace_id = Some(doc.id().to_string());
    cfg.folders[idx].doc_ticket = Some(ticket_str.clone());
    config::save(&cfg).await?;

    let server = pair::resolve_server(cfg.pair_server_url.as_deref());
    let pc = pair::create_code(&server, &ticket_str)
        .await
        .context("publish pairing code")?;

    let mins = pc.expires_unix.saturating_sub(now_unix()) / 60;
    println!("Pairing code:  {}", pc.code);
    println!("Expires in:    {mins} min");
    println!();
    println!("On the other device:  syncbox join {} <folder>", pc.code);
    Ok(())
}

async fn do_join(code: String, folder: PathBuf) -> Result<()> {
    std::fs::create_dir_all(&folder)
        .with_context(|| format!("create folder {}", folder.display()))?;
    let folder = folder
        .canonicalize()
        .context("resolve the folder's absolute path")?;

    let mut cfg = config::load().await?;
    let server = pair::resolve_server(cfg.pair_server_url.as_deref());
    let ticket_str = pair::redeem_code(&server, &code)
        .await
        .context("redeem pairing code")?;

    let node = spawn_node().await?;
    let ticket =
        DocTicket::from_str(ticket_str.trim()).context("pair server returned an invalid ticket")?;
    let doc = node.docs.import(ticket).await.context("import doc")?;
    let namespace = doc.id().to_string();
    let ticket_str = ticket_str.trim().to_string();

    // If this namespace is already in the config (re-joining), just attach the
    // local path; otherwise add it as a new folder.
    match cfg
        .folders
        .iter_mut()
        .find(|f| f.namespace_id.as_deref() == Some(namespace.as_str()))
    {
        Some(f) => {
            f.path = Some(folder.clone());
            f.doc_ticket = Some(ticket_str);
            println!(
                "Already had that folder — set its local path to {}",
                folder.display()
            );
        }
        None => {
            cfg.folders.push(config::FolderConfig {
                path: Some(folder.clone()),
                doc_ticket: Some(ticket_str),
                namespace_id: Some(namespace),
                read_only: false,
            });
            println!("Joined shared folder → {}", folder.display());
        }
    }
    config::save(&cfg).await?;
    println!("Next:  syncbox run");
    Ok(())
}

async fn run_sync() -> Result<()> {
    let mut cfg = config::load().await?;
    if cfg.folders.is_empty() {
        bail!("no folders — run `syncbox init <folder>` first");
    }

    let node = spawn_node().await?;
    // Stable doc author, persisted by iroh-docs across restarts. Must not
    // rotate: doc.del's prefix delete is author-scoped, so a fresh author each
    // run cannot delete folders an earlier run published. Shared by all folders.
    let author = node.docs.author_default().await.context("default author")?;

    // One address sink for every folder; the persister below dedupes new peer
    // addresses into the device-global known_peers list.
    let (addr_tx, mut addr_rx) = mpsc::unbounded_channel::<EndpointAddr>();
    let known = cfg.known_peers.clone();

    let mut shutdowns: Vec<watch::Sender<bool>> = Vec::new();
    let mut handles = Vec::new();

    for idx in 0..cfg.folders.len() {
        let folder_path = match cfg.folders[idx].path.clone() {
            Some(p) if p.is_dir() => p,
            Some(p) => {
                tracing::warn!(folder = %p.display(), "folder missing on disk, skipping");
                continue;
            }
            None => {
                tracing::warn!(index = idx, "folder has no local path, skipping");
                continue;
            }
        };

        let doc = match open_doc(&node, &mut cfg.folders[idx]).await? {
            Some(d) => d,
            None => {
                tracing::warn!(folder = %folder_path.display(), "folder not paired, skipping");
                continue;
            }
        };

        // Connect to peers we knew before plus the nodes in this folder's
        // ticket (the only source right after a fresh `join`).
        let mut peers = known.clone();
        if let Some(t) = &cfg.folders[idx].doc_ticket {
            if let Ok(ticket) = DocTicket::from_str(t) {
                for addr in ticket.nodes {
                    if !peers.iter().any(|p| p.id == addr.id) {
                        peers.push(addr);
                    }
                }
            }
        }
        if !peers.is_empty() {
            let n = peers.len();
            match doc.start_sync(peers).await {
                Ok(()) => {
                    tracing::info!(count = n, folder = %folder_path.display(), "started sync")
                }
                Err(e) => tracing::warn!(error = ?e, "start_sync failed"),
            }
        }

        let ignores = Arc::new(IgnoreSet::load(&folder_path).context("load ignore set")?);
        let read_only = cfg.read_only_local || cfg.folders[idx].read_only;

        let st = SyncState {
            node: node.clone(),
            doc,
            author,
            root: folder_path.clone(),
            host: cfg.display_name(),
            echo: Arc::new(Mutex::new(HashMap::new())),
            peers: Arc::new(Mutex::new(HashMap::new())),
            addr_sink: addr_tx.clone(),
            ignores,
            read_only,
            blocked: Arc::new(Mutex::new(cfg.blocked_peers.iter().cloned().collect())),
            active: Arc::new(AtomicU32::new(0)),
            stats: Arc::new(Mutex::new(Default::default())),
            status: Arc::new(Mutex::new(String::new())),
            log: Arc::new(Mutex::new(Default::default())),
            names: Arc::new(Mutex::new(HashMap::new())),
            fp_cache: Default::default(),
            reconcile_notify: Arc::new(tokio::sync::Notify::new()),
            dl_inflight: Default::default(),
        };
        let (tx, rx) = watch::channel(false);
        shutdowns.push(tx);
        handles.push(tokio::spawn(async move { sync::run(st, rx).await }));
        tracing::info!(folder = %folder_path.display(), "syncing");
    }

    // Drop our own sender; the per-folder clones in each SyncState keep the
    // channel open until every sync loop has stopped.
    drop(addr_tx);

    if handles.is_empty() {
        bail!("no folder ready to sync — check `syncbox status`");
    }
    // open_doc may have discovered namespace ids via import; persist them.
    config::save(&cfg).await?;

    // Persist freshly-seen peer addresses so the next restart can reconnect
    // without waiting on discovery.
    tokio::spawn(async move {
        while let Some(addr) = addr_rx.recv().await {
            let Ok(mut cfg) = config::load().await else {
                continue;
            };
            match cfg.known_peers.iter().position(|a| a.id == addr.id) {
                Some(i) => cfg.known_peers[i] = addr,
                None => cfg.known_peers.push(addr),
            }
            if let Err(e) = config::save(&cfg).await {
                tracing::warn!(error = ?e, "save known_peers failed");
            }
        }
    });

    tokio::signal::ctrl_c().await.ok();
    tracing::info!("shutdown requested");
    for tx in &shutdowns {
        let _ = tx.send(true);
    }
    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = ?e, "sync exited with error"),
            Err(e) => tracing::error!(error = ?e, "sync task panicked"),
        }
    }
    Ok(())
}

async fn do_status() -> Result<()> {
    let cfg = config::load().await?;
    println!("hostname:      {}", cfg.hostname);
    println!("device:        {}", cfg.display_name());
    println!("read-only:     {}", cfg.read_only_local);
    println!("known peers:   {}", cfg.known_peers.len());
    if !cfg.blocked_peers.is_empty() {
        println!("blocked peers: {}", cfg.blocked_peers.len());
    }
    println!(
        "pair server:   {}",
        pair::resolve_server(cfg.pair_server_url.as_deref())
    );

    if cfg.folders.is_empty() {
        println!();
        println!("no folders — run `syncbox init <folder>`");
        return Ok(());
    }
    println!();
    println!("folders ({}):", cfg.folders.len());
    for f in &cfg.folders {
        let path = f
            .path
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "(no local path)".into());
        let paired = f.namespace_id.is_some() || f.doc_ticket.is_some();
        println!("  • {path}");
        println!("      paired:    {}", if paired { "yes" } else { "no" });
        if f.read_only {
            println!("      read-only: yes");
        }
        if let Some(id) = &f.namespace_id {
            println!("      namespace: {id}");
        }
    }
    Ok(())
}

async fn do_list() -> Result<()> {
    let cfg = config::load().await?;
    if cfg.folders.is_empty() {
        println!("no folders");
        return Ok(());
    }
    for (i, f) in cfg.folders.iter().enumerate() {
        let path = f
            .path
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "(no local path)".into());
        println!("{i}: {path}");
    }
    Ok(())
}

async fn do_remove(folder: PathBuf) -> Result<()> {
    let mut cfg = config::load().await?;
    let idx = resolve_folder(&cfg, Some(&folder))?;
    let removed = cfg.folders.remove(idx);
    config::save(&cfg).await?;
    let path = removed
        .path
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    println!("Stopped syncing: {path}");
    println!("(local files were left in place)");
    Ok(())
}

/// Dump every entry in a folder's doc — keys, tombstones, hashes, authors.
async fn do_dump(folder: Option<PathBuf>) -> Result<()> {
    init_tracing();
    let mut cfg = config::load().await?;
    let idx = resolve_folder(&cfg, folder.as_deref())?;
    let node = spawn_node().await?;
    let doc = open_doc(&node, &mut cfg.folders[idx])
        .await?
        .context("not paired — nothing to dump")?;
    config::save(&cfg).await?;

    println!("namespace: {}", doc.id());
    let stream = doc
        .get_many(Query::single_latest_per_key().include_empty())
        .await
        .context("doc.get_many")?;
    tokio::pin!(stream);

    let (mut files, mut tombs) = (0u32, 0u32);
    while let Some(entry) = stream.next().await {
        let entry = entry?;
        let key = String::from_utf8_lossy(entry.key());
        let author = entry.author().to_string();
        let author = &author[..author.len().min(8)];
        if entry.is_empty() {
            tombs += 1;
            println!("  TOMB  {key}  ts={}  author={author}", entry.timestamp());
        } else {
            files += 1;
            let hash = entry.content_hash().to_string();
            println!(
                "  FILE  {key}  hash={}  ts={}  author={author}",
                &hash[..hash.len().min(12)],
                entry.timestamp()
            );
        }
    }
    println!(
        "total: {} ({files} files, {tombs} tombstones)",
        files + tombs
    );
    Ok(())
}

/// Force one blob-store sweep and report what it freed.
///
/// There is no one-shot GC entry point in iroh-blobs 0.103 — GC is a loop that
/// sleeps, then sweeps. So we start a node whose loop ticks in seconds instead
/// of half an hour, watch the store on disk until it stops shrinking, and stop.
async fn do_gc() -> Result<()> {
    init_tracing();
    let iroh_root = config::iroh_root()?;
    let blobs_dir = iroh_root.join("blobs");

    let before = dir_size(&blobs_dir);
    println!("blob store: {}", human(before));

    // Opening the store fails outright if another syncbox holds it, which is
    // what we want — two writers on one redb is how stores get corrupted.
    let node = Node::spawn_with_gc(&iroh_root, GC_FORCED_INTERVAL)
        .await
        .context("could not open the store — is `syncbox run` or the app still going?")?;

    println!("sweeping...");
    wait_until_settled(&blobs_dir.join(BLOB_DATA_DIR)).await;
    let after = dir_size(&blobs_dir);

    match before.checked_sub(after) {
        Some(freed) if freed > 0 => println!("freed {} — now {}", human(freed), human(after)),
        _ => println!("nothing to free"),
    }

    // The blob store is flushed by now, so a peer that won't let go of its
    // connection must not keep a maintenance command alive.
    let _ = tokio::time::timeout(GC_SHUTDOWN_GRACE, node.shutdown()).await;
    std::process::exit(0);
}

/// Poll until `dir` holds steady, or we give up.
///
/// Watches `blobs/data` rather than the whole store: `blobs.db` is rewritten on
/// every GC tick, so its size jitters and a steady reading never comes.
///
/// The first sweep can't happen before the GC interval elapses, so we always
/// wait out one of those before trusting a steady reading.
async fn wait_until_settled(dir: &Path) {
    let start = std::time::Instant::now();
    let mut last = dir_size(dir);
    let mut steady = 0u32;

    loop {
        tokio::time::sleep(GC_POLL).await;
        let now = dir_size(dir);

        steady = if now == last { steady + 1 } else { 0 };
        last = now;

        let swept_once = start.elapsed() > GC_FORCED_INTERVAL + GC_POLL;
        if (swept_once && steady >= GC_SETTLE_POLLS) || start.elapsed() > GC_TIMEOUT {
            return;
        }
    }
}

/// Bytes held under `dir`, recursively. Unreadable entries count as zero.
fn dir_size(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };

    entries
        .flatten()
        .map(|e| match e.metadata() {
            Ok(md) if md.is_dir() => dir_size(&e.path()),
            Ok(md) => md.len(),
            Err(_) => 0,
        })
        .sum()
}

fn human(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut size = bytes as f64;
    let mut unit = 0;

    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }

    format!("{size:.1} {}", UNITS[unit])
}

// ---------- helpers ----------

async fn spawn_node() -> Result<Arc<Node>> {
    let iroh_root = config::iroh_root()?;
    Ok(Arc::new(Node::spawn(&iroh_root).await?))
}

/// Resolve a folder selector to an index into `cfg.folders`. `None` is allowed
/// only when exactly one folder is synced; otherwise the caller must name one.
fn resolve_folder(cfg: &config::Config, sel: Option<&Path>) -> Result<usize> {
    match sel {
        Some(p) => {
            let canon = p.canonicalize().unwrap_or_else(|_| p.to_path_buf());
            cfg.folders
                .iter()
                .position(|f| {
                    f.path.as_deref() == Some(canon.as_path()) || f.path.as_deref() == Some(p)
                })
                .with_context(|| format!("no synced folder at {}", p.display()))
        }
        None => match cfg.folders.len() {
            0 => bail!("no folders — run `syncbox init <folder>` first"),
            1 => Ok(0),
            _ => bail!("several folders synced — name one by path (see `syncbox list`)"),
        },
    }
}

/// Re-open a folder's synced doc. Tries `open(namespace_id)` first (cheap,
/// attaches to the local replica); falls back to `import(ticket)` for the
/// device joining for the first time. Updates `folder.namespace_id` if the
/// import path discovered it — the caller is responsible for saving config.
async fn open_doc(node: &Node, folder: &mut config::FolderConfig) -> Result<Option<Doc>> {
    if let Some(id_str) = &folder.namespace_id {
        if let Ok(id) = NamespaceId::from_str(id_str) {
            match node.docs.open(id).await {
                Ok(Some(d)) => return Ok(Some(d)),
                Ok(None) => tracing::warn!(id = %id_str, "doc not found locally"),
                Err(e) => tracing::warn!(error = ?e, "docs.open failed"),
            }
        }
    }
    if let Some(t) = folder.doc_ticket.clone() {
        if let Ok(ticket) = DocTicket::from_str(&t) {
            match node.docs.import(ticket).await {
                Ok(d) => {
                    folder.namespace_id = Some(d.id().to_string());
                    return Ok(Some(d));
                }
                Err(e) => tracing::warn!(error = ?e, "doc import failed"),
            }
        }
    }
    Ok(None)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}
