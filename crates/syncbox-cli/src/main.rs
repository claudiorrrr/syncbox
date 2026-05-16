//! syncbox — headless command-line client.
//!
//! Same sync engine as the macOS GUI (`syncbox-core`), no window. Built for
//! Linux servers and headless machines: pair once, then run `syncbox run`
//! under systemd.
//!
//! Typical flow:
//!   device A:  syncbox init ~/Sync && syncbox pair      -> prints a code
//!   device B:  syncbox join ABC-123 && syncbox init ~/Sync
//!   both:      syncbox run                              (or via systemd)

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use iroh_docs::{api::Doc, DocTicket, NamespaceId};
use std::{
    collections::HashMap,
    path::PathBuf,
    str::FromStr,
    sync::{atomic::AtomicU32, Arc},
    time::{SystemTime, UNIX_EPOCH},
};
use syncbox_core::{
    config,
    ignore_patterns::IgnoreSet,
    pair,
    peer::{self, Node},
    sync::{self, SyncState},
};
use tokio::sync::{watch, Mutex};

#[derive(Parser)]
#[command(name = "syncbox", version, about = "Folder sync over iroh — headless client")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Set the folder to sync (creates it if missing).
    Init {
        /// Path to the folder.
        folder: PathBuf,
    },
    /// Create a pairing code other devices can use to join this folder.
    Pair {
        /// Share read-only: joiners receive changes but can't push their own.
        #[arg(long)]
        read_only: bool,
    },
    /// Join a folder shared from another device, using its pairing code.
    Join {
        /// The 6-character code, e.g. ABC-123.
        code: String,
    },
    /// Run the sync engine in the foreground (use this under systemd).
    Run,
    /// Show current configuration and pairing state.
    Status,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Init { folder } => do_init(folder).await,
        Cmd::Pair { read_only } => do_pair(read_only).await,
        Cmd::Join { code } => do_join(code).await,
        Cmd::Run => {
            init_tracing();
            run_sync().await
        }
        Cmd::Status => do_status().await,
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
    cfg.folder = Some(folder.clone());
    config::save(&cfg).await?;
    println!("Sync folder set: {}", folder.display());
    Ok(())
}

async fn do_pair(read_only: bool) -> Result<()> {
    let mut cfg = config::load().await?;
    if cfg.folder.is_none() {
        eprintln!(
            "warning: no folder set — run `syncbox init <folder>` so this device \
             has something to share"
        );
    }
    let node = spawn_node().await?;

    // Reuse the existing doc if we already have one; otherwise create it.
    let doc = match open_doc(&node, &mut cfg).await? {
        Some(d) => d,
        None => node.docs.create().await.context("create doc")?,
    };
    let (mode, opts) = peer::share_opts(read_only);
    let ticket = doc.share(mode, opts).await.context("share doc")?;
    let ticket_str = ticket.to_string();

    cfg.namespace_id = Some(doc.id().to_string());
    cfg.doc_ticket = Some(ticket_str.clone());
    config::save(&cfg).await?;

    let server = pair::resolve_server(cfg.pair_server_url.as_deref());
    let pc = pair::create_code(&server, &ticket_str)
        .await
        .context("publish pairing code")?;

    let mins = pc.expires_unix.saturating_sub(now_unix()) / 60;
    println!("Pairing code:  {}", pc.code);
    println!("Expires in:    {mins} min");
    println!();
    println!("On the other device:  syncbox join {}", pc.code);
    Ok(())
}

async fn do_join(code: String) -> Result<()> {
    let mut cfg = config::load().await?;
    let server = pair::resolve_server(cfg.pair_server_url.as_deref());
    let ticket_str = pair::redeem_code(&server, &code)
        .await
        .context("redeem pairing code")?;

    let node = spawn_node().await?;
    let ticket = DocTicket::from_str(ticket_str.trim())
        .context("pair server returned an invalid ticket")?;
    let doc = node.docs.import(ticket).await.context("import doc")?;

    cfg.namespace_id = Some(doc.id().to_string());
    cfg.doc_ticket = Some(ticket_str.trim().to_string());
    config::save(&cfg).await?;

    println!("Paired — joined the shared folder.");
    if cfg.folder.is_none() {
        println!("Next:  syncbox init <folder>   then   syncbox run");
    } else {
        println!("Next:  syncbox run");
    }
    Ok(())
}

async fn run_sync() -> Result<()> {
    let mut cfg = config::load().await?;
    let folder = cfg
        .folder
        .clone()
        .context("no folder set — run `syncbox init <folder>` first")?;
    if !folder.is_dir() {
        bail!("sync folder {} does not exist", folder.display());
    }

    let node = spawn_node().await?;
    let author = node.docs.author_create().await.context("create author")?;

    let doc = open_doc(&node, &mut cfg)
        .await?
        .context("not paired — run `syncbox pair` or `syncbox join` first")?;

    // Tell iroh-docs who to sync with. `open_doc` attaches the local replica
    // via `open`, which — unlike `import` — does not start a sync session, so
    // this step is what actually connects us. Two address sources: peers seen
    // in earlier sessions (known_peers) and the nodes embedded in the pairing
    // ticket (the only source right after a fresh `join`).
    let mut peers = cfg.known_peers.clone();
    if let Some(t) = &cfg.doc_ticket {
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
            Ok(()) => tracing::info!(count = n, "started sync with peers"),
            Err(e) => tracing::warn!(error = ?e, "start_sync failed"),
        }
    }

    let ignores = Arc::new(IgnoreSet::load(&folder).context("load ignore set")?);
    let (addr_tx, mut addr_rx) =
        tokio::sync::mpsc::unbounded_channel::<iroh::EndpointAddr>();

    let st = SyncState {
        node: node.clone(),
        doc,
        author,
        root: folder,
        host: cfg.hostname.clone(),
        echo: Arc::new(Mutex::new(HashMap::new())),
        peers: Arc::new(Mutex::new(HashMap::new())),
        addr_sink: addr_tx,
        ignores,
        read_only: cfg.read_only_local,
        blocked: Arc::new(Mutex::new(cfg.blocked_peers.iter().cloned().collect())),
        active: Arc::new(AtomicU32::new(0)),
        stats: Arc::new(Mutex::new(Default::default())),
        status: Arc::new(Mutex::new(String::new())),
        log: Arc::new(Mutex::new(Default::default())),
    };

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

    let (tx, rx) = watch::channel(false);
    let sync_task = tokio::spawn(async move { sync::run(st, rx).await });

    tokio::signal::ctrl_c().await.ok();
    tracing::info!("shutdown requested");
    let _ = tx.send(true);
    match sync_task.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::error!(error = ?e, "sync exited with error"),
        Err(e) => tracing::error!(error = ?e, "sync task panicked"),
    }
    Ok(())
}

async fn do_status() -> Result<()> {
    let cfg = config::load().await?;
    let folder = cfg
        .folder
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "(not set)".into());
    let paired = cfg.namespace_id.is_some() || cfg.doc_ticket.is_some();

    println!("hostname:      {}", cfg.hostname);
    println!("folder:        {folder}");
    println!("paired:        {}", if paired { "yes" } else { "no" });
    if let Some(id) = &cfg.namespace_id {
        println!("namespace:     {id}");
    }
    println!("read-only:     {}", cfg.read_only_local);
    println!("known peers:   {}", cfg.known_peers.len());
    if !cfg.blocked_peers.is_empty() {
        println!("blocked peers: {}", cfg.blocked_peers.len());
    }
    println!(
        "pair server:   {}",
        pair::resolve_server(cfg.pair_server_url.as_deref())
    );
    Ok(())
}

// ---------- helpers ----------

async fn spawn_node() -> Result<Arc<Node>> {
    let iroh_root = config::iroh_root()?;
    Ok(Arc::new(Node::spawn(&iroh_root).await?))
}

/// Re-open the synced doc on cold start. Tries `open(namespace_id)` first
/// (cheap, attaches to the local replica); falls back to `import(ticket)` for
/// the device that's joining for the first time. Persists the namespace id if
/// the import path discovered it.
async fn open_doc(node: &Node, cfg: &mut config::Config) -> Result<Option<Doc>> {
    if let Some(id_str) = &cfg.namespace_id {
        if let Ok(id) = NamespaceId::from_str(id_str) {
            match node.docs.open(id).await {
                Ok(Some(d)) => return Ok(Some(d)),
                Ok(None) => tracing::warn!(id = %id_str, "doc not found locally"),
                Err(e) => tracing::warn!(error = ?e, "docs.open failed"),
            }
        }
    }
    if let Some(t) = &cfg.doc_ticket {
        if let Ok(ticket) = DocTicket::from_str(t) {
            match node.docs.import(ticket).await {
                Ok(d) => {
                    let id_str = d.id().to_string();
                    if cfg.namespace_id.as_deref() != Some(id_str.as_str()) {
                        cfg.namespace_id = Some(id_str);
                        config::save(cfg).await?;
                    }
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
