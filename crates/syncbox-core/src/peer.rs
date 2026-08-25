use anyhow::{Context, Result};
use futures_lite::StreamExt;
use iroh::{endpoint::presets, protocol::Router, Endpoint, SecretKey};
use iroh_blobs::{
    api::downloader::Downloader,
    store::{
        fs::{options::Options, FsStore},
        GcConfig, ProtectOutcome,
    },
    BlobsProtocol, Hash,
};
use iroh_docs::{
    api::protocol::{AddrInfoOptions, ShareMode},
    protocol::Docs,
    store::Query,
};
use iroh_gossip::net::Gossip;
use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, OnceLock},
    time::Duration,
};

/// How often to garbage-collect the blob store. iroh-blobs keeps a
/// content-addressed copy of every blob, so an edited or deleted file leaves
/// its old blob orphaned; GC drops blobs no longer referenced by any doc
/// entry. 30 minutes — GC walks every blob, so not free, but a small folder
/// makes it cheap.
const GC_INTERVAL_SECS: u64 = 1800;

/// All the long-lived iroh handles for one running app instance.
///
/// `Endpoint`, `FsStore`, `Docs` and `Gossip` are cheap to `clone()` — they're
/// handles around shared internal state. `Router` is kept alive to keep the
/// QUIC server running; dropping it will stop accepting connections.
pub struct Node {
    pub endpoint: Endpoint,
    // Kept alive so the QUIC server keeps accepting; not otherwise read.
    #[allow(dead_code)]
    pub router: Router,
    pub store: FsStore,
    pub docs: Docs,
    // Held so the gossip protocol stays registered on the router.
    #[allow(dead_code)]
    pub gossip: Gossip,
    /// The one blob downloader for this process, shared by every folder's
    /// sync loop.
    ///
    /// Built once, deliberately. `Store::downloader()` is not a cheap
    /// accessor — each call spawns a `DownloaderActor` task *and* a
    /// `ConnectionPool` with its own actor task, and iroh-blobs says so
    /// outright: "this creates an object that has internal state, so don't
    /// create it ad hoc but store it somewhere if you need it multiple
    /// times". `reconcile_remote` used to build one per pass, and a pass can
    /// run several times a second, so unreachable peers left an unbounded
    /// pile of downloader + connection-pool actors that never wound down.
    pub downloader: Downloader,
}

impl Node {
    pub async fn spawn(iroh_root: &Path) -> Result<Self> {
        Self::spawn_with_gc(iroh_root, Duration::from_secs(GC_INTERVAL_SECS)).await
    }

    /// Same, with an explicit GC interval. `syncbox gc` uses a short one to
    /// force a single sweep; tests use it to watch one happen.
    pub async fn spawn_with_gc(iroh_root: &Path, gc_interval: Duration) -> Result<Self> {
        // QUIC endpoint with n0 defaults: public relays and automatic NAT
        // traversal. No local mDNS — same-LAN peers still rendezvous through
        // a relay and node-id discovery.
        //
        // The secret key is this device's permanent node identity. It must be
        // persisted: a fresh key each launch means a new node id every
        // restart, so peers that saved our address can never reconnect — sync
        // would silently die on the first restart.
        let secret_key = load_or_create_secret_key(iroh_root)?;
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .bind()
            .await
            .context("failed to bind iroh endpoint")?;

        let blobs_path = iroh_root.join("blobs");
        std::fs::create_dir_all(&blobs_path)?;

        // Garbage collection. We tell iroh-blobs which blobs are still wanted;
        // it sweeps the rest on the interval below. Without this the blob store
        // grows without bound — each edit and each deleted file orphans a blob.
        //
        // The callback needs `Docs`, but `Docs` needs the store, which needs
        // the callback — so it reads a slot we fill in a few lines below. An
        // unfilled slot aborts the run rather than sweeping blind.
        let docs_slot: Arc<OnceLock<Docs>> = Arc::default();
        let gc_docs = docs_slot.clone();

        let mut store_opts = Options::new(&blobs_path);
        store_opts.gc = Some(GcConfig {
            interval: gc_interval,
            add_protected: Some(Arc::new(move |live| {
                let docs = gc_docs.clone();
                Box::pin(async move { protect_live_entries(&docs, live).await })
            })),
        });
        let store = FsStore::load_with_opts(blobs_path.join("blobs.db"), store_opts)
            .await
            .context("failed to open blob store")?;

        let blobs_proto = BlobsProtocol::new(&store, None);

        let gossip = Gossip::builder().spawn(endpoint.clone());

        let docs_path = iroh_root.join("docs");
        std::fs::create_dir_all(&docs_path)?;
        let docs = Docs::persistent(docs_path)
            .spawn(endpoint.clone(), (*store).clone(), gossip.clone())
            .await
            .context("failed to spawn docs")?;

        let _ = docs_slot.set(docs.clone());

        let router = Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs_proto)
            .accept(iroh_gossip::ALPN, gossip.clone())
            .accept(iroh_docs::ALPN, docs.clone())
            .spawn();

        let downloader = store.downloader(&endpoint);

        Ok(Self {
            endpoint,
            router,
            store,
            docs,
            gossip,
            downloader,
        })
    }

    /// Shut down the QUIC listener and flush the stores.
    pub async fn shutdown(self) -> Result<()> {
        self.router.shutdown().await?;
        Ok(())
    }
}

/// Mark every blob a synced folder can still need, so GC spares it.
///
/// The wanted set is the *winning* entry per key, in every doc — precisely what
/// `reconcile_remote` mirrors to disk.
///
/// Deliberately not iroh-docs' own `ProtectCallbackHandler`: that one protects
/// every row in the records table, and rows are keyed by `(key, author)`. When
/// one device replaces or deletes a file another device wrote, the original
/// author's row lives on pointing at the superseded hash — `doc.del` is
/// author-scoped and an edit only writes a row under the editing author. Those
/// rows would pin their blobs forever, on every peer in the swarm, since the
/// records table is a CRDT and replicates in full. A 9.5 GB pair of folders had
/// grown 8.5 GB of such blobs.
///
/// Any error aborts the run: sweeping against a half-built set deletes live
/// data, and unrecoverably so — iroh-blobs 0.103 can only refetch from a peer.
async fn protect_live_entries(docs: &OnceLock<Docs>, live: &mut HashSet<Hash>) -> ProtectOutcome {
    let Some(docs) = docs.get().cloned() else {
        // Store is up but docs isn't yet. Nothing can be judged dead.
        return ProtectOutcome::Abort;
    };

    // `ProtectCb` demands a `Sync` future, and the doc query streams aren't —
    // so the walk happens in its own task and only the hashes come back.
    match tokio::spawn(live_hashes(docs)).await {
        Ok(Ok(hashes)) => {
            live.extend(hashes);
            ProtectOutcome::Continue
        }
        Ok(Err(e)) => {
            tracing::warn!(error = ?e, "gc: could not list live doc entries; skipping sweep");
            ProtectOutcome::Abort
        }
        Err(e) => {
            tracing::warn!(error = ?e, "gc: protect task failed; skipping sweep");
            ProtectOutcome::Abort
        }
    }
}

/// Content hash of the winning entry of every key, across every doc.
async fn live_hashes(docs: Docs) -> Result<Vec<Hash>> {
    let api = docs.api();
    let mut hashes = Vec::new();

    let mut namespaces = api.list().await.context("list docs")?;

    while let Some(res) = namespaces.next().await {
        let (ns, _cap) = res.context("list docs")?;

        // `open` is idempotent — the store tracks open replicas in a set — and
        // we never close: a close would yank the doc from under a sync loop.
        let Some(doc) = api.open(ns).await.context("open doc")? else {
            continue;
        };

        // No `include_empty`: a key whose winner is a tombstone yields nothing,
        // which is the point — its old content is exactly what should go.
        let entries = doc
            .get_many(Query::single_latest_per_key())
            .await
            .context("query doc")?;
        let mut entries = Box::pin(entries);

        while let Some(entry) = entries.next().await {
            hashes.push(entry.context("read doc entry")?.content_hash());
        }
    }

    Ok(hashes)
}

/// Load the device's persisted iroh secret key, or generate one and save it.
///
/// The key is the node's stable identity across restarts. Stored as 32 raw
/// bytes in `iroh/secret.key`, owner-readable only. A malformed file is
/// replaced rather than treated as fatal.
fn load_or_create_secret_key(iroh_root: &Path) -> Result<SecretKey> {
    let path = iroh_root.join("secret.key");
    if let Ok(bytes) = std::fs::read(&path) {
        match <[u8; 32]>::try_from(bytes.as_slice()) {
            Ok(arr) => return Ok(SecretKey::from_bytes(&arr)),
            Err(_) => tracing::warn!("iroh/secret.key malformed; regenerating identity"),
        }
    }
    std::fs::create_dir_all(iroh_root).context("create iroh dir")?;
    let key = SecretKey::generate();
    std::fs::write(&path, key.to_bytes()).context("write secret.key")?;
    // Unix: lock the key down to owner-only. On Windows, the data dir under
    // %APPDATA% inherits a user-private ACL, so no extra chmod is needed.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .context("chmod secret.key")?;
    }
    Ok(key)
}

/// Default addressing for our tickets: include relay URL + direct addrs so
/// peers can connect over LAN, via the relay, or by direct path.
pub fn share_opts(read_only: bool) -> (ShareMode, AddrInfoOptions) {
    let mode = if read_only {
        ShareMode::Read
    } else {
        ShareMode::Write
    };
    (mode, AddrInfoOptions::RelayAndAddresses)
}
