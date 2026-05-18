use anyhow::{Context, Result};
use iroh::{endpoint::presets, protocol::Router, Endpoint, SecretKey};
use iroh_blobs::{store::fs::FsStore, BlobsProtocol};
use iroh_docs::{
    api::protocol::{AddrInfoOptions, ShareMode},
    protocol::Docs,
};
use iroh_gossip::net::Gossip;
use std::path::Path;

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
}

impl Node {
    pub async fn spawn(iroh_root: &Path) -> Result<Self> {
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
        let store = FsStore::load(&blobs_path)
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

        let router = Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs_proto)
            .accept(iroh_gossip::ALPN, gossip.clone())
            .accept(iroh_docs::ALPN, docs.clone())
            .spawn();

        Ok(Self {
            endpoint,
            router,
            store,
            docs,
            gossip,
        })
    }

    /// Shut down the QUIC listener and flush the stores.
    #[allow(dead_code)]
    pub async fn shutdown(self) -> Result<()> {
        self.router.shutdown().await?;
        Ok(())
    }
}

/// Load the device's persisted iroh secret key, or generate one and save it.
///
/// The key is the node's stable identity across restarts. Stored as 32 raw
/// bytes in `iroh/secret.key`, owner-readable only. A malformed file is
/// replaced rather than treated as fatal.
fn load_or_create_secret_key(iroh_root: &Path) -> Result<SecretKey> {
    use std::os::unix::fs::PermissionsExt;

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
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
        .context("chmod secret.key")?;
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
