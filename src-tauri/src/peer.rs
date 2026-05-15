use anyhow::{Context, Result};
use iroh::{endpoint::presets, protocol::Router, Endpoint};
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
        // QUIC endpoint with n0 defaults: public relays, automatic NAT
        // traversal, and (because we asked for the feature in Cargo.toml)
        // local mDNS discovery for same-LAN peers.
        let endpoint = Endpoint::bind(presets::N0)
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
