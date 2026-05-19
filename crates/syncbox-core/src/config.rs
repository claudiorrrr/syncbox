use anyhow::{Context, Result};
use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;

/// One synced folder: a local directory mirrored into an iroh-docs namespace.
///
/// A device can sync several folders at once; each is one of these. The three
/// `Option` fields can be unset independently — a folder picked but not yet
/// paired has a `path` and no namespace; a folder joined from a pairing code
/// has a namespace and ticket but no `path` until the user places it on disk.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FolderConfig {
    /// Local directory mirrored across devices. `None` right after joining a
    /// shared folder whose local location hasn't been chosen yet.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// The doc ticket for this folder's namespace. Kept around as a fallback
    /// for re-pairing if the local doc DB is wiped.
    #[serde(default)]
    pub doc_ticket: Option<String>,
    /// The persistent identifier of this folder's synced doc. Lets us re-open
    /// the existing local replica on restart without going through `import`,
    /// which fails if the namespace already exists.
    #[serde(default)]
    pub namespace_id: Option<String>,
    /// True if this device joined the folder read-only — it receives changes
    /// but never propagates its own for this folder.
    #[serde(default)]
    pub read_only: bool,
}

impl FolderConfig {
    /// A folder entry for a freshly-picked local directory, not yet paired.
    pub fn for_path(path: PathBuf) -> Self {
        Self {
            path: Some(path),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    /// The folders this device syncs. Empty until the user picks one. A single
    /// entry is the common case; the list supports several shared folders.
    #[serde(default)]
    pub folders: Vec<FolderConfig>,

    // --- legacy single-folder fields, kept for migration only ---
    // Pre-multi-folder configs stored one folder at the top level. `load()`
    // folds these into `folders`; `skip_serializing` means they are never
    // written back, so a config is migrated in place the first time it loads.
    #[serde(default, skip_serializing, rename = "folder")]
    legacy_folder: Option<PathBuf>,
    #[serde(default, skip_serializing, rename = "doc_ticket")]
    legacy_doc_ticket: Option<String>,
    #[serde(default, skip_serializing, rename = "namespace_id")]
    legacy_namespace_id: Option<String>,

    /// The machine's own hostname. Fallback name when `device_name` is unset.
    pub hostname: String,
    /// User-chosen name for this device, shown to paired devices. None → the
    /// `hostname` is used instead. Set in the GUI.
    #[serde(default)]
    pub device_name: Option<String>,
    /// Full EndpointAddrs of peers we've seen synced with. On restart we feed
    /// them to `doc.start_sync` so reconnection doesn't have to wait for fresh
    /// relay/mDNS discovery from scratch. Device-global: a peer address is the
    /// same whichever folder it was seen on.
    #[serde(default)]
    pub known_peers: Vec<EndpointAddr>,
    /// URL of the rendezvous worker that swaps short pairing codes for
    /// iroh DocTickets. Defaults to the placeholder; user overrides in UI.
    #[serde(default)]
    pub pair_server_url: Option<String>,
    /// Endpoint IDs (hex form, matches `PublicKey::to_string()`) we refuse
    /// to apply changes from. Used by the "revoke device" feature.
    #[serde(default)]
    pub blocked_peers: Vec<String>,
    /// If true, this device receives changes but never propagates its own.
    #[serde(default)]
    pub read_only_local: bool,
    /// If true, syncbox runs menu-bar-only — no Dock icon. Applied at startup
    /// and whenever the user toggles it in Settings.
    #[serde(default)]
    pub hide_dock_icon: bool,
    /// Last time (unix seconds) each peer was seen online, keyed by endpoint
    /// id. Persisted so "offline for over a week" survives restarts.
    #[serde(default)]
    pub peer_last_seen: HashMap<String, u64>,
}

/// Fallback pair-server host.
///
/// If `src-tauri/pair-server.txt` existed at build time, `build.rs` baked its
/// contents in here (see `SYNCBOX_DEFAULT_PAIR_SERVER`). Otherwise this is a
/// harmless placeholder — the real host is then supplied per-device via the
/// `SYNCBOX_PAIR_SERVER` env var or the **Advanced → Pair server URL** field.
pub const DEFAULT_PAIR_SERVER: &str = match option_env!("SYNCBOX_DEFAULT_PAIR_SERVER") {
    Some(url) => url,
    None => "https://pair.example.com",
};

impl Config {
    pub fn host() -> String {
        hostname::get()
            .ok()
            .and_then(|s| s.into_string().ok())
            .unwrap_or_else(|| "unknown".into())
    }

    /// The name shown to paired devices and used in `.conflict-<host>-<ts>`
    /// filenames: the user-chosen `device_name` if set, else the `hostname`.
    pub fn display_name(&self) -> String {
        match &self.device_name {
            Some(n) if !n.trim().is_empty() => n.trim().to_string(),
            _ => self.hostname.clone(),
        }
    }

    /// The primary (first) synced folder, if any. Single-folder code paths
    /// operate on this one; multi-folder callers iterate `folders` directly.
    pub fn primary(&self) -> Option<&FolderConfig> {
        self.folders.first()
    }

    /// Mutable primary folder, if one exists.
    pub fn primary_mut(&mut self) -> Option<&mut FolderConfig> {
        self.folders.first_mut()
    }

    /// The primary folder, creating an empty entry if there is none. Used when
    /// pairing or joining needs somewhere to store a namespace/ticket.
    pub fn primary_or_default(&mut self) -> &mut FolderConfig {
        if self.folders.is_empty() {
            self.folders.push(FolderConfig::default());
        }
        &mut self.folders[0]
    }

    /// Fold a pre-multi-folder config (top-level `folder`/`doc_ticket`/
    /// `namespace_id`) into the `folders` list. No-op once migrated.
    fn migrate_legacy_folder(&mut self) {
        if !self.folders.is_empty() {
            return;
        }
        let (path, ticket, ns) = (
            self.legacy_folder.take(),
            self.legacy_doc_ticket.take(),
            self.legacy_namespace_id.take(),
        );
        if path.is_some() || ticket.is_some() || ns.is_some() {
            self.folders.push(FolderConfig {
                path,
                doc_ticket: ticket,
                namespace_id: ns,
                read_only: false,
            });
        }
    }
}

/// Root directory for syncbox's own state — `config.json` plus the iroh blob
/// and doc stores. Defaults to the platform data directory; the
/// `SYNCBOX_DATA_DIR` env var overrides it, which lets several isolated
/// instances run on one machine (handy for testing).
pub fn data_dir() -> Result<PathBuf> {
    let dir = match std::env::var("SYNCBOX_DATA_DIR") {
        Ok(s) if !s.trim().is_empty() => PathBuf::from(s.trim()),
        _ => {
            let base = dirs::data_dir().context("could not locate user data directory")?;
            base.join("dev.syncbox")
        }
    };
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

pub fn config_path() -> Result<PathBuf> {
    Ok(data_dir()?.join("config.json"))
}

pub fn iroh_root() -> Result<PathBuf> {
    let d = data_dir()?.join("iroh");
    std::fs::create_dir_all(&d)?;
    Ok(d)
}

pub async fn load() -> Result<Config> {
    let p = config_path()?;
    if !p.exists() {
        return Ok(Config {
            hostname: Config::host(),
            ..Default::default()
        });
    }
    let raw = fs::read(&p).await?;
    let mut cfg: Config = serde_json::from_slice(&raw)?;
    if cfg.hostname.is_empty() {
        cfg.hostname = Config::host();
    }
    cfg.migrate_legacy_folder();
    Ok(cfg)
}

pub async fn save(cfg: &Config) -> Result<()> {
    let p = config_path()?;
    let raw = serde_json::to_vec_pretty(cfg)?;
    fs::write(&p, raw).await?;
    Ok(())
}
