use anyhow::{Context, Result};
use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    /// The folder we mirror across devices. None until the user picks one.
    pub folder: Option<PathBuf>,
    /// The doc ticket for the namespace we sync into. Kept around as a
    /// fallback for re-pairing if the local doc DB is wiped.
    pub doc_ticket: Option<String>,
    /// The persistent identifier of the synced doc. Lets us re-open the
    /// existing local replica on restart without going through `import`,
    /// which fails if the namespace already exists.
    pub namespace_id: Option<String>,
    /// The machine's own hostname. Fallback name when `device_name` is unset.
    pub hostname: String,
    /// User-chosen name for this device, shown to paired devices. None → the
    /// `hostname` is used instead. Set in the GUI.
    #[serde(default)]
    pub device_name: Option<String>,
    /// Full EndpointAddrs of peers we've seen synced with this doc. On
    /// restart we feed them to `doc.start_sync` so reconnection doesn't
    /// have to wait for fresh relay/mDNS discovery from scratch.
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
    Ok(cfg)
}

pub async fn save(cfg: &Config) -> Result<()> {
    let p = config_path()?;
    let raw = serde_json::to_vec_pretty(cfg)?;
    fs::write(&p, raw).await?;
    Ok(())
}
