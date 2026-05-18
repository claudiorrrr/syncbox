//! syncbox — simple Mac folder sync, built on iroh.
//!
//! This crate is just the macOS menu-bar front-end. The sync engine lives
//! in `syncbox-core` and is shared with the headless CLI.

use anyhow::Result;
use iroh_docs::{api::Doc, AuthorId, DocTicket, NamespaceId};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    str::FromStr,
    sync::{atomic::AtomicU32, Arc},
};
use syncbox_core::ignore_patterns::IgnoreSet;
use syncbox_core::peer::Node;
use syncbox_core::sync::{
    EchoGuard, LogHandle, NameMap, PeerMap, StatsHandle, StatusLine, SyncState,
};
use syncbox_core::{config, pair, peer, sync};
use tauri::{
    menu::{Menu, MenuItem, PredefinedMenuItem},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
    AppHandle, Emitter, Manager, State,
};
use tauri_plugin_autostart::{MacosLauncher, ManagerExt};
use tauri_plugin_dialog::DialogExt;
use tokio::sync::{watch, Mutex};

/// All the runtime handles the Tauri commands need to touch. Kept behind a
/// single Mutex because most operations are user-initiated and rare.
#[derive(Default)]
struct Inner {
    config: config::Config,
    node: Option<Arc<Node>>,
    author: Option<AuthorId>,
    doc: Option<Doc>,
    echo: Option<EchoGuard>,
    peers: Option<PeerMap>,
    names: Option<NameMap>,
    blocked: Arc<Mutex<HashSet<String>>>,
    active: Arc<AtomicU32>,
    stats: StatsHandle,
    status: StatusLine,
    log: LogHandle,
    /// Cached IgnoreSet — rebuilt whenever the user picks a different folder.
    ignores: Option<Arc<IgnoreSet>>,
    shutdown: Option<watch::Sender<bool>>,
    sync_handle: Option<tauri::async_runtime::JoinHandle<()>>,
}

pub struct AppState {
    inner: Mutex<Inner>,
}

#[derive(Debug, Serialize, Clone, Default)]
struct StatusView {
    folder: Option<String>,
    hostname: String,
    has_doc: bool,
    has_ticket: bool,
    paired: bool,
    syncing: bool,
    peers_online: usize,
    peers_known: usize,
    message: String,
    /// App version: semver plus a build number (git commit count), both
    /// resolved at compile time. Shown verbatim in the window footer.
    version: String,
}

#[derive(Debug, Serialize, Clone)]
struct PeerView {
    id: String,
    online: bool,
    last_seen_unix: u64,
    /// Friendly device name, if the peer has published one.
    name: Option<String>,
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,syncbox_lib=debug")),
        )
        .init();

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_process::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_autostart::init(
            MacosLauncher::LaunchAgent,
            None,
        ))
        .manage(AppState {
            inner: Mutex::new(Inner::default()),
        })
        .setup(|app| {
            build_tray(app.handle().clone())?;

            // Hide window on startup — we're a tray app.
            if let Some(win) = app.get_webview_window("main") {
                let _ = win.hide();
            }

            // Bootstrap iroh + config in the background.
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                if let Err(e) = bootstrap(&handle).await {
                    tracing::error!(error = ?e, "bootstrap failed");
                }
            });

            // Tray updater: NotSetup (gray ring), Syncing (blue disc with a
            // rotating comet), InSync (green check). Ticks fast so the
            // Syncing comet animates smoothly.
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let mut last_state: Option<TrayState> = None;
                let mut frame: u32 = 0;
                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                    let state: State<AppState> = handle.state();
                    let inner = state.inner.lock().await;
                    let n = inner.active.load(std::sync::atomic::Ordering::Relaxed);
                    let configured = inner.doc.is_some()
                        && inner.config.folder.is_some()
                        && inner.sync_handle.is_some();
                    // Recent movement in *either* direction. last_activity_ms
                    // is bumped on every upload and download; the 1200 ms
                    // window keeps the animation visible after brief bursts.
                    let recent = {
                        let s = inner.stats.lock().await;
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        s.last_activity_ms != 0 && now.saturating_sub(s.last_activity_ms) < 1200
                    };
                    drop(inner);

                    let tray_state = if !configured {
                        TrayState::NotSetup
                    } else if n > 0 || recent {
                        TrayState::Syncing
                    } else {
                        TrayState::InSync
                    };

                    // Redraw on a state change, and every tick while syncing
                    // so the comet keeps moving.
                    let redraw = last_state != Some(tray_state) || tray_state == TrayState::Syncing;
                    if redraw {
                        if let Some(tray) = handle.tray_by_id("main") {
                            let (rgba, w, h) = status_icon(tray_state, frame);
                            let img = tauri::image::Image::new_owned(rgba, w, h);
                            let _ = tray.set_icon(Some(img));
                            let _ = tray.set_icon_as_template(false);
                            let _ = tray.set_title(None::<&str>);
                        }
                        last_state = Some(tray_state);
                    }
                    frame = frame.wrapping_add(1);
                }
            });

            Ok(())
        })
        .on_window_event(|win, event| {
            if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                // Hide instead of quitting when user closes window.
                let _ = win.hide();
                api.prevent_close();
            }
        })
        .invoke_handler(tauri::generate_handler![
            cmd_get_status,
            cmd_get_log,
            cmd_get_peers,
            cmd_get_ticket,
            cmd_join_with_ticket,
            cmd_make_code,
            cmd_use_code,
            cmd_get_pair_server,
            cmd_set_pair_server,
            cmd_choose_folder,
            cmd_open_folder,
            cmd_set_autostart,
            cmd_get_autostart,
            cmd_start_sync,
            cmd_get_storage_size,
            cmd_block_peer,
            cmd_unblock_peer,
            cmd_list_blocked,
            cmd_set_read_only,
            cmd_get_read_only,
            cmd_write_default_ignore,
            cmd_get_transfer_stats,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

/// The three states the menu-bar icon can show.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrayState {
    /// No folder picked or no doc yet.
    NotSetup,
    /// A transfer is in flight.
    Syncing,
    /// Configured and idle.
    InSync,
}

/// Distance from point `(px,py)` to the line segment `(ax,ay)-(bx,by)`.
fn dist_to_seg(px: f32, py: f32, ax: f32, ay: f32, bx: f32, by: f32) -> f32 {
    let (dx, dy) = (bx - ax, by - ay);
    let len2 = dx * dx + dy * dy;
    let t = if len2 <= 0.0 {
        0.0
    } else {
        (((px - ax) * dx + (py - ay) * dy) / len2).clamp(0.0, 1.0)
    };
    let (cx, cy) = (ax + t * dx, ay + t * dy);
    ((px - cx).powi(2) + (py - cy).powi(2)).sqrt()
}

/// Render the status icon as raw RGBA. Returns `(rgba, width, height)`.
/// `frame` advances the Syncing animation and is ignored by the other states:
///   * NotSetup — a hollow gray ring
///   * Syncing  — a blue disc with a white comet sweeping the rim
///   * InSync   — a solid green disc with a white check mark
fn status_icon(state: TrayState, frame: u32) -> (Vec<u8>, u32, u32) {
    use std::f32::consts::TAU;
    const S: i32 = 32;
    let c = S as f32 / 2.0;
    let radius = 12.0;
    let (r, g, b) = match state {
        TrayState::NotSetup => (142, 142, 147),
        TrayState::Syncing => (10, 132, 255),
        TrayState::InSync => (52, 199, 89),
    };
    let mut buf = vec![0u8; (S * S * 4) as usize];

    // Syncing comet: a bright arc near the rim, rotating one turn per 12 frames.
    let arc_start = (frame % 12) as f32 * (TAU / 12.0);
    let arc_len = 2.4_f32; // radians swept by the comet

    for y in 0..S {
        for x in 0..S {
            let px = x as f32 + 0.5;
            let py = y as f32 + 0.5;
            let dx = px - c;
            let dy = py - c;
            let dist = (dx * dx + dy * dy).sqrt();

            // Base shape alpha.
            let mut alpha = match state {
                TrayState::NotSetup => {
                    // Ring: a 3px-wide annulus.
                    let inner = radius - 3.0;
                    if dist >= inner && dist <= radius {
                        let edge = (radius - dist).min(dist - inner).min(1.0);
                        (edge.max(0.0)) * 255.0
                    } else {
                        0.0
                    }
                }
                _ => {
                    // Solid disc with a 1px soft edge.
                    if dist <= radius - 1.0 {
                        255.0
                    } else if dist >= radius {
                        0.0
                    } else {
                        (radius - dist) * 255.0
                    }
                }
            };

            let (mut cr, mut cg, mut cb) = (r, g, b);

            // InSync: stamp a white check on top of the green disc.
            if matches!(state, TrayState::InSync) && alpha > 0.0 {
                // Two segments: short down-stroke + long up-stroke.
                let d1 = dist_to_seg(px, py, 10.5, 16.0, 14.0, 20.0);
                let d2 = dist_to_seg(px, py, 14.0, 20.0, 22.0, 10.5);
                let d = d1.min(d2);
                if d < 1.8 {
                    cr = 255;
                    cg = 255;
                    cb = 255;
                    alpha = 255.0;
                }
            }

            // Syncing: sweep a white comet around the rim band. The head is
            // bright white, the tail fades back into the blue disc.
            if matches!(state, TrayState::Syncing) && alpha > 0.0 && dist >= radius - 5.0 {
                let mut ang = dy.atan2(dx);
                if ang < 0.0 {
                    ang += TAU;
                }
                let mut rel = ang - arc_start;
                while rel < 0.0 {
                    rel += TAU;
                }
                if rel <= arc_len {
                    let t = 1.0 - rel / arc_len; // 1 at head, 0 at tail
                    cr = (r as f32 + (255.0 - r as f32) * t) as u8;
                    cg = (g as f32 + (255.0 - g as f32) * t) as u8;
                    cb = (b as f32 + (255.0 - b as f32) * t) as u8;
                }
            }

            let i = ((y * S + x) * 4) as usize;
            buf[i] = cr;
            buf[i + 1] = cg;
            buf[i + 2] = cb;
            buf[i + 3] = alpha as u8;
        }
    }
    (buf, S as u32, S as u32)
}

async fn bootstrap(app: &AppHandle) -> Result<()> {
    let state: State<AppState> = app.state();
    let cfg = config::load().await?;
    let iroh_root = config::iroh_root()?;
    let node = Arc::new(Node::spawn(&iroh_root).await?);

    // Always have at least one author per device.
    let author = node.docs.author_create().await?;

    let mut inner = state.inner.lock().await;
    inner.config = cfg.clone();
    inner.node = Some(node.clone());
    inner.author = Some(author);
    inner.echo = Some(Arc::new(Mutex::new(Default::default())));
    inner.peers = Some(Arc::new(Mutex::new(HashMap::new())));
    inner.names = Some(Arc::new(Mutex::new(HashMap::new())));
    {
        let mut blocked = inner.blocked.lock().await;
        blocked.extend(cfg.blocked_peers.iter().cloned());
    }
    if let Some(folder) = &cfg.folder {
        match IgnoreSet::load(folder) {
            Ok(s) => inner.ignores = Some(Arc::new(s)),
            Err(e) => tracing::warn!(error = ?e, "could not load ignore set"),
        }
    }

    // Re-open the persistent doc on cold start. Order: try `open(id)` first
    // (cheap, just attaches to local replica). If we don't yet have an id
    // persisted but do have a ticket, fall through to `import(ticket)` —
    // this is the path on the device that joins for the first time.
    let mut opened: Option<Doc> = None;
    if let Some(id_str) = &cfg.namespace_id {
        if let Ok(id) = NamespaceId::from_str(id_str) {
            match node.docs.open(id).await {
                Ok(Some(d)) => {
                    tracing::info!(id = %id_str, "opened existing doc");
                    opened = Some(d);
                }
                Ok(None) => tracing::warn!(id = %id_str, "doc not found locally"),
                Err(e) => tracing::warn!(error = ?e, "docs.open failed"),
            }
        }
    }
    if opened.is_none() {
        if let Some(t) = &cfg.doc_ticket {
            if let Ok(ticket) = DocTicket::from_str(t) {
                match node.docs.import(ticket).await {
                    Ok(doc) => opened = Some(doc),
                    Err(e) => {
                        tracing::warn!(error = ?e, "doc import failed; will recreate on demand")
                    }
                }
            }
        }
    }
    if let Some(d) = &opened {
        let id_str = d.id().to_string();
        if inner.config.namespace_id.as_deref() != Some(id_str.as_str()) {
            inner.config.namespace_id = Some(id_str);
            if let Err(e) = config::save(&inner.config).await {
                tracing::warn!(error = ?e, "save namespace_id failed");
            }
        }
        // Re-establish sync with peers we knew before the restart, so the
        // first connection doesn't have to wait for fresh discovery.
        if !inner.config.known_peers.is_empty() {
            let peers = inner.config.known_peers.clone();
            let n = peers.len();
            if let Err(e) = d.start_sync(peers).await {
                tracing::warn!(error = ?e, "start_sync with known peers failed");
            } else {
                tracing::info!(count = n, "rejoined known peers");
            }
        }
    }
    inner.doc = opened;

    drop(inner);

    // If everything is in place, kick off sync.
    if let Err(e) = maybe_start_sync(app).await {
        tracing::warn!(error = ?e, "auto-start sync failed");
    }
    Ok(())
}

fn build_tray(app: AppHandle) -> Result<()> {
    let pair_item = MenuItem::with_id(&app, "pair", "Pair / Status…", true, None::<&str>)?;
    let open_item = MenuItem::with_id(
        &app,
        "open_folder",
        "Open synced folder",
        true,
        None::<&str>,
    )?;
    let update_item = MenuItem::with_id(
        &app,
        "check_updates",
        "Check for Updates…",
        true,
        None::<&str>,
    )?;
    let sep = PredefinedMenuItem::separator(&app)?;
    let quit_item = MenuItem::with_id(&app, "quit", "Quit syncbox", true, None::<&str>)?;
    let menu = Menu::with_items(
        &app,
        &[&pair_item, &open_item, &update_item, &sep, &quit_item],
    )?;

    let _tray = TrayIconBuilder::with_id("main")
        .icon(
            app.default_window_icon()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("no default window icon"))?,
        )
        .icon_as_template(true)
        .tooltip("syncbox")
        .menu(&menu)
        .on_menu_event(|app, event| match event.id.as_ref() {
            "pair" => {
                if let Some(win) = app.get_webview_window("main") {
                    let _ = win.show();
                    let _ = win.set_focus();
                }
            }
            "open_folder" => {
                let app_clone = app.clone();
                tauri::async_runtime::spawn(async move {
                    let _ = cmd_open_folder_inner(&app_clone).await;
                });
            }
            "check_updates" => {
                // The update check + dialog + install all live in the webview.
                // Nudge it with an event; the window can stay hidden, the
                // updater's own dialogs surface the result.
                let _ = app.emit("check-update", ());
            }
            "quit" => {
                let app_clone = app.clone();
                tauri::async_runtime::spawn(async move {
                    shutdown(&app_clone).await;
                    app_clone.exit(0);
                });
            }
            _ => {}
        })
        .on_tray_icon_event(|tray, event| {
            // Left click opens the window — matches typical mac menu bar UX.
            if let TrayIconEvent::Click {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                ..
            } = event
            {
                let app = tray.app_handle();
                if let Some(win) = app.get_webview_window("main") {
                    let _ = win.show();
                    let _ = win.set_focus();
                }
            }
        })
        .build(&app)?;
    Ok(())
}

async fn shutdown(app: &AppHandle) {
    let state: State<AppState> = app.state();
    let mut inner = state.inner.lock().await;
    if let Some(tx) = inner.shutdown.take() {
        let _ = tx.send(true);
    }
    if let Some(h) = inner.sync_handle.take() {
        let _ = h.await;
    }
}

/// Start the sync task if (a) we have a node, (b) we have a doc, (c) we
/// have a folder, and (d) sync isn't already running.
async fn maybe_start_sync(app: &AppHandle) -> Result<bool> {
    let state: State<AppState> = app.state();
    let mut inner = state.inner.lock().await;

    if inner.sync_handle.is_some() {
        return Ok(false);
    }
    let Some(node) = inner.node.clone() else {
        return Ok(false);
    };
    let Some(doc) = inner.doc.clone() else {
        return Ok(false);
    };
    let Some(author) = inner.author else {
        return Ok(false);
    };
    let Some(folder) = inner.config.folder.clone() else {
        return Ok(false);
    };
    let Some(echo) = inner.echo.clone() else {
        return Ok(false);
    };
    let Some(peers) = inner.peers.clone() else {
        return Ok(false);
    };
    let Some(names) = inner.names.clone() else {
        return Ok(false);
    };
    // Make sure we have an ignore matcher loaded for the current folder.
    if inner.ignores.is_none() {
        match IgnoreSet::load(&folder) {
            Ok(s) => inner.ignores = Some(Arc::new(s)),
            Err(e) => return Err(anyhow::anyhow!("ignore set: {e}")),
        }
    }
    let ignores = inner.ignores.clone().unwrap();
    let blocked = inner.blocked.clone();
    let active = inner.active.clone();
    let stats = inner.stats.clone();
    let status = inner.status.clone();
    let log = inner.log.clone();
    let read_only = inner.config.read_only_local;

    let host = inner.config.hostname.clone();

    let (tx, rx) = watch::channel(false);
    inner.shutdown = Some(tx);

    // Channel that sync.rs uses to publish freshly-seen peer addresses; the
    // consumer below dedupes them into the persistent config.
    let (addr_tx, mut addr_rx) = tokio::sync::mpsc::unbounded_channel::<iroh::EndpointAddr>();

    let st = SyncState {
        node,
        doc,
        author,
        root: folder,
        host,
        echo,
        peers,
        addr_sink: addr_tx,
        ignores,
        read_only,
        blocked,
        active,
        stats,
        status,
        log,
        names,
    };
    let handle = tauri::async_runtime::spawn(async move {
        if let Err(e) = sync::run(st, rx).await {
            tracing::error!(error = ?e, "sync task exited with error");
        }
    });
    inner.sync_handle = Some(handle);
    drop(inner);

    // Spawn the persister: dedupe addr_rx into config.known_peers and save
    // when a new one arrives. Cheap; runs only on neighbor changes.
    let app_clone = app.clone();
    tauri::async_runtime::spawn(async move {
        while let Some(addr) = addr_rx.recv().await {
            let state: State<AppState> = app_clone.state();
            let mut inner = state.inner.lock().await;
            // Replace any previous entry for this endpoint id; otherwise push.
            let idx = inner
                .config
                .known_peers
                .iter()
                .position(|a| a.id == addr.id);
            match idx {
                Some(i) => inner.config.known_peers[i] = addr,
                None => inner.config.known_peers.push(addr),
            }
            if let Err(e) = config::save(&inner.config).await {
                tracing::warn!(error = ?e, "save known_peers failed");
            }
        }
    });

    Ok(true)
}

// ---------- Tauri commands ----------

#[tauri::command]
async fn cmd_get_status(state: State<'_, AppState>) -> Result<StatusView, String> {
    let inner = state.inner.lock().await;
    let (peers_online, peers_known) = match &inner.peers {
        Some(map) => {
            let m = map.lock().await;
            (m.values().filter(|p| p.online).count(), m.len())
        }
        None => (0, 0),
    };
    let message = inner.status.lock().await.clone();
    Ok(StatusView {
        folder: inner
            .config
            .folder
            .as_ref()
            .map(|p| p.display().to_string()),
        hostname: inner.config.hostname.clone(),
        has_doc: inner.doc.is_some(),
        has_ticket: inner.config.doc_ticket.is_some(),
        paired: inner.doc.is_some() && inner.config.folder.is_some(),
        syncing: inner.sync_handle.is_some(),
        peers_online,
        peers_known,
        message,
        version: format!(
            "{} (build {})",
            env!("CARGO_PKG_VERSION"),
            option_env!("SYNCBOX_BUILD").unwrap_or("?"),
        ),
    })
}

#[tauri::command]
async fn cmd_get_log(state: State<'_, AppState>) -> Result<Vec<String>, String> {
    let inner = state.inner.lock().await;
    let log = inner.log.lock().await;
    Ok(log.iter().cloned().collect())
}

#[tauri::command]
async fn cmd_get_peers(state: State<'_, AppState>) -> Result<Vec<PeerView>, String> {
    let inner = state.inner.lock().await;
    let Some(peers) = inner.peers.clone() else {
        return Ok(vec![]);
    };
    let names = inner.names.clone();
    drop(inner);
    let name_map = match names {
        Some(n) => n.lock().await.clone(),
        None => HashMap::new(),
    };
    let p = peers.lock().await;
    let mut out: Vec<PeerView> = p
        .iter()
        .map(|(id, e)| PeerView {
            id: id.clone(),
            online: e.online,
            last_seen_unix: e.last_seen_unix,
            name: name_map.get(id).cloned(),
        })
        .collect();
    out.sort_by(|a, b| {
        b.online
            .cmp(&a.online)
            .then(b.last_seen_unix.cmp(&a.last_seen_unix))
    });
    Ok(out)
}

#[tauri::command]
async fn cmd_get_ticket(
    app: AppHandle,
    state: State<'_, AppState>,
    read_only: Option<bool>,
) -> Result<String, String> {
    let s = {
        let mut inner = state.inner.lock().await;
        let node = inner.node.clone().ok_or("node not ready")?;

        // Create the doc on demand.
        if inner.doc.is_none() {
            let doc = node.docs.create().await.map_err(|e| e.to_string())?;
            inner.doc = Some(doc);
        }
        let doc = inner.doc.clone().unwrap();
        let (mode, opts) = peer::share_opts(read_only.unwrap_or(false));
        let ticket = doc.share(mode, opts).await.map_err(|e| e.to_string())?;
        let s = ticket.to_string();

        inner.config.doc_ticket = Some(s.clone());
        inner.config.namespace_id = Some(doc.id().to_string());
        config::save(&inner.config)
            .await
            .map_err(|e| e.to_string())?;
        s
    };

    // Creating the doc just now may have unblocked sync (folder already set).
    let _ = maybe_start_sync(&app).await;
    Ok(s)
}

#[tauri::command]
async fn cmd_join_with_ticket(
    app: AppHandle,
    state: State<'_, AppState>,
    ticket: String,
) -> Result<(), String> {
    let parsed = DocTicket::from_str(ticket.trim())
        .map_err(|e| format!("that doesn't look like a valid ticket: {e}"))?;
    {
        let mut inner = state.inner.lock().await;
        let node = inner.node.clone().ok_or("node not ready")?;
        let doc = match node.docs.import(parsed).await {
            Ok(d) => d,
            Err(e) => {
                sync::log_line(&inner.log, format!("pair failed: {e}")).await;
                return Err(format!("could not join: {e}"));
            }
        };
        inner.config.namespace_id = Some(doc.id().to_string());
        inner.config.doc_ticket = Some(ticket.trim().to_string());
        inner.doc = Some(doc);
        config::save(&inner.config)
            .await
            .map_err(|e| e.to_string())?;
        sync::log_line(&inner.log, "paired: joined shared folder, connecting…").await;
    }
    // Joining swaps in a different doc. If a sync task was already running —
    // from an earlier pairing, or because this device created its own doc
    // first — it is still bound to the old namespace and will never see the
    // peer we just joined. Stop it so the start below binds to the new doc.
    shutdown(&app).await;
    let started = maybe_start_sync(&app).await.unwrap_or(false);
    if !started {
        // Doc is set but sync didn't start — almost always "no folder yet".
        let inner = state.inner.lock().await;
        if inner.config.folder.is_none() {
            return Err("joined — now choose a folder to sync".into());
        }
    }
    Ok(())
}

#[tauri::command]
async fn cmd_choose_folder(
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<Option<String>, String> {
    let chosen = app
        .dialog()
        .file()
        .set_title("Pick a folder to sync")
        .blocking_pick_folder();
    let Some(path) = chosen else {
        return Ok(None);
    };
    let pb: PathBuf = path
        .into_path()
        .map_err(|e| format!("path conversion failed: {e}"))?;
    {
        let mut inner = state.inner.lock().await;
        inner.config.folder = Some(pb.clone());
        // Reload .syncboxignore for the new folder.
        match IgnoreSet::load(&pb) {
            Ok(s) => inner.ignores = Some(Arc::new(s)),
            Err(e) => tracing::warn!(error = ?e, "could not load ignore set"),
        }
        config::save(&inner.config)
            .await
            .map_err(|e| e.to_string())?;
    }
    let _ = maybe_start_sync(&app).await;
    Ok(Some(pb.display().to_string()))
}

#[tauri::command]
async fn cmd_open_folder(app: AppHandle, _state: State<'_, AppState>) -> Result<(), String> {
    cmd_open_folder_inner(&app).await.map_err(|e| e.to_string())
}

async fn cmd_open_folder_inner(app: &AppHandle) -> Result<()> {
    let state: State<AppState> = app.state();
    let inner = state.inner.lock().await;
    let Some(path) = inner.config.folder.clone() else {
        return Ok(());
    };
    drop(inner);
    if let Err(e) = tauri_plugin_opener::reveal_item_in_dir(&path) {
        tracing::warn!(error = ?e, "reveal_item_in_dir failed; trying open_path");
        tauri_plugin_opener::open_path(path.display().to_string(), None::<&str>)?;
    }
    Ok(())
}

#[tauri::command]
fn cmd_set_autostart(app: AppHandle, enabled: bool) -> Result<(), String> {
    let mgr = app.autolaunch();
    if enabled {
        mgr.enable().map_err(|e| e.to_string())
    } else {
        mgr.disable().map_err(|e| e.to_string())
    }
}

#[tauri::command]
fn cmd_get_autostart(app: AppHandle) -> Result<bool, String> {
    app.autolaunch().is_enabled().map_err(|e| e.to_string())
}

#[tauri::command]
async fn cmd_start_sync(app: AppHandle) -> Result<bool, String> {
    maybe_start_sync(&app).await.map_err(|e| e.to_string())
}

// ---------- Short-code pairing (rendezvous server) ----------

#[derive(Debug, Serialize, Clone)]
struct PairCodeView {
    code: String,
    expires_unix: u64,
}

/// Resolve the pair-server URL from config. Thin wrapper around
/// [`pair::resolve_server`] (env var > config.json > built-in default).
fn pair_server_url(cfg: &config::Config) -> String {
    pair::resolve_server(cfg.pair_server_url.as_deref())
}

#[tauri::command]
async fn cmd_get_pair_server(state: State<'_, AppState>) -> Result<String, String> {
    let inner = state.inner.lock().await;
    Ok(pair_server_url(&inner.config))
}

#[tauri::command]
async fn cmd_set_pair_server(state: State<'_, AppState>, url: String) -> Result<(), String> {
    let trimmed = url.trim().to_string();
    let mut inner = state.inner.lock().await;
    inner.config.pair_server_url = if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    };
    config::save(&inner.config).await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn cmd_make_code(
    app: AppHandle,
    state: State<'_, AppState>,
    read_only: Option<bool>,
) -> Result<PairCodeView, String> {
    // First, make sure we have a doc + ticket, exactly as cmd_get_ticket does.
    let (server, ticket_str) = {
        let mut inner = state.inner.lock().await;
        let node = inner.node.clone().ok_or("node not ready")?;
        if inner.doc.is_none() {
            let doc = node.docs.create().await.map_err(|e| e.to_string())?;
            inner.doc = Some(doc);
        }
        let doc = inner.doc.clone().unwrap();
        let (mode, opts) = peer::share_opts(read_only.unwrap_or(false));
        let ticket = doc.share(mode, opts).await.map_err(|e| e.to_string())?;
        let s = ticket.to_string();
        inner.config.namespace_id = Some(doc.id().to_string());
        inner.config.doc_ticket = Some(s.clone());
        config::save(&inner.config)
            .await
            .map_err(|e| e.to_string())?;
        (pair_server_url(&inner.config), s)
    };

    // The doc now exists — if a folder was already chosen, this starts the
    // sync task. Without it, the code-issuing device never publishes files.
    let _ = maybe_start_sync(&app).await;

    let pc = pair::create_code(&server, &ticket_str)
        .await
        .map_err(|e| e.to_string())?;
    Ok(PairCodeView {
        code: pc.code,
        expires_unix: pc.expires_unix,
    })
}

#[tauri::command]
async fn cmd_use_code(
    app: AppHandle,
    state: State<'_, AppState>,
    code: String,
) -> Result<(), String> {
    let server = {
        let inner = state.inner.lock().await;
        pair_server_url(&inner.config)
    };
    let ticket = pair::redeem_code(&server, &code)
        .await
        .map_err(|e| e.to_string())?;
    cmd_join_with_ticket(app, state, ticket).await
}

// ---------- Misc commands ----------

#[tauri::command]
async fn cmd_get_storage_size(_state: State<'_, AppState>) -> Result<u64, String> {
    let root = config::iroh_root().map_err(|e| e.to_string())?;
    Ok(dir_size(&root))
}

fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    let walker = walkdir::WalkDir::new(path).follow_links(false);
    for entry in walker.into_iter().flatten() {
        if let Ok(meta) = entry.metadata() {
            if meta.is_file() {
                total = total.saturating_add(meta.len());
            }
        }
    }
    total
}

#[tauri::command]
async fn cmd_block_peer(state: State<'_, AppState>, id: String) -> Result<(), String> {
    let id = id.trim().to_string();
    if id.is_empty() {
        return Err("empty id".into());
    }
    let mut inner = state.inner.lock().await;
    {
        let mut blocked = inner.blocked.lock().await;
        blocked.insert(id.clone());
    }
    if !inner.config.blocked_peers.contains(&id) {
        inner.config.blocked_peers.push(id);
        config::save(&inner.config)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[tauri::command]
async fn cmd_unblock_peer(state: State<'_, AppState>, id: String) -> Result<(), String> {
    let id = id.trim().to_string();
    let mut inner = state.inner.lock().await;
    {
        let mut blocked = inner.blocked.lock().await;
        blocked.remove(&id);
    }
    inner.config.blocked_peers.retain(|x| x != &id);
    config::save(&inner.config)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
async fn cmd_list_blocked(state: State<'_, AppState>) -> Result<Vec<String>, String> {
    let inner = state.inner.lock().await;
    Ok(inner.config.blocked_peers.clone())
}

#[tauri::command]
async fn cmd_set_read_only(
    app: AppHandle,
    state: State<'_, AppState>,
    enabled: bool,
) -> Result<(), String> {
    {
        let mut inner = state.inner.lock().await;
        if inner.config.read_only_local == enabled {
            return Ok(());
        }
        inner.config.read_only_local = enabled;
        config::save(&inner.config)
            .await
            .map_err(|e| e.to_string())?;
    }
    // Restart sync so the new value takes effect.
    restart_sync(&app).await.map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
async fn cmd_get_read_only(state: State<'_, AppState>) -> Result<bool, String> {
    let inner = state.inner.lock().await;
    Ok(inner.config.read_only_local)
}

/// Write a sane default `.syncboxignore` into the synced folder if none exists.
#[tauri::command]
async fn cmd_write_default_ignore(state: State<'_, AppState>) -> Result<bool, String> {
    let folder = {
        let inner = state.inner.lock().await;
        inner.config.folder.clone()
    };
    let Some(folder) = folder else {
        return Err("no folder set".into());
    };
    let p = folder.join(".syncboxignore");
    if p.exists() {
        return Ok(false);
    }
    let body = "# syncbox ignore patterns — gitignore syntax\n\
                # See https://git-scm.com/docs/gitignore for full syntax.\n\
                #\n\
                # Built-in defaults already cover common cases (.git, node_modules,\n\
                # .DS_Store, target/, .venv, *.swp, *.tmp). Add your own below.\n\
                \n";
    tokio::fs::write(&p, body)
        .await
        .map_err(|e| e.to_string())?;
    Ok(true)
}

async fn restart_sync(app: &AppHandle) -> Result<()> {
    shutdown(app).await;
    let _ = maybe_start_sync(app).await?;
    Ok(())
}

#[tauri::command]
async fn cmd_get_transfer_stats(state: State<'_, AppState>) -> Result<sync::TransferStats, String> {
    let inner = state.inner.lock().await;
    let mut s = inner.stats.lock().await.clone();
    // Decay a stale rate: if nothing landed in the last ~3s, call it idle.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    if s.last_update_ms == 0 || now.saturating_sub(s.last_update_ms) > 3000 {
        s.down_rate = 0.0;
    }
    Ok(s)
}
