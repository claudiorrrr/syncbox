//! syncbox — simple Mac folder sync, built on iroh.
//!
//! This crate is just the macOS menu-bar front-end. The sync engine lives
//! in `syncbox-core` and is shared with the headless CLI.

use anyhow::Result;
use iroh_docs::{api::Doc, AuthorId, DocTicket, NamespaceId};
use serde::Serialize;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
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
#[cfg(target_os = "macos")]
use tauri::ActivationPolicy;
use tauri_plugin_autostart::{MacosLauncher, ManagerExt};
use tauri_plugin_dialog::DialogExt;
use tokio::sync::{mpsc, watch, Mutex};

/// Per-folder runtime handles. The `folders` Vec runs parallel to
/// `config.folders` — entry `i` here is the live state for `config.folders[i]`.
/// Created at bootstrap and when a folder is added; dropped on removal.
#[derive(Default)]
struct FolderRuntime {
    doc: Option<Doc>,
    echo: EchoGuard,
    peers: PeerMap,
    names: NameMap,
    /// Cached IgnoreSet for this folder, from its `.syncboxignore`.
    ignores: Option<Arc<IgnoreSet>>,
    active: Arc<AtomicU32>,
    stats: StatsHandle,
    status: StatusLine,
    shutdown: Option<watch::Sender<bool>>,
    sync_handle: Option<tauri::async_runtime::JoinHandle<()>>,
}

/// All the runtime handles the Tauri commands need to touch. Kept behind a
/// single Mutex because most operations are user-initiated and rare.
#[derive(Default)]
struct Inner {
    config: config::Config,
    node: Option<Arc<Node>>,
    author: Option<AuthorId>,
    /// One runtime per synced folder, index-aligned with `config.folders`.
    folders: Vec<FolderRuntime>,
    /// Stop-syncing list — device-global, applies to every folder.
    blocked: Arc<Mutex<HashSet<String>>>,
    /// Debug log ring buffer — device-global, shared by all folders.
    log: LogHandle,
}

impl Inner {
    fn folder_mut(&mut self, idx: usize) -> Option<&mut FolderRuntime> {
        self.folders.get_mut(idx)
    }
}

pub struct AppState {
    inner: Mutex<Inner>,
    /// The tray "Check for Updates…" item, kept so a background update check
    /// can relabel it to "Update to vX available". Filled once `build_tray`
    /// runs; a plain std Mutex since `set_text` is sync and rare.
    update_item: std::sync::Mutex<Option<MenuItem<tauri::Wry>>>,
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
    /// Endpoint id of the folder's owner (first device to share it), and
    /// whether this device is that owner.
    owner_id: Option<String>,
    is_owner: bool,
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
            update_item: std::sync::Mutex::new(None),
        })
        .setup(|app| {
            // Register the standard app menu so Cmd+C/V/X/A work in webview
            // inputs. Without an explicit menu, macOS never wires the system
            // shortcuts to the focused field — paste into the join-code box or
            // the ticket textarea silently does nothing.
            let menu = Menu::default(app.handle())?;
            app.set_menu(menu)?;

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
                    // Aggregate over every folder: the tray icon shows the
                    // busiest state across all of them.
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    let mut configured = false;
                    let mut n = 0u32;
                    let mut pending = 0u32;
                    let mut recent = false;
                    for (i, rt) in inner.folders.iter().enumerate() {
                        n += rt.active.load(std::sync::atomic::Ordering::Relaxed);
                        let has_path = inner
                            .config
                            .folders
                            .get(i)
                            .and_then(|f| f.path.as_ref())
                            .is_some();
                        if rt.doc.is_some() && has_path && rt.sync_handle.is_some() {
                            configured = true;
                        }
                        // last_activity_ms is bumped on every upload and
                        // download; the 1200 ms window keeps the animation
                        // visible after brief bursts. pending_downloads covers
                        // the silent stretch while iroh-blobs pulls content.
                        let s = rt.stats.lock().await;
                        if s.last_activity_ms != 0 && now.saturating_sub(s.last_activity_ms) < 1200
                        {
                            recent = true;
                        }
                        pending += s.pending_downloads;
                    }
                    drop(inner);

                    let tray_state = if !configured {
                        TrayState::NotSetup
                    } else if n > 0 || pending > 0 || recent {
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

            // Persist peer last-seen times every minute, so the window can
            // tell a device that's been gone for a week from one just
            // briefly offline — in-memory state alone resets on restart.
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    let state: State<AppState> = handle.state();
                    let mut inner = state.inner.lock().await;
                    // Union of online peers across every folder.
                    let peer_maps: Vec<PeerMap> =
                        inner.folders.iter().map(|f| f.peers.clone()).collect();
                    let mut online: Vec<String> = Vec::new();
                    for pm in &peer_maps {
                        for (id, e) in pm.lock().await.iter() {
                            if e.online && !online.contains(id) {
                                online.push(id.clone());
                            }
                        }
                    }
                    if online.is_empty() {
                        continue;
                    }
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    for id in online {
                        inner.config.peer_last_seen.insert(id, now);
                    }
                    if let Err(e) = config::save(&inner.config).await {
                        tracing::warn!(error = ?e, "persist peer_last_seen failed");
                    }
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
            cmd_list_folders,
            cmd_pick_folder,
            cmd_remove_folder,
            cmd_open_folder,
            cmd_set_autostart,
            cmd_get_autostart,
            cmd_start_sync,
            cmd_get_storage_size,
            cmd_set_update_available,
            cmd_set_hide_dock_icon,
            cmd_get_hide_dock_icon,
            cmd_block_peer,
            cmd_get_device_name,
            cmd_set_device_name,
            cmd_set_read_only,
            cmd_get_read_only,
            cmd_write_default_ignore,
            cmd_get_transfer_stats,
        ])
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app, event| {
            // Dock-icon click (or any reopen request from macOS) surfaces the
            // window. The app stays in the tray after the window is closed, so
            // without this the user has no way to bring it back via the Dock.
            #[cfg(target_os = "macos")]
            if let tauri::RunEvent::Reopen { .. } = event {
                if let Some(win) = app.get_webview_window("main") {
                    let _ = win.show();
                    let _ = win.set_focus();
                }
            }
            #[cfg(not(target_os = "macos"))]
            {
                let _ = (app, event);
            }
        });
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

/// Re-open a folder's doc on cold start. Tries `open(namespace_id)` first
/// (cheap, attaches to the local replica); falls back to `import(ticket)` for
/// the device joining for the first time. Updates `folder.namespace_id` if the
/// import path discovered it — the caller is responsible for saving config.
async fn open_doc(node: &Node, folder: &mut config::FolderConfig) -> Option<Doc> {
    if let Some(id_str) = &folder.namespace_id {
        if let Ok(id) = NamespaceId::from_str(id_str) {
            match node.docs.open(id).await {
                Ok(Some(d)) => return Some(d),
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
                    return Some(d);
                }
                Err(e) => tracing::warn!(error = ?e, "doc import failed"),
            }
        }
    }
    None
}

async fn bootstrap(app: &AppHandle) -> Result<()> {
    let state: State<AppState> = app.state();
    let mut cfg = config::load().await?;

    // First run / setup not finished: the window is hidden by default (tray
    // app), but with nothing set up the user can't tell syncbox is running.
    // Surface the window. A configured install stays quietly in the tray.
    if cfg.folders.iter().all(|f| f.path.is_none()) {
        if let Some(win) = app.get_webview_window("main") {
            let _ = win.show();
            let _ = win.set_focus();
        }
    }

    // Apply the Dock-icon preference (menu-bar-only when hidden).
    apply_dock_policy(app, cfg.hide_dock_icon);

    let iroh_root = config::iroh_root()?;
    let node = Arc::new(Node::spawn(&iroh_root).await?);

    // The device's stable doc author, persisted by iroh-docs across restarts.
    // Must not rotate: doc.del's prefix delete is author-scoped, so a fresh
    // author each run cannot delete folders an earlier run published.
    let author = node.docs.author_default().await?;

    // Build one runtime per synced folder: load its ignore set, re-open its
    // doc, and rejoin known peers so the first connection skips discovery.
    let mut runtimes: Vec<FolderRuntime> = Vec::with_capacity(cfg.folders.len());
    for fc in &mut cfg.folders {
        let mut rt = FolderRuntime::default();
        if let Some(path) = &fc.path {
            match IgnoreSet::load(path) {
                Ok(s) => rt.ignores = Some(Arc::new(s)),
                Err(e) => tracing::warn!(error = ?e, "could not load ignore set"),
            }
        }
        if let Some(doc) = open_doc(&node, fc).await {
            if !cfg.known_peers.is_empty() {
                if let Err(e) = doc.start_sync(cfg.known_peers.clone()).await {
                    tracing::warn!(error = ?e, "start_sync with known peers failed");
                }
            }
            rt.doc = Some(doc);
        }
        runtimes.push(rt);
    }
    // `open_doc` may have discovered namespace ids via import; persist them
    // (this also writes back any legacy single-folder config migration).
    if let Err(e) = config::save(&cfg).await {
        tracing::warn!(error = ?e, "save config failed");
    }

    {
        let mut inner = state.inner.lock().await;
        inner.node = Some(node);
        inner.author = Some(author);
        inner.folders = runtimes;
        {
            let mut blocked = inner.blocked.lock().await;
            blocked.extend(cfg.blocked_peers.iter().cloned());
        }
        inner.config = cfg;
    }

    // Kick off sync for every folder that's ready.
    maybe_start_all(app).await;
    Ok(())
}

fn build_tray(app: AppHandle) -> Result<()> {
    let pair_item = MenuItem::with_id(&app, "pair", "Open syncbox", true, None::<&str>)?;
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

    // Keep the update item so a background update check can relabel it.
    app.state::<AppState>()
        .update_item
        .lock()
        .unwrap()
        .replace(update_item.clone());

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
                    let _ = cmd_open_folder_inner(&app_clone, 0).await;
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
                    shutdown_all(&app_clone).await;
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

/// Stop the sync task for one folder, if it has one running.
async fn shutdown_folder(app: &AppHandle, idx: usize) {
    let state: State<AppState> = app.state();
    let (tx, handle) = {
        let mut inner = state.inner.lock().await;
        match inner.folder_mut(idx) {
            Some(rt) => (rt.shutdown.take(), rt.sync_handle.take()),
            None => (None, None),
        }
    };
    if let Some(tx) = tx {
        let _ = tx.send(true);
    }
    if let Some(h) = handle {
        let _ = h.await;
    }
}

/// Stop every folder's sync task — used on quit.
async fn shutdown_all(app: &AppHandle) {
    let count = {
        let state: State<AppState> = app.state();
        let n = state.inner.lock().await.folders.len();
        n
    };
    for idx in 0..count {
        shutdown_folder(app, idx).await;
    }
}

/// Start every folder's sync task that's ready and not already running.
async fn maybe_start_all(app: &AppHandle) {
    let count = {
        let state: State<AppState> = app.state();
        let n = state.inner.lock().await.folders.len();
        n
    };
    for idx in 0..count {
        if let Err(e) = maybe_start_folder(app, idx).await {
            tracing::warn!(error = ?e, idx, "auto-start sync failed");
        }
    }
}

/// Start the sync task for folder `idx` if (a) we have a node, (b) the folder
/// has a doc, (c) the folder has a local path, and (d) it isn't already running.
async fn maybe_start_folder(app: &AppHandle, idx: usize) -> Result<bool> {
    let state: State<AppState> = app.state();
    let mut inner = state.inner.lock().await;

    let Some(node) = inner.node.clone() else {
        return Ok(false);
    };
    let Some(author) = inner.author else {
        return Ok(false);
    };
    let Some(fc) = inner.config.folders.get(idx).cloned() else {
        return Ok(false);
    };
    let Some(folder) = fc.path.clone() else {
        return Ok(false);
    };
    let Some(rt) = inner.folders.get(idx) else {
        return Ok(false);
    };
    if rt.sync_handle.is_some() {
        return Ok(false);
    }
    let Some(doc) = rt.doc.clone() else {
        return Ok(false);
    };

    // Make sure we have an ignore matcher loaded for this folder.
    if inner.folders[idx].ignores.is_none() {
        match IgnoreSet::load(&folder) {
            Ok(s) => inner.folders[idx].ignores = Some(Arc::new(s)),
            Err(e) => return Err(anyhow::anyhow!("ignore set: {e}")),
        }
    }

    let rt = &inner.folders[idx];
    let ignores = rt.ignores.clone().unwrap();
    let echo = rt.echo.clone();
    let peers = rt.peers.clone();
    let names = rt.names.clone();
    let active = rt.active.clone();
    let stats = rt.stats.clone();
    let status = rt.status.clone();
    let blocked = inner.blocked.clone();
    let log = inner.log.clone();
    let read_only = inner.config.read_only_local || fc.read_only;
    let host = inner.config.display_name();

    let (tx, rx) = watch::channel(false);

    // Channel that sync.rs uses to publish freshly-seen peer addresses; the
    // consumer below dedupes them into the persistent config.
    let (addr_tx, mut addr_rx) = mpsc::unbounded_channel::<iroh::EndpointAddr>();

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
        fp_cache: Default::default(),
    };
    let handle = tauri::async_runtime::spawn(async move {
        if let Err(e) = sync::run(st, rx).await {
            tracing::error!(error = ?e, "sync task exited with error");
        }
    });
    inner.folders[idx].shutdown = Some(tx);
    inner.folders[idx].sync_handle = Some(handle);
    drop(inner);

    // Spawn the persister: dedupe addr_rx into config.known_peers (device-
    // global) and save when a new one arrives. Runs only on neighbor changes.
    let app_clone = app.clone();
    tauri::async_runtime::spawn(async move {
        while let Some(addr) = addr_rx.recv().await {
            let state: State<AppState> = app_clone.state();
            let mut inner = state.inner.lock().await;
            let pos = inner
                .config
                .known_peers
                .iter()
                .position(|a| a.id == addr.id);
            match pos {
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

/// A folder as shown in the window's folder list.
#[derive(Debug, Serialize, Clone)]
struct FolderView {
    index: usize,
    /// Absolute path, or null if joined but not yet placed on disk.
    path: Option<String>,
    /// Display name — the folder's basename, or a placeholder.
    name: String,
    has_doc: bool,
    syncing: bool,
    online_peers: usize,
}

/// Returns the existing synced path that overlaps `candidate`, if any. Two
/// folders overlap when one is an ancestor of (or equal to) the other —
/// running two watchers over the same files makes them fight each other.
/// `skip_idx` excludes the folder being edited so setting a path doesn't
/// flag itself.
fn overlapping_folder(
    existing: &[config::FolderConfig],
    candidate: &Path,
    skip_idx: Option<usize>,
) -> Option<PathBuf> {
    let cand = std::fs::canonicalize(candidate).unwrap_or_else(|_| candidate.to_path_buf());
    for (i, fc) in existing.iter().enumerate() {
        if Some(i) == skip_idx {
            continue;
        }
        let Some(p) = fc.path.as_ref() else {
            continue;
        };
        let ep = std::fs::canonicalize(p).unwrap_or_else(|_| p.clone());
        if cand == ep || cand.starts_with(&ep) || ep.starts_with(&cand) {
            return Some(ep);
        }
    }
    None
}

/// The folder's basename for display, or a placeholder when it has no path.
fn folder_display_name(path: Option<&PathBuf>) -> String {
    match path {
        Some(p) => p
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| p.display().to_string()),
        None => "New folder".to_string(),
    }
}

#[tauri::command]
async fn cmd_list_folders(state: State<'_, AppState>) -> Result<Vec<FolderView>, String> {
    let inner = state.inner.lock().await;
    let mut out = Vec::with_capacity(inner.config.folders.len());
    for (i, fc) in inner.config.folders.iter().enumerate() {
        let rt = inner.folders.get(i);
        let online_peers = match rt {
            Some(rt) => rt.peers.lock().await.values().filter(|p| p.online).count(),
            None => 0,
        };
        out.push(FolderView {
            index: i,
            path: fc.path.as_ref().map(|p| p.display().to_string()),
            name: folder_display_name(fc.path.as_ref()),
            has_doc: rt.map(|r| r.doc.is_some()).unwrap_or(false),
            syncing: rt.map(|r| r.sync_handle.is_some()).unwrap_or(false),
            online_peers,
        });
    }
    Ok(out)
}

#[tauri::command]
async fn cmd_get_status(state: State<'_, AppState>, idx: usize) -> Result<StatusView, String> {
    let inner = state.inner.lock().await;
    let self_id = inner.node.as_ref().map(|n| n.endpoint.id().to_string());
    let fc = inner.config.folders.get(idx);
    let rt = inner.folders.get(idx);

    let peers_online = match rt {
        Some(rt) => rt.peers.lock().await.values().filter(|p| p.online).count(),
        None => 0,
    };
    // Known = the doc roster (every device that published a name) minus this
    // one — the whole swarm for this folder, not just our live links.
    let peers_known = match rt {
        Some(rt) => rt
            .names
            .lock()
            .await
            .keys()
            .filter(|id| Some(*id) != self_id.as_ref())
            .count(),
        None => 0,
    };
    let message = match rt {
        Some(rt) => rt.status.lock().await.clone(),
        None => String::new(),
    };
    let owner_id = match rt.and_then(|r| r.doc.as_ref()) {
        Some(doc) => sync::owner_id(doc).await.ok().flatten(),
        None => None,
    };
    let is_owner = matches!((&owner_id, &self_id), (Some(o), Some(me)) if o == me);
    let folder_path = fc.and_then(|f| f.path.as_ref());
    let has_doc = rt.map(|r| r.doc.is_some()).unwrap_or(false);
    Ok(StatusView {
        folder: folder_path.map(|p| p.display().to_string()),
        hostname: inner.config.hostname.clone(),
        has_doc,
        has_ticket: fc.and_then(|f| f.doc_ticket.as_ref()).is_some(),
        paired: has_doc && folder_path.is_some(),
        syncing: rt.map(|r| r.sync_handle.is_some()).unwrap_or(false),
        peers_online,
        peers_known,
        message,
        owner_id,
        is_owner,
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
async fn cmd_get_peers(state: State<'_, AppState>, idx: usize) -> Result<Vec<PeerView>, String> {
    let inner = state.inner.lock().await;
    let self_id = inner.node.as_ref().map(|n| n.endpoint.id().to_string());
    let Some(rt) = inner.folders.get(idx) else {
        return Ok(Vec::new());
    };
    let peers = rt.peers.clone();
    let names = rt.names.clone();
    let blocked = inner.blocked.clone();
    let persisted = inner.config.peer_last_seen.clone();
    drop(inner);

    let name_map = names.lock().await.clone();
    let peer_map = peers.lock().await.clone();
    // Devices we've stopped syncing with are hidden entirely — they reappear
    // only if the user pairs again, which clears the block.
    let blocked_set = blocked.lock().await.clone();

    // The list is the whole swarm for this folder, not just our live links.
    // Every device that published a name into the doc is a member, plus
    // anything we have a connection to. A member with no direct link here is
    // shown offline — sync still reaches it transitively through other peers.
    let mut ids: HashSet<String> = name_map.keys().cloned().collect();
    ids.extend(peer_map.keys().cloned());

    let mut out: Vec<PeerView> = ids
        .into_iter()
        .filter(|id| Some(id) != self_id.as_ref())
        .filter(|id| !blocked_set.contains(id))
        .map(|id| {
            let entry = peer_map.get(&id);
            PeerView {
                online: entry.map(|e| e.online).unwrap_or(false),
                last_seen_unix: entry
                    .map(|e| e.last_seen_unix)
                    .unwrap_or(0)
                    .max(persisted.get(&id).copied().unwrap_or(0)),
                name: name_map.get(&id).cloned(),
                id,
            }
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
    idx: usize,
    read_only: Option<bool>,
) -> Result<String, String> {
    let s = {
        let mut inner = state.inner.lock().await;
        let node = inner.node.clone().ok_or("node not ready")?;
        let author = inner.author;
        if inner.config.folders.get(idx).is_none() || inner.folders.get(idx).is_none() {
            return Err("no such folder".into());
        }

        // Create the doc on demand.
        if inner.folders[idx].doc.is_none() {
            let doc = node.docs.create().await.map_err(|e| e.to_string())?;
            // First to create the doc — this device is the folder's owner.
            if let Some(author) = author {
                if let Err(e) = sync::publish_owner(&doc, author, &node).await {
                    tracing::warn!(error = ?e, "publish owner failed");
                }
            }
            inner.folders[idx].doc = Some(doc);
        }
        let doc = inner.folders[idx].doc.clone().unwrap();
        let (mode, opts) = peer::share_opts(read_only.unwrap_or(false));
        let ticket = doc.share(mode, opts).await.map_err(|e| e.to_string())?;
        let s = ticket.to_string();

        inner.config.folders[idx].doc_ticket = Some(s.clone());
        inner.config.folders[idx].namespace_id = Some(doc.id().to_string());
        // Pairing re-authorizes: clear the stop-syncing list so a device
        // stopped earlier can sync again.
        inner.config.blocked_peers.clear();
        inner.blocked.lock().await.clear();
        config::save(&inner.config)
            .await
            .map_err(|e| e.to_string())?;
        s
    };

    // Creating the doc just now may have unblocked sync (folder already set).
    let _ = maybe_start_folder(&app, idx).await;
    Ok(s)
}

#[tauri::command]
async fn cmd_join_with_ticket(
    app: AppHandle,
    state: State<'_, AppState>,
    ticket: String,
) -> Result<usize, String> {
    let parsed = DocTicket::from_str(ticket.trim())
        .map_err(|e| format!("that doesn't look like a valid ticket: {e}"))?;
    let idx = {
        let mut inner = state.inner.lock().await;
        let node = inner.node.clone().ok_or("node not ready")?;
        let doc = match node.docs.import(parsed).await {
            Ok(d) => d,
            Err(e) => {
                sync::log_line(&inner.log, format!("pair failed: {e}")).await;
                return Err(format!("could not join: {e}"));
            }
        };
        let namespace = doc.id().to_string();

        // Already syncing this namespace? Re-use that folder; else add one.
        let existing = inner
            .config
            .folders
            .iter()
            .position(|f| f.namespace_id.as_deref() == Some(namespace.as_str()));
        let idx = match existing {
            Some(i) => {
                inner.config.folders[i].doc_ticket = Some(ticket.trim().to_string());
                inner.folders[i].doc = Some(doc);
                i
            }
            None => {
                inner.config.folders.push(config::FolderConfig {
                    path: None,
                    doc_ticket: Some(ticket.trim().to_string()),
                    namespace_id: Some(namespace),
                    read_only: false,
                });
                inner.folders.push(FolderRuntime {
                    doc: Some(doc),
                    ..Default::default()
                });
                inner.config.folders.len() - 1
            }
        };
        // Pairing re-authorizes: clear the stop-syncing list so a device
        // stopped earlier can sync again.
        inner.config.blocked_peers.clear();
        inner.blocked.lock().await.clear();
        config::save(&inner.config)
            .await
            .map_err(|e| e.to_string())?;
        sync::log_line(&inner.log, "paired: joined shared folder").await;
        idx
    };
    // Re-bind sync to the (possibly new) doc. For a brand-new folder the
    // restart is a no-op and the start does nothing until a path is picked;
    // the GUI then prompts for a location.
    shutdown_folder(&app, idx).await;
    let _ = maybe_start_folder(&app, idx).await;
    Ok(idx)
}

/// Pick a local folder. `idx = Some(i)` sets the path of folder `i` (used
/// after joining, when the folder has no local location yet); `idx = None`
/// adds a brand-new folder. Returns the folder's index, or null if cancelled.
#[tauri::command]
async fn cmd_pick_folder(
    app: AppHandle,
    state: State<'_, AppState>,
    idx: Option<usize>,
) -> Result<Option<usize>, String> {
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
    let ignores = IgnoreSet::load(&pb).ok().map(Arc::new);

    let target = {
        let mut inner = state.inner.lock().await;
        // Reject overlap with any other synced folder — nested or parent —
        // because two overlapping watchers fight over the same files.
        if let Some(conflict) = overlapping_folder(&inner.config.folders, &pb, idx) {
            return Err(format!(
                "Can't sync {} because it overlaps with an already-synced folder ({}). Pick a folder outside the existing one.",
                pb.display(),
                conflict.display(),
            ));
        }
        let target = match idx {
            Some(i) if i < inner.config.folders.len() => {
                inner.config.folders[i].path = Some(pb.clone());
                if let Some(rt) = inner.folders.get_mut(i) {
                    rt.ignores = ignores;
                }
                i
            }
            _ => {
                inner
                    .config
                    .folders
                    .push(config::FolderConfig::for_path(pb.clone()));
                inner.folders.push(FolderRuntime {
                    ignores,
                    ..Default::default()
                });
                inner.config.folders.len() - 1
            }
        };
        config::save(&inner.config)
            .await
            .map_err(|e| e.to_string())?;
        target
    };
    let _ = maybe_start_folder(&app, target).await;
    Ok(Some(target))
}

/// Stop syncing a folder and drop it from the config. Local files are left.
#[tauri::command]
async fn cmd_remove_folder(
    app: AppHandle,
    state: State<'_, AppState>,
    idx: usize,
) -> Result<(), String> {
    shutdown_folder(&app, idx).await;
    let mut inner = state.inner.lock().await;
    if idx >= inner.config.folders.len() {
        return Err("no such folder".into());
    }
    inner.config.folders.remove(idx);
    if idx < inner.folders.len() {
        inner.folders.remove(idx);
    }
    config::save(&inner.config)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
async fn cmd_open_folder(
    app: AppHandle,
    _state: State<'_, AppState>,
    idx: usize,
) -> Result<(), String> {
    cmd_open_folder_inner(&app, idx)
        .await
        .map_err(|e| e.to_string())
}

async fn cmd_open_folder_inner(app: &AppHandle, idx: usize) -> Result<()> {
    let state: State<AppState> = app.state();
    let path = {
        let inner = state.inner.lock().await;
        inner.config.folders.get(idx).and_then(|f| f.path.clone())
    };
    let Some(path) = path else {
        return Ok(());
    };
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
async fn cmd_start_sync(app: AppHandle, idx: usize) -> Result<bool, String> {
    maybe_start_folder(&app, idx)
        .await
        .map_err(|e| e.to_string())
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
    idx: usize,
    read_only: Option<bool>,
) -> Result<PairCodeView, String> {
    // First, make sure the folder has a doc + ticket, as cmd_get_ticket does.
    let (server, ticket_str) = {
        let mut inner = state.inner.lock().await;
        let node = inner.node.clone().ok_or("node not ready")?;
        let author = inner.author;
        if inner.config.folders.get(idx).is_none() || inner.folders.get(idx).is_none() {
            return Err("no such folder".into());
        }
        if inner.folders[idx].doc.is_none() {
            let doc = node.docs.create().await.map_err(|e| e.to_string())?;
            // First to create the doc — this device is the folder's owner.
            if let Some(author) = author {
                if let Err(e) = sync::publish_owner(&doc, author, &node).await {
                    tracing::warn!(error = ?e, "publish owner failed");
                }
            }
            inner.folders[idx].doc = Some(doc);
        }
        let doc = inner.folders[idx].doc.clone().unwrap();
        let (mode, opts) = peer::share_opts(read_only.unwrap_or(false));
        let ticket = doc.share(mode, opts).await.map_err(|e| e.to_string())?;
        let s = ticket.to_string();
        inner.config.folders[idx].namespace_id = Some(doc.id().to_string());
        inner.config.folders[idx].doc_ticket = Some(s.clone());
        // Pairing re-authorizes: clear the stop-syncing list so a device
        // stopped earlier can sync again.
        inner.config.blocked_peers.clear();
        inner.blocked.lock().await.clear();
        config::save(&inner.config)
            .await
            .map_err(|e| e.to_string())?;
        (pair_server_url(&inner.config), s)
    };

    // The doc now exists — if a folder path was already chosen, this starts
    // the sync task. Without it, the code-issuing device publishes no files.
    let _ = maybe_start_folder(&app, idx).await;

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
) -> Result<usize, String> {
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
    // Total size of every synced folder — the content the user actually keeps.
    // Not the iroh blob store: that holds a second, content-addressed copy of
    // every blob and would over-report (badly so before GC reclaims orphans).
    let cfg = config::load().await.map_err(|e| e.to_string())?;
    let mut total = 0u64;
    for f in &cfg.folders {
        if let Some(path) = &f.path {
            total = total.saturating_add(dir_size(path));
        }
    }
    Ok(total)
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

/// Relabel the tray "Check for Updates…" item. `Some(version)` →
/// "Update to vX available"; `None` → reset. The webview calls this after a
/// background update check, so the menu-bar surface nudges without a modal.
#[tauri::command]
fn cmd_set_update_available(state: State<'_, AppState>, version: Option<String>) {
    if let Ok(guard) = state.update_item.lock() {
        if let Some(item) = guard.as_ref() {
            let text = match version {
                Some(v) => format!("Update to {v} available"),
                None => "Check for Updates…".to_string(),
            };
            let _ = item.set_text(text);
        }
    }
}

/// Apply the Dock-icon preference. `Accessory` = menu-bar-only, no Dock icon;
/// `Regular` = normal app with a Dock icon. No-op on non-macOS — the Dock is a
/// macOS concept, tray-only apps on other platforms simply hide the window.
#[cfg(target_os = "macos")]
fn apply_dock_policy(app: &AppHandle, hide: bool) {
    let policy = if hide {
        ActivationPolicy::Accessory
    } else {
        ActivationPolicy::Regular
    };
    if let Err(e) = app.set_activation_policy(policy) {
        tracing::warn!(error = ?e, "set activation policy failed");
    }
}

#[cfg(not(target_os = "macos"))]
fn apply_dock_policy(_app: &AppHandle, _hide: bool) {}

#[tauri::command]
async fn cmd_set_hide_dock_icon(
    app: AppHandle,
    state: State<'_, AppState>,
    enabled: bool,
) -> Result<(), String> {
    {
        let mut inner = state.inner.lock().await;
        inner.config.hide_dock_icon = enabled;
        config::save(&inner.config)
            .await
            .map_err(|e| e.to_string())?;
    }
    apply_dock_policy(&app, enabled);
    Ok(())
}

#[tauri::command]
async fn cmd_get_hide_dock_icon(state: State<'_, AppState>) -> Result<bool, String> {
    Ok(state.inner.lock().await.config.hide_dock_icon)
}

/// Stop syncing with a device: ignore its changes, hide it from the list, and
/// forget its address. The block is undone by pairing again (see the pairing
/// commands, which clear the block list).
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
        inner.config.blocked_peers.push(id.clone());
    }
    // Forget the device's saved address so a later restart doesn't dial it.
    inner.config.known_peers.retain(|p| p.id.to_string() != id);
    config::save(&inner.config)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// The name shown to paired devices: the user-chosen name, or the hostname.
#[tauri::command]
async fn cmd_get_device_name(state: State<'_, AppState>) -> Result<String, String> {
    let inner = state.inner.lock().await;
    Ok(inner.config.display_name())
}

/// Rename this device. An empty name clears the custom name and falls back to
/// the hostname. Returns the resolved name actually in effect. The new name is
/// re-published to the doc so paired devices pick it up without a restart.
#[tauri::command]
async fn cmd_set_device_name(state: State<'_, AppState>, name: String) -> Result<String, String> {
    let mut inner = state.inner.lock().await;
    let trimmed = name.trim();
    inner.config.device_name = if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    };
    config::save(&inner.config)
        .await
        .map_err(|e| e.to_string())?;
    let display = inner.config.display_name();

    // Re-publish the name into every folder's doc so paired devices on each
    // shared folder pick it up without a restart.
    if let (Some(author), Some(node)) = (inner.author, inner.node.clone()) {
        for rt in &inner.folders {
            if let Some(doc) = rt.doc.clone() {
                let names = rt.names.clone();
                if let Err(e) = sync::publish_name(&doc, author, &node, &names, &display).await {
                    tracing::warn!(error = ?e, "re-publish device name failed");
                }
            }
        }
    }
    Ok(display)
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
    // Restart every folder's sync so the new value takes effect.
    restart_all(&app).await;
    Ok(())
}

#[tauri::command]
async fn cmd_get_read_only(state: State<'_, AppState>) -> Result<bool, String> {
    let inner = state.inner.lock().await;
    Ok(inner.config.read_only_local)
}

/// Write a sane default `.syncboxignore` into a synced folder if none exists.
#[tauri::command]
async fn cmd_write_default_ignore(state: State<'_, AppState>, idx: usize) -> Result<bool, String> {
    let folder = {
        let inner = state.inner.lock().await;
        inner.config.folders.get(idx).and_then(|f| f.path.clone())
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

/// Stop and restart sync for every folder.
async fn restart_all(app: &AppHandle) {
    shutdown_all(app).await;
    maybe_start_all(app).await;
}

#[tauri::command]
async fn cmd_get_transfer_stats(
    state: State<'_, AppState>,
    idx: usize,
) -> Result<sync::TransferStats, String> {
    let inner = state.inner.lock().await;
    let Some(rt) = inner.folders.get(idx) else {
        return Ok(sync::TransferStats::default());
    };
    let mut s = rt.stats.lock().await.clone();
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
