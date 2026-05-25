// Frontend glue for the syncbox tray window.
//
// All persistent state lives in the Rust side. This file mirrors what the
// backend reports and calls back into it. A device can sync several folders;
// the window shows one folder at a time and switches between them. The four
// views — add-a-folder, running, add-a-device, settings — show one at a time.

const { invoke } = window.__TAURI__.core;
const el = (id) => document.getElementById(id);

const els = {
  updateBanner: el("update-banner"),
  updateBannerText: el("update-banner-text"),
  btnUpdateInstall: el("btn-update-install"),

  btnBack: el("btn-back"),
  btnSettings: el("btn-settings"),

  folderStep: el("folder-step"),
  folderTitle: el("folder-title"),
  folderChoice: el("folder-choice"),
  btnChooseFolder: el("btn-choose-folder"),
  btnShowJoin: el("btn-show-join"),
  joinCode: el("join-code"),
  btnUseCode: el("btn-use-code"),
  pairResult: el("pair-result"),

  statusIcon: el("status-icon"),
  statusLine: el("status-line"),
  statusSub: el("status-sub"),
  runFolder: el("run-folder"),
  folderSwitch: el("folder-switch"),
  runOwner: el("run-owner"),
  deviceList: el("device-list"),
  noDevices: el("no-devices"),
  btnAddDevice: el("btn-add-device"),
  btnAddFolderRun: el("btn-add-folder-run"),

  adddeviceFolder: el("adddevice-folder"),
  ourCode: el("our-code"),
  codeExpiry: el("code-expiry"),
  btnMakeCode: el("btn-make-code"),
  btnCopyCode: el("btn-copy-code"),
  ticketReadOnly: el("ticket-read-only"),

  deviceName: el("device-name"),
  folderList: el("folder-list"),
  btnAddFolder: el("btn-add-folder"),
  autostart: el("autostart"),
  hideDock: el("hide-dock"),
  readOnlyLocal: el("read-only-local"),
  btnWriteIgnore: el("btn-write-ignore"),
  storageLine: el("storage-line"),
  ourTicket: el("our-ticket"),
  btnMakeTicket: el("btn-make-ticket"),
  btnCopyTicket: el("btn-copy-ticket"),
  joinTicket: el("join-ticket"),
  btnJoin: el("btn-join"),
  pairResultAdv: el("pair-result-adv"),
  pairServer: el("pair-server"),
  btnSavePairServer: el("btn-save-pair-server"),
  debugLog: el("debug-log"),
  btnCopyLog: el("btn-copy-log"),
  appVersion: el("app-version"),
};

function formatBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`;
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`;
}

// ---------- state ----------

const views = {
  folder: el("view-folder"),
  join: el("view-join"),
  running: el("view-running"),
  adddevice: el("view-adddevice"),
  settings: el("view-settings"),
};

// The list of synced folders (from cmd_list_folders) and which one the window
// is currently showing.
let folders = [];
let current = 0;

// 'settings' / 'adddevice' / 'addfolder' when the user navigated there;
// null = let the folder list decide (setup vs running).
let viewOverride = null;
let lastView = null;
let lastStatus = null;
let lastXfer = null;

function pickView(s) {
  if (viewOverride === "settings") return "settings";
  if (viewOverride === "addfolder") return "folder";
  if (viewOverride === "join") return "join";
  if (viewOverride === "adddevice") return "adddevice";
  if (folders.length === 0) return "folder";
  return "running";
}

function render(s) {
  const v = pickView(s);
  for (const name in views) views[name].hidden = name !== v;

  // Header buttons keep their slot (visibility, not display) so the title
  // stays centered. Settings gear only on the running view; Back whenever
  // the user navigated into a screen they can leave.
  els.btnSettings.classList.toggle("invisible", v !== "running");
  const showBack =
    v === "settings" ||
    v === "adddevice" ||
    v === "join" ||
    (v === "folder" && viewOverride === "addfolder");
  els.btnBack.classList.toggle("invisible", !showBack);

  if (v === "folder") {
    // "Step 1" only on the very first run (no folders yet).
    els.folderStep.hidden = folders.length !== 0;
    els.folderTitle.textContent =
      viewOverride === "addfolder" ? "Sync another folder" : "Sync a folder";
  }
  if (v === "adddevice") {
    const f = folders[current];
    els.adddeviceFolder.textContent = f ? f.name : "this folder";
  }
  lastView = v;
}

els.btnSettings.addEventListener("click", () => {
  viewOverride = "settings";
  render(lastStatus);
});
els.btnBack.addEventListener("click", () => {
  // From "Join" pop back to the choice screen, not all the way out — the
  // user opened Join from there, not from running.
  viewOverride = viewOverride === "join" ? "addfolder" : null;
  render(lastStatus);
});
els.btnAddDevice.addEventListener("click", () => {
  viewOverride = "adddevice";
  els.ourCode.textContent = "— — —";
  els.codeExpiry.textContent = "";
  render(lastStatus);
});
function goAddFolder() {
  viewOverride = "addfolder";
  render(lastStatus);
}
els.btnAddFolder.addEventListener("click", goAddFolder);
els.btnAddFolderRun.addEventListener("click", goAddFolder);

els.folderSwitch.addEventListener("change", () => {
  const i = parseInt(els.folderSwitch.value, 10);
  if (!Number.isNaN(i)) {
    current = i;
    refresh();
  }
});

// ---------- status ----------

async function refresh() {
  try {
    folders = await invoke("cmd_list_folders");
  } catch (e) {
    els.statusLine.textContent = `error: ${e}`;
    return;
  }
  if (current >= folders.length) current = Math.max(0, folders.length - 1);

  let s = null;
  if (folders.length > 0) {
    try {
      s = await invoke("cmd_get_status", { idx: current });
    } catch {
      /* keep last */
    }
  }
  lastStatus = s;
  if (s && s.version) els.appVersion.textContent = `v${s.version}`;

  render(s);
  renderFolderSwitch();
  renderFolderListSettings();
  if (s) {
    await refreshDevices(s);
    await refreshTransfer();
    updateHero(s, lastXfer);
  }
  await refreshStorage();
}

// The folder name in the running view doubles as a switcher once there are
// two or more folders; with one it's plain text, exactly as a single-folder
// install looked before.
function renderFolderSwitch() {
  if (folders.length >= 2) {
    els.runFolder.hidden = true;
    els.folderSwitch.hidden = false;
    els.folderSwitch.innerHTML = "";
    for (const f of folders) {
      const o = document.createElement("option");
      o.value = String(f.index);
      o.textContent = f.name;
      if (f.index === current) o.selected = true;
      els.folderSwitch.appendChild(o);
    }
  } else {
    els.folderSwitch.hidden = true;
    els.runFolder.hidden = false;
    els.runFolder.textContent = folders[current] ? folders[current].name : "folder";
  }
}

function updateHero(s, xfer) {
  const dl = xfer ? xfer.active_downloads || 0 : 0;
  let icon = "✓";
  let cls = "ok";
  let line = "Up to date";
  let sub = "";
  if (!s.peers_known) {
    icon = "◌";
    cls = "warn";
    line = "No devices yet";
    sub = "Invite a device to start syncing this folder.";
  } else if (dl > 0) {
    icon = "↻";
    cls = "sync";
    line = `Syncing ${dl} file${dl === 1 ? "" : "s"}…`;
    sub = xfer && xfer.down_rate > 0 ? `${formatBytes(xfer.down_rate)}/s` : "";
  } else if (!s.peers_online) {
    // Folder-level state, not a per-device light: nothing is reachable right
    // now. Honest, and not alarming — a CRDT converges on reconnect.
    icon = "◌";
    cls = "warn";
    line = "Waiting to connect";
    sub = "Your changes sync as soon as a device is reachable.";
  } else {
    icon = "✓";
    cls = "ok";
    line = "Up to date";
    sub = "";
  }
  els.statusIcon.textContent = icon;
  els.statusIcon.className = `run-icon ${cls}`;
  els.statusLine.textContent = line;
  els.statusSub.textContent = sub;
}

async function refreshDevices(s) {
  els.runOwner.textContent = s.is_owner ? "You own this folder" : "";
  els.runOwner.hidden = !s.is_owner;
  try {
    // The list is the whole swarm — every device that shares this folder.
    // The backend already hides devices we've stopped syncing with.
    const peers = await invoke("cmd_get_peers", { idx: current });
    renderDevices(
      peers.map((p) => ({
        id: p.id,
        label: p.name || `${p.id.slice(0, 10)}…`,
        online: p.online,
        lastSeen: p.last_seen_unix || 0,
      })),
      s.owner_id,
    );
  } catch {
    /* keep last */
  }
}

// green dot = connected now, gray dot = offline, hollow + dimmed = gone for
// over a week (likely a retired device). The owner (first device to share
// the folder) is badged and cannot be removed.
function renderDevices(devices, ownerId) {
  const now = Math.floor(Date.now() / 1000);
  const WEEK = 7 * 86400;
  els.deviceList.innerHTML = "";
  els.noDevices.hidden = devices.length > 0;
  for (const d of devices) {
    const li = document.createElement("li");
    li.className = "device";
    let when = "";
    if (d.online) {
      li.classList.add("online");
    } else if (d.lastSeen > 0 && now - d.lastSeen > WEEK) {
      li.classList.add("stale");
      const days = Math.floor((now - d.lastSeen) / 86400);
      when =
        days >= 14
          ? `last seen ${Math.floor(days / 7)} weeks ago`
          : `last seen ${days} days ago`;
    }
    const dot = document.createElement("span");
    dot.className = "dot";
    const name = document.createElement("span");
    name.className = "dname";
    name.textContent = d.label;
    const meta = document.createElement("span");
    meta.className = "dwhen muted small";
    meta.textContent = when;
    li.append(dot, name, meta);
    if (d.id === ownerId) {
      // The folder owner is badged and has no removal control.
      const badge = document.createElement("span");
      badge.className = "owner-badge";
      badge.textContent = "owner";
      li.append(badge);
    } else {
      // Removal is its own deliberate control — never the row itself, so a
      // stray click on a device name can't drop it.
      const remove = document.createElement("button");
      remove.className = "link tiny dremove";
      remove.textContent = "Stop syncing";
      remove.addEventListener("click", () => stopSyncing(d));
      li.append(remove);
    }
    els.deviceList.appendChild(li);
  }
}

async function stopSyncing(d) {
  // A real native modal — window.confirm() is unreliable inside the Tauri
  // webview, and dropping a device must never happen on a misfired click.
  const ok = await window.__TAURI__.dialog.ask(
    `Stop syncing with "${d.label}"?\n\n` +
      "It leaves the list and its changes are ignored here. " +
      "Pair again to re-add it.",
    {
      title: "Stop syncing",
      kind: "warning",
      okLabel: "Stop syncing",
      cancelLabel: "Cancel",
    },
  );
  if (!ok) return;
  try {
    await invoke("cmd_block_peer", { id: d.id });
    refresh();
  } catch (e) {
    alert(`Could not stop syncing with the device: ${e}`);
  }
}

// ---------- settings: folder list ----------

function renderFolderListSettings() {
  els.folderList.innerHTML = "";
  for (const f of folders) {
    const li = document.createElement("li");
    li.className = "device";
    const icon = document.createElement("span");
    icon.textContent = "📁";
    icon.style.flexShrink = "0";
    const name = document.createElement("span");
    name.className = "dname";
    name.textContent = f.path || `${f.name} (no local folder yet)`;
    li.append(icon, name);

    if (f.path) {
      const reveal = document.createElement("button");
      reveal.className = "link tiny";
      reveal.textContent = "Reveal";
      reveal.style.flexShrink = "0";
      reveal.addEventListener("click", () =>
        invoke("cmd_open_folder", { idx: f.index }),
      );
      li.append(reveal);
    }
    const remove = document.createElement("button");
    remove.className = "link tiny dremove";
    remove.textContent = "Stop syncing";
    remove.addEventListener("click", () => removeFolder(f));
    li.append(remove);
    els.folderList.appendChild(li);
  }
}

async function removeFolder(f) {
  const ok = await window.__TAURI__.dialog.ask(
    `Stop syncing the folder "${f.name}"?\n\n` +
      "syncbox stops tracking it. The files stay on this Mac — nothing is " +
      "deleted. Other devices keep their copies.",
    {
      title: "Stop syncing folder",
      kind: "warning",
      okLabel: "Stop syncing",
      cancelLabel: "Cancel",
    },
  );
  if (!ok) return;
  try {
    await invoke("cmd_remove_folder", { idx: f.index });
    if (current >= f.index && current > 0) current -= 1;
    refresh();
  } catch (e) {
    alert(`Could not stop syncing the folder: ${e}`);
  }
}

async function refreshStorage() {
  try {
    const bytes = await invoke("cmd_get_storage_size");
    els.storageLine.textContent = `Storage: ${formatBytes(bytes)}`;
  } catch {
    /* not critical */
  }
}

async function refreshTransfer() {
  try {
    lastXfer = await invoke("cmd_get_transfer_stats", { idx: current });
  } catch {
    /* keep last */
  }
  return lastXfer;
}

// ---------- pairing code countdown ----------

let codeExpiryTimer = null;
function startCodeCountdown(expiresUnix) {
  clearInterval(codeExpiryTimer);
  function tick() {
    const left = expiresUnix - Math.floor(Date.now() / 1000);
    if (left <= 0) {
      els.codeExpiry.textContent = "expired";
      els.ourCode.textContent = "— — —";
      clearInterval(codeExpiryTimer);
      return;
    }
    const m = Math.floor(left / 60);
    const s = String(left % 60).padStart(2, "0");
    els.codeExpiry.textContent = `expires in ${m}:${s}`;
  }
  tick();
  codeExpiryTimer = setInterval(tick, 1000);
}

function showPairResult(node, ok, msg) {
  node.textContent = msg;
  node.className = `pair-result ${ok ? "ok" : "err"}`;
}

// ---------- add a folder ----------

// Pick a local folder. `idx` null adds a new folder; a number sets the local
// path of an already-joined folder. After it lands, show that folder.
async function pickFolder(idx) {
  try {
    const newIdx = await invoke("cmd_pick_folder", { idx: idx ?? null });
    if (newIdx === null || newIdx === undefined) return false;
    current = newIdx;
    viewOverride = null;
    await refresh();
    return true;
  } catch (e) {
    alert(`Could not add the folder: ${e}`);
    return false;
  }
}

els.btnChooseFolder.addEventListener("click", () => pickFolder(null));

els.btnShowJoin.addEventListener("click", () => {
  viewOverride = "join";
  els.joinCode.value = "";
  showPairResult(els.pairResult, true, "");
  render(lastStatus);
  els.joinCode.focus();
});

els.btnUseCode.addEventListener("click", async () => {
  const code = els.joinCode.value.trim();
  if (!code) return;
  els.btnUseCode.disabled = true;
  showPairResult(els.pairResult, true, "Connecting…");
  try {
    const idx = await invoke("cmd_use_code", { code });
    els.joinCode.value = "";
    // A joined folder has no local path yet — ask where to put it.
    const f = (await invoke("cmd_list_folders"))[idx];
    if (f && !f.path) {
      showPairResult(els.pairResult, true, "Connected — choose where to keep it.");
      await pickFolder(idx);
    } else {
      current = idx;
      viewOverride = null;
      await refresh();
    }
  } catch (e) {
    showPairResult(els.pairResult, false, `${e}`);
  } finally {
    els.btnUseCode.disabled = false;
  }
});

// ---------- add a device (share the current folder) ----------

els.btnMakeCode.addEventListener("click", async () => {
  if (!folders[current]) return;
  els.btnMakeCode.disabled = true;
  try {
    const r = await invoke("cmd_make_code", {
      idx: current,
      readOnly: els.ticketReadOnly.checked,
    });
    els.ourCode.textContent = r.code;
    startCodeCountdown(r.expires_unix);
  } catch (e) {
    alert(`Could not get code: ${e}`);
  } finally {
    els.btnMakeCode.disabled = false;
  }
});

els.btnCopyCode.addEventListener("click", async () => {
  const t = els.ourCode.textContent.trim();
  if (!t || t === "— — —") return;
  try {
    await navigator.clipboard.writeText(t);
    els.btnCopyCode.textContent = "Copied!";
    setTimeout(() => (els.btnCopyCode.textContent = "Copy"), 1200);
  } catch {
    /* clipboard denied */
  }
});

// ---------- settings ----------

els.deviceName.addEventListener("change", async () => {
  try {
    els.deviceName.value = await invoke("cmd_set_device_name", {
      name: els.deviceName.value,
    });
  } catch (e) {
    alert(`Could not save the device name: ${e}`);
  }
});
els.deviceName.addEventListener("keydown", (ev) => {
  if (ev.key === "Enter") els.deviceName.blur();
});

els.autostart.addEventListener("change", async () => {
  try {
    await invoke("cmd_set_autostart", { enabled: els.autostart.checked });
  } catch (e) {
    alert(`Could not change autostart: ${e}`);
    els.autostart.checked = !els.autostart.checked;
  }
});

els.hideDock.addEventListener("change", async () => {
  try {
    await invoke("cmd_set_hide_dock_icon", { enabled: els.hideDock.checked });
  } catch (e) {
    alert(`Could not change the Dock setting: ${e}`);
    els.hideDock.checked = !els.hideDock.checked;
  }
});

els.readOnlyLocal.addEventListener("change", async () => {
  try {
    await invoke("cmd_set_read_only", { enabled: els.readOnlyLocal.checked });
  } catch (e) {
    alert(`Could not change read-only: ${e}`);
    els.readOnlyLocal.checked = !els.readOnlyLocal.checked;
  }
});

els.btnWriteIgnore.addEventListener("click", async () => {
  if (!folders[current]) return;
  try {
    const created = await invoke("cmd_write_default_ignore", { idx: current });
    els.btnWriteIgnore.textContent = created
      ? "Created — open it in your editor"
      : "Already exists";
    setTimeout(
      () =>
        (els.btnWriteIgnore.textContent =
          "Create .syncboxignore in the current folder"),
      1500,
    );
  } catch (e) {
    alert(`Could not write ignore file: ${e}`);
  }
});

els.btnMakeTicket.addEventListener("click", async () => {
  if (!folders[current]) return;
  els.btnMakeTicket.disabled = true;
  try {
    els.ourTicket.value = await invoke("cmd_get_ticket", { idx: current });
    refresh();
  } catch (e) {
    alert(`Could not get ticket: ${e}`);
  } finally {
    els.btnMakeTicket.disabled = false;
  }
});

els.btnCopyTicket.addEventListener("click", async () => {
  if (!els.ourTicket.value) return;
  try {
    await navigator.clipboard.writeText(els.ourTicket.value);
    els.btnCopyTicket.textContent = "Copied!";
    setTimeout(() => (els.btnCopyTicket.textContent = "Copy"), 1200);
  } catch {
    els.ourTicket.select();
  }
});

els.btnJoin.addEventListener("click", async () => {
  const t = els.joinTicket.value.trim();
  if (!t) return;
  els.btnJoin.disabled = true;
  showPairResult(els.pairResultAdv, true, "Joining…");
  try {
    const idx = await invoke("cmd_join_with_ticket", { ticket: t });
    els.joinTicket.value = "";
    showPairResult(els.pairResultAdv, true, "Joined.");
    const f = (await invoke("cmd_list_folders"))[idx];
    if (f && !f.path) {
      await pickFolder(idx);
    } else {
      current = idx;
      viewOverride = null;
      refresh();
    }
  } catch (e) {
    showPairResult(els.pairResultAdv, false, `${e}`);
  } finally {
    els.btnJoin.disabled = false;
  }
});

els.btnSavePairServer.addEventListener("click", async () => {
  try {
    await invoke("cmd_set_pair_server", { url: els.pairServer.value.trim() });
    els.btnSavePairServer.textContent = "Saved";
    setTimeout(
      () => (els.btnSavePairServer.textContent = "Save server URL"),
      1200,
    );
  } catch (e) {
    alert(`Could not save: ${e}`);
  }
});

els.btnCopyLog.addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(els.debugLog.textContent || "");
    els.btnCopyLog.textContent = "Copied!";
    setTimeout(() => (els.btnCopyLog.textContent = "Copy log"), 1200);
  } catch {
    /* clipboard denied */
  }
});

async function refreshLog() {
  try {
    const lines = await invoke("cmd_get_log");
    if (lines.length) {
      const atBottom =
        els.debugLog.scrollTop + els.debugLog.clientHeight >=
        els.debugLog.scrollHeight - 4;
      els.debugLog.textContent = lines.join("\n");
      if (atBottom) els.debugLog.scrollTop = els.debugLog.scrollHeight;
    }
  } catch {
    /* not critical */
  }
}

async function loadSettings() {
  try {
    els.deviceName.value = await invoke("cmd_get_device_name");
  } catch {
    /* not critical */
  }
  try {
    els.autostart.checked = await invoke("cmd_get_autostart");
  } catch {
    /* not critical */
  }
  try {
    els.hideDock.checked = await invoke("cmd_get_hide_dock_icon");
  } catch {
    /* not critical */
  }
  try {
    els.readOnlyLocal.checked = await invoke("cmd_get_read_only");
  } catch {
    /* not critical */
  }
  try {
    els.pairServer.value = await invoke("cmd_get_pair_server");
  } catch {
    /* not critical */
  }
}

// ---------- updates ----------
//
// A background check (launch + every 6h) never interrupts: it shows a banner
// in the window and relabels the tray item. A manual check (tray "Check for
// Updates…") uses a modal, since the user explicitly asked. Silent on any
// failure (offline, GitHub down) unless the check was manual.

let pendingUpdate = null;

function showUpdateBanner(version) {
  els.updateBannerText.textContent = `syncbox ${version} is available.`;
  els.updateBanner.hidden = false;
}

function hideUpdateBanner() {
  els.updateBanner.hidden = true;
}

async function installUpdate() {
  if (!pendingUpdate) return;
  els.btnUpdateInstall.disabled = true;
  els.btnUpdateInstall.textContent = "Installing…";
  try {
    await pendingUpdate.downloadAndInstall();
    await window.__TAURI__.process.relaunch();
  } catch (e) {
    console.warn("update install failed:", e);
    els.btnUpdateInstall.disabled = false;
    els.btnUpdateInstall.textContent = "Install & restart";
    await window.__TAURI__.dialog.message(`Could not install update: ${e}`, {
      title: "Update",
      kind: "error",
    });
  }
}

async function checkForUpdates(manual = false) {
  try {
    const update = await window.__TAURI__.updater.check();
    if (!update) {
      pendingUpdate = null;
      hideUpdateBanner();
      await invoke("cmd_set_update_available", { version: null });
      if (manual) {
        await window.__TAURI__.dialog.message("syncbox is up to date.", {
          title: "Check for Updates",
        });
      }
      return;
    }
    pendingUpdate = update;
    if (manual) {
      const ok = await window.__TAURI__.dialog.ask(
        `syncbox ${update.version} is available (you have ${update.currentVersion}).\n\n` +
          "Install it now? The app will restart.",
        {
          title: "Update available",
          kind: "info",
          okLabel: "Install",
          cancelLabel: "Later",
        },
      );
      if (ok) await installUpdate();
    } else {
      showUpdateBanner(update.version);
      await invoke("cmd_set_update_available", { version: update.version });
    }
  } catch (e) {
    console.warn("update check failed:", e);
    if (manual) {
      await window.__TAURI__.dialog.message(
        `Could not check for updates: ${e}`,
        { title: "Check for Updates", kind: "error" },
      );
    }
  }
}

els.btnUpdateInstall.addEventListener("click", installUpdate);
window.__TAURI__.event.listen("check-update", () => checkForUpdates(true));

// ---------- boot ----------

refresh();
loadSettings();
refreshLog();
checkForUpdates();
setInterval(refresh, 3000);
// Transfer rate + debug log update faster so they feel live.
setInterval(async () => {
  await refreshTransfer();
  if (lastStatus) updateHero(lastStatus, lastXfer);
}, 1000);
setInterval(refreshLog, 1500);
// Re-check for updates every 6h — a tray app can run for weeks without a relaunch.
setInterval(() => checkForUpdates(false), 6 * 60 * 60 * 1000);
