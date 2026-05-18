// Frontend glue for the syncbox tray window.
//
// All persistent state lives in the Rust side. This file only mirrors what
// the backend reports and calls back into it.

const { invoke } = window.__TAURI__.core;

const els = {
  hostname: document.getElementById("hostname"),
  folderPath: document.getElementById("folder-path"),
  btnChooseFolder: document.getElementById("btn-choose-folder"),
  btnOpenFolder: document.getElementById("btn-open-folder"),
  ourCode: document.getElementById("our-code"),
  codeExpiry: document.getElementById("code-expiry"),
  btnMakeCode: document.getElementById("btn-make-code"),
  btnCopyCode: document.getElementById("btn-copy-code"),
  joinCode: document.getElementById("join-code"),
  btnUseCode: document.getElementById("btn-use-code"),
  ticketReadOnly: document.getElementById("ticket-read-only"),
  readOnlyLocal: document.getElementById("read-only-local"),
  btnWriteIgnore: document.getElementById("btn-write-ignore"),
  storageSize: document.getElementById("storage-size"),
  xferRate: document.getElementById("xfer-rate"),
  pairResult: document.getElementById("pair-result"),
  debugLog: document.getElementById("debug-log"),
  btnCopyLog: document.getElementById("btn-copy-log"),
  ourTicket: document.getElementById("our-ticket"),
  btnMakeTicket: document.getElementById("btn-make-ticket"),
  btnCopyTicket: document.getElementById("btn-copy-ticket"),
  joinTicket: document.getElementById("join-ticket"),
  btnJoin: document.getElementById("btn-join"),
  pairServer: document.getElementById("pair-server"),
  btnSavePairServer: document.getElementById("btn-save-pair-server"),
  statusText: document.getElementById("status-text"),
  autostart: document.getElementById("autostart"),
  peerCount: document.getElementById("peer-count"),
  peerList: document.getElementById("peer-list"),
  appVersion: document.getElementById("app-version"),
};

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

async function refresh() {
  try {
    const s = await invoke("cmd_get_status");
    els.hostname.textContent = s.hostname || "this device";
    els.folderPath.textContent = s.folder || "(not set)";
    els.btnOpenFolder.hidden = !s.folder;
    els.statusText.textContent = describeStatus(s);
    if (s.version) els.appVersion.textContent = `v${s.version}`;
    els.peerCount.textContent = `${s.peers_online} / ${s.peers_known} online`;
    els.btnMakeTicket.disabled = false;
    await refreshPeers();
    await refreshStorage();
    await refreshTransfer();
  } catch (e) {
    els.statusText.textContent = `error: ${e}`;
  }
}

async function refreshPeers() {
  try {
    const [peers, blocked] = await Promise.all([
      invoke("cmd_get_peers"),
      invoke("cmd_list_blocked"),
    ]);
    const blockedSet = new Set(blocked);
    const live = new Set(peers.map((p) => p.id));
    // A revoked device drops out of the live peer list once it's offline —
    // append blocked-but-not-live peers so a revoke is always visible and
    // undoable, not silently buried in config.
    const rows = [...peers];
    for (const id of blocked) {
      if (!live.has(id)) rows.push({ id, online: false, last_seen_unix: 0 });
    }
    els.peerList.innerHTML = "";
    for (const p of rows) {
      const li = document.createElement("li");
      if (p.online) li.classList.add("online");
      const isBlocked = blockedSet.has(p.id);
      if (isBlocked) li.classList.add("blocked");
      li.innerHTML = `
        <span class="dot"></span>
        <span class="pid" title="${p.id}">${p.id.slice(0, 16)}…</span>
        ${isBlocked ? '<span class="tag">revoked</span>' : ""}
        <button class="link tiny" data-act="${isBlocked ? "unblock" : "block"}" data-id="${p.id}">
          ${isBlocked ? "unblock" : "revoke"}
        </button>
      `;
      els.peerList.appendChild(li);
    }
    els.peerList.querySelectorAll("button[data-act]").forEach((btn) => {
      btn.addEventListener("click", async (ev) => {
        const id = ev.target.dataset.id;
        const act = ev.target.dataset.act;
        // Revoking silently stops sync with a device — confirm before doing it.
        if (
          act === "block" &&
          !confirm(
            "Revoke this device?\n\nsyncbox will ignore every change from it " +
              "until you unblock it. Syncing with it stops.",
          )
        ) {
          return;
        }
        const cmd = act === "block" ? "cmd_block_peer" : "cmd_unblock_peer";
        try {
          await invoke(cmd, { id });
          refreshPeers();
        } catch (e) {
          alert(`Could not ${act}: ${e}`);
        }
      });
    });
  } catch {
    /* not critical */
  }
}

async function refreshStorage() {
  try {
    const bytes = await invoke("cmd_get_storage_size");
    els.storageSize.textContent = formatBytes(bytes);
  } catch {
    /* not critical */
  }
}

async function refreshTransfer() {
  try {
    const s = await invoke("cmd_get_transfer_stats");
    if (s.active_downloads > 0 && s.down_rate > 0) {
      els.xferRate.textContent =
        `${formatBytes(s.down_rate)}/s · ${s.active_downloads} file(s) · ` +
        `${formatBytes(s.down_total)} total`;
    } else if (s.down_total > 0) {
      els.xferRate.textContent = `idle · ${formatBytes(s.down_total)} total`;
    } else {
      els.xferRate.textContent = "idle";
    }
  } catch {
    /* not critical */
  }
}

function formatBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`;
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`;
}

function describeStatus(s) {
  if (!s.folder) return "pick a folder";
  if (!s.has_doc) return "get a code, or enter one from another device";
  if (!s.syncing) return "starting…";
  // The sync engine reports a live message once running; prefer it.
  if (s.message) return s.message;
  return "watching for changes";
}

els.btnMakeCode.addEventListener("click", async () => {
  els.btnMakeCode.disabled = true;
  try {
    const r = await invoke("cmd_make_code", {
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

els.readOnlyLocal.addEventListener("change", async () => {
  try {
    await invoke("cmd_set_read_only", { enabled: els.readOnlyLocal.checked });
  } catch (e) {
    alert(`Could not change read-only: ${e}`);
    els.readOnlyLocal.checked = !els.readOnlyLocal.checked;
  }
});

els.btnWriteIgnore.addEventListener("click", async () => {
  try {
    const created = await invoke("cmd_write_default_ignore");
    els.btnWriteIgnore.textContent = created
      ? "Created — open in your editor"
      : "Already exists";
    setTimeout(
      () => (els.btnWriteIgnore.textContent = "Create .syncboxignore in folder"),
      1500,
    );
  } catch (e) {
    alert(`Could not write ignore file: ${e}`);
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

function showPairResult(ok, msg) {
  els.pairResult.textContent = msg;
  els.pairResult.className = `pair-result ${ok ? "ok" : "err"}`;
}

els.btnUseCode.addEventListener("click", async () => {
  const code = els.joinCode.value.trim();
  if (!code) return;
  els.btnUseCode.disabled = true;
  showPairResult(true, "Joining…");
  try {
    await invoke("cmd_use_code", { code });
    els.joinCode.value = "";
    showPairResult(true, "Joined. Connecting to the other device…");
    refresh();
  } catch (e) {
    showPairResult(false, `${e}`);
  } finally {
    els.btnUseCode.disabled = false;
  }
});

els.btnSavePairServer.addEventListener("click", async () => {
  try {
    await invoke("cmd_set_pair_server", { url: els.pairServer.value.trim() });
    els.btnSavePairServer.textContent = "Saved";
    setTimeout(() => (els.btnSavePairServer.textContent = "Save server URL"), 1200);
  } catch (e) {
    alert(`Could not save: ${e}`);
  }
});

els.btnChooseFolder.addEventListener("click", async () => {
  try {
    const p = await invoke("cmd_choose_folder");
    if (p) els.folderPath.textContent = p;
    refresh();
  } catch (e) {
    alert(`Could not pick folder: ${e}`);
  }
});

els.btnOpenFolder.addEventListener("click", async () => {
  await invoke("cmd_open_folder");
});

els.btnMakeTicket.addEventListener("click", async () => {
  els.btnMakeTicket.disabled = true;
  try {
    const t = await invoke("cmd_get_ticket");
    els.ourTicket.value = t;
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
  showPairResult(true, "Joining…");
  try {
    await invoke("cmd_join_with_ticket", { ticket: t });
    els.joinTicket.value = "";
    showPairResult(true, "Joined. Connecting to the other device…");
    refresh();
  } catch (e) {
    showPairResult(false, `${e}`);
  } finally {
    els.btnJoin.disabled = false;
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

els.autostart.addEventListener("change", async () => {
  try {
    await invoke("cmd_set_autostart", { enabled: els.autostart.checked });
  } catch (e) {
    alert(`Could not change autostart: ${e}`);
    els.autostart.checked = !els.autostart.checked;
  }
});

async function loadAutostart() {
  try {
    els.autostart.checked = await invoke("cmd_get_autostart");
  } catch {
    /* not critical */
  }
}

async function loadPairServer() {
  try {
    els.pairServer.value = await invoke("cmd_get_pair_server");
  } catch {
    /* not critical */
  }
}

async function loadReadOnly() {
  try {
    els.readOnlyLocal.checked = await invoke("cmd_get_read_only");
  } catch {
    /* not critical */
  }
}

// On launch, ask GitHub whether a newer release exists. If so, prompt the
// user; on yes, download, install, and relaunch into the new version. Silent
// on any failure (offline, GitHub down): a missed check is never worth
// interrupting the user over.
async function checkForUpdates() {
  try {
    const update = await window.__TAURI__.updater.check();
    if (!update) return;
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
    if (!ok) return;
    await update.downloadAndInstall();
    await window.__TAURI__.process.relaunch();
  } catch (e) {
    console.warn("update check failed:", e);
  }
}

refresh();
loadAutostart();
loadPairServer();
loadReadOnly();
checkForUpdates();
refreshLog();
setInterval(refresh, 3000);
// Transfer rate + debug log update faster so they feel live.
setInterval(refreshTransfer, 1000);
setInterval(refreshLog, 1500);
