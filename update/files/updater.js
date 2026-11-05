/* ChatRoom over-the-air updater (self-contained: own CSS, own DOM).
 *
 * Reads:  window.CHATROOM_VERSION (version.js), window.CHATROOM_UPDATE_URL (update-config.js)
 * Fetches <CHATROOM_UPDATE_URL>update/manifest.json and update/files/<path>.
 * Install targets:
 *   electron: window.electronAPI.installUpdate / resetUpdate / reloadApp / nativeVersion / onDeepLink
 *   android:  Capacitor Filesystem (Directory.Data 'ota/v<N>/') + WebView.setServerBasePath;
 *             the new path is persisted only once the new bundle has booted (see bootAndroid).
 *   browser:  check only.
 * Deep link chatroom://update -> check + confirm + install.
 */
(function () {
  'use strict';

  const CUR = Number(window.CHATROOM_VERSION) || 0;
  const BASE = String(window.CHATROOM_UPDATE_URL || '');
  const CONFIGURED = /^https?:\/\//i.test(BASE) && !/PLACEHOLDER/i.test(BASE);
  const cap = window.Capacitor;
  const isAndroid = !!(cap && typeof cap.getPlatform === 'function' && cap.getPlatform() === 'android');
  const api = window.electronAPI;
  const isElectron = !!(api && typeof api.installUpdate === 'function');
  const kind = isElectron ? 'electron' : (isAndroid ? 'android' : 'browser');
  const plug = (n) => {
    if (!cap) return null;
    if (cap.Plugins && cap.Plugins[n]) return cap.Plugins[n];
    try { return typeof cap.registerPlugin === 'function' ? cap.registerPlugin(n) : null; } catch (e) { return null; }
  };

  const LS_LAST = 'chatroom.update.lastCheck';
  const LS_AVAIL = 'chatroom.update.available';
  const LS_JUST = 'chatroom.update.justInstalled';
  const LS_PERSIST = 'chatroom.update.persistPending';
  const DAY = 24 * 3600 * 1000;

  const ls = {
    get(k) { try { return localStorage.getItem(k); } catch (e) { return null; } },
    set(k, v) { try { localStorage.setItem(k, v); } catch (e) { /* ignore */ } },
    del(k) { try { localStorage.removeItem(k); } catch (e) { /* ignore */ } }
  };

  // ---------------------------------------------------------------- helpers
  function baseUrl() { return BASE.endsWith('/') ? BASE : BASE + '/'; }
  function manifestUrl() { return new URL('update/manifest.json', baseUrl()).href; }
  function fileUrl(p) {
    return new URL('update/files/' + p.split('/').map(encodeURIComponent).join('/'), baseUrl()).href;
  }
  function siteUrl() { return baseUrl(); }
  function fmtBytes(n) {
    if (n < 1024) return n + ' B';
    if (n < 1048576) return (n / 1024).toFixed(0) + ' KB';
    return (n / 1048576).toFixed(1) + ' MB';
  }
  function safePath(p) {
    return typeof p === 'string' && p.length > 0 && p.length < 300 && !/^[\\/]|^[a-z]:|\\|(^|\/)\.\.?(\/|$)|[\u0000-\u001f]/i.test(p);
  }
  function hex(buf) {
    const b = new Uint8Array(buf); let s = '';
    for (let i = 0; i < b.length; i++) s += (b[i] < 16 ? '0' : '') + b[i].toString(16);
    return s;
  }

  // Small SHA-256 fallback for contexts without crypto.subtle.
  function sha256Fallback(data) {
    const K = new Uint32Array([0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2]);
    const H = new Uint32Array([0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19]);
    const msg = new Uint8Array(data);
    const len = msg.length;
    const total = Math.ceil((len + 9) / 64) * 64;
    const buf = new Uint8Array(total);
    buf.set(msg); buf[len] = 0x80;
    const dv = new DataView(buf.buffer);
    dv.setUint32(total - 8, Math.floor(len / 0x20000000)); dv.setUint32(total - 4, (len * 8) >>> 0);
    const W = new Uint32Array(64);
    const rot = (x, n) => (x >>> n) | (x << (32 - n));
    for (let o = 0; o < total; o += 64) {
      for (let i = 0; i < 16; i++) W[i] = dv.getUint32(o + i * 4);
      for (let i = 16; i < 64; i++) {
        const s0 = rot(W[i - 15], 7) ^ rot(W[i - 15], 18) ^ (W[i - 15] >>> 3);
        const s1 = rot(W[i - 2], 17) ^ rot(W[i - 2], 19) ^ (W[i - 2] >>> 10);
        W[i] = (W[i - 16] + s0 + W[i - 7] + s1) >>> 0;
      }
      let [a, b, c, d, e, f, g, h] = H;
      for (let i = 0; i < 64; i++) {
        const t1 = (h + (rot(e, 6) ^ rot(e, 11) ^ rot(e, 25)) + ((e & f) ^ (~e & g)) + K[i] + W[i]) >>> 0;
        const t2 = ((rot(a, 2) ^ rot(a, 13) ^ rot(a, 22)) + ((a & b) ^ (a & c) ^ (b & c))) >>> 0;
        h = g; g = f; f = e; e = (d + t1) >>> 0; d = c; c = b; b = a; a = (t1 + t2) >>> 0;
      }
      H[0] += a; H[1] += b; H[2] += c; H[3] += d; H[4] += e; H[5] += f; H[6] += g; H[7] += h;
    }
    const out = new DataView(new ArrayBuffer(32));
    for (let i = 0; i < 8; i++) out.setUint32(i * 4, H[i]);
    return out.buffer;
  }
  async function sha256Hex(buf) {
    if (window.crypto && crypto.subtle && crypto.subtle.digest) {
      try { return hex(await crypto.subtle.digest('SHA-256', buf)); } catch (e) { /* fall back */ }
    }
    return hex(sha256Fallback(buf));
  }

  function bufToBase64(buf) {
    return new Promise((resolve, reject) => {
      const r = new FileReader();
      r.onload = () => { const s = String(r.result); const i = s.indexOf(','); resolve(i >= 0 ? s.slice(i + 1) : s); };
      r.onerror = () => reject(r.error || new Error('read failed'));
      r.readAsDataURL(new Blob([buf]));
    });
  }

  let nativeVersionCache;
  async function nativeVersion() {
    if (nativeVersionCache !== undefined) return nativeVersionCache;
    let v = null;
    if (isElectron) v = Number(api.nativeVersion) || 1;
    else if (isAndroid) {
      try { const info = await plug('App').getInfo(); v = parseInt(info.build, 10) || 1; } catch (e) { v = 1; }
    }
    nativeVersionCache = v;
    return v;
  }

  // ---------------------------------------------------------------- manifest / download
  async function fetchManifest() {
    if (!CONFIGURED) throw new Error('Update server is not configured yet.');
    const url = manifestUrl() + (manifestUrl().includes('?') ? '&' : '?') + 't=' + Date.now();
    const res = await fetch(url, { cache: 'no-store' });
    if (!res.ok) throw new Error('Update server answered HTTP ' + res.status);
    const m = await res.json();
    if (!m || !Number.isInteger(m.version) || !Array.isArray(m.files) || !m.files.length) throw new Error('Invalid manifest');
    for (const f of m.files) {
      if (!f || !safePath(f.path) || !/^[0-9a-f]{64}$/i.test(f.sha256 || '')) throw new Error('Invalid manifest entry ' + (f && f.path));
    }
    if (!m.files.some((f) => f.path === 'index.html')) throw new Error('Manifest has no index.html');
    ls.set(LS_LAST, String(Date.now()));
    return m;
  }

  function neededNative(m) {
    const n = m.nativeMinVersion || {};
    if (kind === 'electron') return Number(n.windows) || 1;
    if (kind === 'android') return Number(n.android) || 1;
    return 1;
  }

  async function downloadAll(m, onProgress) {
    const total = m.files.reduce((s, f) => s + (Number(f.size) || 0), 0);
    let done = 0;
    const out = [];
    for (const f of m.files) {
      onProgress(done, total, f.path);
      const res = await fetch(fileUrl(f.path) + '?v=' + m.version, { cache: 'no-store' });
      if (!res.ok) throw new Error('Download failed (' + res.status + '): ' + f.path);
      const data = await res.arrayBuffer();
      if (typeof f.size === 'number' && data.byteLength !== f.size) throw new Error('Size mismatch: ' + f.path);
      const h = await sha256Hex(data);
      if (h.toLowerCase() !== f.sha256.toLowerCase()) throw new Error('Checksum mismatch: ' + f.path);
      out.push({ path: f.path, data });
      done += data.byteLength;
      onProgress(done, total, f.path);
    }
    return out;
  }

  // ---------------------------------------------------------------- platform install / reset
  const ANDROID_DIR = 'DATA';

  async function androidRmdir(path) {
    const FS = plug('Filesystem');
    try { await FS.rmdir({ path, directory: ANDROID_DIR, recursive: true }); } catch (e) { /* not there */ }
  }

  async function installAndroid(version, files, onProgress) {
    const FS = plug('Filesystem');
    const WV = plug('WebView');
    if (!FS || !WV) throw new Error('Filesystem/WebView plugin missing');
    const dir = 'ota/v' + version;
    await androidRmdir(dir);
    let i = 0;
    for (const f of files) {
      await FS.writeFile({ path: dir + '/' + f.path, directory: ANDROID_DIR, data: await bufToBase64(f.data), recursive: true });
      onProgress && onProgress(++i, files.length);
    }
    const { uri } = await FS.getUri({ path: dir, directory: ANDROID_DIR });
    const p = decodeURIComponent(String(uri).replace(/^file:\/\//, ''));
    ls.set(LS_JUST, String(version));
    // Persisted by the NEW bundle once it boots (bootAndroid). If it never boots, the next cold
    // start falls back to the previously persisted bundle.
    ls.set(LS_PERSIST, '1');
    await WV.setServerBasePath({ path: p }); // reloads the WebView from the new folder
  }

  async function resetAndroid() {
    const WV = plug('WebView');
    if (!WV) throw new Error('WebView plugin missing');
    ls.set(LS_PERSIST, '1');
    ls.del(LS_AVAIL);
    // 'public' = Capacitor's built-in asset dir. Persisting it stores "public", which is not an
    // existing file path, so Capacitor ignores it on the next cold start -> bundled app.
    await WV.setServerAssetPath({ path: 'public' });
  }

  async function bootAndroid() {
    const WV = plug('WebView');
    if (!WV) return;
    let base = '';
    try { base = (await WV.getServerBasePath()).path || ''; } catch (e) { return; }
    if (ls.get(LS_PERSIST)) {
      try { await WV.persistServerBasePath(); ls.del(LS_PERSIST); } catch (e) { console.warn('persist failed', e); }
    }
    // remove OTA folders that are not in use
    const m = /\/ota\/(v\d+)\/?$/.exec(base);
    const keep = m ? m[1] : null;
    try {
      const FS = plug('Filesystem');
      const r = await FS.readdir({ path: 'ota', directory: ANDROID_DIR });
      for (const e of r.files || []) {
        const name = typeof e === 'string' ? e : e.name;
        if (name && name !== keep) await androidRmdir('ota/' + name);
      }
    } catch (e) { /* no ota dir */ }
    state.runningOta = !!keep;
  }

  async function install(m, files, onProgress) {
    if (kind === 'electron') {
      ls.set(LS_JUST, String(m.version));
      await api.installUpdate(m.version, files);
      if (api.reloadApp) await api.reloadApp(); else location.reload();
    } else if (kind === 'android') {
      await installAndroid(m.version, files, onProgress);
    } else {
      throw new Error('Open the ChatRoom app to install updates.');
    }
  }

  async function resetToBuiltIn() {
    if (kind === 'electron') {
      await api.resetUpdate();
      ls.del(LS_AVAIL);
      if (api.reloadApp) await api.reloadApp(); else location.reload();
    } else if (kind === 'android') {
      await resetAndroid();
    }
  }

  function openExternal(url) {
    if (kind === 'electron' && api.openExternal) return api.openExternal(url);
    if (kind === 'android') { location.href = url; return; } // Capacitor opens non-app hosts in the browser
    window.open(url, '_blank', 'noopener');
  }

  // ---------------------------------------------------------------- state
  const state = {
    manifest: null,
    checking: false,
    busy: false,
    runningOta: null,
    bundledVersion: null
  };

  function storedAvailable() {
    try { const a = JSON.parse(ls.get(LS_AVAIL) || 'null'); return a && a.version > CUR ? a : null; } catch (e) { return null; }
  }

  // ---------------------------------------------------------------- CSS + DOM
  const css = `
.cru-overlay{position:fixed;inset:0;z-index:2147483000;background:var(--overlay,rgba(5,8,14,.62));-webkit-backdrop-filter:blur(3px);backdrop-filter:blur(3px);display:flex;align-items:center;justify-content:center;padding:16px;font:14px/1.45 var(--font,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif)}
.cru-overlay[hidden]{display:none}
.cru-dialog{background:var(--surface,#161b25);color:var(--text,#e9edf5);border:1px solid var(--line-2,rgba(255,255,255,.11));border-radius:22px;box-shadow:var(--shadow,0 18px 50px rgba(0,0,0,.5));width:100%;max-width:420px;max-height:90vh;overflow:auto;padding:22px 22px 16px}
.cru-dialog h3{margin:0 0 6px;font-size:19px;letter-spacing:-.015em}
.cru-meta{color:var(--muted,#9aa3b5);font-size:12.5px;margin:0 0 12px}
.cru-status{margin:8px 0;white-space:pre-wrap;word-break:break-word;color:var(--text-2,#c9cfdb)}
.cru-status.err{color:var(--err,#ff8a8a)}
.cru-notes{background:var(--surface-2,#1d2330);border:1px solid var(--line,rgba(255,255,255,.06));border-radius:12px;padding:10px 12px;margin:8px 0;white-space:pre-wrap;font-size:13px;color:var(--text-2,#c9cfdb);max-height:160px;overflow:auto}
.cru-bar{height:6px;background:var(--surface-2,#1d2330);border-radius:6px;overflow:hidden;margin:12px 0}
.cru-bar>i{display:block;height:100%;width:0;background:var(--grad,linear-gradient(135deg,#4f7cff,#8a5cff));border-radius:6px;transition:width .15s}
.cru-row{display:flex;flex-wrap:wrap;gap:8px;justify-content:flex-end;margin-top:16px}
.cru-btn{appearance:none;border:1px solid var(--line-2,rgba(255,255,255,.11));background:var(--surface-2,#1d2330);color:var(--text,#e9edf5);border-radius:12px;padding:9px 16px;font:inherit;font-weight:600;cursor:pointer;transition:background .15s,filter .15s}
.cru-btn:hover{background:var(--surface-3,#262d3c)}
.cru-btn:disabled{opacity:.5;cursor:default}
.cru-btn.primary{background:var(--grad,linear-gradient(135deg,#4f7cff,#8a5cff));border-color:transparent;color:#fff;box-shadow:0 6px 18px rgba(91,100,255,.32)}
.cru-btn.primary:hover{filter:brightness(1.08)}
.cru-link{appearance:none;background:none;border:0;color:var(--accent,#8aa6ff);font:inherit;font-size:12.5px;cursor:pointer;padding:0;text-decoration:underline;text-underline-offset:2px;margin-top:12px}
.cru-link:disabled{opacity:.5}
.cru-badge{appearance:none;border:0;background:var(--grad,linear-gradient(135deg,#4f7cff,#8a5cff));color:#fff;font:600 12px/1 var(--font,system-ui,sans-serif);border-radius:999px;padding:7px 11px;cursor:pointer;margin:0 6px;white-space:nowrap;box-shadow:0 4px 12px rgba(91,100,255,.4)}
.cru-badge.floating{position:fixed;right:12px;bottom:84px;z-index:2147482000}
.cru-badge[hidden]{display:none}
.cru-entry{display:flex;align-items:center;justify-content:space-between;gap:10px;margin:16px 0 4px;padding:10px 10px 10px 14px;border:1px solid var(--line-2,rgba(127,140,170,.25));border-radius:14px;background:var(--surface-2,transparent)}
.cru-entry span{font-size:13px;font-weight:600;color:var(--text-2,inherit)}
.cru-entry .cru-dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--accent,#4f7cff);margin-left:6px;vertical-align:middle}
.cru-toast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%);z-index:2147483001;background:var(--surface,#161b25);color:var(--text,#e9edf5);border:1px solid var(--line-2,rgba(255,255,255,.11));border-radius:14px;padding:10px 14px;font:14px var(--font,system-ui,sans-serif);box-shadow:var(--shadow,0 8px 24px rgba(0,0,0,.4));max-width:calc(100vw - 32px)}
`;

  let ui = null;
  function el(tag, cls, text) {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }

  function ensureUi() {
    if (ui) return ui;
    const style = el('style'); style.textContent = css; document.head.appendChild(style);
    const overlay = el('div', 'cru-overlay'); overlay.hidden = true;
    const dlg = el('div', 'cru-dialog'); dlg.setAttribute('role', 'dialog'); dlg.setAttribute('aria-modal', 'true');
    const h = el('h3', null, 'App updates');
    const meta = el('p', 'cru-meta');
    const status = el('div', 'cru-status');
    const notes = el('div', 'cru-notes'); notes.hidden = true;
    const bar = el('div', 'cru-bar'); const fill = el('i'); bar.appendChild(fill); bar.hidden = true;
    const row = el('div', 'cru-row');
    const close = el('button', 'cru-btn', 'Close');
    const check = el('button', 'cru-btn', 'Check for updates');
    const act = el('button', 'cru-btn primary', 'Install'); act.hidden = true;
    row.append(close, check, act);
    const reset = el('button', 'cru-link', 'Reset to built-in version');
    reset.hidden = kind === 'browser';
    dlg.append(h, meta, status, notes, bar, row, reset);
    overlay.appendChild(dlg);
    document.body.appendChild(overlay);
    overlay.addEventListener('click', (e) => { if (e.target === overlay && !state.busy) hidePanel(); });
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && !overlay.hidden && !state.busy) hidePanel(); });
    close.onclick = hidePanel;
    check.onclick = () => runCheck(false);
    reset.onclick = onReset;
    ui = { overlay, meta, status, notes, bar, fill, check, act, close, reset };
    return ui;
  }

  function setStatus(text, isErr) { const u = ensureUi(); u.status.textContent = text; u.status.classList.toggle('err', !!isErr); }
  function setBusy(b) {
    state.busy = b; const u = ensureUi();
    u.check.disabled = b; u.close.disabled = b; u.reset.disabled = b; u.act.disabled = b;
  }
  function setProgress(frac) {
    const u = ensureUi();
    if (frac == null) { u.bar.hidden = true; return; }
    u.bar.hidden = false; u.fill.style.width = Math.round(Math.max(0, Math.min(1, frac)) * 100) + '%';
  }

  async function refreshMeta() {
    const u = ensureUi();
    const nv = await nativeVersion();
    let t = 'Installed version: v' + CUR;
    if (kind === 'electron' && api.getUpdateInfo) {
      try {
        const info = await api.getUpdateInfo();
        state.runningOta = info.running === 'ota';
        state.bundledVersion = info.bundledVersion;
      } catch (e) { /* ignore */ }
    }
    if (state.runningOta === true) t += ' (downloaded update' + (state.bundledVersion ? ', built-in v' + state.bundledVersion : '') + ')';
    else if (state.runningOta === false) t += ' (built-in)';
    t += ' · ' + kind + (nv ? ' shell ' + nv : '');
    u.meta.textContent = t;
    u.reset.hidden = kind === 'browser' || state.runningOta === false;
  }

  function showPanel() {
    const u = ensureUi();
    u.overlay.hidden = false;
    refreshMeta();
    if (!state.busy && !state.manifest) {
      u.act.hidden = true; u.notes.hidden = true; setProgress(null);
      setStatus(CONFIGURED ? 'Press "Check for updates" to look for a newer version.' : 'Update server is not configured yet.');
    }
  }
  function hidePanel() { if (ui) ui.overlay.hidden = true; }

  let toastTimer = null;
  function toast(msg, ms) {
    ensureUi();
    let t = document.querySelector('.cru-toast');
    if (!t) { t = el('div', 'cru-toast'); document.body.appendChild(t); }
    t.textContent = msg; t.hidden = false;
    clearTimeout(toastTimer); toastTimer = setTimeout(() => { t.hidden = true; }, ms || 3500);
  }

  // ---------------------------------------------------------------- badge + settings hook
  let badge = null;
  function updateBadge() {
    const a = storedAvailable();
    if (!a) { if (badge) badge.hidden = true; markSettingsEntry(); return; }
    ensureUi();
    if (!badge) {
      badge = el('button', 'cru-badge', 'Update available');
      badge.title = 'A new ChatRoom version is available';
      badge.onclick = () => { showPanel(); runCheck(false); };
      const topbar = document.querySelector('#home .topbar');
      const btn = document.getElementById('btnSettings');
      if (topbar && btn && btn.parentNode === topbar) topbar.insertBefore(badge, btn);
      else { badge.classList.add('floating'); document.body.appendChild(badge); }
    }
    badge.textContent = 'Update v' + a.version;
    badge.hidden = false;
    markSettingsEntry();
  }

  function markSettingsEntry() {
    const span = document.querySelector('.cru-entry span');
    if (!span) return;
    const a = storedAvailable();
    span.textContent = 'Version ' + CUR + (a ? ' · v' + a.version + ' available' : '');
    if (a) span.appendChild(el('i', 'cru-dot'));
  }

  // Inject an "Updates" row into the app's Settings dialog (built by app.js inside #modalBody).
  function hookSettings() {
    const check = () => {
      const body = document.getElementById('modalBody');
      if (!body || body.querySelector('.cru-entry')) return;
      const h = body.querySelector('h3');
      if (!h || h.textContent.trim().toLowerCase() !== 'settings') return;
      ensureUi();
      const entry = el('div', 'cru-entry');
      entry.appendChild(el('span'));
      const b = el('button', 'btn small', 'Check for updates');
      b.type = 'button';
      b.onclick = (e) => { e.preventDefault(); showPanel(); runCheck(false); };
      entry.appendChild(b);
      const rows = body.querySelectorAll('.row');
      const last = rows.length ? rows[rows.length - 1] : null;
      if (last && last.parentNode) last.parentNode.insertBefore(entry, last); else body.appendChild(entry);
      markSettingsEntry();
    };
    new MutationObserver(check).observe(document.body, { childList: true, subtree: true });
    check();
  }

  // ---------------------------------------------------------------- flows
  async function runCheck(silent, fromDeepLink) {
    if (state.checking || state.busy) { if (!silent) showPanel(); return; }
    state.checking = true;
    const u = silent ? null : ensureUi();
    if (!silent) { showPanel(); setBusy(true); u.act.hidden = true; u.notes.hidden = true; setProgress(null); setStatus('Checking for updates…'); }
    try {
      const m = await fetchManifest();
      state.manifest = m;
      if (m.version > CUR) ls.set(LS_AVAIL, JSON.stringify({ version: m.version, notes: m.notes || '' }));
      else ls.del(LS_AVAIL);
      updateBadge();
      if (!silent) {
        setBusy(false);
        await presentManifest(m, fromDeepLink);
      }
    } catch (e) {
      if (!silent) {
        setBusy(false);
        setStatus('Could not check for updates: ' + (e && e.message ? e.message : e) +
          '\nThe app keeps working normally.', true);
      }
    } finally {
      state.checking = false;
    }
  }

  async function presentManifest(m, fromDeepLink) {
    const u = ensureUi();
    u.act.hidden = true; u.act.onclick = null;
    u.notes.hidden = !m.notes; u.notes.textContent = m.notes || '';
    if (m.version <= CUR) {
      u.notes.hidden = true;
      setStatus('You have the latest version (v' + CUR + ').');
      return;
    }
    if (kind === 'browser') {
      setStatus('Version ' + m.version + ' is available. Open the ChatRoom app to install it (this browser page cannot update itself).');
      u.act.textContent = 'Download page'; u.act.hidden = false;
      u.act.onclick = () => openExternal(siteUrl());
      return;
    }
    const nv = await nativeVersion();
    const need = neededNative(m);
    if (nv < need) {
      setStatus('Version ' + m.version + ' is available, but this update needs a reinstall: download and install the latest ' +
        (kind === 'android' ? 'APK' : 'installer') + ' from the website. Your chats are kept.');
      u.act.textContent = 'Open download page'; u.act.hidden = false;
      u.act.onclick = () => openExternal(siteUrl());
      return;
    }
    const total = m.files.reduce((s, f) => s + (Number(f.size) || 0), 0);
    setStatus('Update to v' + m.version + '? (' + fmtBytes(total) + ')' + (fromDeepLink ? '' : ''));
    u.act.textContent = 'Update to v' + m.version; u.act.hidden = false;
    u.act.onclick = () => doInstall(m);
  }

  async function doInstall(m) {
    const u = ensureUi();
    setBusy(true); u.act.hidden = true;
    try {
      setStatus('Downloading v' + m.version + '…'); setProgress(0);
      const files = await downloadAll(m, (done, total) => {
        setProgress(total ? done / total * 0.8 : 0);
        setStatus('Downloading v' + m.version + '… ' + fmtBytes(done) + ' / ' + fmtBytes(total));
      });
      setStatus('Installing…'); setProgress(0.85);
      await install(m, files, (i, n) => setProgress(0.85 + 0.15 * i / n));
      setProgress(1);
      setStatus('Installed. Restarting…');
    } catch (e) {
      setBusy(false); setProgress(null);
      ls.del(LS_JUST);
      setStatus('Update failed: ' + (e && e.message ? e.message : e), true);
      u.act.textContent = 'Retry'; u.act.hidden = false; u.act.onclick = () => doInstall(m);
    }
  }

  async function onReset() {
    if (!window.confirm('Go back to the version that came with the installed app? Your chats are kept.')) return;
    setBusy(true);
    try { setStatus('Resetting…'); await resetToBuiltIn(); }
    catch (e) { setBusy(false); setStatus('Reset failed: ' + (e && e.message ? e.message : e), true); }
  }

  function handleDeepLink(url) {
    if (!/^chatroom:/i.test(String(url || ''))) return;
    const rest = String(url).replace(/^chatroom:\/*/i, '').toLowerCase();
    if (rest === '' || rest.startsWith('update')) runCheck(false, true);
  }

  // ---------------------------------------------------------------- boot
  async function boot() {
    ensureUi();
    hookSettings();
    updateBadge();

    const just = ls.get(LS_JUST);
    let suppressLaunchUrl = false;
    if (just) {
      ls.del(LS_JUST);
      suppressLaunchUrl = true;
      if (Number(just) === CUR) { toast('ChatRoom updated to v' + CUR); ls.del(LS_AVAIL); ls.del('chatroom.update.tries'); updateBadge(); }
    }

    if (kind === 'android') {
      await bootAndroid().catch(() => {});
      const App = plug('App');
      if (App) {
        try { App.addListener('appUrlOpen', (e) => handleDeepLink(e && e.url)); } catch (e) { /* ignore */ }
        try {
          const r = await App.getLaunchUrl();
          const url = r && r.url;
          // getLaunchUrl keeps returning the same intent after a WebView reload; only act once per session
          let seen = null; try { seen = sessionStorage.getItem('chatroom.update.launchSeen'); } catch (e) { /* ignore */ }
          if (url && !suppressLaunchUrl && seen !== url) handleDeepLink(url);
          if (url) try { sessionStorage.setItem('chatroom.update.launchSeen', url); } catch (e) { /* ignore */ }
        } catch (e) { /* ignore */ }
      }
    } else if (kind === 'electron') {
      if (api.otaBooted) api.otaBooted().catch(() => {});
      if (api.onDeepLink) api.onDeepLink(handleDeepLink);
    }

    if (CONFIGURED) {
      // Check on every start, and on Android also whenever the app comes back to the foreground.
      setTimeout(autoUpdate, 1500);
      if (kind === 'android') {
        const App = plug('App');
        if (App) try { App.addListener('appStateChange', (s) => { if (s && s.isActive) autoUpdate(); }); } catch (e) { /* ignore */ }
      }
    }
  }

  // Automatic update: check in the background; if a newer version exists, download + install it
  // right away and show the progress popup. Gives up on a version after 3 failed attempts so a broken
  // release can't trap the app in a loop.
  const LS_TRIES = 'chatroom.update.tries';
  async function autoUpdate() {
    if (state.checking || state.busy || kind === 'browser') return;
    state.checking = true;
    let m;
    try { m = await fetchManifest(); } catch (e) { return; } finally { state.checking = false; }
    state.manifest = m;
    if (!(m.version > CUR)) { ls.del(LS_AVAIL); ls.del(LS_TRIES); updateBadge(); return; }
    ls.set(LS_AVAIL, JSON.stringify({ version: m.version, notes: m.notes || '' }));
    updateBadge();
    const u = ensureUi();
    const nv = await nativeVersion();
    if (nv < neededNative(m)) {
      // Can't be applied in place: tell the user once per version.
      if (ls.get('chatroom.update.nativeToldFor') === String(m.version)) return;
      ls.set('chatroom.update.nativeToldFor', String(m.version));
      showPanel(); await presentManifest(m);
      return;
    }
    let tries = {}; try { tries = JSON.parse(ls.get(LS_TRIES) || '{}'); } catch (e) { tries = {}; }
    const n = Number(tries[m.version]) || 0;
    if (n >= 3) return;
    tries = {}; tries[m.version] = n + 1; ls.set(LS_TRIES, JSON.stringify(tries));
    showPanel();
    u.notes.hidden = !m.notes; u.notes.textContent = m.notes || '';
    await doInstall(m);
  }

  window.ChatUpdater = {
    version: CUR, kind, configured: CONFIGURED,
    open: showPanel, check: () => runCheck(false), checkSilently: () => runCheck(true),
    handleDeepLink, _sha256Hex: sha256Hex, _sha256Fallback: sha256Fallback
  };

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
