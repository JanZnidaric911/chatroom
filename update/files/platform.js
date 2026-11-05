/* Linoa platform bridge: window.ChatPlatform
 * kind: 'electron' | 'android' | 'browser'
 * saveFile(name, mime, blob) -> Promise<string>  (saved path or human readable message)
 * copyText(text) -> Promise<void>
 * Electron extras: showInFolder(path), openPath(path)
 */
(function () {
  'use strict';

  const cap = window.Capacitor;
  const isAndroid = !!(cap && typeof cap.getPlatform === 'function' && cap.getPlatform() === 'android');
  const isElectron = !!window.electronAPI;
  const kind = isElectron ? 'electron' : (isAndroid ? 'android' : 'browser');

  function sanitizeName(name) {
    let n = String(name || 'file').replace(/[\\/:*?"<>|\u0000-\u001f]/g, '_').trim();
    if (!n || n === '.' || n === '..') n = 'file';
    return n.slice(0, 180);
  }

  function blobToBase64(blob) {
    return new Promise((resolve, reject) => {
      const r = new FileReader();
      r.onload = () => {
        const s = String(r.result);
        const i = s.indexOf(',');
        resolve(i >= 0 ? s.slice(i + 1) : s);
      };
      r.onerror = () => reject(r.error || new Error('read failed'));
      r.readAsDataURL(blob);
    });
  }

  // ---------- Electron ----------
  async function saveElectron(name, mime, blob) {
    const api = window.electronAPI;
    if (api.saveFileBegin && api.saveFileChunk && api.saveFileEnd) {
      // stream in 8 MB pieces so multi-GB videos don't have to fit in memory at once
      const id = await api.saveFileBegin(sanitizeName(name));
      const STEP = 8 * 1024 * 1024;
      for (let off = 0; off < blob.size; off += STEP) {
        await api.saveFileChunk(id, await blob.slice(off, off + STEP).arrayBuffer());
      }
      return await api.saveFileEnd(id);
    }
    const buf = await blob.arrayBuffer();
    return await api.saveFile(sanitizeName(name), buf);
  }

  // ---------- Android (Capacitor Filesystem) ----------
  const CHUNK_RAW = 3 * 349525; // ~1 MB, multiple of 3 so base64 chunks concatenate cleanly

  async function fsExists(FS, path, directory) {
    try { await FS.stat({ path, directory }); return true; } catch (e) { return false; }
  }

  async function uniquePath(FS, dir, name, directory) {
    const dot = name.lastIndexOf('.');
    const base = dot > 0 ? name.slice(0, dot) : name;
    const ext = dot > 0 ? name.slice(dot) : '';
    let candidate = dir + '/' + name;
    for (let i = 1; i < 1000 && await fsExists(FS, candidate, directory); i++) {
      candidate = dir + '/' + base + ' (' + i + ')' + ext;
    }
    return candidate;
  }

  async function writeChunked(FS, path, directory, blob) {
    if (blob.size === 0) {
      await FS.writeFile({ path, directory, data: '', recursive: true });
      return;
    }
    for (let off = 0; off < blob.size; off += CHUNK_RAW) {
      const b64 = await blobToBase64(blob.slice(off, Math.min(blob.size, off + CHUNK_RAW)));
      if (off === 0) await FS.writeFile({ path, directory, data: b64, recursive: true });
      else await FS.appendFile({ path, directory, data: b64 });
    }
  }

  async function saveAndroid(name, mime, blob) {
    const FS = cap.Plugins && cap.Plugins.Filesystem;
    if (!FS) throw new Error('Filesystem plugin not available');
    const safe = sanitizeName(name);
    try {
      try { if (FS.requestPermissions) await FS.requestPermissions(); } catch (e) { /* ignore */ }
      const path = await uniquePath(FS, 'Download/Linoa', safe, 'EXTERNAL_STORAGE');
      await writeChunked(FS, path, 'EXTERNAL_STORAGE', blob);
      return 'Internal storage/' + path;
    } catch (e) {
      console.warn('ExternalStorage save failed, falling back to Documents', e);
      const path = await uniquePath(FS, 'Linoa', safe, 'DOCUMENTS');
      await writeChunked(FS, path, 'DOCUMENTS', blob);
      return 'Documents/' + path;
    }
  }

  // ---------- Browser ----------
  async function saveBrowser(name, mime, blob) {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = sanitizeName(name);
    a.style.display = 'none';
    document.body.appendChild(a);
    a.click();
    setTimeout(() => { a.remove(); URL.revokeObjectURL(url); }, 60000);
    return 'Download started: ' + a.download;
  }

  async function copyText(text) {
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(text);
        return;
      }
    } catch (e) { /* fall through */ }
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.setAttribute('readonly', '');
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand('copy'); } finally { ta.remove(); }
  }

  window.ChatPlatform = {
    kind,
    saveFile(name, mime, blob) {
      if (kind === 'electron') return saveElectron(name, mime, blob);
      if (kind === 'android') return saveAndroid(name, mime, blob);
      return saveBrowser(name, mime, blob);
    },
    copyText,
    canShowInFolder: kind === 'electron' && typeof (window.electronAPI || {}).showInFolder === 'function',
    canOpenPath: kind === 'electron' && typeof (window.electronAPI || {}).openPath === 'function',
    showInFolder(p) { if (window.electronAPI && window.electronAPI.showInFolder) return window.electronAPI.showInFolder(p); },
    openPath(p) { if (window.electronAPI && window.electronAPI.openPath) return window.electronAPI.openPath(p); }
  };
})();
