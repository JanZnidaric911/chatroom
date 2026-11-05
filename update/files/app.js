/* ChatRoom web app — shared by Electron (desktop) and Capacitor (Android).
 *
 * Wire protocol (one PeerJS DataConnection, serialization 'raw', reliable/ordered):
 *   Control messages are JSON strings: {t: ...}
 *     hello      {t,v,name}                 start of sync, followed by
 *     have       {t,ids:[...]}              batches (<=1000) of every message id this device has COMPLETELY stored,
 *                                           plus every tombstoned (deleted) id, so deleted messages are never re-sent
 *     have-end   {t}                        -> peer now sends each of ITS OWN messages missing from that set;
 *                                           followed by `del` batches of all unacked delete-for-everyone tombstones
 *     msg        {t,m:{id,from,ts,type:'text',text}}
 *     file-start {t,m:{id,from,ts,type:'file',fileName,mime,size}, chunks}
 *     file-end   {t,id,chunks,size}
 *     file-abort {t,id}                     either direction: sender -> drop partial data; receiver -> stop sending
 *     ack        {t,id}                     receiver stored the message (text or full file) or knows it as deleted
 *     del        {t,ids:[...]}              delete for everyone (batches <=1000): peer deletes + tombstones them
 *     del-ack    {t,ids:[...]}              peer applied the delete -> tombstones no longer need sending
 *     ping/pong  {t}                        keep-alive
 *   Binary chunk = ArrayBuffer: [36 bytes ASCII message id][4 bytes uint32 BE chunk index][payload <= 64 KB]
 */
(function () {
  'use strict';

  // ---------------------------------------------------------------- constants
  const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  const ID_PREFIX = 'jzchatroom-';
  const CHUNK = 64 * 1024;
  const READ_BLOCK = 1024 * 1024;           // read source Blob 1 MB at a time
  const BUF_HIGH = 8 * 1024 * 1024 - 256 * 1024; // stay just below PeerJS' own 8 MB internal buffering threshold
  const BUF_LOW = 2 * 1024 * 1024;
  const RX_GROUP = 16 * 1024 * 1024;        // receiver: merge chunks into a Blob every 16 MB
  const HEADER = 40;
  const RETRY_MS = 5000;
  const CONNECT_TIMEOUT = 15000;
  const PING_MS = 5000;
  const DEAD_MS = 45000;
  const DEFAULT_ICE = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    // PeerJS's free public TURN relays (fallback when direct P2P fails, e.g. mobile data / CGNAT)
    { urls: ['turn:eu-0.turn.peerjs.com:3478', 'turn:us-0.turn.peerjs.com:3478'], username: 'peerjs', credential: 'peerjsp' }
  ];

  // ---------------------------------------------------------------- profile namespacing
  const params = new URLSearchParams(location.search);
  const PROFILE = (params.get('profile') || '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 32);
  const NS = 'chatroom' + (PROFILE ? '.' + PROFILE : '');
  const lsKey = (k) => NS + '.' + k;
  function lsGet(k, d) { try { const v = localStorage.getItem(lsKey(k)); return v == null ? d : v; } catch (e) { return d; } }
  function lsSet(k, v) { try { localStorage.setItem(lsKey(k), v); } catch (e) { /* ignore */ } }

  const P = window.ChatPlatform;
  const isTouch = P.kind === 'android' || (window.matchMedia && matchMedia('(pointer: coarse)').matches);

  // ---------------------------------------------------------------- utils
  const $ = (s, r) => (r || document).querySelector(s);
  function el(tag, cls, text) {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }
  function uuid() {
    if (crypto.randomUUID) return crypto.randomUUID();
    const b = crypto.getRandomValues(new Uint8Array(16));
    b[6] = (b[6] & 0x0f) | 0x40; b[8] = (b[8] & 0x3f) | 0x80;
    const h = [...b].map((x) => x.toString(16).padStart(2, '0')).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  function genCode() {
    const r = crypto.getRandomValues(new Uint8Array(8));
    let s = '';
    for (let i = 0; i < 8; i++) s += ALPHABET[r[i] % ALPHABET.length];
    return s;
  }
  const fmtCode = (c) => c.slice(0, 4) + '-' + c.slice(4);
  function validCode(c) { return c.length === 8 && [...c].every((ch) => ALPHABET.includes(ch)); }
  function fmtSize(n) {
    if (n < 1024) return n + ' B';
    const u = ['KB', 'MB', 'GB', 'TB'];
    let i = -1;
    do { n /= 1024; i++; } while (n >= 1024 && i < u.length - 1);
    return (n >= 100 ? n.toFixed(0) : n >= 10 ? n.toFixed(1) : n.toFixed(2)) + ' ' + u[i];
  }
  const pad2 = (n) => String(n).padStart(2, '0');
  function fmtTime(ts) { const d = new Date(ts); return pad2(d.getHours()) + ':' + pad2(d.getMinutes()); }
  function dayLabel(ts) {
    const d = new Date(ts); const now = new Date();
    const sameDay = (a, b) => a.toDateString() === b.toDateString();
    if (sameDay(d, now)) return 'Today';
    const y = new Date(now); y.setDate(now.getDate() - 1);
    if (sameDay(d, y)) return 'Yesterday';
    return d.toLocaleDateString(undefined, { day: 'numeric', month: 'short', year: d.getFullYear() === now.getFullYear() ? undefined : 'numeric' });
  }
  function fileKind(mime, name) {
    mime = mime || '';
    if (mime.startsWith('image/') && !/tiff|x-icon|heic|heif/.test(mime)) return 'image';
    if (mime.startsWith('video/')) return 'video';
    const ext = (name || '').split('.').pop().toLowerCase();
    if (!mime && ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'].includes(ext)) return 'image';
    if (!mime && ['mp4', 'webm', 'mov', 'm4v'].includes(ext)) return 'video';
    return 'file';
  }
  function extOf(name) {
    const i = (name || '').lastIndexOf('.');
    return i > 0 ? name.slice(i + 1, i + 5) : 'file';
  }
  const URL_RE = /\b((?:https?:\/\/|www\.)[^\s<>"']+[^\s<>"'.,;:!?)\]}])/gi;
  function linkify(text) {
    const frag = document.createDocumentFragment();
    let last = 0;
    text.replace(URL_RE, (m, _g, idx) => {
      if (idx > last) frag.appendChild(document.createTextNode(text.slice(last, idx)));
      const a = document.createElement('a');
      a.href = m.startsWith('www.') ? 'https://' + m : m;
      a.target = '_blank'; a.rel = 'noopener noreferrer';
      a.textContent = m;
      frag.appendChild(a);
      last = idx + m.length;
      return m;
    });
    if (last < text.length) frag.appendChild(document.createTextNode(text.slice(last)));
    return frag;
  }

  // ---------------------------------------------------------------- toast
  let toastTimer = null;
  function toast(msg, action, ms) {
    const t = $('#toast');
    t.innerHTML = '';
    t.appendChild(el('span', null, msg));
    if (action) {
      const b = el('button', 'btn small primary', action.label);
      b.onclick = () => { t.hidden = true; action.fn(); };
      t.appendChild(b);
    }
    t.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { t.hidden = true; }, ms || (action ? 7000 : 2600));
  }

  // ---------------------------------------------------------------- settings
  const settings = {
    get deviceName() { return lsGet('deviceName', P.kind === 'android' ? 'Phone' : P.kind === 'electron' ? 'PC' : 'Browser'); },
    set deviceName(v) { lsSet('deviceName', v); },
    get turnJson() { return lsGet('turn', ''); },
    set turnJson(v) { lsSet('turn', v); },
    iceServers() {
      let extra = [];
      try {
        const j = this.turnJson.trim();
        if (j) { const a = JSON.parse(j); if (Array.isArray(a)) extra = a; }
      } catch (e) { console.warn('Bad TURN JSON', e); }
      return DEFAULT_ICE.concat(extra);
    }
  };

  // ---------------------------------------------------------------- IndexedDB
  let dbp = null;
  function db() {
    if (dbp) return dbp;
    dbp = new Promise((resolve, reject) => {
      const req = indexedDB.open(NS, 2);
      req.onupgradeneeded = () => {
        const d = req.result;
        if (!d.objectStoreNames.contains('tombs')) {
          // tombstones of deleted messages: {id, room, ts, everyone, acked}
          const t = d.createObjectStore('tombs', { keyPath: 'id' });
          t.createIndex('room', 'room');
        }
        if (!d.objectStoreNames.contains('rooms')) d.createObjectStore('rooms', { keyPath: 'code' });
        if (!d.objectStoreNames.contains('messages')) {
          const s = d.createObjectStore('messages', { keyPath: 'id' });
          s.createIndex('room', 'room');
        }
        if (!d.objectStoreNames.contains('blobs')) d.createObjectStore('blobs', { keyPath: 'id' });
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    return dbp;
  }
  function reqP(r) { return new Promise((res, rej) => { r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error); }); }
  async function store(name, mode) { return (await db()).transaction(name, mode || 'readonly').objectStore(name); }
  async function dbPut(name, v) {
    const d = await db();
    return new Promise((res, rej) => {
      const tx = d.transaction(name, 'readwrite');
      tx.objectStore(name).put(v);
      tx.oncomplete = () => res(); tx.onerror = () => rej(tx.error); tx.onabort = () => rej(tx.error);
    });
  }
  async function dbGet(name, k) { return reqP((await store(name)).get(k)); }
  async function dbAll(name) { return reqP((await store(name)).getAll()); }
  async function dbDel(name, k) {
    const d = await db();
    return new Promise((res, rej) => {
      const tx = d.transaction(name, 'readwrite');
      tx.objectStore(name).delete(k);
      tx.oncomplete = () => res(); tx.onerror = () => rej(tx.error);
    });
  }
  async function roomMessages(code) {
    const s = await store('messages');
    const list = await reqP(s.index('room').getAll(code));
    // Order by local arrival time (rts) so clock skew between devices never reshuffles the chat.
    list.sort((a, b) => (a.rts || a.ts) - (b.rts || b.ts) || (a.id < b.id ? -1 : 1));
    return list;
  }
  async function putMessageAndBlob(m, blob) {
    const d = await db();
    return new Promise((res, rej) => {
      const tx = d.transaction(['messages', 'blobs'], 'readwrite');
      tx.objectStore('blobs').put({ id: m.id, blob });
      tx.objectStore('messages').put(m);
      tx.oncomplete = () => res(); tx.onerror = () => rej(tx.error); tx.onabort = () => rej(tx.error || new Error('aborted'));
    });
  }
  async function getBlob(id) { const r = await dbGet('blobs', id); return r ? r.blob : null; }

  // ---------- tombstones / deleting
  async function roomTombs(code) { return reqP((await store('tombs')).index('room').getAll(code)); }
  async function getTomb(code, id) { const t = await dbGet('tombs', id); return t && t.room === code ? t : null; }
  const RANK = { pending: 0, sent: 1, delivered: 2 };
  // Atomically raise a message's status (never downgrades, never resurrects a deleted message).
  async function setStatus(id, status) {
    const d = await db();
    return new Promise((res, rej) => {
      const tx = d.transaction('messages', 'readwrite');
      const s = tx.objectStore('messages');
      let out = null;
      const g = s.get(id);
      g.onsuccess = () => {
        const m = g.result;
        if (m && RANK[status] > RANK[m.status || 'pending']) { m.status = status; s.put(m); out = m; }
      };
      tx.oncomplete = () => res(out); tx.onerror = () => rej(tx.error); tx.onabort = () => rej(tx.error);
    });
  }
  // Delete messages + blobs of room `code` and write tombstones.
  // mode: 'me' (local only), 'local' (for everyone, must still be sent to the peer), 'remote' (peer asked us).
  async function deleteLocal(code, ids, mode) {
    const d = await db();
    const now = Date.now();
    return new Promise((res, rej) => {
      const tx = d.transaction(['messages', 'blobs', 'tombs'], 'readwrite');
      const ms = tx.objectStore('messages'), bs = tx.objectStore('blobs'), ts = tx.objectStore('tombs');
      for (const id of ids) {
        const g = ms.get(id);
        g.onsuccess = () => {
          const m = g.result;
          if (m && m.room !== code) return; // never touch another room
          if (m) { ms.delete(id); bs.delete(id); }
          const tg = ts.get(id);
          tg.onsuccess = () => {
            const old = tg.result;
            if (old && old.room !== code) return;
            if (mode === 'me' && old) return;
            const everyone = mode !== 'me';
            ts.put({ id, room: code, ts: old ? old.ts : now, everyone,
              acked: !everyone || mode === 'remote' || !!(old && old.everyone && old.acked) });
          };
        };
      }
      tx.oncomplete = () => res(); tx.onerror = () => rej(tx.error); tx.onabort = () => rej(tx.error || new Error('aborted'));
    });
  }
  async function markTombsAcked(code, ids) {
    const d = await db();
    return new Promise((res, rej) => {
      const tx = d.transaction('tombs', 'readwrite');
      const ts = tx.objectStore('tombs');
      for (const id of ids) {
        const g = ts.get(id);
        g.onsuccess = () => { const t = g.result; if (t && t.room === code && t.everyone && !t.acked) { t.acked = true; ts.put(t); } };
      }
      tx.oncomplete = () => res(); tx.onerror = () => rej(tx.error);
    });
  }
  // Store a received message unless it is (or concurrently became) tombstoned. Returns true if stored.
  async function storeIncoming(code, m, blob) {
    if (await getTomb(code, m.id)) return false;
    if (blob) await putMessageAndBlob(m, blob); else await dbPut('messages', m);
    if (await getTomb(code, m.id)) { await deleteLocal(code, [m.id], 'me'); return false; }
    return true;
  }
  const validIds = (a) => (Array.isArray(a) ? a.filter((x) => typeof x === 'string' && x.length === 36).slice(0, 5000) : []);

  async function deleteRoomData(code) {
    const msgs = await roomMessages(code);
    const tombs = await roomTombs(code);
    const d = await db();
    await new Promise((res, rej) => {
      const tx = d.transaction(['rooms', 'messages', 'blobs', 'tombs'], 'readwrite');
      for (const m of msgs) { tx.objectStore('messages').delete(m.id); tx.objectStore('blobs').delete(m.id); }
      for (const t of tombs) tx.objectStore('tombs').delete(t.id);
      tx.objectStore('rooms').delete(code);
      tx.oncomplete = () => res(); tx.onerror = () => rej(tx.error);
    });
  }

  // ---------------------------------------------------------------- state
  const rooms = new Map();       // code -> room record
  const conns = new Map();       // code -> RoomConn
  const lastMsg = new Map();     // code -> last message (home preview)
  let current = null;            // code of open chat
  const ui = { els: new Map(), urls: new Map(), msgs: [], savedPaths: new Map(), select: null /* Set of ids in select mode */ };

  // ---------------------------------------------------------------- binary header
  const enc = new TextEncoder();
  const dec = new TextDecoder();
  function makePacket(id, idx, data /* Uint8Array */) {
    const p = new Uint8Array(HEADER + data.byteLength);
    p.set(enc.encode(id.padEnd(36).slice(0, 36)), 0);
    new DataView(p.buffer).setUint32(36, idx);
    p.set(data, HEADER);
    return p.buffer;
  }
  function parsePacket(buf) {
    const u = new Uint8Array(buf);
    return {
      id: dec.decode(u.subarray(0, 36)).trim(),
      idx: new DataView(buf, 36, 4).getUint32(0),
      data: u.subarray(HEADER)
    };
  }

  // ================================================================ RoomConn
  class RoomConn {
    constructor(room) {
      this.room = room;
      this.code = room.code;
      this.slot = room.slot;
      this.otherSlot = room.slot === 1 ? 2 : 1;
      this.myId = ID_PREFIX + room.code + '-' + this.slot;
      this.otherId = ID_PREFIX + room.code + '-' + this.otherSlot;
      this.peer = null;
      this.conn = null;          // active, open DataConnection
      this.pendingOut = null;    // outgoing attempt in flight
      this.state = 'connecting'; // connecting | waiting | connected
      this.hint = '';
      this.lastFail = '';        // 'offline' | 'timeout' | 'server'
      this.failCount = 0;
      this.queue = [];           // file messages waiting to be sent
      this.queued = new Set();
      this.pumping = false;
      this.sending = null;       // {id, sent, size}
      this.cancelled = new Set(); // ids whose outgoing transfer must stop (deleted / aborted by peer)
      this.incoming = new Map(); // id -> rx state
      this.lastRx = 0;
      this.gen = 0;              // bumps on every connection change
      this.synced = false;       // peer's id list received on the current connection
      this.theirIds = null;
      this.rxChain = Promise.resolve();
      this.stopped = false;
      this.retryTimer = null;
      this.tickTimer = setInterval(() => this.tick(), RETRY_MS);
      this.pingTimer = setInterval(() => this.ping(), PING_MS);
      this.createPeer();
    }

    log(...a) { console.log('[' + this.code + '/' + this.slot + ']', ...a); }

    setState(s, hint) {
      this.state = s;
      if (hint !== undefined) this.hint = hint;
      onConnState(this);
    }

    createPeer() {
      if (this.stopped) return;
      if (this.peer && !this.peer.destroyed) { try { this.peer.destroy(); } catch (e) { /* */ } }
      this.setState('connecting', 'Contacting signalling server…');
      const peer = new Peer(this.myId, { config: { iceServers: settings.iceServers() }, debug: 0 });
      this.peer = peer;
      peer.on('open', () => {
        if (peer !== this.peer) return;
        this.log('peer open as', this.myId);
        this.failCount = 0;
        if (!this.conn) this.setState('waiting', '');
        this.tryConnect();
      });
      peer.on('connection', (c) => {
        if (peer !== this.peer) { c.close(); return; }
        if (c.peer !== this.otherId) { this.log('rejecting unknown peer', c.peer); c.close(); return; }
        this.log('incoming connection');
        this.setupConn(c, false);
      });
      peer.on('disconnected', () => {
        if (peer !== this.peer || this.stopped || peer.destroyed) return;
        this.log('signalling disconnected, reconnecting');
        setTimeout(() => {
          if (peer === this.peer && !peer.destroyed && peer.disconnected) {
            try { peer.reconnect(); } catch (e) { this.scheduleRecreate(RETRY_MS); }
          }
        }, 1500);
        if (!this.conn) this.setState('connecting', 'Reconnecting to signalling server…');
      });
      peer.on('close', () => {
        if (peer !== this.peer || this.stopped) return;
        this.scheduleRecreate(RETRY_MS);
      });
      peer.on('error', (err) => {
        if (peer !== this.peer) return;
        const type = err && err.type;
        if (type === 'peer-unavailable') {
          // The other device is offline / not registered: totally normal, keep trying quietly.
          this.lastFail = 'offline';
          this.dropPendingOut();
          if (!this.conn) this.setState('waiting', '');
          return;
        }
        this.log('peer error', type, err && err.message);
        if (type === 'unavailable-id') {
          // Our id is still held by a stale session on the server; retry soon.
          this.setState('connecting', 'Previous session still registered, retrying…');
          this.scheduleRecreate(4000);
        } else if (type === 'network' || type === 'server-error' || type === 'socket-error' || type === 'socket-closed') {
          this.lastFail = 'server';
          this.failCount++;
          if (!this.conn) this.setState('connecting', 'Can’t reach signalling server — check internet connection.');
          if (peer.destroyed) this.scheduleRecreate(RETRY_MS);
        } else if (type === 'browser-incompatible') {
          this.setState('connecting', 'WebRTC is not supported here.');
        } else if (type === 'webrtc') {
          this.lastFail = 'timeout';
          this.dropPendingOut();
        } else if (peer.destroyed) {
          this.scheduleRecreate(RETRY_MS);
        }
      });
    }

    scheduleRecreate(ms) {
      if (this.stopped) return;
      clearTimeout(this.retryTimer);
      this.retryTimer = setTimeout(() => { this.retryTimer = null; this.createPeer(); }, ms);
    }

    dropPendingOut() {
      if (this.pendingOut) {
        const c = this.pendingOut;
        this.pendingOut = null;
        clearTimeout(c._timeout);
        try { c.close(); } catch (e) { /* */ }
      }
    }

    tick() {
      if (this.stopped) return;
      const p = this.peer;
      if (!p || p.destroyed) { if (!this.retryTimer) this.scheduleRecreate(100); return; }
      if (p.disconnected) { try { p.reconnect(); } catch (e) { /* */ } return; }
      if (!this.conn) this.tryConnect();
    }

    tryConnect() {
      const p = this.peer;
      if (this.stopped || this.conn || this.pendingOut || !p || !p.open) return;
      const c = p.connect(this.otherId, { serialization: 'raw', reliable: true });
      if (!c) return;
      this.pendingOut = c;
      c._timeout = setTimeout(() => {
        if (this.pendingOut === c && !c.open) {
          // Signalling reached the other device but no data channel came up -> NAT trouble.
          if (this.lastFail !== 'offline') this.lastFail = 'timeout';
          this.log('connect attempt timed out');
          this.dropPendingOut();
          this.updateWaitingHint();
        }
      }, CONNECT_TIMEOUT);
      this.lastFail = '';
      this.setupConn(c, true);
    }

    updateWaitingHint() {
      if (this.conn) return;
      if (this.lastFail === 'timeout') {
        this.setState('waiting', 'Other device seems online but a direct connection failed. If this persists, add a TURN server in Settings.');
      }
    }

    setupConn(c, outgoing) {
      c._outgoing = outgoing;
      c.on('open', () => this.onOpen(c));
      c.on('data', (d) => { if (c === this.conn) this.onData(d); });
      c.on('close', () => {
        if (c === this.pendingOut) { clearTimeout(c._timeout); this.pendingOut = null; }
        if (c === this.conn) this.onClose('closed');
      });
      c.on('error', (e) => {
        this.log('conn error', e && e.type, e && e.message);
        if (c === this.conn) { try { c.close(); } catch (x) { /* */ } this.onClose('error'); }
      });
    }

    initiatorSlot(c) { return c._outgoing ? this.slot : this.otherSlot; }

    onOpen(c) {
      if (c === this.pendingOut) { clearTimeout(c._timeout); this.pendingOut = null; }
      if (this.stopped) { c.close(); return; }
      if (this.conn && this.conn.open && this.conn !== c) {
        // Both sides dialled at the same time (existing conn is very fresh): keep the connection
        // initiated by slot 1 on both ends. Otherwise the newer connection wins, because the other
        // side evidently considers the old one dead.
        const race = Date.now() - (this.conn._openedAt || 0) < 4000;
        if (race && this.initiatorSlot(this.conn) === 1 && this.initiatorSlot(c) !== 1) {
          this.log('duplicate connection, keeping existing');
          setTimeout(() => c.close(), 500);
          return;
        }
        this.log('duplicate connection, switching');
        const old = this.conn;
        this.conn = null;
        this.abortTransfers();
        setTimeout(() => { try { old.close(); } catch (e) { /* */ } }, 500);
      }
      this.conn = c;
      c._openedAt = Date.now();
      this.gen++;
      this.synced = false;
      this.theirIds = null;
      this.lastRx = Date.now();
      this.lastFail = '';
      const pc = c.peerConnection;
      if (pc) {
        pc.addEventListener('iceconnectionstatechange', () => {
          if (c === this.conn && (pc.iceConnectionState === 'failed' || pc.iceConnectionState === 'closed')) {
            try { c.close(); } catch (e) { /* */ }
            this.onClose('ice ' + pc.iceConnectionState);
          }
        });
      }
      this.log('connected (initiator slot ' + this.initiatorSlot(c) + ')');
      this.setState('connected', '');
      this.sendHello();
    }

    onClose(why) {
      this.log('connection closed:', why);
      this.conn = null;
      this.abortTransfers();
      if (!this.stopped) {
        this.setState(this.peer && this.peer.open ? 'waiting' : 'connecting', '');
        setTimeout(() => this.tick(), 1000);
      }
    }

    abortTransfers() {
      // Outgoing: everything not yet acked goes back to pending and is resent from scratch after the next hello.
      this.gen++;
      this.synced = false;
      this.theirIds = null;
      this.queue = [];
      this.queued.clear();
      if (this.sending) onTxProgress(this.code, this.sending.id, null);
      this.sending = null;
      // Incoming: drop partial data.
      for (const id of this.incoming.keys()) onRxProgress(this.code, id, null);
      this.incoming.clear();
    }

    ping() {
      if (!this.conn) return;
      if (Date.now() - this.lastRx > DEAD_MS) {
        this.log('connection dead (no data)');
        const c = this.conn;
        try { c.close(); } catch (e) { /* */ }
        if (c === this.conn) this.onClose('timeout');
        return;
      }
      this.sendJSON({ t: 'ping' });
    }

    sendJSON(o) {
      const c = this.conn;
      if (!c || !c.open) return false;
      try { c.send(JSON.stringify(o)); return true; } catch (e) { this.log('send failed', e); return false; }
    }

    async sendHello(nameOnly) {
      const conn = this.conn;
      if (!this.sendJSON({ t: 'hello', v: 1, name: settings.deviceName, sync: !nameOnly })) return;
      if (nameOnly) return;
      const [msgs, tombs] = await Promise.all([roomMessages(this.code), roomTombs(this.code)]);
      if (conn !== this.conn) return;
      // Tombstoned ids count as "known" so the peer never re-sends a deleted message.
      const ids = msgs.map((m) => m.id).concat(tombs.map((t) => t.id));
      for (let i = 0; i < ids.length; i += 1000) this.sendJSON({ t: 'have', ids: ids.slice(i, i + 1000) });
      this.sendJSON({ t: 'have-end' });
      this.sendDel(tombs.filter((t) => t.everyone && !t.acked).map((t) => t.id));
    }

    // 1000 ids * ~39 bytes ≈ 39 KB per message, well below the ~256 KB data-channel limit.
    sendDel(ids) {
      for (let i = 0; i < ids.length; i += 1000) this.sendJSON({ t: 'del', ids: ids.slice(i, i + 1000) });
    }

    // Stop any transfer (either direction) of these message ids.
    cancelTransfers(ids) {
      const set = new Set(ids);
      for (const id of set) {
        if (this.incoming.has(id)) {
          this.incoming.delete(id);
          onRxProgress(this.code, id, null);
          // tell the sender to stop; the ack marks it as handled so it is not re-sent mid-session
          this.sendJSON({ t: 'file-abort', id });
          this.sendJSON({ t: 'ack', id });
        }
      }
      if (this.queue.some((m) => set.has(m.id))) {
        this.queue = this.queue.filter((m) => !set.has(m.id));
        for (const id of set) this.queued.delete(id);
      }
      if (this.sending && set.has(this.sending.id)) this.cancelled.add(this.sending.id);
    }

    // ---------- receiving (strictly serialized so chunks never overtake their file-start)
    onData(d) {
      this.lastRx = Date.now();
      const conn = this.conn;
      this.rxChain = this.rxChain.then(() => (conn === this.conn ? this.handleData(d) : null))
        .catch((e) => console.error('rx failed', e));
    }

    async handleData(d) {
      if (typeof d === 'string') {
        let o;
        try { o = JSON.parse(d); } catch (e) { this.log('bad json'); return; }
        try { await this.onControl(o); } catch (e) { console.error('control handling failed', o && o.t, e); }
      } else if (d instanceof ArrayBuffer) {
        this.onChunk(d);
      } else if (d && d.buffer instanceof ArrayBuffer) {
        this.onChunk(d.buffer.slice(d.byteOffset, d.byteOffset + d.byteLength));
      } else if (d instanceof Blob) {
        this.onChunk(await d.arrayBuffer());
      }
    }

    async onControl(o) {
      switch (o.t) {
        case 'ping': this.sendJSON({ t: 'pong' }); break;
        case 'pong': break;
        case 'hello': await this.onHello(o); break;
        case 'have':
          if (this.theirIds && Array.isArray(o.ids)) for (const id of o.ids) this.theirIds.add(id);
          break;
        case 'have-end': await this.onSync(); break;
        case 'msg': {
          const m = sanitizeMeta(o.m, this.code, this.otherSlot);
          if (!m || m.type !== 'text') return;
          m.rts = Date.now();
          const exists = await dbGet('messages', m.id);
          if (!exists) {
            m.status = 'delivered';
            if (await storeIncoming(this.code, m, null)) onNewMessage(m);
          }
          this.sendJSON({ t: 'ack', id: m.id });
          break;
        }
        case 'file-start': {
          const m = sanitizeMeta(o.m, this.code, this.otherSlot);
          if (!m || m.type !== 'file') return;
          if (await getTomb(this.code, m.id)) {
            // deleted here: refuse the transfer, but ack so the sender considers it handled
            this.sendJSON({ t: 'file-abort', id: m.id });
            this.sendJSON({ t: 'ack', id: m.id });
            this.incoming.set(m.id, { meta: m, skip: true });
            return;
          }
          const exists = await dbGet('messages', m.id);
          const rx = { meta: m, parts: [], cur: [], curSize: 0, received: 0, next: 0, skip: !!exists, lastUi: 0 };
          this.incoming.set(m.id, rx);
          if (!exists) onRxProgress(this.code, m.id, rx);
          break;
        }
        case 'file-end': {
          const rx = this.incoming.get(o.id);
          if (!rx) return;
          this.incoming.delete(o.id);
          if (rx.skip) { this.sendJSON({ t: 'ack', id: o.id }); return; }
          if (rx.received !== rx.meta.size || rx.next !== o.chunks) {
            this.log('file incomplete', rx.received, rx.meta.size, rx.next, o.chunks);
            onRxProgress(this.code, o.id, null);
            return; // no ack -> sender resends on next connect
          }
          if (rx.cur.length) rx.parts.push(new Blob(rx.cur));
          const blob = new Blob(rx.parts, { type: rx.meta.mime || 'application/octet-stream' });
          rx.parts = []; rx.cur = [];
          const m = rx.meta;
          m.status = 'delivered';
          m.rts = Date.now();
          const stored = await storeIncoming(this.code, m, blob);
          onRxProgress(this.code, m.id, null);
          if (stored) onNewMessage(m);
          this.sendJSON({ t: 'ack', id: m.id });
          break;
        }
        case 'file-abort': {
          if (this.incoming.has(o.id)) { this.incoming.delete(o.id); onRxProgress(this.code, o.id, null); }
          // receiver refused / dropped our transfer -> stop sending chunks
          if (this.sending && this.sending.id === o.id) this.cancelled.add(o.id);
          if (this.queued.has(o.id)) { this.queue = this.queue.filter((m) => m.id !== o.id); this.queued.delete(o.id); }
          break;
        }
        case 'ack': {
          const m = await dbGet('messages', o.id);
          if (m && m.room === this.code) {
            const u = await setStatus(o.id, 'delivered');
            if (u) onMessageUpdated(u);
          }
          break;
        }
        case 'del': {
          const ids = validIds(o.ids);
          if (!ids.length) return;
          await applyDelete(this.code, ids, 'remote');
          this.sendJSON({ t: 'del-ack', ids });
          break;
        }
        case 'del-ack': {
          const ids = validIds(o.ids);
          if (ids.length) await markTombsAcked(this.code, ids);
          break;
        }
        default: this.log('unknown control', o.t);
      }
    }

    onChunk(buf) {
      if (buf.byteLength < HEADER) return;
      const { id, idx, data } = parsePacket(buf);
      const rx = this.incoming.get(id);
      if (!rx) return;
      if (rx.skip) return;
      if (idx !== rx.next) {
        this.log('chunk out of order', idx, rx.next);
        this.incoming.delete(id);
        onRxProgress(this.code, id, null);
        return;
      }
      rx.next++;
      rx.cur.push(data);
      rx.curSize += data.byteLength;
      rx.received += data.byteLength;
      if (rx.curSize >= RX_GROUP) {
        rx.parts.push(new Blob(rx.cur));
        rx.cur = []; rx.curSize = 0;
      }
      const now = Date.now();
      if (now - rx.lastUi > 200) { rx.lastUi = now; onRxProgress(this.code, id, rx); }
    }

    async onHello(o) {
      this.room.peerName = String(o.name || '').slice(0, 60);
      rooms.set(this.code, this.room);
      dbPut('rooms', this.room).catch(() => {});
      onConnState(this);
      if (o.sync !== false) this.theirIds = new Set();
    }

    async onSync() {
      const theirs = this.theirIds;
      if (!theirs) return;
      this.theirIds = null;
      this.synced = true;
      const msgs = await roomMessages(this.code);
      for (const m of msgs) {
        if (m.from !== this.slot) continue;
        if (theirs.has(m.id)) {
          if (m.status !== 'delivered') {
            const u = await setStatus(m.id, 'delivered');
            if (u) onMessageUpdated(u);
          }
        } else {
          this.enqueue(m);
        }
      }
    }

    // ---------- sending
    enqueue(m) {
      // Before the peer's id list arrived, onSync() will pick the message up anyway.
      if (!this.conn || !this.synced) return;
      if (m.type === 'text') {
        if (this.sendJSON({ t: 'msg', m: wireMeta(m) }) && m.status === 'pending') {
          setStatus(m.id, 'sent').then((u) => { if (u) onMessageUpdated(u); }).catch(() => {});
        }
        return;
      }
      if (this.queued.has(m.id)) return;
      this.queued.add(m.id);
      this.queue.push(m);
      this.pump();
    }

    async pump() {
      if (this.pumping) return;
      this.pumping = true;
      try {
        while (this.queue.length && this.conn) {
          const m = this.queue.shift();
          const conn = this.conn;
          const gen = this.gen;
          try {
            await this.sendFile(m, conn);
          } catch (e) {
            this.log('file send aborted:', e && e.message);
            if (conn === this.conn) {
              // unexpected failure on a still-open connection: tell peer to drop partial
              this.sendJSON({ t: 'file-abort', id: m.id });
            }
          } finally {
            this.cancelled.delete(m.id);
            if (gen === this.gen) {
              this.queued.delete(m.id);
              if (this.sending && this.sending.id === m.id) this.sending = null;
              onTxProgress(this.code, m.id, null);
            }
          }
        }
      } finally {
        this.pumping = false;
      }
    }

    async sendFile(m, conn) {
      const blob = await getBlob(m.id);
      if (!blob) throw new Error('blob missing');
      const size = blob.size;
      const chunks = Math.ceil(size / CHUNK);
      const alive = () => {
        if (this.cancelled.has(m.id)) throw new Error('cancelled');
        return conn === this.conn && conn.open;
      };
      this.sending = { id: m.id, sent: 0, size };
      onTxProgress(this.code, m.id, this.sending);
      if (!this.sendJSON({ t: 'file-start', m: wireMeta(m), chunks })) throw new Error('not connected');
      let idx = 0;
      let lastUi = 0;
      for (let off = 0; off < size; off += READ_BLOCK) {
        const block = new Uint8Array(await blob.slice(off, Math.min(size, off + READ_BLOCK)).arrayBuffer());
        for (let p = 0; p < block.byteLength; p += CHUNK) {
          if (!alive()) throw new Error('connection lost');
          await this.waitBuffer(conn);
          if (!alive()) throw new Error('connection lost');
          conn.send(makePacket(m.id, idx++, block.subarray(p, Math.min(block.byteLength, p + CHUNK))));
          this.sending.sent = Math.min(size, off + p + CHUNK);
          const now = Date.now();
          if (now - lastUi > 200) { lastUi = now; onTxProgress(this.code, m.id, this.sending); }
        }
      }
      if (!alive()) throw new Error('connection lost');
      this.sendJSON({ t: 'file-end', id: m.id, chunks, size });
      if (m.status === 'pending') {
        const u = await setStatus(m.id, 'sent');
        if (u) onMessageUpdated(u);
      }
    }

    waitBuffer(conn) {
      const dc = conn.dataChannel;
      if (!dc) return Promise.resolve();
      const full = () => dc.bufferedAmount + (conn.bufferSize || 0) * CHUNK > BUF_HIGH;
      if (!full()) return Promise.resolve();
      return new Promise((resolve) => {
        dc.bufferedAmountLowThreshold = BUF_LOW;
        let timer = null;
        const done = () => {
          dc.removeEventListener('bufferedamountlow', check);
          clearInterval(timer);
          resolve();
        };
        const check = () => { if (!full() || conn !== this.conn || dc.readyState !== 'open') done(); };
        dc.addEventListener('bufferedamountlow', check);
        timer = setInterval(check, 250); // safety net
      });
    }

    stop() {
      this.stopped = true;
      clearInterval(this.tickTimer);
      clearInterval(this.pingTimer);
      clearTimeout(this.retryTimer);
      this.dropPendingOut();
      if (this.conn) { try { this.conn.close(); } catch (e) { /* */ } }
      this.conn = null;
      if (this.peer) { try { this.peer.destroy(); } catch (e) { /* */ } }
    }
  }

  function wireMeta(m) {
    const o = { id: m.id, from: m.from, ts: m.ts, type: m.type };
    if (m.type === 'text') o.text = m.text;
    else { o.fileName = m.fileName; o.mime = m.mime; o.size = m.size; }
    return o;
  }
  function sanitizeMeta(m, room, from) {
    if (!m || typeof m.id !== 'string' || m.id.length !== 36) return null;
    const o = { id: m.id, room, from, ts: Number(m.ts) || Date.now(), type: m.type === 'file' ? 'file' : 'text' };
    if (o.type === 'text') o.text = String(m.text == null ? '' : m.text);
    else {
      o.fileName = String(m.fileName || 'file').slice(0, 255);
      o.mime = String(m.mime || '');
      o.size = Math.max(0, Number(m.size) || 0);
    }
    return o;
  }

  // ================================================================ app actions
  async function startRoom(room) {
    if (conns.has(room.code)) return;
    conns.set(room.code, new RoomConn(room));
  }

  async function createMessage(code, partial, blob) {
    const room = rooms.get(code);
    const now = Date.now();
    const m = Object.assign({ id: uuid(), room: code, from: room.slot, ts: now, rts: now, status: 'pending' }, partial);
    if (blob) await putMessageAndBlob(m, blob);
    else await dbPut('messages', m);
    onNewMessage(m);
    const rc = conns.get(code);
    if (rc) rc.enqueue(m);
    return m;
  }

  // mode: 'me' | 'local' (for everyone, initiated here) | 'remote' (for everyone, initiated by peer)
  async function applyDelete(code, ids, mode) {
    ids = [...new Set(ids)];
    if (!ids.length) return;
    const rc = conns.get(code);
    if (rc) rc.cancelTransfers(ids);
    await deleteLocal(code, ids, mode);
    onMessagesDeleted(code, ids);
    if (mode === 'local' && rc && rc.conn) rc.sendDel(ids); // offline -> sent from sendHello() on next connect
  }
  const deleteMessages = (code, ids, everyone) => applyDelete(code, ids, everyone ? 'local' : 'me');

  async function sendText(text) {
    if (!current) return;
    text = text.replace(/\s+$/, '');
    if (!text.trim()) return;
    if (text.length > 50000) {
      // Data channel messages are limited (~256 KB) -> very long texts travel as a .txt file.
      const f = new Blob([text], { type: 'text/plain' });
      await createMessage(current, { type: 'file', fileName: 'message-' + Date.now() + '.txt', mime: 'text/plain', size: f.size }, f);
      return;
    }
    await createMessage(current, { type: 'text', text });
  }

  async function sendFiles(files) {
    if (!current || !files) return;
    const code = current;
    for (const f of Array.from(files)) {
      const name = f.name || ('pasted-' + new Date().toISOString().replace(/[:.]/g, '-') + extFromMime(f.type));
      await createMessage(code, { type: 'file', fileName: name, mime: f.type || '', size: f.size }, f);
    }
  }
  function extFromMime(m) {
    const map = { 'image/png': '.png', 'image/jpeg': '.jpg', 'image/gif': '.gif', 'image/webp': '.webp', 'text/plain': '.txt' };
    return map[m] || '';
  }

  // ================================================================ UI: screens
  function show(id) {
    for (const s of document.querySelectorAll('.screen')) s.classList.toggle('active', s.id === id);
  }

  async function goHome() {
    exitSelect();
    current = null;
    closeViewer();
    revokeUrls();
    $('#messages').innerHTML = '';
    show('home');
    await renderHome();
  }

  function revokeUrls() {
    for (const u of ui.urls.values()) URL.revokeObjectURL(u);
    ui.urls.clear();
    ui.els.clear();
  }

  // ---------- home
  async function renderHome() {
    const list = $('#roomList');
    list.innerHTML = '';
    const arr = [...rooms.values()].sort((a, b) => (lastTs(b) - lastTs(a)));
    $('#emptyHome').hidden = arr.length > 0;
    for (const r of arr) {
      const li = el('li', 'room');
      li.dataset.code = r.code;
      const av = el('div', 'room-avatar', (r.name || '?').trim().charAt(0).toUpperCase() || '#');
      const dot = el('span', 'dot ' + stateOf(r.code));
      av.appendChild(dot);
      const info = el('div', 'room-info');
      info.appendChild(el('div', 'room-name', r.name));
      const lm = lastMsg.get(r.code);
      info.appendChild(el('div', 'room-last', lm ? previewText(lm, r) : stateLabel(r.code)));
      info.appendChild(el('div', 'room-code', fmtCode(r.code) + (r.peerName ? ' · ' + r.peerName : '')));
      const more = el('button', 'icon-btn');
      more.title = 'Room options';
      more.innerHTML = '<svg viewBox="0 0 24 24" width="20" height="20"><path fill="currentColor" d="M12 8a2 2 0 1 0 0-4 2 2 0 0 0 0 4Zm0 2a2 2 0 1 0 0 4 2 2 0 0 0 0-4Zm0 6a2 2 0 1 0 0 4 2 2 0 0 0 0-4Z"/></svg>';
      more.onclick = (e) => { e.stopPropagation(); roomMenu(r, more); };
      li.append(av, info, more);
      li.onclick = () => openChat(r.code);
      list.appendChild(li);
    }
  }
  function lastTs(r) { const m = lastMsg.get(r.code); return m ? m.ts : (r.created || 0); }
  function previewText(m, r) {
    const me = m.from === r.slot ? 'You: ' : '';
    return me + (m.type === 'text' ? m.text.split('\n')[0] : '\u{1F4CE} ' + m.fileName);
  }
  function stateOf(code) { const c = conns.get(code); return c ? c.state : 'connecting'; }
  function stateLabel(code) {
    const s = stateOf(code);
    return s === 'connected' ? 'Connected' : s === 'waiting' ? 'Waiting for other device…' : 'Connecting…';
  }

  function roomMenu(r, anchor) {
    openMenu(anchor, [
      { label: 'Show code', fn: () => showCode(r, false) },
      { label: 'Rename', fn: () => renameRoom(r) },
      { label: 'Clear chat for everyone', danger: true, fn: () => clearChat(r) },
      { label: 'Delete', danger: true, fn: () => deleteRoom(r) }
    ]);
  }

  function openMenu(anchor, items) {
    closeMenu();
    const m = el('div', 'menu');
    m.id = 'menu';
    for (const it of items) {
      const b = el('button', it.danger ? 'danger' : '', it.label);
      b.onclick = (e) => { e.stopPropagation(); closeMenu(); it.fn(); };
      m.appendChild(b);
    }
    document.body.appendChild(m);
    const rc = anchor.getBoundingClientRect();
    const w = m.offsetWidth, h = m.offsetHeight;
    m.style.left = Math.max(8, Math.min(innerWidth - w - 8, rc.right - w)) + 'px';
    m.style.top = (rc.bottom + h + 8 > innerHeight ? Math.max(8, rc.top - h - 4) : rc.bottom + 4) + 'px';
    setTimeout(() => document.addEventListener('click', closeMenu, { once: true }), 0);
  }
  function closeMenu() { const m = $('#menu'); if (m) m.remove(); }

  // ---------- modal
  function openModal(build) {
    const body = $('#modalBody');
    body.innerHTML = '';
    build(body);
    $('#modal').hidden = false;
    const f = body.querySelector('input, textarea');
    if (f && !isTouch) setTimeout(() => f.focus(), 30);
  }
  function closeModal() { $('#modal').hidden = true; $('#modalBody').innerHTML = ''; }
  function confirmDialog(title, text, okLabel, danger) {
    return new Promise((resolve) => {
      openModal((b) => {
        b.appendChild(el('h3', null, title));
        b.appendChild(el('p', null, text));
        const row = el('div', 'row');
        const cancel = el('button', 'btn', 'Cancel');
        const ok = el('button', 'btn ' + (danger ? 'danger' : 'primary'), okLabel);
        cancel.onclick = () => { closeModal(); resolve(false); };
        ok.onclick = () => { closeModal(); resolve(true); };
        row.append(cancel, ok);
        b.appendChild(row);
      });
    });
  }
  function promptDialog(title, label, value, okLabel) {
    return new Promise((resolve) => {
      openModal((b) => {
        b.appendChild(el('h3', null, title));
        const l = el('label', null, label);
        const inp = el('input');
        inp.value = value || '';
        inp.maxLength = 60;
        const row = el('div', 'row');
        const cancel = el('button', 'btn', 'Cancel');
        const ok = el('button', 'btn primary', okLabel || 'OK');
        cancel.onclick = () => { closeModal(); resolve(null); };
        ok.onclick = () => { closeModal(); resolve(inp.value.trim()); };
        inp.onkeydown = (e) => { if (e.key === 'Enter') ok.click(); };
        row.append(cancel, ok);
        b.append(l, inp, row);
      });
    });
  }

  // ---------- room actions
  async function createRoom() {
    let code;
    do { code = genCode(); } while (rooms.has(code));
    const r = { code, slot: 1, name: 'Room ' + fmtCode(code), created: Date.now() };
    await dbPut('rooms', r);
    rooms.set(code, r);
    startRoom(r);
    await renderHome();
    showCode(r, true);
  }

  function showCode(r, isNew) {
    openModal((b) => {
      b.appendChild(el('h3', null, isNew ? 'Room created' : r.name));
      b.appendChild(el('p', null, r.slot === 1
        ? 'Type this code on your other device (Join room) to connect it to this room.'
        : 'This device joined the room with this code.'));
      b.appendChild(el('div', 'code-big', fmtCode(r.code)));
      const row = el('div', 'row');
      const copy = el('button', 'btn', 'Copy code');
      copy.onclick = async () => { await P.copyText(fmtCode(r.code)); toast('Code copied'); };
      const open = el('button', 'btn primary', 'Open chat');
      open.onclick = () => { closeModal(); openChat(r.code); };
      row.append(copy, open);
      b.appendChild(row);
    });
  }

  function joinRoom() {
    openModal((b) => {
      b.appendChild(el('h3', null, 'Join room'));
      b.appendChild(el('p', null, 'Enter the 8-character code shown on your other device.'));
      const inp = el('input', 'code-input');
      inp.placeholder = 'XXXX-XXXX';
      inp.autocapitalize = 'characters';
      inp.spellcheck = false;
      inp.maxLength = 12;
      const err = el('div', 'err');
      const row = el('div', 'row');
      const cancel = el('button', 'btn', 'Cancel');
      const ok = el('button', 'btn primary', 'Join');
      cancel.onclick = closeModal;
      const go = async () => {
        const code = String(inp.value).toUpperCase().replace(/[^A-Z0-9]/g, '');
        if (!validCode(code)) { err.textContent = 'The code has 8 characters (letters and digits, no 0/O/1/I).'; return; }
        if (rooms.has(code)) { closeModal(); openChat(code); return; }
        const r = { code, slot: 2, name: 'Room ' + fmtCode(code), created: Date.now() };
        await dbPut('rooms', r);
        rooms.set(code, r);
        startRoom(r);
        closeModal();
        await renderHome();
        openChat(code);
      };
      ok.onclick = go;
      inp.onkeydown = (e) => { if (e.key === 'Enter') go(); };
      inp.oninput = () => {
        const raw = inp.value.toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
        const f = raw.length > 4 ? raw.slice(0, 4) + '-' + raw.slice(4) : raw;
        if (f !== inp.value) inp.value = f;
        err.textContent = '';
      };
      row.append(cancel, ok);
      b.append(inp, err, row);
    });
  }

  async function renameRoom(r) {
    const name = await promptDialog('Rename room', 'Name', r.name, 'Save');
    if (!name) return;
    r.name = name;
    await dbPut('rooms', r);
    if (current === r.code) renderChatHeader();
    else renderHome();
  }

  // ---------- deleting messages
  function peerLabel(code) { const r = rooms.get(code); return (r && r.peerName) || 'the other device'; }

  async function confirmDelete(code, ids, everyone) {
    ids = [...new Set(ids)];
    const n = ids.length;
    if (!n) return false;
    const hasFile = ids.some((id) => {
      const m = ui.msgs.find((x) => x.id === id);
      return !m || m.type === 'file';
    });
    const what = n === 1 ? 'this message' : n + ' messages';
    const title = (n === 1 ? 'Delete message' : 'Delete ' + n + ' messages') + (everyone ? ' for everyone?' : ' for me?');
    let text = everyone
      ? 'Removes ' + what + ' from this device and from ' + peerLabel(code) + '. If it is offline, it is removed there as soon as it reconnects.'
      : 'Removes ' + what + ' only from this device. ' + peerLabel(code).replace(/^the/, 'The') + ' keeps its copy.';
    if (hasFile) text += ' Saved copies in Downloads are kept.';
    const ok = await confirmDialog(title, text, 'Delete', true);
    if (!ok) return false;
    try {
      await deleteMessages(code, ids, everyone);
      toast(n === 1 ? 'Message deleted' : n + ' messages deleted');
    } catch (e) { console.error('delete failed', e); toast('Delete failed: ' + (e && e.message ? e.message : e), null, 5000); }
    return true;
  }

  async function clearChat(r) {
    const ok = await confirmDialog('Clear chat for everyone?',
      'Deletes every message in "' + r.name + '" on this device and on ' + peerLabel(r.code) +
      ' (as soon as it reconnects, if it is offline). Saved copies in Downloads are kept.', 'Clear chat', true);
    if (!ok) return;
    try {
      const [msgs, tombs] = await Promise.all([roomMessages(r.code), roomTombs(r.code)]);
      const rc = conns.get(r.code);
      const ids = msgs.map((m) => m.id)
        .concat(tombs.filter((t) => !t.everyone).map((t) => t.id)) // earlier "delete for me" -> now for everyone too
        .concat(rc ? [...rc.incoming.keys()] : []);
      if (current === r.code) exitSelect();
      await deleteMessages(r.code, ids, true);
      toast('Chat cleared');
    } catch (e) { console.error('clear failed', e); toast('Clear failed: ' + (e && e.message ? e.message : e), null, 5000); }
  }

  async function deleteRoom(r) {
    const ok = await confirmDialog('Delete room?', 'This deletes "' + r.name + '" and its whole message history on this device. The other device keeps its copy.', 'Delete', true);
    if (!ok) return;
    const c = conns.get(r.code);
    if (c) c.stop();
    conns.delete(r.code);
    rooms.delete(r.code);
    lastMsg.delete(r.code);
    await deleteRoomData(r.code);
    if (current === r.code) await goHome();
    else renderHome();
  }

  // ---------- settings
  function openSettings() {
    openModal((b) => {
      b.appendChild(el('h3', null, 'Settings'));
      const l1 = el('label', null, 'Device name (shown to the other device)');
      const name = el('input');
      name.value = settings.deviceName;
      name.maxLength = 40;
      const l2 = el('label', null, 'TURN servers (optional, JSON array of RTCIceServer)');
      const ta = el('textarea');
      ta.spellcheck = false;
      ta.placeholder = '[\n  {\n    "urls": "turn:global.relay.metered.ca:80",\n    "username": "…",\n    "credential": "…"\n  }\n]';
      ta.value = settings.turnJson;
      const hint = el('p', 'hint');
      hint.innerHTML = 'Usually not needed. On some mobile networks a direct connection is impossible and the devices never connect — then a TURN relay is required, e.g. a free <b>metered.ca</b> account. Paste its ICE server list here on <b>both</b> devices.';
      const err = el('div', 'err');
      const info = el('p', 'hint', 'Platform: ' + P.kind + (PROFILE ? ' · profile ' + PROFILE : ''));
      const row = el('div', 'row');
      const cancel = el('button', 'btn', 'Cancel');
      const save = el('button', 'btn primary', 'Save');
      cancel.onclick = closeModal;
      save.onclick = () => {
        const t = ta.value.trim();
        if (t) {
          try {
            const a = JSON.parse(t);
            if (!Array.isArray(a) || !a.every((x) => x && (typeof x.urls === 'string' || Array.isArray(x.urls)))) throw new Error('Must be an array of objects with "urls"');
          } catch (e) { err.textContent = 'Invalid JSON: ' + e.message; return; }
        }
        const turnChanged = t !== settings.turnJson.trim();
        const nameChanged = name.value.trim() !== settings.deviceName;
        settings.deviceName = name.value.trim() || settings.deviceName;
        settings.turnJson = t;
        closeModal();
        toast('Settings saved');
        if (turnChanged) restartAll();
        else if (nameChanged) for (const c of conns.values()) if (c.conn) c.sendHello(true);
      };
      row.append(cancel, save);
      b.append(l1, name, l2, ta, hint, err, info, row);
    });
  }
  function restartAll() {
    for (const [code, c] of conns) { c.stop(); conns.delete(code); }
    for (const r of rooms.values()) startRoom(r);
  }

  // ---------- chat
  async function openChat(code) {
    const r = rooms.get(code);
    if (!r) return;
    closeMenu();
    exitSelect();
    revokeUrls();
    current = code;
    show('chat');
    renderChatHeader();
    const box = $('#messages');
    box.innerHTML = '';
    const inner = el('div', 'inner');
    box.appendChild(inner);
    const msgs = await roomMessages(code);
    if (current !== code) return;
    ui.msgs = msgs;
    if (!msgs.length) inner.appendChild(emptyChatEl());
    let prev = null;
    for (const m of msgs) { appendMessage(m, prev, inner); prev = m; }
    // show in-progress receives
    const rc = conns.get(code);
    if (rc) {
      for (const [id, rx] of rc.incoming) if (!rx.skip) onRxProgress(code, id, rx);
      if (rc.sending) onTxProgress(code, rc.sending.id, rc.sending);
    }
    scrollBottom(true);
    if (!isTouch) $('#input').focus();
  }
  function emptyChatEl() {
    const e = el('div', 'empty-chat', 'No messages yet. Say hi! Files, photos and videos are sent in full quality.');
    e.id = 'emptyChat';
    return e;
  }

  function renderChatHeader() {
    const r = rooms.get(current);
    if (!r) return;
    $('#chatName').textContent = r.name;
    const c = conns.get(current);
    const st = c ? c.state : 'connecting';
    $('#chatSub').textContent = fmtCode(r.code) + (r.peerName ? ' · ' + r.peerName : '');
    $('#chatDot').className = 'dot ' + st;
    const bar = $('#statusBar');
    bar.className = 'status-bar ' + st;
    $('#statusText').textContent = st === 'connected'
      ? 'Connected' + (r.peerName ? ' to ' + r.peerName : '')
      : st === 'waiting' ? 'Waiting for other device…' : 'Connecting…';
    let hint = c ? c.hint : '';
    if (!hint && st === 'waiting') hint = 'Open ChatRoom on the other device. Messages are queued and delivered automatically.';
    $('#statusHint').textContent = hint;
  }

  function scrollBottom(force) {
    const box = $('#messages');
    const near = box.scrollHeight - box.scrollTop - box.clientHeight < 160;
    if (force || near) requestAnimationFrame(() => { box.scrollTop = box.scrollHeight; });
  }

  function appendMessage(m, prev, inner) {
    inner = inner || $('#messages .inner');
    if (!inner) return;
    const ec = $('#emptyChat');
    if (ec) ec.remove();
    if (!prev || new Date(prev.ts).toDateString() !== new Date(m.ts).toDateString()) {
      inner.appendChild(el('div', 'day-sep', dayLabel(m.ts)));
    }
    const node = buildMessage(m);
    if (prev && prev.from !== m.from) node.classList.add('gap');
    inner.appendChild(node);
    ui.els.set(m.id, node);
  }

  function buildMessage(m) {
    const r = rooms.get(m.room);
    const mine = m.from === r.slot;
    const row = el('div', 'msg ' + (mine ? 'mine' : 'theirs'));
    row.dataset.id = m.id;
    const b = el('div', 'bubble');
    row.appendChild(b);
    if (m.type === 'text') {
      const t = el('div', 'text');
      t.appendChild(linkify(m.text));
      b.appendChild(t);
    } else {
      buildFileContent(b, m, mine);
    }
    b.appendChild(buildMeta(m, mine));
    attachMsgMenu(row, b, m);
    if (ui.select && ui.select.has(m.id)) row.classList.add('selected');
    return row;
  }

  function buildMeta(m, mine) {
    const meta = el('div', 'meta');
    if (m.type === 'text') {
      const cp = el('button', 'copy', '⧉');
      cp.title = 'Copy text';
      cp.onclick = (e) => { e.stopPropagation(); copyMsg(m.text); };
      meta.appendChild(cp);
    }
    meta.appendChild(el('span', 'time', fmtTime(m.ts)));
    if (mine) {
      const s = m.status || 'pending';
      const tk = el('span', 'tick ' + s, s === 'delivered' ? '✓✓' : s === 'sent' ? '✓' : '◷');
      tk.title = s === 'delivered' ? 'Delivered' : s === 'sent' ? 'Sent' : 'Pending — waiting for the other device';
      if (s === 'pending') tk.style.letterSpacing = '0';
      meta.appendChild(tk);
    }
    return meta;
  }

  // ---------- message menu (right-click / long-press) + multi-select
  let lastTouch = 0;
  let suppressClick = false;
  function msgMenuItems(m, partial) {
    const items = [];
    if (!partial) {
      if (m.type === 'text') items.push({ label: 'Copy text', fn: () => copyMsg(m.text) });
      else items.push({ label: 'Save', fn: () => saveMsg(m) });
      items.push({ label: 'Select', fn: () => enterSelect(m.id) });
    }
    items.push({ label: 'Delete for me', danger: true, fn: () => confirmDelete(m.room, [m.id], false) });
    items.push({ label: 'Delete for everyone', danger: true, fn: () => confirmDelete(m.room, [m.id], true) });
    return items;
  }
  function attachMsgMenu(row, b, m, partial) {
    b.addEventListener('contextmenu', (e) => {
      if (Date.now() - lastTouch < 1500) { e.preventDefault(); return; } // touch long-press handled below
      if (ui.select) { e.preventDefault(); return; }
      if (m.type === 'text' && window.getSelection && String(window.getSelection()).length) return; // native copy of selection
      e.preventDefault();
      openMenuAt(e.clientX, e.clientY, msgMenuItems(m, partial));
    });
    let timer = null, sx = 0, sy = 0;
    const cancel = () => { if (timer) { clearTimeout(timer); timer = null; } };
    b.addEventListener('touchstart', (e) => {
      lastTouch = Date.now();
      const t = e.touches[0];
      sx = t.clientX; sy = t.clientY;
      cancel();
      timer = setTimeout(() => {
        timer = null;
        lastTouch = Date.now();
        suppressClick = true;
        try { if (navigator.vibrate) navigator.vibrate(15); } catch (x) { /* */ }
        if (ui.select) { if (!partial) toggleSelect(m.id); } else openMenuAt(sx, sy, msgMenuItems(m, partial));
      }, 550);
    }, { passive: true });
    b.addEventListener('touchmove', (e) => {
      const t = e.touches[0];
      if (Math.abs(t.clientX - sx) > 10 || Math.abs(t.clientY - sy) > 10) cancel();
    }, { passive: true });
    b.addEventListener('touchend', () => { lastTouch = Date.now(); cancel(); });
    b.addEventListener('touchcancel', cancel);
    if (!partial) {
      // In select mode a tap toggles the message instead of opening images / playing videos.
      row.addEventListener('click', (e) => {
        if (!ui.select) return;
        e.preventDefault(); e.stopPropagation();
        toggleSelect(m.id);
      }, true);
    }
  }

  function enterSelect(firstId) {
    if (!current) return;
    closeMenu();
    ui.select = new Set();
    if (firstId) ui.select.add(firstId);
    $('#chat').classList.add('selecting');
    $('#selectBar').hidden = false;
    renderSelection();
  }
  function exitSelect() {
    if (!ui.select) return;
    ui.select = null;
    $('#chat').classList.remove('selecting');
    $('#selectBar').hidden = true;
    for (const row of ui.els.values()) row.classList.remove('selected');
  }
  function toggleSelect(id) {
    if (!ui.select) return;
    if (ui.select.has(id)) ui.select.delete(id); else ui.select.add(id);
    renderSelection();
  }
  function renderSelection() {
    if (!ui.select) return;
    for (const [id, row] of ui.els) row.classList.toggle('selected', ui.select.has(id));
    const n = ui.select.size;
    $('#selCount').textContent = n + ' selected';
    $('#selDelMe').disabled = $('#selDelAll').disabled = n === 0;
    $('#selAll').textContent = n && n === ui.msgs.length ? 'None' : 'All';
  }
  async function deleteSelected(everyone) {
    if (!ui.select || !ui.select.size || !current) return;
    const ids = [...ui.select];
    if (await confirmDelete(current, ids, everyone)) exitSelect();
  }

  // Remove deleted messages from the UI and free their resources.
  function onMessagesDeleted(code, ids) {
    const set = new Set(ids);
    for (const id of ids) ui.savedPaths.delete(id);
    if (code === current) {
      if (viewerMsg && set.has(viewerMsg.m.id)) closeViewer();
      for (const id of ids) {
        const row = ui.els.get(id);
        if (row) {
          for (const v of row.querySelectorAll('video')) { try { v.pause(); v.removeAttribute('src'); v.load(); } catch (e) { /* */ } }
          for (const img of row.querySelectorAll('img')) img.removeAttribute('src');
          row.remove();
          ui.els.delete(id);
        }
        const u = ui.urls.get(id);
        if (u) { URL.revokeObjectURL(u); ui.urls.delete(id); }
        const rx = $('#messages .msg[data-rx="' + id + '"]');
        if (rx) rx.remove();
        if (ui.select) ui.select.delete(id);
      }
      ui.msgs = ui.msgs.filter((m) => !set.has(m.id));
      tidyChat();
      renderSelection();
    }
    const lm = lastMsg.get(code);
    if (lm && set.has(lm.id)) {
      roomMessages(code).then((msgs) => {
        if (msgs.length) lastMsg.set(code, msgs[msgs.length - 1]); else lastMsg.delete(code);
        if ($('#home').classList.contains('active')) renderHome();
      }).catch(() => {});
    }
  }
  // After removals: drop orphaned day separators, recompute sender-change gaps, show the empty hint.
  function tidyChat() {
    const inner = $('#messages .inner');
    if (!inner) return;
    for (const sep of [...inner.querySelectorAll('.day-sep')]) {
      const n = sep.nextElementSibling;
      if (!n || n.classList.contains('day-sep')) sep.remove();
    }
    let prev = null;
    for (const m of ui.msgs) {
      const row = ui.els.get(m.id);
      if (row) row.classList.toggle('gap', !!prev && prev.from !== m.from);
      prev = m;
    }
    if (!ui.msgs.length && !inner.querySelector('.msg') && !$('#emptyChat')) inner.appendChild(emptyChatEl());
  }
  function openMenuAt(x, y, items) {
    const fake = { getBoundingClientRect: () => ({ left: x, right: x + 170, top: y, bottom: y }) };
    openMenu(fake, items);
  }
  async function copyMsg(text) {
    try { await P.copyText(text); toast('Copied'); } catch (e) { toast('Copy failed'); }
  }

  function objUrl(id, blob) {
    let u = ui.urls.get(id);
    if (!u) { u = URL.createObjectURL(blob); ui.urls.set(id, u); }
    return u;
  }

  function buildFileContent(b, m, mine) {
    const kind = fileKind(m.mime, m.fileName);
    const holder = el('div', 'file-holder');
    b.appendChild(holder);
    if (kind === 'image' || kind === 'video') b.classList.add('media');
    getBlob(m.id).then((blob) => {
      if (!holder.isConnected || ui.els.get(m.id) !== holder.closest('.msg')) return; // deleted / chat closed meanwhile
      if (!blob) { holder.appendChild(el('div', 'file-size', 'File data missing')); return; }
      if (kind === 'image') {
        const img = el('img', 'thumb');
        img.alt = m.fileName;
        img.loading = 'lazy';
        img.src = objUrl(m.id, blob);
        img.onload = () => scrollBottom(false);
        img.onerror = () => { img.replaceWith(fileCard(m, mine, blob)); };
        img.onclick = () => openViewer(m, blob);
        holder.appendChild(img);
        holder.appendChild(mediaCaption(m, blob));
      } else if (kind === 'video') {
        const v = el('video');
        v.controls = true;
        v.preload = 'metadata';
        v.playsInline = true;
        v.src = objUrl(m.id, blob);
        v.onloadedmetadata = () => scrollBottom(false);
        holder.appendChild(v);
        holder.appendChild(mediaCaption(m, blob));
      } else {
        holder.appendChild(fileCard(m, mine, blob));
      }
    });
  }

  function mediaCaption(m, blob) {
    const c = el('div', 'media-caption');
    c.appendChild(el('span', 'name', m.fileName + ' · ' + fmtSize(m.size)));
    const s = el('button', 'link-btn', 'Save');
    s.onclick = (e) => { e.stopPropagation(); saveMsg(m, blob); };
    c.appendChild(s);
    return c;
  }

  function fileCard(m, mine, blob) {
    const card = el('div');
    const top = el('div', 'file-card');
    top.appendChild(el('div', 'file-icon', extOf(m.fileName)));
    const info = el('div', 'file-info');
    info.appendChild(el('div', 'file-name', m.fileName));
    info.appendChild(el('div', 'file-size', fmtSize(m.size)));
    top.appendChild(info);
    card.appendChild(top);
    const acts = el('div', 'file-actions');
    const save = el('button', 'btn small', 'Save');
    save.onclick = () => saveMsg(m, blob, acts);
    acts.appendChild(save);
    if (P.kind === 'browser') {
      const open = el('button', 'btn small', 'Open');
      open.onclick = () => window.open(objUrl(m.id, blob), '_blank');
      acts.appendChild(open);
    }
    card.appendChild(acts);
    const saved = ui.savedPaths.get(m.id);
    if (saved) addElectronButtons(acts, saved);
    return card;
  }

  function addElectronButtons(acts, path) {
    if (!acts || acts.querySelector('.ebtn')) return;
    if (P.canOpenPath) {
      const o = el('button', 'btn small ebtn', 'Open');
      o.onclick = () => P.openPath(path);
      acts.appendChild(o);
    }
    if (P.canShowInFolder) {
      const f = el('button', 'btn small ebtn', 'Show in folder');
      f.onclick = () => P.showInFolder(path);
      acts.appendChild(f);
    }
  }

  async function saveMsg(m, blob, acts) {
    try {
      blob = blob || await getBlob(m.id);
      if (!blob) { toast('File data missing'); return; }
      if (P.kind !== 'browser') toast('Saving ' + m.fileName + '…', null, 60000);
      const res = await P.saveFile(m.fileName, m.mime, blob);
      if (P.kind === 'electron') {
        ui.savedPaths.set(m.id, res);
        addElectronButtons(acts, res);
        toast('Saved to ' + res, P.canShowInFolder ? { label: 'Show in folder', fn: () => P.showInFolder(res) } : null);
      } else if (P.kind === 'android') {
        toast('Saved to ' + res, null, 5000);
      } else {
        toast(res);
      }
    } catch (e) {
      console.error(e);
      toast('Save failed: ' + (e && e.message ? e.message : e), null, 5000);
    }
  }

  // ---------- progress
  function progressEl(row) {
    let p = row.querySelector('.progress-wrap');
    if (!p) {
      p = el('div', 'progress-wrap');
      const bar = el('div', 'progress'); bar.appendChild(el('div'));
      p.append(bar, el('div', 'progress-label'));
      const bubble = row.querySelector('.bubble');
      bubble.insertBefore(p, bubble.querySelector('.meta'));
    }
    return p;
  }
  function setProgress(row, done, total, verb) {
    const p = progressEl(row);
    const pct = total ? Math.floor(done / total * 100) : 100;
    p.querySelector('.progress > div').style.width = pct + '%';
    p.querySelector('.progress-label').textContent = verb + ' ' + pct + '% · ' + fmtSize(done) + ' / ' + fmtSize(total);
  }

  function onTxProgress(code, id, s) {
    if (code !== current) return;
    const row = ui.els.get(id);
    if (!row) return;
    if (!s) { const p = row.querySelector('.progress-wrap'); if (p) p.remove(); return; }
    setProgress(row, s.sent, s.size, 'Sending');
  }

  function onRxProgress(code, id, rx) {
    if (code !== current) return;
    let row = $('#messages .msg[data-rx="' + id + '"]');
    if (!rx) { if (row) row.remove(); return; }
    if (!row) {
      const inner = $('#messages .inner');
      if (!inner) return;
      const ec = $('#emptyChat'); if (ec) ec.remove();
      row = el('div', 'msg theirs');
      row.dataset.rx = id;
      const b = el('div', 'bubble');
      const card = el('div', 'file-card');
      card.appendChild(el('div', 'file-icon', extOf(rx.meta.fileName)));
      const info = el('div', 'file-info');
      info.appendChild(el('div', 'file-name', rx.meta.fileName));
      info.appendChild(el('div', 'file-size', fmtSize(rx.meta.size)));
      card.appendChild(info);
      b.appendChild(card);
      b.appendChild(el('div', 'meta', fmtTime(rx.meta.ts)));
      attachMsgMenu(row, b, rx.meta, true); // delete while receiving -> aborts the transfer
      row.appendChild(b);
      inner.appendChild(row);
      scrollBottom(false);
    }
    setProgress(row, rx.received, rx.meta.size, 'Receiving');
  }

  // ---------- events from RoomConn / storage
  function onNewMessage(m) {
    lastMsg.set(m.room, m);
    if (m.room !== current) { if ($('#home').classList.contains('active')) renderHome(); return; }
    if (ui.els.has(m.id)) return;
    const rxRow = $('#messages .msg[data-rx="' + m.id + '"]');
    if (rxRow) rxRow.remove();
    const prev = ui.msgs.length ? ui.msgs[ui.msgs.length - 1] : null;
    ui.msgs.push(m);
    appendMessage(m, prev);
    scrollBottom(m.from === rooms.get(m.room).slot);
  }

  function onMessageUpdated(m) {
    if (m.room !== current) return;
    const i = ui.msgs.findIndex((x) => x.id === m.id);
    if (i >= 0) ui.msgs[i] = m;
    const row = ui.els.get(m.id);
    if (!row) return;
    const r = rooms.get(m.room);
    const old = row.querySelector('.bubble > .meta');
    if (old) old.replaceWith(buildMeta(m, m.from === r.slot));
  }

  function onConnState(c) {
    if (c.code === current) renderChatHeader();
    if ($('#home').classList.contains('active')) {
      const li = $('#roomList .room[data-code="' + c.code + '"]');
      if (li) {
        li.querySelector('.dot').className = 'dot ' + c.state;
        if (!lastMsg.get(c.code)) li.querySelector('.room-last').textContent = stateLabel(c.code);
      }
    }
  }

  // ---------- viewer
  let viewerMsg = null;
  function openViewer(m, blob) {
    viewerMsg = { m, blob };
    $('#viewerImg').src = objUrl(m.id, blob);
    $('#viewerName').textContent = m.fileName + ' · ' + fmtSize(m.size);
    $('#viewer').hidden = false;
  }
  function closeViewer() { $('#viewer').hidden = true; $('#viewerImg').removeAttribute('src'); viewerMsg = null; }

  // ---------- composer
  function autosize() {
    const t = $('#input');
    t.style.height = 'auto';
    t.style.height = Math.min(160, t.scrollHeight + 2) + 'px';
  }
  function doSend() {
    const t = $('#input');
    const v = t.value;
    if (!v.trim()) return;
    t.value = '';
    autosize();
    sendText(v);
    if (!isTouch) t.focus();
  }

  function bindUI() {
    $('#btnCreate').onclick = createRoom;
    $('#btnJoin').onclick = joinRoom;
    $('#btnSettings').onclick = openSettings;
    $('#btnBack').onclick = goHome;
    $('#chatTitle').onclick = () => { const r = rooms.get(current); if (r) showCode(r, false); };
    $('#btnSettings2').onclick = (e) => {
      e.stopPropagation();
      const r = rooms.get(current);
      if (!r) return;
      openMenu($('#btnSettings2'), [
        { label: 'Show code', fn: () => showCode(r, false) },
        { label: 'Rename room', fn: () => renameRoom(r) },
        { label: 'Select messages', fn: () => enterSelect() },
        { label: 'Settings', fn: openSettings },
        { label: 'Clear chat for everyone', danger: true, fn: () => clearChat(r) },
        { label: 'Delete room', danger: true, fn: () => deleteRoom(r) }
      ]);
    };
    const input = $('#input');
    input.addEventListener('input', autosize);
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && !e.shiftKey && !isTouch && !e.isComposing) { e.preventDefault(); doSend(); }
    });
    $('#btnSend').addEventListener('mousedown', (e) => e.preventDefault()); // keep keyboard open on mobile
    $('#btnSend').onclick = doSend;
    $('#btnAttach').onclick = () => $('#fileInput').click();
    $('#fileInput').onchange = (e) => { const f = [...e.target.files]; e.target.value = ''; sendFiles(f); };

    document.addEventListener('paste', (e) => {
      if (!current || !$('#modal').hidden) return;
      const files = e.clipboardData && e.clipboardData.files;
      if (files && files.length) { e.preventDefault(); sendFiles([...files]); }
    });

    let dragDepth = 0;
    const ov = $('#dropOverlay');
    const hasFiles = (e) => e.dataTransfer && [...(e.dataTransfer.types || [])].includes('Files');
    window.addEventListener('dragenter', (e) => { if (!current || !hasFiles(e)) return; e.preventDefault(); dragDepth++; ov.classList.add('show'); });
    window.addEventListener('dragover', (e) => { if (!hasFiles(e)) return; e.preventDefault(); e.dataTransfer.dropEffect = current ? 'copy' : 'none'; });
    window.addEventListener('dragleave', () => { if (--dragDepth <= 0) { dragDepth = 0; ov.classList.remove('show'); } });
    window.addEventListener('drop', (e) => {
      if (!hasFiles(e)) return;
      e.preventDefault();
      dragDepth = 0; ov.classList.remove('show');
      if (current && e.dataTransfer.files.length) sendFiles([...e.dataTransfer.files]);
    });

    $('#modal').addEventListener('mousedown', (e) => { if (e.target.id === 'modal') closeModal(); });
    $('#viewerClose').onclick = closeViewer;
    $('#viewer').addEventListener('click', (e) => { if (e.target.id === 'viewer' || e.target.id === 'viewerImg') closeViewer(); });
    $('#viewerSave').onclick = (e) => { e.stopPropagation(); if (viewerMsg) saveMsg(viewerMsg.m, viewerMsg.blob); };
    document.addEventListener('keydown', (e) => {
      if (e.key !== 'Escape') return;
      if (!$('#viewer').hidden) closeViewer();
      else if (!$('#modal').hidden) closeModal();
      else if ($('#menu')) closeMenu();
      else exitSelect();
    });

    // select bar
    $('#selCancel').onclick = exitSelect;
    $('#selAll').onclick = () => {
      if (!ui.select) return;
      if (ui.select.size && ui.select.size === ui.msgs.length) ui.select.clear();
      else for (const m of ui.msgs) ui.select.add(m.id);
      renderSelection();
    };
    $('#selDelMe').onclick = () => deleteSelected(false);
    $('#selDelAll').onclick = () => deleteSelected(true);
    // swallow the click that some browsers emit at the end of a long-press
    window.addEventListener('touchstart', () => { suppressClick = false; }, { capture: true, passive: true });
    window.addEventListener('click', (e) => {
      if (suppressClick) { suppressClick = false; e.preventDefault(); e.stopPropagation(); }
    }, true);

    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible') for (const c of conns.values()) c.tick();
    });
    window.addEventListener('online', () => { for (const c of conns.values()) c.tick(); });

    // Android hardware back button
    const App = window.Capacitor && window.Capacitor.Plugins && window.Capacitor.Plugins.App;
    if (App && App.addListener) {
      App.addListener('backButton', () => {
        if ($('#menu')) closeMenu();
        else if (!$('#viewer').hidden) closeViewer();
        else if (!$('#modal').hidden) closeModal();
        else if (ui.select) exitSelect();
        else if (current) goHome();
        else if (App.exitApp) App.exitApp();
      });
    }
  }

  // ================================================================ init
  async function init() {
    bindUI();
    if (typeof Peer === 'undefined') {
      toast('PeerJS failed to load', null, 10000);
      return;
    }
    const list = await dbAll('rooms');
    for (const r of list) {
      rooms.set(r.code, r);
      const msgs = await roomMessages(r.code);
      if (msgs.length) lastMsg.set(r.code, msgs[msgs.length - 1]);
    }
    for (const r of rooms.values()) startRoom(r);
    if (rooms.size === 1) await openChat([...rooms.keys()][0]);
    else await renderHome();
  }

  // debug/testing hook
  window.ChatRoomDebug = { rooms, conns, get current() { return current; }, sendFiles, sendText, getBlob, roomMessages, roomTombs, deleteMessages, PROFILE };

  init().catch((e) => { console.error('init failed', e); toast('Startup failed: ' + e.message, null, 10000); });
})();
