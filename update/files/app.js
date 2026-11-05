/* Linoa web app — shared by Electron (desktop) and Capacitor (Android).
 *
 * Transports: a direct PeerJS DataConnection (serialization 'raw', reliable/ordered) or, when direct P2P
 * is impossible (CGNAT etc.), an end-to-end encrypted relay conn (relay.js, LinoaRelay over public MQTT)
 * with the same send/data semantics. Both run side by side; direct is preferred. Exactly one conn is
 * active; a `hello` arriving on another open conn means the peer switched to it, so we follow.
 *
 * Wire protocol (same on either transport):
 *   Control messages are JSON strings: {t: ...}
 *     hello      {t,v,name,sync,caps}       start of sync, followed by. caps (v3+): optional features the
 *                                           sender understands, e.g. ['typing','upload']. Optional messages
 *                                           below are only sent to a peer whose hello advertised them, so
 *                                           older clients (no caps) never see them.
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
 *   Optional (cap-gated):
 *     typing     {t,on:bool}                cap 'typing': on=true refreshed every <=3 s while typing; receiver
 *                                           hides the indicator on on=false, on a new message, or after 6 s
 *     upload     {t,items:[{id,ts,fileName,mime,size}]}
 *                                           cap 'upload': files that are about to be sent (picked / queued);
 *                                           receiver shows placeholders until file-start/-end or file-abort
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
  const CAPS = ['typing', 'upload'];
  const RETRY_MS = 5000;
  const CONNECT_TIMEOUT = 15000;
  const PING_MS = 5000;
  const DEAD_MS = 45000;
  const DEFAULT_ICE = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
    // (PeerJS's free TURN is dead; the relay transport is the fallback now, user TURN still optional)
  ];
  const RELAY_HIGH = 2 * 1024 * 1024;       // relay conn backpressure (bytes not yet acked by peer)
  const RELAY_LOW = 1024 * 1024;

  // ---------------------------------------------------------------- profile namespacing
  const params = new URLSearchParams(location.search);
  const PROFILE = (params.get('profile') || '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 32);
  const NS = 'chatroom' + (PROFILE ? '.' + PROFILE : '');
  const lsKey = (k) => NS + '.' + k;
  function lsGet(k, d) { try { const v = localStorage.getItem(lsKey(k)); return v == null ? d : v; } catch (e) { return d; } }
  function lsSet(k, v) { try { localStorage.setItem(lsKey(k), v); } catch (e) { /* ignore */ } }
  // Debug: ?relayonly=1 (or ls 'relayOnly'='1') skips PeerJS entirely; ?norelay=1 (or 'noRelay') disables the relay.
  const RELAY_ONLY = params.get('relayonly') === '1' || lsGet('relayOnly', '') === '1';
  const HAS_RELAY = !!(window.LinoaRelay && window.LinoaRelay.Link) &&
    !(params.get('norelay') === '1' || lsGet('noRelay', '') === '1');

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
      this.peerCaps = new Set(); // optional features the peer advertised in its hello
      this.announced = new Map(); // id -> meta: files the peer announced but has not started sending yet
      this.annOut = new Set();   // ids we announced to the peer on the current connection
      this.peerTyping = false;
      this.typingTimer = null;
      this.stopped = false;
      this.retryTimer = null;
      this.relayConn = null;     // open relay conn (active, or standby while direct is active)
      this.directWait = null;    // open direct conn waiting for transfers on the relay to finish
      this.link = null;
      this.tickTimer = setInterval(() => this.tick(), RETRY_MS);
      this.pingTimer = setInterval(() => this.ping(), PING_MS);
      if (HAS_RELAY) {
        try {
          this.link = new LinoaRelay.Link({ code: this.code, slot: this.slot, log: (...a) => this.log('relay:', ...a) });
          this.link.on('conn', (c) => this.onRelayConn(c));
          this.link.on('state', () => { if (!this.conn) this.setState(this.idleState()); });
        } catch (e) { console.error('relay init failed', e); this.link = null; }
      }
      if (RELAY_ONLY) this.setState(this.idleState(), '');
      else this.createPeer();
    }

    get viaRelay() { return !!(this.conn && this.conn.isRelay); }
    // State while no conn is active: 'waiting' once some transport could reach the peer.
    idleState() {
      return (this.peer && this.peer.open) || (this.link && this.link.state === 'ready') ? 'waiting' : 'connecting';
    }
    // Transfer in progress in either direction (direct takes over from the relay only when idle).
    busy() { return !!this.sending || this.queue.length > 0 || this.incoming.size > 0 || this.announced.size > 0; }

    onRelayConn(c) {
      if (this.stopped) { try { c.close(); } catch (e) { /* */ } return; }
      const old = this.relayConn;
      this.relayConn = c;
      this.setupConn(c, false);
      if (old && old !== c && old !== this.conn) { try { old.close(); } catch (e) { /* */ } }
      if (!this.conn || this.conn.isRelay) this.onOpen(c);
      else this.log('relay ready (standby)');
    }

    // Peer sent `hello` on conn c that is not our active one: it switched transport, follow it.
    shouldFollow(c) {
      if (!c.open || this.stopped) return false;
      const a = this.conn;
      if (a && !!a.isRelay === !!c.isRelay) return false; // direct vs direct: onOpen's race logic decides
      return c.isRelay ? c === this.relayConn : true;
    }

    upgradeSoon() { if (this.directWait) setTimeout(() => this.maybeUpgrade(), 300); } // let acks flush first

    // Deferred direct takeover from the relay, once no transfer is running.
    maybeUpgrade() {
      const d = this.directWait;
      if (!d) return;
      if (this.stopped || !d.open || (this.conn && !this.conn.isRelay)) {
        this.directWait = null;
        if (d !== this.conn) setTimeout(() => { try { d.close(); } catch (e) { /* */ } }, 500);
        return;
      }
      if (this.conn && this.busy()) return;
      this.directWait = null;
      this.log('relay idle, switching to direct');
      this.onOpen(d, true);
    }

    log(...a) { console.log('[' + this.code + '/' + this.slot + ']', ...a); }

    setState(s, hint) {
      this.state = s;
      if (hint !== undefined) this.hint = hint;
      onConnState(this);
    }

    createPeer() {
      if (this.stopped || RELAY_ONLY) return;
      if (this.peer && !this.peer.destroyed) { try { this.peer.destroy(); } catch (e) { /* */ } }
      if (!this.viaRelay) this.setState('connecting', 'Contacting signalling server…');
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
          if (!this.conn) this.setState('connecting', 'Previous session still registered, retrying…');
          this.scheduleRecreate(4000);
        } else if (type === 'network' || type === 'server-error' || type === 'socket-error' || type === 'socket-closed') {
          this.lastFail = 'server';
          this.failCount++;
          if (!this.conn) this.setState('connecting', 'Can’t reach signalling server — check internet connection.');
          if (peer.destroyed) this.scheduleRecreate(RETRY_MS);
        } else if (type === 'browser-incompatible') {
          if (!this.conn) this.setState('connecting', 'WebRTC is not supported here.');
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
      this.maybeUpgrade();
      if (RELAY_ONLY) return;
      const p = this.peer;
      if (!p || p.destroyed) { if (!this.retryTimer) this.scheduleRecreate(100); return; }
      if (p.disconnected) { try { p.reconnect(); } catch (e) { /* */ } return; }
      if (!this.conn || this.conn.isRelay) this.tryConnect();
    }

    tryConnect() {
      const p = this.peer;
      // keep dialling direct while on the relay (direct is preferred)
      if (this.stopped || (this.conn && !this.conn.isRelay) || this.directWait || this.pendingOut || !p || !p.open) return;
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
        this.setState('waiting', this.link
          ? 'Other device seems online but a direct connection failed — connecting through the encrypted relay…'
          : 'Other device seems online but a direct connection failed. A TURN server in Settings may help.');
      }
    }

    setupConn(c, outgoing) {
      c._outgoing = outgoing;
      if (!c.isRelay) c.on('open', () => this.onOpen(c)); // relay conns arrive already open
      c.on('data', (d) => {
        if (c === this.conn) { this.onData(d); return; }
        // other open conn of ours: only a hello matters (peer switched transport), the rest is ignored
        if (typeof d === 'string' && d.startsWith('{"t":"hello"') && this.shouldFollow(c)) {
          this.log('peer switched to ' + (c.isRelay ? 'relay' : 'direct') + ', following');
          this.onOpen(c, true);
          if (c === this.conn) this.onData(d);
        }
      });
      c.on('close', () => {
        if (c === this.pendingOut) { clearTimeout(c._timeout); this.pendingOut = null; }
        if (c === this.relayConn) this.relayConn = null;
        if (c === this.directWait) this.directWait = null;
        if (c === this.conn) this.onClose('closed');
      });
      c.on('error', (e) => {
        this.log('conn error', e && e.type, e && e.message);
        if (c === this.conn) this.dropConn(c, 'error');
      });
    }

    // Close c; if it was the active conn, run onClose once (close may already have fired synchronously).
    dropConn(c, why) {
      try { c.close(); } catch (e) { /* */ }
      if (c === this.conn) this.onClose(why);
    }

    initiatorSlot(c) { return c._outgoing ? this.slot : this.otherSlot; }

    // Two direct conns: keep the existing one only if both sides dialled at the same time (existing is
    // very fresh) and it was initiated by slot 1, same on both ends. Otherwise the newer one wins,
    // because the other side evidently considers the old one dead.
    keepOld(old, c) {
      const race = Date.now() - (old._openedAt || 0) < 4000;
      return race && this.initiatorSlot(old) === 1 && this.initiatorSlot(c) !== 1;
    }

    // c is open (direct 'open' event, relay conn, or peer switched to it). force: peer already uses c.
    onOpen(c, force) {
      if (c === this.pendingOut) { clearTimeout(c._timeout); this.pendingOut = null; }
      if (this.stopped) { c.close(); return; }
      if (c === this.conn) return;
      const cur = this.conn && this.conn.open ? this.conn : null;
      if (cur && !cur.isRelay && !c.isRelay) {
        if (this.keepOld(cur, c)) {
          this.log('duplicate connection, keeping existing');
          setTimeout(() => c.close(), 500);
          return;
        }
        this.log('duplicate connection, switching');
      } else if (cur && !cur.isRelay && c.isRelay && !force) {
        this.log('relay ready (standby)');
        return;
      } else if (cur && cur.isRelay && !c.isRelay && !force) {
        // direct came up while on the relay: take over only when no transfer is running
        const w = this.directWait;
        if (w && w !== c && w.open) {
          if (this.keepOld(w, c)) { setTimeout(() => c.close(), 500); return; }
          setTimeout(() => { try { w.close(); } catch (e) { /* */ } }, 500);
        }
        this.directWait = c;
        c._openedAt = Date.now();
        if (this.busy()) { this.log('direct ready, switching when transfers finish'); return; }
        this.directWait = null;
      }
      if (cur) {
        this.log('switching ' + (cur.isRelay ? 'relay' : 'direct') + ' -> ' + (c.isRelay ? 'relay' : 'direct'));
        this.conn = null;
        this.abortTransfers();
        // a relay conn being left stays as standby; a left direct conn is dead to the peer -> close it
        if (!cur.isRelay || (c.isRelay && cur !== this.relayConn)) {
          setTimeout(() => { try { cur.close(); } catch (e) { /* */ } }, 500);
        }
      }
      if (!c.isRelay && this.directWait && this.directWait !== c) {
        const w = this.directWait;
        setTimeout(() => { try { w.close(); } catch (e) { /* */ } }, 500);
      }
      if (!c.isRelay || this.directWait === c) this.directWait = null;
      this.conn = c;
      c._openedAt = Date.now();
      this.gen++;
      this.synced = false;
      this.theirIds = null;
      this.peerCaps = new Set(); // until this connection's hello arrives
      this.lastRx = Date.now();
      this.lastFail = '';
      const pc = c.peerConnection;
      if (pc && !c._iceWatch) {
        c._iceWatch = true;
        pc.addEventListener('iceconnectionstatechange', () => {
          if (c === this.conn && (pc.iceConnectionState === 'failed' || pc.iceConnectionState === 'closed')) {
            this.dropConn(c, 'ice ' + pc.iceConnectionState);
          }
        });
      }
      this.log('connected ' + (c.isRelay ? 'via relay' : '(initiator slot ' + this.initiatorSlot(c) + ')'));
      this.setState('connected', '');
      this.sendHello();
    }

    onClose(why) {
      this.log('connection closed:', why);
      this.conn = null;
      this.abortTransfers();
      if (this.stopped) return;
      // fall back to another open conn right away (direct waiting to take over, else relay standby)
      const next = [this.directWait, this.relayConn].find((x) => x && x.open);
      this.directWait = null;
      if (next) { this.onOpen(next, true); return; }
      this.setState(this.idleState(), '');
      setTimeout(() => this.tick(), 1000);
    }

    abortTransfers() {
      // Outgoing: everything not yet acked goes back to pending and is resent from scratch after the next hello.
      this.gen++;
      this.synced = false;
      this.theirIds = null;
      for (const m of this.queue) onTxProgress(this.code, m.id, null);
      this.queue = [];
      this.queued.clear();
      this.annOut.clear();
      if (this.sending) onTxProgress(this.code, this.sending.id, null);
      this.sending = null;
      // Incoming: drop partial data and announced placeholders.
      const rxIds = [...this.incoming.keys(), ...this.announced.keys()];
      this.incoming.clear();
      this.announced.clear();
      for (const id of rxIds) onRxProgress(this.code, id, null);
      this.setPeerTyping(false);
    }

    can(cap) { return !!this.conn && this.peerCaps.has(cap); }

    // Files the peer is sending / about to send (announced + in progress), in arrival order.
    incomingList() {
      const out = [...this.announced.values()];
      for (const rx of this.incoming.values()) if (!rx.skip) out.push(rx.meta);
      return out;
    }

    setPeerTyping(on) {
      clearTimeout(this.typingTimer);
      this.typingTimer = null;
      if (on) this.typingTimer = setTimeout(() => this.setPeerTyping(false), 6000);
      if (this.peerTyping === !!on) return;
      this.peerTyping = !!on;
      onTyping(this);
    }

    // Tell the peer which files are coming (cap 'upload') so it can show placeholders right away.
    announce(ms) {
      if (!this.conn || !this.synced || !this.peerCaps.has('upload')) return;
      const items = ms.filter((m) => m.type === 'file' && !this.annOut.has(m.id))
        .map((m) => ({ id: m.id, ts: m.ts, fileName: m.fileName, mime: m.mime, size: m.size }));
      for (let i = 0; i < items.length; i += 200) {
        const part = items.slice(i, i + 200);
        if (this.sendJSON({ t: 'upload', items: part })) for (const it of part) this.annOut.add(it.id);
      }
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
      if (!this.sendJSON({ t: 'hello', v: 1, name: settings.deviceName, sync: !nameOnly, caps: CAPS })) return;
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
        if (this.announced.has(id)) {
          // announced but not started: the tombstone makes us refuse it when file-start arrives
          this.announced.delete(id);
          onRxProgress(this.code, id, null);
        }
      }
      if (this.queue.some((m) => set.has(m.id))) {
        this.queue = this.queue.filter((m) => !set.has(m.id));
        for (const id of set) {
          this.queued.delete(id);
          // announced queued file withdrawn -> remove the receiver's placeholder
          if (this.annOut.delete(id)) this.sendJSON({ t: 'file-abort', id });
        }
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
          this.setPeerTyping(false);
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
          const wasAnnounced = this.announced.delete(m.id);
          if (await getTomb(this.code, m.id)) {
            // deleted here: refuse the transfer, but ack so the sender considers it handled
            this.sendJSON({ t: 'file-abort', id: m.id });
            this.sendJSON({ t: 'ack', id: m.id });
            this.incoming.set(m.id, { meta: m, skip: true });
            if (wasAnnounced) onRxProgress(this.code, m.id, null);
            return;
          }
          const exists = await dbGet('messages', m.id);
          const rx = { meta: m, parts: [], cur: [], curSize: 0, received: 0, next: 0, skip: !!exists, lastUi: 0 };
          this.incoming.set(m.id, rx);
          if (!exists) onRxProgress(this.code, m.id, rx);
          else if (wasAnnounced) onRxProgress(this.code, m.id, null);
          break;
        }
        case 'upload': {
          // cap 'upload': placeholders for files the peer is about to send
          if (!Array.isArray(o.items)) return;
          const added = [];
          for (const it of o.items.slice(0, 1000)) {
            const m = sanitizeMeta(Object.assign({}, it, { type: 'file' }), this.code, this.otherSlot);
            if (!m || this.incoming.has(m.id) || this.announced.has(m.id)) continue;
            if (await getTomb(this.code, m.id) || await dbGet('messages', m.id)) continue;
            this.announced.set(m.id, m);
            added.push(m.id);
          }
          if (added.length) onAnnounce(this.code, added);
          break;
        }
        case 'typing': this.setPeerTyping(!!o.on); break;
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
          if (stored) onNewMessage(m); // replaces the placeholder in place
          onRxProgress(this.code, m.id, null);
          this.sendJSON({ t: 'ack', id: m.id });
          this.upgradeSoon();
          break;
        }
        case 'file-abort': {
          if (this.incoming.has(o.id)) { this.incoming.delete(o.id); onRxProgress(this.code, o.id, null); }
          if (this.announced.delete(o.id)) onRxProgress(this.code, o.id, null);
          // receiver refused / dropped our transfer -> stop sending chunks
          if (this.sending && this.sending.id === o.id) this.cancelled.add(o.id);
          if (this.queued.has(o.id)) { this.queue = this.queue.filter((m) => m.id !== o.id); this.queued.delete(o.id); }
          this.upgradeSoon();
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
      this.peerCaps = new Set(Array.isArray(o.caps) ? o.caps.filter((x) => typeof x === 'string').slice(0, 32) : []);
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
      this.announce([m]);
      onTxProgress(this.code, m.id, { sent: 0, size: m.size, queued: true });
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
              this.annOut.delete(m.id);
              if (this.sending && this.sending.id === m.id) this.sending = null;
              onTxProgress(this.code, m.id, null);
            }
          }
        }
      } finally {
        this.pumping = false;
        this.upgradeSoon();
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
      if (conn.isRelay) {
        if (!(conn.bufferedAmount > RELAY_HIGH)) return Promise.resolve();
        return new Promise((resolve) => {
          let timer = null;
          const done = () => { clearInterval(timer); resolve(); };
          timer = setInterval(() => { if (conn !== this.conn || !conn.open) done(); }, 250);
          conn.waitDrain(RELAY_LOW).then(done, done);
        });
      }
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
      clearTimeout(this.typingTimer);
      clearInterval(this.tickTimer);
      clearInterval(this.pingTimer);
      clearTimeout(this.retryTimer);
      this.dropPendingOut();
      for (const c of new Set([this.conn, this.relayConn, this.directWait])) if (c) { try { c.close(); } catch (e) { /* */ } }
      this.conn = null;
      this.relayConn = null;
      this.directWait = null;
      if (this.link) { try { this.link.stop(); } catch (e) { /* */ } }
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
    const now = Date.now();
    const items = Array.from(files).map((f, i) => ({
      f,
      partial: {
        id: uuid(), ts: now + i, rts: now + i, type: 'file', mime: f.type || '', size: f.size,
        fileName: f.name || ('pasted-' + new Date().toISOString().replace(/[:.]/g, '-') + extFromMime(f.type))
      }
    }));
    if (!items.length) return;
    // Announce everything first, so the other side sees placeholders while blobs are still being stored.
    const rc = conns.get(code);
    if (rc) rc.announce(items.map((x) => x.partial));
    for (const { f, partial } of items) {
      try {
        await createMessage(code, partial, f);
      } catch (e) {
        console.error('could not store file', e);
        toast('Could not send ' + partial.fileName + ': ' + (e && e.message ? e.message : e), null, 5000, 'alert');
        if (rc && rc.annOut.delete(partial.id)) rc.sendJSON({ t: 'file-abort', id: partial.id });
      }
    }
  }
  function extFromMime(m) {
    const map = { 'image/png': '.png', 'image/jpeg': '.jpg', 'image/gif': '.gif', 'image/webp': '.webp', 'text/plain': '.txt' };
    return map[m] || '';
  }

  // ================================================================ UI: icons (inline SVG, Lucide-style)
  const ICONS = {
    'settings': '<path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"/><circle cx="12" cy="12" r="3"/>',
    'chevron-left': '<path d="m15 18-6-6 6-6"/>',
    'chevron-down': '<path d="m6 9 6 6 6-6"/>',
    'more-vertical': '<circle cx="12" cy="12" r="1"/><circle cx="12" cy="5" r="1"/><circle cx="12" cy="19" r="1"/>',
    'more': '<circle cx="12" cy="12" r="1"/><circle cx="19" cy="12" r="1"/><circle cx="5" cy="12" r="1"/>',
    'x': '<path d="M18 6 6 18"/><path d="m6 6 12 12"/>',
    'paperclip': '<path d="m21.44 11.05-9.19 9.19a6 6 0 0 1-8.49-8.49l8.57-8.57A4 4 0 1 1 18 8.84l-8.59 8.57a2 2 0 0 1-2.83-2.83l8.49-8.48"/>',
    'send': '<path d="M14.54 21.69a.5.5 0 0 0 .94-.03l6.5-19a.5.5 0 0 0-.64-.64l-19 6.5a.5.5 0 0 0-.03.94l7.93 3.18a2 2 0 0 1 1.11 1.11z"/><path d="m21.85 2.15-10.94 10.94"/>',
    'download': '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><path d="m7 10 5 5 5-5"/><path d="M12 15V3"/>',
    'upload': '<path d="M4 14.9A7 7 0 1 1 15.7 8h1.8a4.5 4.5 0 0 1 2.5 8.24"/><path d="M12 12v9"/><path d="m16 16-4-4-4 4"/>',
    'copy': '<rect width="14" height="14" x="8" y="8" rx="2" ry="2"/><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/>',
    'trash': '<path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/><path d="M10 11v6"/><path d="M14 11v6"/>',
    'user': '<path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/>',
    'users': '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>',
    'folder': '<path d="m6 14 1.5-2.9A2 2 0 0 1 9.24 10H20a2 2 0 0 1 1.94 2.5l-1.54 6a2 2 0 0 1-1.95 1.5H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h3.9a2 2 0 0 1 1.69.9l.81 1.2a2 2 0 0 0 1.67.9H18a2 2 0 0 1 2 2v2"/>',
    'external': '<path d="M15 3h6v6"/><path d="M10 14 21 3"/><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>',
    'check': '<path d="M20 6 9 17l-5-5"/>',
    'check-check': '<path d="M18 6 7 17l-5-5"/><path d="m22 10-7.5 7.5L13 16"/>',
    'clock': '<circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/>',
    'image': '<rect width="18" height="18" x="3" y="3" rx="2" ry="2"/><circle cx="9" cy="9" r="2"/><path d="m21 15-3.09-3.09a2 2 0 0 0-2.82 0L6 21"/>',
    'video': '<path d="m16 13 5.22 3.48a.5.5 0 0 0 .78-.42V7.94a.5.5 0 0 0-.76-.43L16 10.5"/><rect x="2" y="6" width="14" height="12" rx="2"/>',
    'file': '<path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z"/><path d="M14 2v4a2 2 0 0 0 2 2h4"/>',
    'plus': '<path d="M5 12h14"/><path d="M12 5v14"/>',
    'log-in': '<path d="M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4"/><path d="m10 17 5-5-5-5"/><path d="M15 12H3"/>',
    'pencil': '<path d="M21.17 6.81a1 1 0 0 0-3.99-3.99L3.84 16.17a2 2 0 0 0-.5.83l-1.32 4.35a.5.5 0 0 0 .62.62l4.35-1.32a2 2 0 0 0 .83-.5z"/><path d="m15 5 4 4"/>',
    'hash': '<path d="M4 9h16"/><path d="M4 15h16"/><path d="M10 3 8 21"/><path d="M16 3l-2 18"/>',
    'circle-check': '<circle cx="12" cy="12" r="10"/><path d="m9 12 2 2 4-4"/>',
    'sun': '<circle cx="12" cy="12" r="4"/><path d="M12 2v2"/><path d="M12 20v2"/><path d="m4.93 4.93 1.41 1.41"/><path d="m17.66 17.66 1.41 1.41"/><path d="M2 12h2"/><path d="M20 12h2"/><path d="m6.34 17.66-1.41 1.41"/><path d="m19.07 4.93-1.41 1.41"/>',
    'moon': '<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/>',
    'monitor': '<rect width="20" height="14" x="2" y="3" rx="2"/><path d="M8 21h8"/><path d="M12 17v4"/>',
    'play': '<path d="M7 4.5v15a1 1 0 0 0 1.52.85l12-7.5a1 1 0 0 0 0-1.7l-12-7.5A1 1 0 0 0 7 4.5Z" fill="currentColor"/>',
    'info': '<circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/>',
    'alert': '<circle cx="12" cy="12" r="10"/><path d="M12 8v4"/><path d="M12 16h.01"/>',
    'monitor-off': '<path d="M12 20h.01"/><path d="M8.5 16.43a5 5 0 0 1 7 0"/><path d="M5 12.86a10 10 0 0 1 5.17-2.69"/><path d="M19 12.86a10 10 0 0 0-2-1.52"/><path d="M2 8.82a15 15 0 0 1 4.18-2.64"/><path d="M22 8.82a15 15 0 0 0-11.29-3.76"/><path d="m2 2 20 20"/>'
  };
  function svg(name, size, cls) {
    const s = size || 20;
    return '<svg class="ic' + (cls ? ' ' + cls : '') + '" viewBox="0 0 24 24" width="' + s + '" height="' + s +
      '" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
      (ICONS[name] || '') + '</svg>';
  }
  function iconBtn(name, title, cls, size) {
    const b = el('button', 'icon-btn' + (cls ? ' ' + cls : ''));
    b.type = 'button';
    b.title = title;
    b.setAttribute('aria-label', title);
    b.innerHTML = svg(name, size || 18);
    return b;
  }
  function hydrateIcons(root) {
    for (const e of (root || document).querySelectorAll('[data-icon]')) {
      if (e.querySelector(':scope > svg.ic')) continue;
      e.insertAdjacentHTML('afterbegin', svg(e.dataset.icon, Number(e.dataset.size) || 20));
    }
  }

  // ================================================================ UI: theme + layout
  function themePref() { const t = lsGet('theme', 'system'); return t === 'dark' || t === 'light' ? t : 'system'; }
  function applyTheme() {
    const t = themePref();
    if (t === 'system') document.documentElement.removeAttribute('data-theme');
    else document.documentElement.setAttribute('data-theme', t);
    const light = t === 'light' || (t === 'system' && window.matchMedia && matchMedia('(prefers-color-scheme: light)').matches);
    const meta = document.querySelector('meta[name="theme-color"]');
    if (meta) meta.content = light ? '#ffffff' : '#0f1218';
  }
  applyTheme();
  if (window.matchMedia) {
    const mq = matchMedia('(prefers-color-scheme: light)');
    if (mq.addEventListener) mq.addEventListener('change', applyTheme);
  }
  const wideMq = window.matchMedia ? matchMedia('(min-width: 900px)') : { matches: false };
  const isWide = () => wideMq.matches;
  const homeVisible = () => isWide() || $('#home').classList.contains('active');
  const reduceMotion = () => !!(window.matchMedia && matchMedia('(prefers-reduced-motion: reduce)').matches);

  // ================================================================ UI: screens
  function show(id) {
    for (const s of document.querySelectorAll('.screen')) s.classList.toggle('active', s.id === id);
    document.body.classList.toggle('in-chat', id === 'chat');
  }

  async function goHome() {
    exitSelect();
    stopTyping();
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

  // ---------- avatars / small formatters
  function hashStr(s) { let h = 0; for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0; return Math.abs(h); }
  function initials(r) {
    const n = String(r.name || '').trim();
    if (!n || /^Room [A-Z0-9]{4}-[A-Z0-9]{4}$/.test(n)) return r.code.slice(0, 2);
    const w = n.split(/\s+/).filter(Boolean);
    const a = [...(w[0] || '')][0] || '#';
    const b = w.length > 1 ? ([...w[1]][0] || '') : '';
    return (a + b).toUpperCase();
  }
  function fillAvatar(av, r, state) {
    const h = hashStr(r.code) % 360;
    av.style.setProperty('--h1', h);
    av.style.setProperty('--h2', (h + 40) % 360);
    av.textContent = initials(r);
    const d = el('span', 'presence ' + (state || 'connecting'));
    av.appendChild(d);
  }
  function fmtShort(ts) {
    const d = new Date(ts), now = new Date();
    if (d.toDateString() === now.toDateString()) return fmtTime(ts);
    const y = new Date(now); y.setDate(now.getDate() - 1);
    if (d.toDateString() === y.toDateString()) return 'Yesterday';
    if (now - d < 6 * 86400000) return d.toLocaleDateString(undefined, { weekday: 'short' });
    return d.toLocaleDateString(undefined, { day: 'numeric', month: 'short' });
  }
  const kindNoun = (k) => (k === 'image' ? 'photo' : k === 'video' ? 'video' : 'file');

  // ---------- home
  let homeTimer = null;
  function scheduleHome() {
    if (homeTimer || !homeVisible()) return;
    homeTimer = setTimeout(() => { homeTimer = null; renderHome(); }, 60);
  }
  async function renderHome() {
    const list = $('#roomList');
    list.innerHTML = '';
    const arr = [...rooms.values()].sort((a, b) => (lastTs(b) - lastTs(a)));
    $('#emptyHome').hidden = arr.length > 0;
    for (const r of arr) list.appendChild(roomItem(r));
  }
  function roomItem(r) {
    const li = el('li', 'room' + (r.code === current ? ' active' : ''));
    li.dataset.code = r.code;
    li.tabIndex = 0;
    const av = el('div', 'avatar');
    fillAvatar(av, r, stateOf(r.code));
    const info = el('div', 'room-info');
    const l1 = el('div', 'room-line');
    const lm = lastMsg.get(r.code);
    l1.append(el('span', 'room-name', r.name), el('span', 'room-time', lm ? fmtShort(lm.ts) : ''));
    const l2 = el('div', 'room-line');
    const last = el('span', 'room-last');
    l2.appendChild(last);
    info.append(l1, l2);
    fillRoomLast(last, r);
    const more = iconBtn('more-vertical', 'Room options', 'room-more');
    more.onclick = (e) => { e.stopPropagation(); roomMenu(r, more); };
    li.append(av, info, more);
    li.onclick = () => openChat(r.code);
    li.onkeydown = (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openChat(r.code); } };
    return li;
  }
  function fillRoomLast(node, r) {
    const act = activityText(r.code);
    node.classList.toggle('activity', !!act);
    node.innerHTML = '';
    if (act) { node.textContent = act; return; }
    const lm = lastMsg.get(r.code);
    if (!lm) { node.textContent = stateLabel(r.code); return; }
    if (lm.from === r.slot) node.appendChild(el('span', 'you', 'You: '));
    if (lm.type === 'text') node.appendChild(document.createTextNode(lm.text.split('\n')[0]));
    else {
      const k = fileKind(lm.mime, lm.fileName);
      node.insertAdjacentHTML('beforeend', svg(k === 'image' ? 'image' : k === 'video' ? 'video' : 'file', 14, 'inline'));
      node.appendChild(document.createTextNode(' ' + (k === 'file' ? lm.fileName : k === 'image' ? 'Photo' : 'Video')));
    }
  }
  function updateRoomRow(code) {
    if (!homeVisible()) return;
    const li = $('#roomList .room[data-code="' + code + '"]');
    const r = rooms.get(code);
    if (!li || !r) return;
    const p = li.querySelector('.presence');
    if (p) p.className = 'presence ' + stateOf(code);
    fillRoomLast(li.querySelector('.room-last'), r);
  }
  function markActiveRoom() {
    for (const li of document.querySelectorAll('#roomList .room')) li.classList.toggle('active', li.dataset.code === current);
  }
  function lastTs(r) { const m = lastMsg.get(r.code); return m ? m.ts : (r.created || 0); }
  function stateOf(code) { const c = conns.get(code); return c ? c.state : 'connecting'; }
  function stateLabel(code) {
    const s = stateOf(code);
    // The room works like a group: being alone in it is normal, so no "waiting" wording.
    if (s !== 'connected') return 'Offline';
    const c = conns.get(code);
    return c && c.viaRelay ? 'Online · via relay' : 'Online';
  }

  function roomMenu(r, anchor) {
    openMenu(anchor, [
      { icon: 'hash', label: 'Show code', fn: () => showCode(r, false) },
      { icon: 'pencil', label: 'Rename', fn: () => renameRoom(r) },
      { sep: true },
      { icon: 'trash', label: 'Remove from this device', danger: true, fn: () => deleteRoom(r) }
    ]);
  }

  // ---------- popup menu
  let menuClose = null;
  function openMenu(anchor, items, onClose) {
    closeMenu();
    const m = el('div', 'menu');
    m.id = 'menu';
    m.setAttribute('role', 'menu');
    for (const it of items) {
      if (it.sep) { m.appendChild(el('div', 'menu-sep')); continue; }
      if (it.title) { m.appendChild(el('div', 'menu-title', it.title)); continue; }
      const b = el('button', it.danger ? 'danger' : '');
      b.type = 'button';
      b.setAttribute('role', 'menuitem');
      if (it.icon) b.insertAdjacentHTML('afterbegin', svg(it.icon, 17));
      b.appendChild(el('span', null, it.label));
      b.onclick = (e) => { e.stopPropagation(); closeMenu(); it.fn(); };
      m.appendChild(b);
    }
    document.body.appendChild(m);
    menuClose = onClose || null;
    const rc = anchor.getBoundingClientRect();
    const w = m.offsetWidth, h = m.offsetHeight;
    // open towards the side with more room (left-aligned for anchors in the left half)
    const alignLeft = rc.right - rc.left > 0 && rc.right - rc.left < 200 && (rc.left + rc.right) / 2 < innerWidth / 2 && rc.left > 300;
    const left = Math.max(8, Math.min(innerWidth - w - 8, alignLeft ? rc.left : rc.right - w));
    const below = rc.bottom + h + 8 <= innerHeight;
    m.style.left = left + 'px';
    m.style.top = (below ? rc.bottom + 4 : Math.max(8, rc.top - h - 4)) + 'px';
    m.style.transformOrigin = (rc.right - left > w / 2 ? 'right ' : 'left ') + (below ? 'top' : 'bottom');
    const first = m.querySelector('button');
    if (first && !isTouch) first.focus({ preventScroll: true });
    setTimeout(() => document.addEventListener('click', closeMenu, { once: true }), 0);
  }
  function closeMenu() {
    const m = $('#menu');
    if (m) m.remove();
    const f = menuClose; menuClose = null;
    if (f) f();
  }

  // ---------- modal
  function openModal(build, cls) {
    const body = $('#modalBody');
    body.innerHTML = '';
    $('#dialog').className = 'dialog' + (cls ? ' ' + cls : '');
    build(body);
    hydrateIcons(body);
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
        const cancel = el('button', 'btn ghost', 'Cancel');
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
        const cancel = el('button', 'btn ghost', 'Cancel');
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

  function codeChips(code) {
    const w = el('div', 'code-chips');
    w.setAttribute('aria-label', fmtCode(code));
    [...code].forEach((ch, i) => {
      if (i === 4) w.appendChild(el('span', 'code-dash'));
      const c = el('span', 'code-chip', ch);
      c.style.animationDelay = (i * 35) + 'ms';
      w.appendChild(c);
    });
    return w;
  }

  function showCode(r, isNew) {
    openModal((b) => {
      const head = el('div', 'dlg-head');
      head.innerHTML = svg(isNew ? 'circle-check' : 'hash', 22);
      head.appendChild(el('h3', null, isNew ? 'Room created' : r.name));
      b.appendChild(head);
      b.appendChild(el('p', null, r.slot === 1
        ? 'On your other device choose Join room and enter this code.'
        : 'This device joined the room with this code.'));
      const box = el('div', 'code-box');
      box.appendChild(codeChips(r.code));
      const copy = iconBtn('copy', 'Copy code', 'code-copy', 19);
      copy.onclick = async () => {
        await P.copyText(fmtCode(r.code));
        copy.innerHTML = svg('check', 19);
        copy.classList.add('done');
        toast('Code copied', null, 1800, 'check');
        setTimeout(() => { copy.innerHTML = svg('copy', 19); copy.classList.remove('done'); }, 1600);
      };
      box.appendChild(copy);
      b.appendChild(box);
      const row = el('div', 'row');
      const close = el('button', 'btn ghost', 'Close');
      close.onclick = closeModal;
      const open = el('button', 'btn primary', current === r.code ? 'Done' : 'Open chat');
      open.onclick = () => { closeModal(); if (current !== r.code) openChat(r.code); };
      row.append(close, open);
      b.appendChild(row);
    });
  }

  function joinRoom() {
    openModal((b) => {
      const head = el('div', 'dlg-head');
      head.innerHTML = svg('log-in', 22);
      head.appendChild(el('h3', null, 'Join room'));
      b.appendChild(head);
      b.appendChild(el('p', null, 'Enter the 8-character code shown on your other device.'));
      const inp = el('input', 'code-input');
      inp.placeholder = 'XXXX-XXXX';
      inp.autocapitalize = 'characters';
      inp.autocomplete = 'off';
      inp.spellcheck = false;
      inp.maxLength = 12;
      const err = el('div', 'err');
      const row = el('div', 'row');
      const cancel = el('button', 'btn ghost', 'Cancel');
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
    const name = await promptDialog('Rename room', 'Name (only on this device)', r.name, 'Save');
    if (!name) return;
    r.name = name;
    await dbPut('rooms', r);
    if (current === r.code) renderChatHeader();
    scheduleHome();
  }

  // ---------- deleting messages
  function peerLabel(code) { const r = rooms.get(code); return (r && r.peerName) || 'the other device'; }

  // Used for multi-select (several messages at once): asks first.
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
    await deleteNow(code, ids, everyone);
    return true;
  }
  // Single message from the action bar / long-press menu: the menu choice itself is the confirmation.
  async function deleteNow(code, ids, everyone) {
    const n = ids.length;
    try {
      await deleteMessages(code, ids, everyone);
      toast((n === 1 ? 'Message deleted' : n + ' messages deleted') + (everyone ? ' for everyone' : ' for you'), null, 2200, 'trash');
    } catch (e) { console.error('delete failed', e); toast('Delete failed: ' + (e && e.message ? e.message : e), null, 5000, 'alert'); }
  }

  async function deleteRoom(r) {
    const ok = await confirmDialog('Remove room from this device?',
      'Removes "' + r.name + '" and its message history from this device only. Nothing is deleted for ' + peerLabel(r.code) +
      ' — it keeps the room and all messages. You can re-join later with the code.', 'Remove', true);
    if (!ok) return;
    const c = conns.get(r.code);
    if (c) c.stop();
    conns.delete(r.code);
    rooms.delete(r.code);
    lastMsg.delete(r.code);
    await deleteRoomData(r.code);
    if (current === r.code) await goHome();
    else renderHome();
    toast('Room removed from this device', null, 2400, 'check');
  }

  // ---------- settings
  function openSettings() {
    openModal((b) => {
      b.appendChild(el('h3', null, 'Settings'));
      const sec1 = el('div', 'set-section');
      sec1.appendChild(el('label', null, 'Device name (shown to the other device)'));
      const name = el('input');
      name.value = settings.deviceName;
      name.maxLength = 40;
      sec1.appendChild(name);

      const sec2 = el('div', 'set-section');
      sec2.appendChild(el('label', null, 'Appearance'));
      const seg = el('div', 'segmented');
      seg.setAttribute('role', 'radiogroup');
      for (const [val, ic, lab] of [['system', 'monitor', 'System'], ['dark', 'moon', 'Dark'], ['light', 'sun', 'Light']]) {
        const o = el('button', 'seg' + (themePref() === val ? ' on' : ''));
        o.type = 'button';
        o.setAttribute('role', 'radio');
        o.setAttribute('aria-checked', String(themePref() === val));
        o.innerHTML = svg(ic, 16);
        o.appendChild(el('span', null, lab));
        o.onclick = () => {
          lsSet('theme', val);
          applyTheme();
          for (const x of seg.children) { x.classList.toggle('on', x === o); x.setAttribute('aria-checked', String(x === o)); }
        };
        seg.appendChild(o);
      }
      sec2.appendChild(seg);

      const adv = el('details', 'set-adv');
      if (settings.turnJson.trim()) adv.open = true;
      adv.appendChild(el('summary', null, 'Connection · TURN servers'));
      const l2 = el('label', null, 'TURN servers (optional, JSON array of RTCIceServer)');
      const ta = el('textarea');
      ta.spellcheck = false;
      ta.placeholder = '[\n  {\n    "urls": "turn:global.relay.metered.ca:80",\n    "username": "…",\n    "credential": "…"\n  }\n]';
      ta.value = settings.turnJson;
      const hint = el('p', 'hint');
      hint.innerHTML = 'Optional. When a direct connection is impossible (e.g. on mobile data), Linoa automatically falls back to an encrypted relay. A TURN server (e.g. a free <b>metered.ca</b> account) can make big transfers faster then. Paste its ICE server list here on <b>both</b> devices.';
      adv.append(l2, ta, hint);

      const err = el('div', 'err');
      const info = el('p', 'hint', 'Platform: ' + P.kind + (PROFILE ? ' · profile ' + PROFILE : ''));
      const row = el('div', 'row');
      const cancel = el('button', 'btn ghost', 'Cancel');
      const save = el('button', 'btn primary', 'Save');
      cancel.onclick = closeModal;
      save.onclick = () => {
        const t = ta.value.trim();
        if (t) {
          try {
            const a = JSON.parse(t);
            if (!Array.isArray(a) || !a.every((x) => x && (typeof x.urls === 'string' || Array.isArray(x.urls)))) throw new Error('Must be an array of objects with "urls"');
          } catch (e) { err.textContent = 'Invalid JSON: ' + e.message; adv.open = true; return; }
        }
        const turnChanged = t !== settings.turnJson.trim();
        const nameChanged = name.value.trim() !== settings.deviceName;
        settings.deviceName = name.value.trim() || settings.deviceName;
        settings.turnJson = t;
        closeModal();
        toast('Settings saved', null, 2000, 'check');
        if (turnChanged) restartAll();
        else if (nameChanged) for (const c of conns.values()) if (c.conn) c.sendHello(true);
      };
      row.append(cancel, save);
      b.append(sec1, sec2, adv, err, info, row);
    }, 'sheet');
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
    if (current && current !== code) stopTyping();
    revokeUrls();
    current = code;
    ui.unread = 0;
    ui.barState = null;
    show('chat');
    markActiveRoom();
    renderChatHeader();
    const box = $('#messages');
    box.innerHTML = '';
    box.classList.add('loading');
    const inner = el('div', 'inner');
    const live = el('div', 'live');
    live.id = 'live';
    box.append(inner, live);
    const msgs = await roomMessages(code);
    if (current !== code) return;
    ui.msgs = msgs;
    if (!msgs.length) inner.appendChild(emptyChatEl());
    let prev = null;
    for (const m of msgs) { appendMessage(m, prev, inner); prev = m; }
    regroup();
    // show in-progress transfers
    const rc = conns.get(code);
    if (rc) {
      for (const id of rc.announced.keys()) renderPlaceholder(code, id);
      for (const [id, rx] of rc.incoming) if (!rx.skip) onRxProgress(code, id, rx);
      for (const m of rc.queue) onTxProgress(code, m.id, { sent: 0, size: m.size, queued: true });
      if (rc.sending) onTxProgress(code, rc.sending.id, rc.sending);
      renderTyping();
    }
    updateFab();
    scrollBottom(true);
    requestAnimationFrame(() => box.classList.remove('loading'));
    if (!isTouch) $('#input').focus();
  }
  function emptyChatEl() {
    const e = el('div', 'empty-chat');
    e.id = 'emptyChat';
    e.innerHTML = '<svg class="illus" viewBox="0 0 160 120" width="150" height="112" aria-hidden="true">' +
      '<defs><linearGradient id="ig3" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#4f7cff"/><stop offset="1" stop-color="#8a5cff"/></linearGradient></defs>' +
      '<ellipse cx="80" cy="108" rx="50" ry="6" class="illus-shadow"/>' +
      '<rect x="18" y="20" width="78" height="34" rx="17" class="illus-bubble"/>' +
      '<rect x="62" y="62" width="80" height="34" rx="17" fill="url(#ig3)"/>' +
      '<circle cx="40" cy="37" r="4" class="illus-dot"/><circle cx="57" cy="37" r="4" class="illus-dot"/><circle cx="74" cy="37" r="4" class="illus-dot"/>' +
      '<path d="M88 79h30" stroke="#fff" stroke-width="4" stroke-linecap="round"/></svg>';
    e.appendChild(el('h3', null, 'No messages yet'));
    e.appendChild(el('p', null, 'Say hi! Photos, videos and files are sent in full quality.'));
    return e;
  }

  // What the other side is doing right now (typing / sending files), '' if nothing.
  function activityText(code) {
    const c = conns.get(code);
    if (!c || c.state !== 'connected') return '';
    const ups = c.incomingList();
    if (ups.length > 1) return ups.length + ' files coming…';
    if (ups.length === 1) return 'sending a ' + kindNoun(fileKind(ups[0].mime, ups[0].fileName)) + '…';
    if (c.peerTyping) return 'typing…';
    return '';
  }
  function onActivity(code) {
    if (code === current) renderChatHeader();
    updateRoomRow(code);
  }

  function renderChatHeader() {
    const r = rooms.get(current);
    if (!r) return;
    $('#chatName').textContent = r.name;
    const c = conns.get(current);
    const st = c ? c.state : 'connecting';
    const av = $('#chatAvatar');
    av.innerHTML = '';
    fillAvatar(av, r, st);
    const sub = $('#chatSub');
    const act = activityText(current);
    sub.className = act ? 'activity' : 'st-' + st;
    sub.innerHTML = '';
    if (act) {
      sub.appendChild(el('span', 'mini-dots', ''));
      sub.lastChild.innerHTML = '<i></i><i></i><i></i>';
      sub.appendChild(document.createTextNode(act));
    } else {
      sub.textContent = st === 'connected' ? 'online' + (c.viaRelay ? ' · via relay' : '') + (r.peerName ? ' · ' + r.peerName : '') : 'offline';
    }
    // The room works like a group: being the only one there is normal, so no "waiting for the
    // other device" banner. Messages are simply delivered whenever the other device is online.
    const bar = $('#statusBar');
    $('#statusText').textContent = '';
    $('#statusHint').textContent = '';
    ui.barState = st;
    clearTimeout(ui.barTimer);
    bar.className = 'status-bar';
  }

  // ---------- scrolling + floating "scroll to bottom" button
  function distBottom() { const box = $('#messages'); return box.scrollHeight - box.scrollTop - box.clientHeight; }
  function scrollBottom(force, smooth) {
    const box = $('#messages');
    if (force || distBottom() < 160) {
      requestAnimationFrame(() => {
        if (smooth && !reduceMotion() && box.scrollTo) box.scrollTo({ top: box.scrollHeight, behavior: 'smooth' });
        else box.scrollTop = box.scrollHeight;
      });
    }
  }
  function updateFab() {
    const far = current && distBottom() > 320;
    if (!far && distBottom() < 60) ui.unread = 0;
    const fab = $('#scrollDown');
    fab.hidden = !far;
    const badge = $('#unreadBadge');
    badge.hidden = !ui.unread;
    badge.textContent = ui.unread > 99 ? '99+' : String(ui.unread || '');
  }

  // ---------- message list
  const GROUP_MS = 5 * 60 * 1000;
  const dayKey = (ts) => new Date(ts).toDateString();
  function sameGroup(a, b) {
    return !!a && !!b && a.from === b.from && dayKey(a.ts) === dayKey(b.ts) && Math.abs((b.rts || b.ts) - (a.rts || a.ts)) < GROUP_MS;
  }
  function applyGroup(i) {
    const m = ui.msgs[i];
    const row = m && ui.els.get(m.id);
    if (!row) return;
    row.classList.toggle('grp-first', !sameGroup(ui.msgs[i - 1], m));
    row.classList.toggle('grp-last', !sameGroup(m, ui.msgs[i + 1]));
  }
  function regroup() { for (let i = 0; i < ui.msgs.length; i++) applyGroup(i); }

  function appendMessage(m, prev, inner, animate) {
    inner = inner || $('#messages .inner');
    if (!inner) return;
    const ec = $('#emptyChat');
    if (ec) ec.remove();
    if (!prev || dayKey(prev.ts) !== dayKey(m.ts)) {
      const sep = el('div', 'day-sep');
      sep.appendChild(el('span', null, dayLabel(m.ts)));
      inner.appendChild(sep);
    }
    const node = buildMessage(m);
    if (animate) node.classList.add('enter');
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
    const meta = buildMeta(m, mine);
    if (m.type === 'text') {
      b.classList.add('is-text');
      const t = el('div', 'text');
      t.appendChild(linkify(m.text));
      t.appendChild(el('span', 'meta-spacer' + (mine ? ' mine' : '')));
      b.appendChild(t);
      if (/^\p{Extended_Pictographic}(‍?\p{Extended_Pictographic}|️|\p{Emoji_Modifier}){0,6}$/u.test(m.text.trim())) b.classList.add('jumbo');
      b.appendChild(meta);
    } else {
      buildFileContent(b, m, mine, meta);
    }
    row.appendChild(buildActions(row, m));
    attachMsgMenu(row, b, m);
    if (ui.select && ui.select.has(m.id)) row.classList.add('selected');
    return row;
  }

  function buildMeta(m, mine) {
    const meta = el('div', 'meta');
    meta.appendChild(el('span', 'time', fmtTime(m.ts)));
    if (mine) {
      const s = m.status || 'pending';
      const tk = el('span', 'tick ' + s);
      tk.innerHTML = svg(s === 'delivered' ? 'check-check' : s === 'sent' ? 'check' : 'clock', s === 'pending' ? 12 : 15);
      tk.title = s === 'delivered' ? 'Delivered' : s === 'sent' ? 'Sent' : 'Not delivered yet — arrives when the other device is online';
      meta.appendChild(tk);
    }
    return meta;
  }

  // Hover action bar (desktop): save · copy · delete (for everyone / for me) · more
  function buildActions(row, m) {
    const bar = el('div', 'msg-actions');
    const keep = (btn, items) => {
      bar.classList.add('open');
      openMenu(btn, items, () => bar.classList.remove('open'));
    };
    if (m.type === 'file') {
      const s = iconBtn('download', 'Save', 'act', 17);
      s.onclick = (e) => { e.stopPropagation(); saveMsg(m); };
      bar.appendChild(s);
    } else {
      const c = iconBtn('copy', 'Copy text', 'act', 17);
      c.onclick = (e) => { e.stopPropagation(); copyMsg(m.text); };
      bar.appendChild(c);
    }
    const d = iconBtn('trash', 'Delete', 'act', 17);
    d.onclick = (e) => { e.stopPropagation(); keep(d, deleteItems(m)); };
    const more = iconBtn('more', 'More', 'act', 17);
    more.onclick = (e) => { e.stopPropagation(); keep(more, msgMenuItems(m)); };
    bar.append(d, more);
    return bar;
  }
  function deleteItems(m) {
    return [
      { title: 'Delete message' },
      { icon: 'users', label: 'Delete for everyone', danger: true, fn: () => deleteNow(m.room, [m.id], true) },
      { icon: 'user', label: 'Delete for me', danger: true, fn: () => deleteNow(m.room, [m.id], false) }
    ];
  }

  // ---------- message menu (right-click / long-press) + multi-select
  let lastTouch = 0;
  let suppressClick = false;
  function msgMenuItems(m, partial) {
    const items = [];
    if (!partial) {
      if (m.type === 'text') items.push({ icon: 'copy', label: 'Copy text', fn: () => copyMsg(m.text) });
      else {
        items.push({ icon: 'download', label: 'Save', fn: () => saveMsg(m) });
        const saved = ui.savedPaths.get(m.id);
        if (saved && P.canOpenPath) items.push({ icon: 'external', label: 'Open', fn: () => P.openPath(saved) });
        if (saved && P.canShowInFolder) items.push({ icon: 'folder', label: 'Show in folder', fn: () => P.showInFolder(saved) });
      }
      items.push({ icon: 'circle-check', label: 'Select', fn: () => enterSelect(m.id) });
      items.push({ sep: true });
    }
    items.push({ icon: 'users', label: 'Delete for everyone', danger: true, fn: () => deleteNow(m.room, [m.id], true) });
    items.push({ icon: 'user', label: 'Delete for me', danger: true, fn: () => deleteNow(m.room, [m.id], false) });
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
    const cancel = () => { if (timer) { clearTimeout(timer); timer = null; } b.classList.remove('pressing'); };
    b.addEventListener('touchstart', (e) => {
      lastTouch = Date.now();
      const t = e.touches[0];
      sx = t.clientX; sy = t.clientY;
      cancel();
      b.classList.add('pressing');
      timer = setTimeout(() => {
        timer = null;
        b.classList.remove('pressing');
        lastTouch = Date.now();
        suppressClick = true;
        try { if (navigator.vibrate) navigator.vibrate(15); } catch (x) { /* */ }
        if (ui.select) { if (!partial) toggleSelect(m.id); } else openMenuAt(sx, sy, msgMenuItems(m, partial));
      }, 500);
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
          ui.els.delete(id);
          removeAnimated(row, () => {
            for (const img of row.querySelectorAll('img')) img.removeAttribute('src');
          });
        }
        const u = ui.urls.get(id);
        if (u) { URL.revokeObjectURL(u); ui.urls.delete(id); }
        const rx = $('#messages .msg[data-rx="' + id + '"]');
        if (rx) removeAnimated(rx);
        if (ui.select) ui.select.delete(id);
      }
      ui.msgs = ui.msgs.filter((m) => !set.has(m.id));
      setTimeout(tidyChat, reduceMotion() ? 0 : 190);
      regroup();
      renderSelection();
      renderLiveHead();
    }
    const lm = lastMsg.get(code);
    if (lm && set.has(lm.id)) {
      roomMessages(code).then((msgs) => {
        if (msgs.length) lastMsg.set(code, msgs[msgs.length - 1]); else lastMsg.delete(code);
        scheduleHome();
      }).catch(() => {});
    }
  }
  function removeAnimated(node, after) {
    if (reduceMotion() || !node.isConnected) { node.remove(); if (after) after(); return; }
    node.style.height = node.offsetHeight + 'px';
    node.classList.add('leaving');
    requestAnimationFrame(() => { node.style.height = '0px'; });
    setTimeout(() => { node.remove(); if (after) after(); }, 180);
  }
  // After removals: drop orphaned day separators, show the empty hint.
  function tidyChat() {
    const inner = $('#messages .inner');
    if (!inner) return;
    for (const sep of [...inner.querySelectorAll('.day-sep')]) {
      const n = sep.nextElementSibling;
      if (!n || n.classList.contains('day-sep')) sep.remove();
    }
    regroup();
    if (!ui.msgs.length && !inner.querySelector('.msg') && !$('#emptyChat') && !$('#live .msg')) inner.appendChild(emptyChatEl());
  }
  function openMenuAt(x, y, items) {
    const fake = { getBoundingClientRect: () => ({ left: x, right: x + 200, top: y, bottom: y }) };
    openMenu(fake, items);
  }
  async function copyMsg(text) {
    try { await P.copyText(text); toast('Copied to clipboard', null, 1800, 'copy'); } catch (e) { toast('Copy failed', null, 2600, 'alert'); }
  }

  function objUrl(id, blob) {
    let u = ui.urls.get(id);
    if (!u) { u = URL.createObjectURL(blob); ui.urls.set(id, u); }
    return u;
  }

  // ---------- file / media content
  const EXT_COLORS = [
    [/^(pdf)$/, 'red'], [/^(docx?|odt|rtf|pages)$/, 'blue'], [/^(xlsx?|csv|ods|numbers)$/, 'green'],
    [/^(pptx?|odp|key)$/, 'orange'], [/^(zip|rar|7z|tar|gz|tgz|bz2|xz)$/, 'amber'],
    [/^(mp3|wav|flac|ogg|m4a|aac|opus|wma)$/, 'pink'], [/^(apk|exe|msi|dmg|deb|appimage)$/, 'violet'],
    [/^(txt|md|json|js|ts|html|css|xml|yml|yaml|log|py|java|c|cpp|cs|sh|bat|ini)$/, 'teal'],
    [/^(jpe?g|png|gif|webp|bmp|heic|heif|tiff?|svg|ico)$/, 'cyan'], [/^(mp4|mov|webm|mkv|avi|m4v|3gp)$/, 'violet']
  ];
  function extColor(name) {
    const e = extOf(name).toLowerCase();
    for (const [re, c] of EXT_COLORS) if (re.test(e)) return c;
    return 'slate';
  }
  function fileIconEl(name, kind) {
    const fi = el('div', 'file-icon c-' + extColor(name));
    fi.innerHTML = svg(kind === 'image' ? 'image' : kind === 'video' ? 'video' : 'file', 22);
    fi.appendChild(el('span', 'ext', extOf(name).toUpperCase().slice(0, 4)));
    return fi;
  }

  function buildFileContent(b, m, mine, meta) {
    const kind = fileKind(m.mime, m.fileName);
    const holder = el('div', 'file-holder');
    b.appendChild(holder);
    const asCard = () => {
      holder.innerHTML = '';
      b.classList.remove('media');
      holder.appendChild(fileCard(m, mine));
      b.appendChild(meta);
    };
    let box = null;
    if (kind === 'image' || kind === 'video') {
      b.classList.add('media', 'media-' + kind);
      box = el('div', 'media-box loading');
      box.appendChild(meta);
      holder.appendChild(box);
      holder.appendChild(mediaCaption(m));
    } else {
      holder.appendChild(fileCard(m, mine));
      b.appendChild(meta);
    }
    getBlob(m.id).then((blob) => {
      if (!holder.isConnected || ui.els.get(m.id) !== holder.closest('.msg')) return; // deleted / chat closed meanwhile
      if (!blob) {
        if (box) { box.classList.remove('loading'); box.classList.add('missing'); box.insertAdjacentHTML('afterbegin', svg('alert', 28)); }
        else holder.querySelector('.file-size').textContent = 'File data missing';
        return;
      }
      if (kind === 'image') {
        const img = el('img', 'thumb');
        img.alt = m.fileName;
        img.decoding = 'async';
        img.onload = () => {
          box.classList.remove('loading');
          const ar = img.naturalWidth && img.naturalHeight ? img.naturalWidth / img.naturalHeight : 1;
          box.style.setProperty('--ar', String(Math.max(0.4, Math.min(2.4, ar))));
          box.classList.add('ready');
          scrollBottom(false);
        };
        img.onerror = asCard;
        img.onclick = () => { if (!ui.select) openViewer(m, blob); };
        img.src = objUrl(m.id, blob);
        box.insertBefore(img, meta);
      } else if (kind === 'video') {
        const v = el('video');
        v.preload = 'metadata';
        v.playsInline = true;
        v.src = objUrl(m.id, blob);
        const play = el('button', 'play-btn');
        play.type = 'button';
        play.title = 'Play';
        play.setAttribute('aria-label', 'Play video');
        play.innerHTML = svg('play', 26);
        const dur = el('span', 'dur');
        v.onloadedmetadata = () => {
          box.classList.remove('loading');
          if (v.videoWidth && v.videoHeight) box.style.setProperty('--ar', String(Math.max(0.4, Math.min(2.4, v.videoWidth / v.videoHeight))));
          box.classList.add('ready');
          if (isFinite(v.duration)) dur.textContent = fmtDur(v.duration);
          scrollBottom(false);
        };
        v.onerror = () => { box.classList.remove('loading'); };
        play.onclick = (e) => {
          e.stopPropagation();
          if (ui.select) return;
          box.classList.add('playing');
          v.controls = true;
          const p = v.play();
          if (p && p.catch) p.catch(() => {});
        };
        v.onended = () => { box.classList.remove('playing'); v.controls = false; };
        box.insertBefore(v, meta);
        box.insertBefore(play, meta);
        box.insertBefore(dur, meta);
      }
    });
  }
  function fmtDur(s) {
    s = Math.round(s);
    const h = Math.floor(s / 3600), mm = Math.floor(s / 60) % 60, ss = s % 60;
    return (h ? h + ':' + pad2(mm) : mm) + ':' + pad2(ss);
  }

  function saveSlot(m) {
    const slot = el('div', 'save-slot');
    const s = iconBtn('download', 'Save', 'save-btn', 18);
    s.onclick = (e) => { e.stopPropagation(); saveMsg(m); };
    slot.appendChild(s);
    const saved = ui.savedPaths.get(m.id);
    if (saved) addSavedButtons(slot, saved);
    return slot;
  }

  function mediaCaption(m) {
    const c = el('div', 'media-caption');
    const t = el('div', 'cap-text');
    t.appendChild(el('span', 'name', m.fileName));
    t.appendChild(el('span', 'size', fmtSize(m.size)));
    c.append(t, saveSlot(m));
    return c;
  }

  function fileCard(m, mine) {
    const card = el('div', 'file-card');
    card.appendChild(fileIconEl(m.fileName, fileKind(m.mime, m.fileName)));
    const info = el('div', 'file-info');
    info.appendChild(el('div', 'file-name', m.fileName));
    info.appendChild(el('div', 'file-size', fmtSize(m.size) + ' · ' + extOf(m.fileName).toUpperCase()));
    card.appendChild(info);
    const slot = saveSlot(m);
    if (P.kind === 'browser') {
      const open = iconBtn('external', 'Open in new tab', 'save-btn', 18);
      open.onclick = async (e) => {
        e.stopPropagation();
        const blob = await getBlob(m.id);
        if (blob) window.open(objUrl(m.id, blob), '_blank');
      };
      slot.appendChild(open);
    }
    card.appendChild(slot);
    return card;
  }

  // After saving on PC: icon buttons to open the file / show it in its folder.
  function addSavedButtons(slot, path) {
    if (!slot || slot.querySelector('.ebtn')) return;
    const before = slot.querySelector('#viewerClose');
    const add = (b) => { if (before) slot.insertBefore(b, before); else slot.appendChild(b); };
    if (P.canOpenPath) {
      const o = iconBtn('external', 'Open file', 'save-btn ebtn', 18);
      o.onclick = (e) => { e.stopPropagation(); P.openPath(path); };
      add(o);
    }
    if (P.canShowInFolder) {
      const f = iconBtn('folder', 'Show in folder', 'save-btn ebtn', 18);
      f.onclick = (e) => { e.stopPropagation(); P.showInFolder(path); };
      add(f);
    }
  }
  function refreshSavedButtons(id, path) {
    const row = ui.els.get(id);
    if (row) for (const s of row.querySelectorAll('.save-slot')) addSavedButtons(s, path);
    if (viewerMsg && viewerMsg.m.id === id) addSavedButtons($('#viewerSlot'), path);
  }

  async function saveMsg(m, blob) {
    try {
      blob = blob || await getBlob(m.id);
      if (!blob) { toast('File data missing', null, 2600, 'alert'); return; }
      if (P.kind !== 'browser') toast('Saving ' + m.fileName + '…', null, 60000, 'download');
      const res = await P.saveFile(m.fileName, m.mime, blob);
      if (P.kind === 'electron') {
        ui.savedPaths.set(m.id, res);
        refreshSavedButtons(m.id, res);
        toast('Saved to ' + res, P.canShowInFolder ? { label: 'Show in folder', fn: () => P.showInFolder(res) } : null, null, 'check');
      } else if (P.kind === 'android') {
        toast('Saved to ' + res, null, 5000, 'check');
      } else {
        toast(res, null, 2600, 'download');
      }
    } catch (e) {
      console.error(e);
      toast('Save failed: ' + (e && e.message ? e.message : e), null, 5000, 'alert');
    }
  }

  // ---------- progress (sender: own bubble with progress + cancel)
  function onTxProgress(code, id, s) {
    if (code !== current) return;
    const row = ui.els.get(id);
    if (!row) return;
    let x = row.querySelector('.xfer');
    if (!s) { if (x) x.remove(); row.classList.remove('sending'); return; }
    if (!x) {
      x = el('div', 'xfer');
      const top = el('div', 'xfer-top');
      top.appendChild(el('span', 'xfer-label'));
      const cancel = iconBtn('x', 'Cancel sending (deletes for everyone)', 'xfer-cancel', 15);
      cancel.onclick = (e) => { e.stopPropagation(); deleteNow(code, [id], true); };
      top.appendChild(cancel);
      const bar = el('div', 'progress');
      bar.appendChild(el('div'));
      x.append(top, bar);
      const b = row.querySelector('.bubble');
      const meta = b.querySelector(':scope > .meta');
      b.insertBefore(x, meta || null);
      row.classList.add('sending');
    }
    const pct = s.size ? Math.floor(s.sent / s.size * 100) : 100;
    x.classList.toggle('queued', !!s.queued);
    x.querySelector('.progress > div').style.width = (s.queued ? 0 : pct) + '%';
    x.querySelector('.xfer-label').textContent = s.queued ? 'Queued…' : 'Sending ' + pct + '% · ' + fmtSize(s.sent) + ' / ' + fmtSize(s.size);
  }

  // ---------- receiver placeholders ("sending a photo…" bubbles, bottom-left)
  function placeholderRow(meta) {
    const row = el('div', 'msg theirs placeholder enter grp-first grp-last');
    row.dataset.rx = meta.id;
    const b = el('div', 'bubble up-card');
    const kind = fileKind(meta.mime, meta.fileName);
    const ic = fileIconEl(meta.fileName, kind);
    ic.classList.add('spin');
    const info = el('div', 'up-info');
    info.appendChild(el('div', 'file-name', meta.fileName));
    info.appendChild(el('div', 'up-sub'));
    const bar = el('div', 'progress shimmer');
    bar.appendChild(el('div'));
    info.appendChild(bar);
    const top = el('div', 'up-top');
    top.append(ic, info);
    b.appendChild(top);
    row.appendChild(b);
    attachMsgMenu(row, b, meta, true); // delete while receiving -> aborts the transfer
    return row;
  }
  function renderPlaceholder(code, id) {
    if (code !== current) return;
    const rc = conns.get(code);
    const live = $('#live');
    if (!rc || !live) return;
    const rx = rc.incoming.get(id);
    const meta = rx ? rx.meta : rc.announced.get(id);
    let row = live.querySelector('.msg[data-rx="' + id + '"]');
    if (!meta || (rx && rx.skip)) { if (row) removeAnimated(row); renderLiveHead(); return; }
    if (!row) {
      const ec = $('#emptyChat'); if (ec) ec.remove();
      row = placeholderRow(meta);
      const typing = live.querySelector('.typing-row');
      live.insertBefore(row, typing || null);
      scrollBottom(false);
    }
    const sub = row.querySelector('.up-sub');
    const fill = row.querySelector('.progress > div');
    if (rx) {
      const pct = meta.size ? Math.floor(rx.received / meta.size * 100) : 100;
      row.classList.remove('queued');
      fill.style.width = pct + '%';
      sub.textContent = pct + '% · ' + fmtSize(rx.received) + ' / ' + fmtSize(meta.size);
    } else {
      row.classList.add('queued');
      fill.style.width = '0%';
      sub.textContent = fmtSize(meta.size) + ' · waiting…';
    }
    renderLiveHead();
  }
  function renderLiveHead() {
    const live = $('#live');
    if (!live) return;
    const n = live.querySelectorAll('.msg.placeholder:not(.leaving)').length;
    let h = live.querySelector('.live-head');
    if (n > 1) {
      if (!h) { h = el('div', 'live-head'); live.insertBefore(h, live.firstChild); }
      h.innerHTML = svg('download', 13);
      h.appendChild(document.createTextNode(n + ' files coming'));
    } else if (h) h.remove();
  }
  function onRxProgress(code, id, rx) {
    renderPlaceholder(code, id);
    if (!rx || !rx.lastUi || rx.received === 0) onActivity(code);
  }
  function onAnnounce(code, ids) {
    for (const id of ids) renderPlaceholder(code, id);
    onActivity(code);
  }

  // ---------- typing indicator
  function renderTyping() {
    const live = $('#live');
    if (!live) return;
    const rc = conns.get(current);
    const on = !!(rc && rc.peerTyping && rc.state === 'connected');
    let row = live.querySelector('.typing-row');
    if (on && !row) {
      row = el('div', 'msg theirs typing-row enter grp-first grp-last');
      const b = el('div', 'bubble typing');
      b.setAttribute('aria-label', 'typing');
      b.innerHTML = '<i></i><i></i><i></i>';
      row.appendChild(b);
      live.appendChild(row);
      const ec = $('#emptyChat'); if (ec) ec.style.display = 'none';
      scrollBottom(false);
    } else if (!on && row) {
      row.classList.remove('typing-row');
      removeAnimated(row);
      const ec = $('#emptyChat'); if (ec) ec.style.display = '';
    }
  }
  function onTyping(c) {
    if (c.code === current) renderTyping();
    onActivity(c.code);
  }
  // outgoing: start at most every 3 s while typing; stop on send / empty box / 4 s idle
  const typingOut = { code: null, last: 0, idle: null, on: false };
  function typingPing() {
    if (!current) return;
    if (!$('#input').value.trim()) { stopTyping(); return; }
    const rc = conns.get(current);
    if (!rc || !rc.can('typing')) return;
    if (typingOut.on && typingOut.code !== current) stopTyping();
    const now = Date.now();
    if (!typingOut.on || now - typingOut.last >= 3000) {
      if (rc.sendJSON({ t: 'typing', on: true })) { typingOut.on = true; typingOut.code = current; typingOut.last = now; }
    }
    clearTimeout(typingOut.idle);
    typingOut.idle = setTimeout(stopTyping, 4000);
  }
  function stopTyping() {
    clearTimeout(typingOut.idle);
    if (!typingOut.on) return;
    typingOut.on = false;
    const rc = conns.get(typingOut.code);
    if (rc && rc.can('typing')) rc.sendJSON({ t: 'typing', on: false });
  }

  // ---------- events from RoomConn / storage
  function onNewMessage(m) {
    lastMsg.set(m.room, m);
    const rc = conns.get(m.room);
    const r = rooms.get(m.room);
    const mine = r && m.from === r.slot;
    if (!mine && rc && rc.peerTyping) rc.setPeerTyping(false);
    scheduleHome();
    if (m.room !== current) return;
    if (ui.els.has(m.id)) return;
    const rxRow = $('#messages .msg[data-rx="' + m.id + '"]');
    if (rxRow) { rxRow.remove(); renderLiveHead(); }
    const wasNear = distBottom() < 160;
    const prev = ui.msgs.length ? ui.msgs[ui.msgs.length - 1] : null;
    ui.msgs.push(m);
    appendMessage(m, prev, null, true);
    applyGroup(ui.msgs.length - 2);
    applyGroup(ui.msgs.length - 1);
    if (mine) scrollBottom(true, true);
    else if (wasNear) scrollBottom(true, true);
    else { ui.unread = (ui.unread || 0) + 1; updateFab(); }
  }

  function onMessageUpdated(m) {
    if (m.room !== current) return;
    const i = ui.msgs.findIndex((x) => x.id === m.id);
    if (i >= 0) ui.msgs[i] = m;
    const row = ui.els.get(m.id);
    if (!row) return;
    const r = rooms.get(m.room);
    const old = row.querySelector('.meta');
    if (old) old.replaceWith(buildMeta(m, m.from === r.slot));
  }

  function onConnState(c) {
    if (c.code === current) { renderChatHeader(); renderTyping(); }
    updateRoomRow(c.code);
  }

  // ---------- viewer
  let viewerMsg = null;
  function openViewer(m, blob) {
    viewerMsg = { m, blob };
    for (const b of $('#viewerSlot').querySelectorAll('.ebtn')) b.remove();
    const saved = ui.savedPaths.get(m.id);
    if (saved) addSavedButtons($('#viewerSlot'), saved);
    $('#viewerImg').src = objUrl(m.id, blob);
    $('#viewerName').textContent = m.fileName + ' · ' + fmtSize(m.size);
    $('#viewer').hidden = false;
  }
  function closeViewer() { $('#viewer').hidden = true; $('#viewerImg').removeAttribute('src'); viewerMsg = null; }

  // ---------- toast
  let toastTimer = null;
  function toast(msg, action, ms, ic) {
    const t = $('#toast');
    t.innerHTML = '';
    if (ic) t.insertAdjacentHTML('beforeend', svg(ic, 17, 'toast-ic'));
    t.appendChild(el('span', 'toast-msg', msg));
    if (action) {
      const b = el('button', 'btn small primary', action.label);
      b.onclick = () => { t.hidden = true; action.fn(); };
      t.appendChild(b);
    }
    t.hidden = false;
    t.classList.remove('in');
    void t.offsetWidth;
    t.classList.add('in');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { t.hidden = true; }, ms || (action ? 7000 : 2600));
  }

  // ---------- composer
  function autosize() {
    const t = $('#input');
    t.style.height = 'auto';
    t.style.height = Math.min(160, t.scrollHeight) + 'px';
    $('#composer').classList.toggle('has-text', !!t.value.trim());
  }
  function doSend() {
    const t = $('#input');
    const v = t.value;
    if (!v.trim()) return;
    t.value = '';
    autosize();
    stopTyping();
    sendText(v);
    if (!isTouch) t.focus();
  }

  function bindUI() {
    hydrateIcons();
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
        { icon: 'hash', label: 'Show code', fn: () => showCode(r, false) },
        { icon: 'pencil', label: 'Rename room', fn: () => renameRoom(r) },
        { icon: 'circle-check', label: 'Select messages', fn: () => enterSelect() },
        { icon: 'settings', label: 'Settings', fn: openSettings },
        { sep: true },
        { icon: 'trash', label: 'Remove room from this device', danger: true, fn: () => deleteRoom(r) }
      ]);
    };
    const input = $('#input');
    input.addEventListener('input', () => { autosize(); typingPing(); });
    input.addEventListener('blur', () => { if (!input.value.trim()) stopTyping(); });
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && !e.shiftKey && !isTouch && !e.isComposing) { e.preventDefault(); doSend(); }
    });
    $('#btnSend').addEventListener('mousedown', (e) => e.preventDefault()); // keep keyboard open on mobile
    $('#btnSend').onclick = doSend;
    $('#btnAttach').onclick = () => $('#fileInput').click();
    $('#fileInput').onchange = (e) => { const f = [...e.target.files]; e.target.value = ''; sendFiles(f); };

    const box = $('#messages');
    box.addEventListener('scroll', () => { if (!ui.fabRaf) ui.fabRaf = requestAnimationFrame(() => { ui.fabRaf = 0; updateFab(); }); }, { passive: true });
    $('#scrollDown').onclick = () => { ui.unread = 0; scrollBottom(true, true); setTimeout(updateFab, 400); };

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
      if ($('#menu')) closeMenu();
      else if (!$('#viewer').hidden) closeViewer();
      else if (!$('#modal').hidden) closeModal();
      else exitSelect();
    });
    window.addEventListener('resize', () => { closeMenu(); updateFab(); });
    if (wideMq.addEventListener) wideMq.addEventListener('change', () => { if (isWide()) renderHome(); });

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
      else stopTyping();
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
      toast('PeerJS failed to load', null, 10000, 'alert');
      return;
    }
    const list = await dbAll('rooms');
    for (const r of list) {
      rooms.set(r.code, r);
      const msgs = await roomMessages(r.code);
      if (msgs.length) lastMsg.set(r.code, msgs[msgs.length - 1]);
    }
    for (const r of rooms.values()) startRoom(r);
    await renderHome();
    if (rooms.size === 1) await openChat([...rooms.keys()][0]);
  }

  // debug/testing hook
  window.ChatRoomDebug = { rooms, conns, get current() { return current; }, sendFiles, sendText, getBlob, roomMessages, roomTombs, deleteMessages, PROFILE };

  init().catch((e) => { console.error('init failed', e); toast('Startup failed: ' + e.message, null, 10000); });
})();
