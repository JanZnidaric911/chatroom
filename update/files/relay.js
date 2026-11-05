/* Linoa relay transport — fallback when WebRTC P2P is impossible (e.g. phone on mobile-data CGNAT).
 *
 * Minimal MQTT 3.1.1 client over secure WebSocket to free public brokers (no accounts). Both devices
 * connect to every broker; everything is end-to-end encrypted (AES-GCM-256, key = PBKDF2(room code)),
 * so brokers only ever see random bytes. On top runs a small reliable, ordered transport (seq numbers,
 * cumulative + selective ACKs, sliding window, adaptive retransmit) exposed as a PeerJS-like conn.
 *
 *   const link = new LinoaRelay.Link({ code, slot, log });
 *   link.on('conn', (conn) => ...)   // already-open conn, again after each re-handshake
 *   link.on('state', (s) => ...)     // 'connecting' | 'ready'
 *   link.stop();
 *
 * Topics: base = 'linoa/v1/' + hex(SHA-256('linoa-topic:' + code))[0..32]; receive on base/<mySlot>,
 * publish to base/<otherSlot>. MQTT payload = 12-byte IV + AES-GCM(frame). Frame:
 *   [u8 type][8 B sender session][8 B receiver session (zeros = unknown)] + body
 *   1 HELLO  [u8 flags: 1 = sender has an open conn]
 *   2 DATA   [u32 seq][u8 flags: 1 = binary, 2 = last fragment of a message][payload]
 *   3 ACK    [u32 next expected seq][u8 n][n x (u32 from, u32 to)) received ranges]; doubles as keepalive
 *   4 CLOSE
 * A session id is random per Link and renewed whenever a conn closes. A conn opens when a frame from the
 * peer names our current session (both sides know each other); a new peer session closes the old conn.
 */
(function () {
  'use strict';

  const BROKERS = ['wss://broker.hivemq.com:8884/mqtt', 'wss://broker.emqx.io:8084/mqtt'];
  // Per-broker limits (measured 2026-11): HiveMQ public showed no limit (>10 MB/s, 4 MB payloads OK).
  // EMQX public allows ~10 msg/s and ~200 KB/s per client; excess is silently dropped (or the socket is
  // closed), payloads > 512 KB disconnect. So EMQX is paced and uses bigger segments (1 app chunk = 1 msg).
  function profile(url) {
    if (/emqx/i.test(url)) return { seg: 66 * 1024, bps: 160 * 1024, mps: 7, win: 320 * 1024 };
    return { seg: 32 * 1024 - 64, bps: 0, mps: 0, win: 1024 * 1024 };  // seg: MQTT payload <= 32 KB
  }
  const ID_PREFIX = 'jzchatroom-';
  const CFG = {
    winInit: 256 * 1024,     // initial congestion window (bytes in flight); max is per broker
    winMin: 64 * 1024,
    rtoInit: 2000,
    helloMs: 2500,
    keepMs: 5000,            // conn keepalive (ACK) when idle
    deadMs: 30000,           // nothing from peer -> close
    stallMs: 30000,          // no ACK progress while data in flight -> close
    tieMs: 400,              // handshake done on a secondary broker: wait this long for the primary
    mqttKeepalive: 45
  };
  const T_HELLO = 1, T_DATA = 2, T_ACK = 3, T_CLOSE = 4;
  const HDR = 17, DHDR = 22, SEG_COST = 64, RX_SPAN = 4096, MAX_RANGES = 16;
  const ZERO = '0000000000000000';
  const enc = new TextEncoder();
  const dec = new TextDecoder();
  const subtle = crypto.subtle;

  function rand(n) { return crypto.getRandomValues(new Uint8Array(n)); }
  function hex(u) { let s = ''; for (let i = 0; i < u.length; i++) s += (u[i] < 16 ? '0' : '') + u[i].toString(16); return s; }
  function unhex(s) { const u = new Uint8Array(s.length / 2); for (let i = 0; i < u.length; i++) u[i] = parseInt(s.substr(i * 2, 2), 16); return u; }
  function concat(list, total) {
    const out = new Uint8Array(total);
    let o = 0;
    for (const p of list) { out.set(p, o); o += p.byteLength; }
    return out;
  }
  function isAB(d) { return d instanceof ArrayBuffer || Object.prototype.toString.call(d) === '[object ArrayBuffer]'; }

  // tiny event emitter (PeerJS-style on/off/once)
  class Emitter {
    constructor() { this._h = {}; }
    on(ev, fn) { (this._h[ev] = this._h[ev] || []).push(fn); return this; }
    off(ev, fn) { const l = this._h[ev]; if (l) this._h[ev] = fn ? l.filter((f) => f !== fn && f._o !== fn) : []; return this; }
    once(ev, fn) { const w = (...a) => { this.off(ev, w); fn(...a); }; w._o = fn; return this.on(ev, w); }
    removeAllListeners(ev) { if (ev) delete this._h[ev]; else this._h = {}; return this; }
    emit(ev, ...a) {
      for (const fn of (this._h[ev] || []).slice()) {
        try { fn(...a); } catch (e) { setTimeout(() => { throw e; }); }
      }
    }
  }

  // ================================================================ MQTT 3.1.1 over WebSocket
  function mqttStr(s) { const b = enc.encode(s); const o = new Uint8Array(2 + b.length); o[0] = b.length >> 8; o[1] = b.length & 255; o.set(b, 2); return o; }
  function mqttPkt(first, parts) {
    let len = 0;
    for (const p of parts) len += p.byteLength;
    const lb = [];
    let x = len;
    do { let d = x % 128; x = Math.floor(x / 128); if (x > 0) d |= 128; lb.push(d); } while (x > 0);
    const out = new Uint8Array(1 + lb.length + len);
    out[0] = first; out.set(lb, 1);
    let o = 1 + lb.length;
    for (const p of parts) { out.set(p, o); o += p.byteLength; }
    return out;
  }

  class Mqtt {
    constructor(url, subTopic, log, onMsg, onState) {
      this.url = url; this.sub = subTopic; this.log = log; this.onMsg = onMsg; this.onState = onState;
      this.ready = false; this.stopped = false; this.ws = null; this.backoff = 1000; this.buf = null;
      this.connect();
    }

    connect() {
      if (this.stopped) return;
      let ws;
      try { ws = new WebSocket(this.url, 'mqtt'); } catch (e) { this.retry(); return; }
      ws.binaryType = 'arraybuffer';
      this.ws = ws; this.buf = null; this.lastRx = Date.now();
      this.connTimer = setTimeout(() => { if (ws === this.ws && !this.ready) this.drop('connect timeout'); }, 12000);
      ws.onopen = () => {
        if (ws !== this.ws) return;
        const id = 'linoa' + hex(rand(8));
        // protocol 'MQTT', level 4, flags: clean session, keepalive
        const vh = new Uint8Array([0, 4, 77, 81, 84, 84, 4, 2, CFG.mqttKeepalive >> 8, CFG.mqttKeepalive & 255]);
        this.raw(mqttPkt(0x10, [vh, mqttStr(id)]));
      };
      ws.onmessage = (ev) => { if (ws === this.ws) this.onBytes(new Uint8Array(ev.data)); };
      ws.onclose = () => { if (ws === this.ws) this.drop('closed'); };
      ws.onerror = () => { /* onclose follows */ };
    }

    raw(b) {
      const ws = this.ws;
      if (!ws || ws.readyState !== 1) return false;
      try { ws.send(b); return true; } catch (e) { return false; }
    }

    drop(why) {
      const ws = this.ws, was = this.ready;
      this.ws = null; this.ready = false; this.buf = null;
      clearTimeout(this.connTimer); clearInterval(this.pingTimer);
      if (ws) { ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null; try { ws.close(); } catch (e) { /* */ } }
      if (!this.stopped) this.log('relay: broker', this.url, 'down:', why);
      if (was) this.onState(false);
      this.retry();
    }

    retry() {
      if (this.stopped) return;
      clearTimeout(this.retryTimer);
      this.retryTimer = setTimeout(() => this.connect(), this.backoff * (0.75 + Math.random() * 0.5));
      this.backoff = Math.min(30000, this.backoff * 2);
    }

    onBytes(b) {
      this.lastRx = Date.now();
      if (this.buf && this.buf.length) { const n = new Uint8Array(this.buf.length + b.length); n.set(this.buf); n.set(b, this.buf.length); b = n; }
      let p = 0;
      while (this.ws && b.length - p >= 2) {
        let len = 0, mul = 1, i = p + 1, d;
        do {
          if (i >= b.length) { d = -1; break; }
          d = b[i++]; len += (d & 127) * mul; mul *= 128;
          if (mul > 128 * 128 * 128 * 128) { this.drop('bad length'); return; }
        } while (d & 128);
        if (d < 0 || b.length - i < len) break;          // incomplete: wait for more frames
        this.handle(b[p] >> 4, b[p] & 15, b.subarray(i, i + len));
        p = i + len;
      }
      this.buf = p < b.length ? b.slice(p) : null;
    }

    handle(type, flags, body) {
      if (type === 2) {                                   // CONNACK
        if (body[1] !== 0) { this.drop('refused ' + body[1]); return; }
        this.raw(mqttPkt(0x82, [new Uint8Array([0, 1]), mqttStr(this.sub), new Uint8Array([0])]));
      } else if (type === 9) {                            // SUBACK
        if (body[2] === 0x80) { this.drop('subscribe refused'); return; }
        clearTimeout(this.connTimer);
        this.ready = true; this.backoff = 1000;
        this.pingTimer = setInterval(() => {
          if (Date.now() - this.lastRx > 40000) this.drop('ping timeout');
          else this.raw(new Uint8Array([0xC0, 0]));
        }, 15000);
        this.onState(true);
      } else if (type === 3) {                            // PUBLISH
        let o = 2 + ((body[0] << 8) | body[1]);
        if ((flags >> 1) & 3) o += 2;
        if (o <= body.length) this.onMsg(body.slice(o));
      }
      // PINGRESP (13) etc: lastRx already updated
    }

    publish(topicBytes, payload) {
      if (!this.ready) return false;
      return this.raw(mqttPkt(0x30, [topicBytes, payload]));
    }

    get buffered() { return (this.ws && this.ws.bufferedAmount) || 0; }

    stop() {
      this.stopped = true;
      clearTimeout(this.retryTimer); clearTimeout(this.connTimer); clearInterval(this.pingTimer);
      const ws = this.ws;
      this.ws = null; this.ready = false;
      if (ws) {
        try { if (ws.readyState === 1) ws.send(new Uint8Array([0xE0, 0])); } catch (e) { /* */ }
        ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null;
        try { ws.close(); } catch (e) { /* */ }
      }
    }
  }

  // ================================================================ reliable conn
  class Conn extends Emitter {
    constructor(link, peerSess, broker) {
      super();
      this.link = link; this.peerSess = peerSess; this.broker = broker;
      this.isRelay = true; this.open = true; this.serialization = 'raw'; this.reliable = true;
      this.label = 'relay'; this.metadata = null;
      this.peer = ID_PREFIX + link.code + '-' + link.other;
      const now = Date.now();
      // tx
      this.nextSeq = 0; this.q = []; this.qBytes = 0;
      this.inflight = new Map(); this.inflightBytes = 0;   // seq -> seg (insertion order == seq order)
      this.cwnd = CFG.winInit; this.ssthresh = 1 << 30;
      this.seen = [];                                      // per broker: last time a frame from the peer arrived there
      this.srtt = 0; this.rttvar = 0; this.rto = CFG.rtoInit; this.lastCut = 0; this.lastProgress = now;
      this.waiters = [];
      // rx
      this.expect = 0; this.ooo = new Map(); this.parts = []; this.partsLen = 0;
      this.unacked = 0; this.ackTimer = null;
      this.lastRecv = now; this.lastSent = now;
      this.stats = { sent: 0, resent: 0, recv: 0, dup: 0 };
    }

    get bufferedAmount() { return this.qBytes + this.inflightBytes; }

    send(data) {
      if (!this.open) return false;
      let bytes, bin = true;
      if (typeof data === 'string') { bytes = enc.encode(data); bin = false; }
      else if (isAB(data)) bytes = new Uint8Array(data.slice(0));
      else if (ArrayBuffer.isView(data)) bytes = new Uint8Array(data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength));
      else { bytes = enc.encode(String(data)); bin = false; }
      this.q.push({ bytes, bin, off: 0 });
      this.qBytes += bytes.byteLength;
      this.pump();
      return true;
    }

    waitDrain(low) {
      low = low || 0;
      if (!this.open || this.bufferedAmount <= low) return Promise.resolve();
      return new Promise((resolve) => this.waiters.push({ low, resolve }));
    }

    checkWaiters() {
      if (!this.waiters.length) return;
      const b = this.bufferedAmount;
      this.waiters = this.waiters.filter((w) => { if (!this.open || b <= w.low) { w.resolve(); return false; } return true; });
    }

    close() { this._close('closed locally', true); }

    _close(why, notify) {
      if (!this.open) return;
      this.open = false;
      clearTimeout(this.ackTimer);
      this.q = []; this.qBytes = 0; this.inflight.clear(); this.inflightBytes = 0; this.ooo.clear(); this.parts = [];
      this.link.log('relay: conn closed:', why);
      this.link.connClosed(this, notify);
      this.checkWaiters();
      this.emit('close');
    }

    frame(type, bodyLen) {
      const f = new Uint8Array(HDR + bodyLen);
      f[0] = type; f.set(this.link.mySessB, 1); f.set(unhex(this.peerSess), 9);
      return f;
    }

    out(f) { this.lastSent = Date.now(); this.link.sendFrame(this.broker, f); }

    pump() {
      const now = Date.now(), prof = this.link.brokers[this.broker].prof;
      const win = Math.min(this.cwnd, prof.win);
      while (this.open && this.q.length && this.inflightBytes + this.inflight.size * SEG_COST < win && this.link.canSend(this.broker, now)) {
        const m = this.q[0];
        const n = Math.min(prof.seg, m.bytes.byteLength - m.off);
        const last = m.off + n >= m.bytes.byteLength;
        const seg = { seq: this.nextSeq++, flags: (m.bin ? 1 : 0) | (last ? 2 : 0), data: m.bytes.subarray(m.off, m.off + n), sentAt: 0, tries: 0 };
        m.off += n; this.qBytes -= n;
        if (last) this.q.shift();
        if (!this.inflight.size) this.lastProgress = now;
        this.inflight.set(seg.seq, seg); this.inflightBytes += n;
        this.xmit(seg, now);
      }
    }

    xmit(seg, now) {
      const f = this.frame(T_DATA, 5 + seg.data.byteLength);
      const dv = new DataView(f.buffer);
      dv.setUint32(HDR, seg.seq); f[HDR + 4] = seg.flags; f.set(seg.data, DHDR);
      if (seg.tries) this.stats.resent++; else this.stats.sent++;
      seg.sentAt = now; seg.tries++;
      this.out(f);
    }

    sendAck() {
      clearTimeout(this.ackTimer); this.ackTimer = null; this.unacked = 0;
      if (!this.open) return;
      const ranges = [];
      if (this.ooo.size) {
        const keys = [...this.ooo.keys()].sort((a, b) => a - b);
        let a = keys[0], b = a + 1;
        for (let i = 1; i <= keys.length; i++) {
          if (i < keys.length && keys[i] === b) { b++; continue; }
          ranges.push([a, b]);
          if (ranges.length >= MAX_RANGES || i === keys.length) break;
          a = keys[i]; b = a + 1;
        }
      }
      const f = this.frame(T_ACK, 5 + ranges.length * 8);
      const dv = new DataView(f.buffer);
      dv.setUint32(HDR, this.expect); f[HDR + 4] = ranges.length;
      ranges.forEach((r, i) => { dv.setUint32(DHDR + i * 8, r[0]); dv.setUint32(DHDR + i * 8 + 4, r[1]); });
      this.out(f);
    }

    // ---- receive
    onFrame(type, f) {
      this.lastRecv = Date.now();
      const dv = new DataView(f.buffer, f.byteOffset, f.byteLength);
      if (type === T_DATA && f.length >= DHDR) this.onData(dv.getUint32(HDR), f[HDR + 4], f.subarray(DHDR));
      else if (type === T_ACK && f.length >= DHDR) {
        const n = Math.min(f[HDR + 4], (f.length - DHDR) >> 3), ranges = [];
        for (let i = 0; i < n; i++) ranges.push([dv.getUint32(DHDR + i * 8), dv.getUint32(DHDR + i * 8 + 4)]);
        this.onAck(dv.getUint32(HDR), ranges);
      }
    }

    onData(seq, flags, data) {
      this.stats.recv++;
      if (seq < this.expect) { this.stats.dup++; this.sendAck(); return; }
      if (seq >= this.expect + RX_SPAN) return;
      if (seq > this.expect) {
        if (!this.ooo.has(seq)) this.ooo.set(seq, { flags, data }); else this.stats.dup++;
        this.sendAck();                                       // immediate: lets the sender fast-retransmit
        return;
      }
      this.deliver(flags, data); this.expect++;
      while (this.open && this.ooo.has(this.expect)) {
        const s = this.ooo.get(this.expect);
        this.ooo.delete(this.expect); this.expect++;
        this.deliver(s.flags, s.data);
      }
      if (!this.open) return;
      if (++this.unacked >= 4 || this.ooo.size) this.sendAck();
      else if (!this.ackTimer) this.ackTimer = setTimeout(() => this.sendAck(), 20);
    }

    deliver(flags, data) {
      this.parts.push(data); this.partsLen += data.byteLength;
      if (!(flags & 2)) return;
      const all = this.parts.length === 1 ? data.slice() : concat(this.parts, this.partsLen);
      this.parts = []; this.partsLen = 0;
      if (this.open) this.emit('data', (flags & 1) ? all.buffer : dec.decode(all));
    }

    onAck(cum, ranges) {
      const now = Date.now();
      let acked = 0, sample = -1, top = -1;
      const ack = (seg) => {
        if (seg.tries === 1) sample = now - seg.sentAt;
        acked += seg.data.byteLength;
        this.inflightBytes -= seg.data.byteLength;
        this.inflight.delete(seg.seq);
      };
      for (const [seq, seg] of this.inflight) { if (seq >= cum) break; ack(seg); }
      for (const [a, b] of ranges) {
        if (b <= a || b - a > RX_SPAN) continue;
        top = Math.max(top, b - 1);
        for (let s = a; s < b; s++) { const seg = this.inflight.get(s); if (seg) ack(seg); }
      }
      if (sample >= 0) {
        if (!this.srtt) { this.srtt = sample; this.rttvar = sample / 2; }
        else { this.rttvar = 0.75 * this.rttvar + 0.25 * Math.abs(this.srtt - sample); this.srtt = 0.875 * this.srtt + 0.125 * sample; }
        this.rto = Math.min(10000, Math.max(400, this.srtt + 4 * this.rttvar + 50));
      }
      if (acked) {
        this.lastProgress = now;
        if (this.cwnd < this.ssthresh) this.cwnd += acked;
        else this.cwnd += Math.max(1, Math.floor(32768 * acked / this.cwnd));
        this.cwnd = Math.min(this.cwnd, this.link.brokers[this.broker].prof.win);
      }
      // fast retransmit: a later segment arrived, so older unacked ones sent >~1 RTT ago were lost
      if (top >= 0 && this.srtt) {
        const late = this.srtt * 1.25 + 30;
        let lost = false;
        for (const [seq, seg] of this.inflight) {
          if (seq >= top) break;
          if (now - seg.sentAt > late && this.link.canSend(this.broker, now)) { this.xmit(seg, now); lost = true; }
        }
        if (lost) this.cut(now);
      }
      this.checkWaiters();
      this.pump();
    }

    cut(now) {
      if (now - this.lastCut < (this.srtt || this.rto)) return;   // once per RTT
      this.lastCut = now;
      this.ssthresh = Math.max(CFG.winMin, Math.floor(this.cwnd / 2));
      this.cwnd = this.ssthresh;
    }

    tick(now) {
      if (!this.open) return;
      if (now - this.lastRecv > CFG.deadMs) { this._close('peer silent', true); return; }
      if (this.inflight.size && now - this.lastProgress > CFG.stallMs) { this._close('retransmits failing', true); return; }
      let lost = false;
      for (const seg of this.inflight.values()) {
        if (!this.link.canSend(this.broker, now)) break;
        if (now - seg.sentAt > Math.min(15000, this.rto * Math.pow(2, Math.min(4, seg.tries - 1)))) { this.xmit(seg, now); lost = true; }
      }
      if (lost) this.cut(now);
      this.pump();                                          // paced brokers: continue as budget refills
      if (now - this.lastSent > CFG.keepMs) this.sendAck();
    }
  }

  // ================================================================ Link
  class Link extends Emitter {
    constructor(opts) {
      super();
      this.code = String(opts.code || '').toUpperCase();
      this.slot = Number(opts.slot) === 2 ? 2 : 1;
      this.other = 3 - this.slot;
      this.log = opts.log || (() => {});
      this.urls = (opts.brokers || BROKERS).slice();
      this.state = 'connecting';
      this.stopped = false;
      this.brokers = [];
      this.conn = null;
      this.pendingOpen = null;
      this.peerSess = null;
      this.dead = new Set();
      this.tx = Promise.resolve();
      this.newSess();
      this.init().catch((e) => this.log('relay: init failed', e && e.message));
    }

    newSess() { this.mySessB = rand(8); this.mySess = hex(this.mySessB); }

    async init() {
      const h = new Uint8Array(await subtle.digest('SHA-256', enc.encode('linoa-topic:' + this.code)));
      const base = 'linoa/v1/' + hex(h).slice(0, 32);
      const km = await subtle.importKey('raw', enc.encode(this.code), 'PBKDF2', false, ['deriveKey']);
      this.key = await subtle.deriveKey(
        { name: 'PBKDF2', hash: 'SHA-256', salt: enc.encode('linoa-relay-v1:' + this.code), iterations: 150000 },
        km, { name: 'AES-GCM', length: 256 }, false, ['encrypt', 'decrypt']);
      if (this.stopped) return;
      this.pubTopic = mqttStr(base + '/' + this.other);
      this.brokers = this.urls.map((u, i) => {
        const url = typeof u === 'string' ? u : u.url;
        const prof = Object.assign(profile(url), typeof u === 'string' ? {} : u);
        const b = { i, url, prof, rx: Promise.resolve(), lastHello: 0, lastReply: 0, mq: null, tokB: prof.bps, tokM: prof.mps, fill: Date.now() };
        b.mq = new Mqtt(url, base + '/' + this.slot, this.log,
          (payload) => this.onPayload(b, payload),
          (ready) => this.onBrokerState(b, ready));
        return b;
      });
      this.timer = setInterval(() => this.tick(), 100);
    }

    stop() {
      if (this.stopped) return;
      if (this.conn) this.conn._close('link stopped', true);
      this.stopped = true;
      clearInterval(this.timer);
      const brokers = this.brokers;
      const end = () => brokers.forEach((b) => b.mq.stop());
      const t = setTimeout(end, 1500);
      this.tx.then(() => { clearTimeout(t); end(); });       // let the CLOSE frames go out first
    }

    onBrokerState(b, ready) {
      if (this.stopped) return;
      this.log('relay: broker', b.url, ready ? 'ready' : 'lost');
      if (ready) b.lastHello = 0;
      else {
        if (this.conn && this.conn.broker === b.i) this.conn._close('broker lost', true);
        if (this.pendingOpen && this.pendingOpen.b === b.i) this.pendingOpen = null;
      }
      const s = this.brokers.some((x) => x.mq.ready) ? 'ready' : 'connecting';
      if (s !== this.state) { this.state = s; this.emit('state', s); }
    }

    // token bucket per broker (1 s burst); control frames always go out but still consume budget
    canSend(bi, now) {
      const b = this.brokers[bi], p = b.prof;
      if (!p.bps && !p.mps) return true;
      const dt = (now - b.fill) / 1000;
      b.fill = now;
      b.tokB = Math.min(p.bps, b.tokB + p.bps * dt);
      b.tokM = Math.min(p.mps, b.tokM + p.mps * dt);
      return (!p.bps || b.tokB > 0) && (!p.mps || b.tokM >= 1);
    }

    sendFrame(bi, f) {
      const b = this.brokers[bi];
      if (!b || !b.mq.ready || !this.key) return;
      b.tokB -= f.length + 60; b.tokM -= 1;
      this.tx = this.tx.then(async () => {
        const iv = rand(12);
        const ct = new Uint8Array(await subtle.encrypt({ name: 'AES-GCM', iv }, this.key, f));
        const p = new Uint8Array(12 + ct.length);
        p.set(iv); p.set(ct, 12);
        b.mq.publish(this.pubTopic, p);
      }).catch(() => {});
    }

    hello(b, now) {
      const open = !!this.conn;
      const f = new Uint8Array(HDR + 1);
      f[0] = T_HELLO; f.set(this.mySessB, 1); f.set(unhex(open ? this.conn.peerSess : (this.peerSess || ZERO)), 9);
      f[HDR] = open ? 1 : 0;
      b.lastHello = b.lastReply = now;
      this.sendFrame(b.i, f);
    }

    sendClose(peerSess) {
      const f = new Uint8Array(HDR);
      f[0] = T_CLOSE; f.set(this.mySessB, 1); f.set(unhex(peerSess), 9);
      for (const b of this.brokers) if (b.mq.ready) this.sendFrame(b.i, f);
    }

    onPayload(b, p) {
      if (this.stopped || p.length < 12 + 16 + HDR || !this.key) return;
      b.rx = b.rx.then(() => subtle.decrypt({ name: 'AES-GCM', iv: p.subarray(0, 12) }, this.key, p.subarray(12)))
        .then((pt) => { if (!this.stopped) this.onFrame(b, new Uint8Array(pt)); }, () => { /* not ours / corrupt: drop */ })
        .catch((e) => this.log('relay: frame error', e && e.message));
    }

    forget(sess) {
      if (!sess || sess === ZERO) return;
      this.dead.add(sess);
      if (this.dead.size > 64) this.dead.delete(this.dead.values().next().value);
    }

    onFrame(b, f) {
      if (f.length < HDR) return;
      const type = f[0], from = hex(f.subarray(1, 9)), to = hex(f.subarray(9, 17));
      if (from === this.mySess || from === ZERO || this.dead.has(from)) return;
      const now = Date.now();
      if (type === T_CLOSE) {
        if (to !== this.mySess) return;
        if (this.conn && this.conn.peerSess === from) this.conn._close('peer closed', false);
        else if (this.peerSess === from) this.peerSess = null;
        this.forget(from);
        return;
      }
      if (this.conn && this.conn.peerSess !== from) {
        if (type !== T_HELLO) return;                      // stray frame
        this.conn._close('peer restarted', true);          // new peer session -> old conn is gone
      }
      if (this.conn && to === this.mySess) this.conn.seen[b.i] = now;
      if (!this.conn && this.peerSess !== from) {
        if (this.peerSess) this.forget(this.peerSess);
        this.peerSess = from; this.pendingOpen = null;
      }
      if (!this.conn && to === this.mySess) this.complete(b.i, now);
      if (type === T_HELLO) {
        if (!(f[HDR] & 1) && now - b.lastReply > 200) this.hello(b, now);   // peer still waiting: answer
      } else if (this.conn && to === this.mySess) {
        this.conn.onFrame(type, f);
      }
    }

    complete(bi, now) {
      if (this.conn) return;
      if (bi !== 0 && this.brokers[0] && this.brokers[0].mq.ready) {
        if (!this.pendingOpen) this.pendingOpen = { b: bi, at: now + CFG.tieMs };
        return;
      }
      this.openConn(bi);
    }

    openConn(bi) {
      this.pendingOpen = null;
      if (!this.brokers[bi].mq.ready) return;
      const c = new Conn(this, this.peerSess, bi);
      this.conn = c;
      this.log('relay: conn open via', this.brokers[bi].url);
      this.hello(this.brokers[bi], Date.now());            // tells the peer we know it
      this.emit('conn', c);
    }

    connClosed(c, notify) {
      if (this.conn !== c) return;
      this.conn = null;
      if (notify) this.sendClose(c.peerSess);
      this.forget(c.peerSess);
      this.peerSess = null;
      this.newSess();
      for (const b of this.brokers) b.lastHello = 0;
    }

    tick() {
      if (this.stopped) return;
      const now = Date.now();
      const c = this.conn;
      if (c) {
        // conn on a fallback broker: probe the preferred ones (HELLO with open flag, never answered) and
        // move our sending there once the peer is heard on it. The receiver accepts frames on any broker.
        for (const b of this.brokers) {
          if (b.i >= c.broker || !b.mq.ready) continue;
          if (now - (c.seen[b.i] || 0) < 6000) { this.log('relay: conn moves to', b.url); c.broker = b.i; break; }
          if (now - b.lastHello >= CFG.helloMs) this.hello(b, now);
        }
        c.tick(now);
        return;
      }
      if (this.pendingOpen && now >= this.pendingOpen.at) { this.openConn(this.pendingOpen.b); if (this.conn) return; }
      for (const b of this.brokers) if (b.mq.ready && now - b.lastHello >= CFG.helloMs) this.hello(b, now);
    }
  }

  window.LinoaRelay = { Link, BROKERS, config: CFG, _Mqtt: Mqtt };
})();
