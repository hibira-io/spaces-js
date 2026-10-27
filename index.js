// Multiplayer collaboration for pusher-js and Laravel Echo on Hibira presence
// channels: who is here (avatars), where they are (location, follow), live
// cursors and component locks.
//
// A space is a presence channel (presence-space-<id>, say). Nothing runs on
// the server besides what every Pusher-compatible server already does:
//
//   - members and their user_info come from the presence channel;
//   - profile, location, cursors and locks are client events
//     (client-spaces-*), so the app needs client events turned on;
//   - when someone joins, every member sends them a hello with its own
//     profile, location and locks, and the newcomer sends its own;
//   - a member's locks and cursor go away when it leaves the channel.
//
// Client events are not echoed back to the sender, so the space applies your
// own changes locally. The server stamps client events on presence channels
// with the sender's user_id, so a member can only speak for itself.
//
// Servers limit client events per connection (10 per second on Hibira and
// Pusher), so everything this sends goes through a small token bucket:
// cursor moves are batched into one event per cursorInterval, and profile
// and location updates waiting for a token are merged.

const EVENT = {
  hello: 'client-spaces-hello',
  profile: 'client-spaces-profile',
  location: 'client-spaces-location',
  cursor: 'client-spaces-cursor',
  lock: 'client-spaces-lock',
  unlock: 'client-spaces-unlock',
};

const DEFAULTS = {
  profile: null,
  location: null,
  cursorInterval: 200,
  cursorTimeout: 10000,
  cursorReplay: true,
  maxCursorPoints: 20,
  lockGrace: 300,
  helloDelay: 300,
  maxRate: 8,
};

export class LockedError extends Error {
  constructor(id, holder) {
    super(`Space: "${id}" is locked by ${holder}`);
    this.name = 'LockedError';
    this.id = id;
    this.holder = holder;
  }
}

export class Space {
  /**
   * Subscribe to a space channel and track it.
   * @param {import('pusher-js').default} pusher
   * @param {string} channelName a presence- channel, e.g. "presence-space-doc-42"
   * @param {object} [options] see constructor
   */
  static enter(pusher, channelName, options = {}) {
    if (!channelName.startsWith('presence-') || channelName.startsWith('presence-large-')) {
      throw new Error(`Space: "${channelName}" is not a presence channel (presence-large- channels do not allow client events)`);
    }
    const space = new Space(pusher.subscribe(channelName), options);
    space._unsubscribe = () => pusher.unsubscribe(channelName);
    return space;
  }

  /**
   * Track a pusher-js presence channel, e.g.
   * `new Space(Echo.join('doc.42').subscription)`. Create it right after
   * subscribing so no event is missed.
   * @param {object} channel pusher-js PresenceChannel
   * @param {object} [options]
   * @param {object} [options.profile] your profile (color, status...), merged with setProfile()
   * @param {any} [options.location] where you are (page, slide, field...), changed with setLocation()
   * @param {number} [options.cursorInterval=200] ms between cursor batches you send
   * @param {number} [options.cursorTimeout=10000] ms after which a still cursor is hidden
   * @param {boolean} [options.cursorReplay=true] replay others' batched points at their original pace
   * @param {number} [options.maxCursorPoints=20] points kept per batch (the latest ones)
   * @param {number} [options.lockGrace=300] ms lock() waits for a competing request before it resolves
   * @param {number} [options.helloDelay=300] up to this many ms before greeting a new member
   * @param {number} [options.maxRate=8] client events per second this space sends at most
   */
  constructor(channel, options = {}) {
    const o = { ...DEFAULTS, ...options };
    this.channel = channel;
    this.name = channel.name;
    this.cursorInterval = o.cursorInterval;
    this.cursorTimeout = o.cursorTimeout;
    this.cursorReplay = o.cursorReplay;
    this.maxCursorPoints = o.maxCursorPoints;
    this.lockGrace = o.lockGrace;
    this.helloDelay = o.helloDelay;
    this.maxRate = o.maxRate;
    /** This connection's member id (null until subscribed). */
    this.myID = null;
    this.ready = false;
    this._profile = o.profile ?? {};
    this._location = o.location ?? null;
    this._members = new Map(); // id -> {info, profile, location}
    this._cursors = new Map(); // id -> {x, y, data, at}
    this._cursorTimers = new Map(); // id -> [hide timer, ...replay timers]
    this._locks = new Map(); // lock id -> {member, at, attrs}
    this._waiting = new Map(); // lock id -> {resolve, reject, timer}
    this._follows = new Map(); // member id -> Set(fn)
    this._handlers = new Map();
    this._points = []; // own cursor points not sent yet: [x, y, t, data]
    this._cursorShown = false;
    this._queue = []; // [{event, build, key}]
    this._tokens = this.maxRate;
    this._refilled = Date.now();
    this._pumpTimer = null;
    this._cursorTimer = null;
    this._helloTimer = null;
    this._unsubscribe = null;
    this._left = false;

    this._bindings = [
      ['pusher:subscription_succeeded', (m) => this._snapshot(m)],
      ['pusher:member_added', (m) => this._added(m)],
      ['pusher:member_removed', (m) => this._removed(m)],
      [EVENT.hello, (d, meta) => this._onHello(d, meta)],
      [EVENT.profile, (d, meta) => this._onProfile(d, meta)],
      [EVENT.location, (d, meta) => this._onLocation(d, meta)],
      [EVENT.cursor, (d, meta) => this._onCursor(d, meta)],
      [EVENT.lock, (d, meta) => this._onLock(d, meta)],
      [EVENT.unlock, (d, meta) => this._onUnlock(d, meta)],
    ];
    for (const [ev, fn] of this._bindings) channel.bind(ev, fn);
    if (channel.subscribed && channel.members && channel.members.count) {
      this._snapshot(channel.members);
    }
  }

  /**
   * Register a handler; returns a function that removes it. Events: ready,
   * members, cursor, lock, error.
   */
  on(event, fn) {
    if (!this._handlers.has(event)) this._handlers.set(event, new Set());
    this._handlers.get(event).add(fn);
    return () => this.off(event, fn);
  }

  off(event, fn) {
    this._handlers.get(event)?.delete(fn);
  }

  _emit(event, payload) {
    for (const fn of this._handlers.get(event) ?? []) {
      try {
        fn(payload);
      } catch (err) {
        if (event === 'error') throw err;
        this._emit('error', err);
      }
    }
  }

  // ---- members -----------------------------------------------------------

  /** Everyone in the space, you first: [{id, info, profile, location, me}]. */
  get members() {
    return [...this._members.keys()]
      .sort((a, b) => (a === this.myID ? -1 : b === this.myID ? 1 : a < b ? -1 : a > b ? 1 : 0))
      .map((id) => this._member(id));
  }

  /** Everyone but you. */
  get others() {
    return this.members.filter((m) => !m.me);
  }

  /** Your own member, or null before subscription. */
  get me() {
    return this.myID == null || !this._members.has(this.myID) ? null : this._member(this.myID);
  }

  /** A member by user id, or null. */
  member(id) {
    return this._members.has(id) ? this._member(id) : null;
  }

  /** Members whose location equals `location` (compared as JSON). */
  membersAt(location) {
    const key = JSON.stringify(location ?? null);
    return this.members.filter((m) => JSON.stringify(m.location) === key);
  }

  _member(id) {
    const m = this._members.get(id);
    return { id, info: m?.info ?? null, profile: m?.profile ?? {}, location: m?.location ?? null, me: id === this.myID };
  }

  /** Merge `patch` into your profile and tell the space. */
  setProfile(patch) {
    this._profile = { ...this._profile, ...patch };
    if (!this._joined()) return;
    this._members.get(this.myID).profile = this._profile;
    this._send(EVENT.profile, () => ({ profile: this._profile }), 'profile');
    this._emit('members', { members: this.members, updated: this.me });
  }

  /** Set where you are (a page, a slide, a field id...) and tell the space. */
  setLocation(location) {
    this._location = location ?? null;
    if (!this._joined()) return;
    this._members.get(this.myID).location = this._location;
    this._send(EVENT.location, () => ({ location: this._location }), 'location');
    this._emit('members', { members: this.members, updated: this.me });
  }

  /**
   * Call `fn(location, member)` now and whenever member `id` moves; returns
   * a function that stops following. Following ends when they leave
   * (`fn(null, null)`).
   */
  follow(id, fn) {
    if (!this._follows.has(id)) this._follows.set(id, new Set());
    this._follows.get(id).add(fn);
    if (this._members.has(id)) fn(this._members.get(id).location ?? null, this._member(id));
    return () => this._follows.get(id)?.delete(fn);
  }

  _moved(id) {
    const fns = this._follows.get(id);
    if (!fns?.size) return;
    const m = this._members.has(id) ? this._member(id) : null;
    for (const fn of fns) {
      try {
        fn(m?.location ?? null, m);
      } catch (err) {
        this._emit('error', err);
      }
    }
    if (!m) this._follows.delete(id);
  }

  // ---- cursors -----------------------------------------------------------

  /**
   * Move your cursor. Call it on every pointer move; points are sent in
   * batches every cursorInterval ms. Use coordinates every member can map,
   * such as fractions of a shared canvas. `data` (optional) rides along,
   * e.g. a selection or the element under the cursor.
   */
  moveCursor(x, y, data) {
    if (!this._joined()) return;
    this._points.push([x, y, Date.now(), data]);
    if (this._points.length > this.maxCursorPoints) this._points.splice(0, this._points.length - this.maxCursorPoints);
    if (this._cursorTimer) return;
    this._cursorTimer = setTimeout(() => {
      this._cursorTimer = null;
      this._send(EVENT.cursor, () => this._cursorBatch(), 'cursor');
    }, this.cursorInterval);
  }

  /** Hide your cursor for everyone, e.g. when the pointer leaves the canvas. */
  hideCursor() {
    if (!this._joined()) return;
    clearTimeout(this._cursorTimer);
    this._cursorTimer = null;
    this._points = [];
    this._send(EVENT.cursor, () => {
      if (!this._cursorShown) return null;
      this._cursorShown = false;
      return { hide: true };
    }, 'cursor');
  }

  /** Others' visible cursors: [{id, member, x, y, data}]. */
  get cursors() {
    return [...this._cursors].map(([id, c]) => ({ id, member: this._member(id), x: c.x, y: c.y, data: c.data }));
  }

  _cursorBatch() {
    const pts = this._points;
    this._points = [];
    if (!pts.length) return null;
    this._cursorShown = true;
    const t0 = pts[0][2];
    const last = pts.at(-1)[3];
    const out = { p: pts.map(([x, y, t]) => [x, y, t - t0]) };
    if (last !== undefined) out.data = last;
    return out;
  }

  _onCursor(d, meta) {
    const id = meta?.user_id;
    if (id == null || id === this.myID || !this._members.has(id)) return;
    d = parse(d);
    this._clearCursorTimers(id);
    if (d.hide) {
      this._hideCursor(id);
      return;
    }
    const pts = Array.isArray(d.p) ? d.p.filter((p) => Array.isArray(p) && typeof p[0] === 'number' && typeof p[1] === 'number') : [];
    if (!pts.length) return;
    const timers = [];
    const show = ([x, y]) => {
      this._cursors.set(id, { x, y, data: d.data ?? null });
      this._emit('cursor', { id, member: this._member(id), x, y, data: d.data ?? null, hidden: false });
    };
    if (this.cursorReplay) {
      const base = pts[0][2] ?? 0;
      for (const p of pts) {
        const wait = Math.min(Math.max((p[2] ?? 0) - base, 0), this.cursorInterval * 2);
        if (wait === 0) show(p);
        else {
          const t = setTimeout(() => show(p), wait);
          timers.push(t);
        }
      }
    } else {
      show(pts.at(-1));
    }
    const hide = setTimeout(() => this._hideCursor(id), this.cursorTimeout);
    hide.unref?.();
    timers.push(hide);
    this._cursorTimers.set(id, timers);
  }

  _hideCursor(id) {
    this._clearCursorTimers(id);
    if (!this._cursors.delete(id)) return;
    this._emit('cursor', { id, member: this._member(id), x: null, y: null, data: null, hidden: true });
  }

  _clearCursorTimers(id) {
    for (const t of this._cursorTimers.get(id) ?? []) clearTimeout(t);
    this._cursorTimers.delete(id);
  }

  // ---- locks -------------------------------------------------------------

  /**
   * Lock a component (a form field, a cell, a shape) so others see it is
   * yours. Resolves with the lock once nobody contested it for lockGrace
   * ms; rejects with LockedError when someone else holds it or wins the
   * race. Locks are hints for the UI: the server does not enforce them, so
   * check edits in your backend if it matters.
   * @param {string} id
   * @param {object} [attrs] anything others should see, e.g. {label: 'Title'}
   */
  lock(id, attrs = null) {
    if (!this._joined()) return Promise.reject(new Error('Space: not entered yet'));
    id = String(id);
    const cur = this._locks.get(id);
    if (cur && cur.member !== this.myID) return Promise.reject(new LockedError(id, cur.member));
    if (cur) return Promise.resolve(this._lockView(id));
    const lock = { member: this.myID, at: Date.now(), attrs };
    this._setLock(id, lock);
    this._send(EVENT.lock, () => ({ id, at: lock.at, attrs }));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._waiting.delete(id);
        resolve(this._lockView(id));
      }, this.lockGrace);
      this._waiting.set(id, { resolve, reject, timer });
    });
  }

  /** Release a lock you hold. */
  unlock(id) {
    id = String(id);
    if (this._locks.get(id)?.member !== this.myID) return;
    this._settle(id, new Error(`Space: "${id}" was unlocked`));
    this._setLock(id, null);
    this._send(EVENT.unlock, () => ({ id }));
  }

  /** The lock on `id`: {id, member, attrs, mine}, or null. */
  lockOf(id) {
    return this._locks.has(String(id)) ? this._lockView(String(id)) : null;
  }

  /** All locks: [{id, member, attrs, mine}]. */
  get locks() {
    return [...this._locks.keys()].map((id) => this._lockView(id));
  }

  _lockView(id) {
    const l = this._locks.get(id);
    return l ? { id, member: this._member(l.member), attrs: l.attrs ?? null, mine: l.member === this.myID } : null;
  }

  // Every member orders competing requests for a lock the same way, by the
  // requester's clock and then user id, so they agree on the holder whatever
  // order the events arrive in.
  _offerLock(id, lock) {
    const cur = this._locks.get(id);
    if (cur && cur.member !== lock.member && !earlier(lock, cur)) return;
    if (cur && cur.member === this.myID && lock.member !== this.myID) {
      this._settle(id, new LockedError(id, lock.member));
    }
    this._setLock(id, lock);
  }

  _setLock(id, lock) {
    const prev = this._locks.has(id) ? this._lockView(id) : null;
    if (lock) this._locks.set(id, lock);
    else this._locks.delete(id);
    this._emit('lock', { id, lock: lock ? this._lockView(id) : null, previous: prev });
  }

  _settle(id, err) {
    const w = this._waiting.get(id);
    if (!w) return;
    clearTimeout(w.timer);
    this._waiting.delete(id);
    w.reject(err);
  }

  _onLock(d, meta) {
    const from = meta?.user_id;
    if (from == null || from === this.myID || !this._members.has(from)) return;
    d = parse(d);
    if (typeof d.id !== 'string' || typeof d.at !== 'number') return;
    this._offerLock(d.id, { member: from, at: d.at, attrs: d.attrs ?? null });
  }

  _onUnlock(d, meta) {
    const from = meta?.user_id;
    d = parse(d);
    if (from == null || typeof d.id !== 'string' || this._locks.get(d.id)?.member !== from) return;
    this._setLock(d.id, null);
  }

  // ---- presence ----------------------------------------------------------

  _snapshot(members) {
    // pusher-js passes its Members object: .members is the user_id -> info
    // hash and .myID our own id. A resubscription after a reconnect lands
    // here too: everyone else will greet us again.
    this._reset();
    this.myID = members.myID ?? null;
    for (const [id, info] of Object.entries(members.members ?? {})) {
      this._members.set(id, { info, profile: {}, location: null });
    }
    if (this.myID != null) {
      this._members.set(this.myID, { info: members.members?.[this.myID] ?? null, profile: this._profile, location: this._location });
    }
    this.ready = true;
    if (this._members.size > 1) this._send(EVENT.hello, () => this._hello(), 'hello');
    this._emit('ready', { members: this.members });
  }

  _reset() {
    for (const id of [...this._cursorTimers.keys()]) this._clearCursorTimers(id);
    for (const id of [...this._waiting.keys()]) this._settle(id, new Error('Space: resubscribed'));
    this._cursors.clear();
    this._locks.clear();
    this._members.clear();
    this._queue = [];
    clearTimeout(this._helloTimer);
    this._helloTimer = null;
  }

  _added(m) {
    this._members.set(m.id, { info: m.info ?? null, profile: {}, location: null });
    this._emit('members', { members: this.members, joined: this._member(m.id) });
    this._moved(m.id);
    // Greet newcomers after a random delay, so a burst of joins (or a whole
    // room greeting one newcomer) spreads out and is sent once.
    if (this._helloTimer) return;
    this._helloTimer = setTimeout(() => {
      this._helloTimer = null;
      if (this._joined()) this._send(EVENT.hello, () => this._hello(), 'hello');
    }, Math.random() * this.helloDelay);
  }

  _removed(m) {
    if (!this._members.has(m.id)) return;
    const gone = this._member(m.id);
    this._hideCursor(m.id);
    for (const [id, l] of [...this._locks]) if (l.member === m.id) this._setLock(id, null);
    this._members.delete(m.id);
    this._emit('members', { members: this.members, left: gone });
    this._moved(m.id);
  }

  _hello() {
    const locks = [];
    for (const [id, l] of this._locks) if (l.member === this.myID) locks.push({ id, at: l.at, attrs: l.attrs ?? null });
    return { profile: this._profile, location: this._location, locks };
  }

  _onHello(d, meta) {
    const from = meta?.user_id;
    if (from == null || from === this.myID || !this._members.has(from)) return;
    d = parse(d);
    const m = this._members.get(from);
    m.profile = isObject(d.profile) ? d.profile : {};
    const moved = JSON.stringify(m.location) !== JSON.stringify(d.location ?? null);
    m.location = d.location ?? null;
    for (const l of Array.isArray(d.locks) ? d.locks : []) {
      if (l && typeof l.id === 'string' && typeof l.at === 'number') this._offerLock(l.id, { member: from, at: l.at, attrs: l.attrs ?? null });
    }
    this._emit('members', { members: this.members, updated: this._member(from) });
    if (moved) this._moved(from);
  }

  _onProfile(d, meta) {
    const from = meta?.user_id;
    if (from == null || from === this.myID || !this._members.has(from)) return;
    d = parse(d);
    this._members.get(from).profile = isObject(d.profile) ? d.profile : {};
    this._emit('members', { members: this.members, updated: this._member(from) });
  }

  _onLocation(d, meta) {
    const from = meta?.user_id;
    if (from == null || from === this.myID || !this._members.has(from)) return;
    this._members.get(from).location = parse(d).location ?? null;
    this._emit('members', { members: this.members, updated: this._member(from) });
    this._moved(from);
  }

  // ---- sending -----------------------------------------------------------

  _joined() {
    return this.ready && !this._left && this.myID != null && this._members.has(this.myID);
  }

  // Queue a client event. build() makes the data when a token is free, so a
  // waiting cursor batch picks up later points and a waiting profile update
  // sends the latest profile; events with the same key are merged.
  _send(event, build, key) {
    if (key) {
      const queued = this._queue.find((q) => q.key === key);
      if (queued) {
        queued.event = event;
        queued.build = build;
        this._pump();
        return;
      }
    }
    this._queue.push({ event, build, key });
    this._pump();
  }

  _pump() {
    const now = Date.now();
    this._tokens = Math.min(this.maxRate, this._tokens + (Math.max(now - this._refilled, 0) / 1000) * this.maxRate);
    this._refilled = now;
    while (this._queue.length && this._tokens >= 1) {
      const { event, build } = this._queue.shift();
      const data = build();
      if (data == null) continue;
      this._tokens -= 1;
      if (!this.channel.trigger(event, data)) {
        this._emit('error', new Error(`Space: could not send ${event} (not connected?)`));
      }
    }
    if (this._queue.length && !this._pumpTimer) {
      this._pumpTimer = setTimeout(() => {
        this._pumpTimer = null;
        this._pump();
      }, Math.ceil(((1 - this._tokens) * 1000) / this.maxRate));
    }
  }

  /**
   * Stop tracking. Your locks are released and your cursor hidden for
   * everyone; when Space.enter created the subscription, it unsubscribes.
   */
  leave() {
    if (this._left) return;
    if (this._joined() && !this._unsubscribe) {
      // Still subscribed (Echo), so tell the others directly.
      for (const [id, l] of this._locks) if (l.member === this.myID) this.channel.trigger(EVENT.unlock, { id });
      if (this._cursorShown || this._cursorTimer) this.channel.trigger(EVENT.cursor, { hide: true });
    }
    this._left = true;
    for (const [ev, fn] of this._bindings) this.channel.unbind(ev, fn);
    for (const id of [...this._waiting.keys()]) this._settle(id, new Error('Space: left'));
    for (const id of [...this._cursorTimers.keys()]) this._clearCursorTimers(id);
    clearTimeout(this._cursorTimer);
    clearTimeout(this._pumpTimer);
    clearTimeout(this._helloTimer);
    this._queue = [];
    if (this._unsubscribe) this._unsubscribe();
    this._handlers.clear();
    this._follows.clear();
  }
}

function earlier(a, b) {
  if (a.at !== b.at) return a.at < b.at;
  return a.member < b.member;
}

function isObject(v) {
  return v != null && typeof v === 'object' && !Array.isArray(v);
}

function parse(d) {
  if (typeof d === 'string') {
    try {
      return JSON.parse(d) ?? {};
    } catch {
      return {};
    }
  }
  return d ?? {};
}

export default Space;
