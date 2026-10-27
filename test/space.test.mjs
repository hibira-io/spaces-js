import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Space, LockedError } from '../index.js';

// A stand-in for a pusher-js presence channel: bind/unbind, trigger (recorded)
// and a way to emit events with pusher-js's (data, metadata) arguments.
function fakeChannel(name = 'presence-space-1') {
  const handlers = new Map();
  return {
    name,
    sent: [],
    connected: true,
    bind(ev, fn) {
      if (!handlers.has(ev)) handlers.set(ev, new Set());
      handlers.get(ev).add(fn);
    },
    unbind(ev, fn) {
      handlers.get(ev)?.delete(fn);
    },
    trigger(ev, data) {
      if (!this.connected) return false;
      this.sent.push([ev, data]);
      return true;
    },
    emit(ev, data, meta) {
      for (const fn of handlers.get(ev) ?? []) fn(data, meta);
    },
    sentOf(ev) {
      return this.sent.filter(([e]) => e === ev).map(([, d]) => d);
    },
    handlers,
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function entered(members, myID, options = {}) {
  const ch = fakeChannel();
  const space = new Space(ch, { helloDelay: 0, lockGrace: 20, cursorInterval: 10, ...options });
  ch.emit('pusher:subscription_succeeded', { members, myID, count: Object.keys(members).length });
  return { ch, space };
}

test('members come from presence, you first, with profile and location', () => {
  const { ch, space } = entered({ c: { name: 'C' }, a: { name: 'A' }, b: { name: 'B' } }, 'b', { profile: { color: 'red' }, location: 'p1' });
  assert.deepEqual(space.members.map((m) => m.id), ['b', 'a', 'c']);
  assert.deepEqual(space.me, { id: 'b', info: { name: 'B' }, profile: { color: 'red' }, location: 'p1', me: true });
  assert.deepEqual(space.others.map((m) => m.id), ['a', 'c']);
  // Entering a space with others in it sends your hello.
  assert.deepEqual(ch.sentOf('client-spaces-hello'), [{ profile: { color: 'red' }, location: 'p1', locks: [] }]);
});

test('alone in the space, nothing is sent', () => {
  const { ch } = entered({ a: {} }, 'a');
  assert.deepEqual(ch.sent, []);
});

test('hello, profile and location from others update members', () => {
  const { ch, space } = entered({ a: {}, b: {} }, 'a');
  const updates = [];
  space.on('members', (e) => updates.push(e.updated?.id));
  ch.emit('client-spaces-hello', { profile: { color: 'blue' }, location: 'slide-2', locks: [] }, { user_id: 'b' });
  assert.deepEqual(space.member('b').profile, { color: 'blue' });
  assert.equal(space.member('b').location, 'slide-2');
  ch.emit('client-spaces-profile', JSON.stringify({ profile: { color: 'green' } }), { user_id: 'b' });
  ch.emit('client-spaces-location', { location: { slide: 3 } }, { user_id: 'b' });
  assert.deepEqual(space.member('b').profile, { color: 'green' });
  assert.deepEqual(space.membersAt({ slide: 3 }).map((m) => m.id), ['b']);
  // Strangers and unsigned events are ignored.
  ch.emit('client-spaces-location', { location: 'x' }, { user_id: 'stranger' });
  ch.emit('client-spaces-location', { location: 'x' }, {});
  assert.equal(space.member('stranger'), null);
  assert.deepEqual(updates, ['b', 'b', 'b']);
});

test('own profile and location are applied locally and sent', () => {
  const { ch, space } = entered({ a: {}, b: {} }, 'a');
  space.setProfile({ color: 'red' });
  space.setProfile({ status: 'away' });
  space.setLocation('cell-B2');
  assert.deepEqual(space.me.profile, { color: 'red', status: 'away' });
  assert.equal(space.me.location, 'cell-B2');
  assert.deepEqual(ch.sentOf('client-spaces-profile').at(-1), { profile: { color: 'red', status: 'away' } });
  assert.deepEqual(ch.sentOf('client-spaces-location'), [{ location: 'cell-B2' }]);
});

test('a newcomer is greeted with your profile, location and locks', async () => {
  const { ch, space } = entered({ a: {}, b: {} }, 'a', { location: 'p1' });
  await space.lock('title', { label: 'Title' });
  ch.sent.length = 0;
  ch.emit('pusher:member_added', { id: 'c', info: { name: 'C' } });
  await sleep(5);
  const [hello] = ch.sentOf('client-spaces-hello');
  assert.equal(hello.location, 'p1');
  assert.deepEqual(hello.locks.map((l) => [l.id, l.attrs]), [['title', { label: 'Title' }]]);
  // A newcomer learns the lock from the hello.
  const n = entered({ a: {}, c: {} }, 'c');
  n.ch.emit('client-spaces-hello', hello, { user_id: 'a' });
  assert.equal(n.space.lockOf('title').member.id, 'a');
  assert.equal(n.space.lockOf('title').mine, false);
});

test('follow reports a member\'s location until they leave', () => {
  const { ch, space } = entered({ a: {}, b: {} }, 'a');
  const seen = [];
  const stop = space.follow('b', (loc, m) => seen.push([loc, m?.id ?? null]));
  ch.emit('client-spaces-location', { location: 'p2' }, { user_id: 'b' });
  ch.emit('client-spaces-location', { location: 'p3' }, { user_id: 'b' });
  ch.emit('pusher:member_removed', { id: 'b' });
  assert.deepEqual(seen, [[null, 'b'], ['p2', 'b'], ['p3', 'b'], [null, null]]);
  stop();
});

test('cursor moves are batched and replayed at their pace', async () => {
  const { ch, space } = entered({ a: {}, b: {} }, 'a', { cursorInterval: 30 });
  space.moveCursor(0.1, 0.1);
  space.moveCursor(0.2, 0.2);
  space.moveCursor(0.3, 0.3, { tool: 'pen' });
  assert.equal(ch.sentOf('client-spaces-cursor').length, 0);
  await sleep(50);
  const batches = ch.sentOf('client-spaces-cursor');
  assert.equal(batches.length, 1);
  assert.deepEqual(batches[0].p.map(([x, y]) => [x, y]), [[0.1, 0.1], [0.2, 0.2], [0.3, 0.3]]);
  assert.equal(batches[0].p[0][2], 0);
  assert.deepEqual(batches[0].data, { tool: 'pen' });

  // The receiving side shows every point, the last one last.
  const r = entered({ a: {}, b: {} }, 'b');
  const shown = [];
  r.space.on('cursor', (c) => shown.push([c.id, c.x, c.hidden]));
  r.ch.emit('client-spaces-cursor', { p: [[1, 1, 0], [2, 2, 10], [3, 3, 20]], data: 'x' }, { user_id: 'a' });
  assert.deepEqual(shown, [['a', 1, false]]);
  await sleep(40);
  assert.deepEqual(shown.map((s) => s[1]), [1, 2, 3]);
  assert.deepEqual(r.space.cursors, [{ id: 'a', member: r.space.member('a'), x: 3, y: 3, data: 'x' }]);
  r.ch.emit('client-spaces-cursor', { hide: true }, { user_id: 'a' });
  assert.deepEqual(shown.at(-1), ['a', null, true]);
  assert.deepEqual(r.space.cursors, []);
});

test('cursors hide after cursorTimeout and when the member leaves', async () => {
  const { ch, space } = entered({ a: {}, b: {}, c: {} }, 'a', { cursorTimeout: 20, cursorReplay: false });
  const hidden = [];
  space.on('cursor', (c) => c.hidden && hidden.push(c.id));
  ch.emit('client-spaces-cursor', { p: [[1, 1, 0], [5, 5, 10]] }, { user_id: 'b' });
  assert.equal(space.cursors[0].x, 5);
  ch.emit('client-spaces-cursor', { p: [[1, 1, 0]] }, { user_id: 'c' });
  ch.emit('pusher:member_removed', { id: 'c' });
  assert.deepEqual(hidden, ['c']);
  await sleep(40);
  assert.deepEqual(hidden, ['c', 'b']);
});

test('hideCursor sends a hide only after the cursor was shown', async () => {
  const { ch, space } = entered({ a: {}, b: {} }, 'a');
  space.hideCursor();
  await sleep(5);
  assert.equal(ch.sentOf('client-spaces-cursor').length, 0);
  space.moveCursor(1, 1);
  await sleep(20);
  space.hideCursor();
  await sleep(5);
  assert.deepEqual(ch.sentOf('client-spaces-cursor').at(-1), { hide: true });
});

test('lock resolves after the grace period and others see it', async () => {
  const { ch, space } = entered({ a: {}, b: {} }, 'a');
  const events = [];
  space.on('lock', (e) => events.push([e.id, e.lock?.member.id ?? null]));
  const lock = await space.lock('field-1', { label: 'Name' });
  assert.deepEqual([lock.id, lock.mine, lock.attrs], ['field-1', true, { label: 'Name' }]);
  const [sent] = ch.sentOf('client-spaces-lock');
  assert.equal(sent.id, 'field-1');
  // Locking again is a no-op; someone else trying it is ignored.
  assert.equal((await space.lock('field-1')).mine, true);
  ch.emit('client-spaces-lock', { id: 'field-1', at: sent.at + 50 }, { user_id: 'b' });
  assert.equal(space.lockOf('field-1').mine, true);
  space.unlock('field-1');
  assert.equal(space.lockOf('field-1'), null);
  assert.deepEqual(ch.sentOf('client-spaces-unlock'), [{ id: 'field-1' }]);
  assert.deepEqual(events, [['field-1', 'a'], ['field-1', null]]);
});

test('lock held by someone else rejects', async () => {
  const { ch, space } = entered({ a: {}, b: {} }, 'a');
  ch.emit('client-spaces-lock', { id: 'f', at: 1 }, { user_id: 'b' });
  await assert.rejects(space.lock('f'), (err) => err instanceof LockedError && err.holder === 'b');
  // Only the holder can unlock; leaving frees its locks.
  ch.emit('client-spaces-unlock', { id: 'f' }, { user_id: 'a' });
  assert.equal(space.lockOf('f').member.id, 'b');
  ch.emit('pusher:member_removed', { id: 'b' });
  assert.equal(space.lockOf('f'), null);
});

test('a race is won by the earlier request, then the lower user id, on every side', async () => {
  // b asks at t=100 and a at t=100: a wins (lower id), whatever arrives first.
  const A = entered({ a: {}, b: {} }, 'a');
  const B = entered({ a: {}, b: {} }, 'b');
  const now = Date.now;
  Date.now = () => 100;
  let pa, pb;
  try {
    pa = A.space.lock('x');
    pb = B.space.lock('x');
  } finally {
    Date.now = now;
  }
  const lost = assert.rejects(pb, LockedError);
  const [la] = A.ch.sentOf('client-spaces-lock');
  const [lb] = B.ch.sentOf('client-spaces-lock');
  A.ch.emit('client-spaces-lock', lb, { user_id: 'b' });
  B.ch.emit('client-spaces-lock', la, { user_id: 'a' });
  assert.equal((await pa).mine, true);
  await lost;
  assert.equal(A.space.lockOf('x').member.id, 'a');
  assert.equal(B.space.lockOf('x').member.id, 'a');

  // An earlier request from b takes the lock from a, even after a resolved.
  const taken = [];
  A.space.on('lock', (e) => e.previous?.mine && taken.push(e.lock.member.id));
  A.ch.emit('client-spaces-lock', { id: 'x', at: 50 }, { user_id: 'b' });
  assert.deepEqual(taken, ['b']);
});

test('sending stays under maxRate, merging waiting updates', async () => {
  const { ch, space } = entered({ a: {}, b: {} }, 'a', { maxRate: 4 });
  ch.sent.length = 0;
  // The hello used one token: three left.
  for (let i = 0; i < 10; i++) space.setLocation(i);
  space.lock('l1');
  space.lock('l2');
  assert.equal(ch.sent.length, 3);
  await sleep(900);
  const locs = ch.sentOf('client-spaces-location').map((d) => d.location);
  assert.deepEqual(locs, [0, 1, 2, 9]);
  assert.deepEqual(ch.sentOf('client-spaces-lock').map((d) => d.id), ['l1', 'l2']);
  space.leave();
});

test('a resubscription starts over and drops your locks', async () => {
  const { ch, space } = entered({ a: {}, b: {} }, 'a');
  await space.lock('f');
  ch.emit('client-spaces-lock', { id: 'g', at: 1 }, { user_id: 'b' });
  ch.emit('pusher:subscription_succeeded', { members: { a: {}, b: {} }, myID: 'a', count: 2 });
  assert.deepEqual(space.locks, []);
  assert.equal(space.members.length, 2);
});

test('leave stops tracking, releases locks and unsubscribes when it subscribed', async () => {
  const ch = fakeChannel('presence-space-2');
  let unsubscribed = null;
  const pusher = { subscribe: () => ch, unsubscribe: (n) => (unsubscribed = n) };
  const space = Space.enter(pusher, 'presence-space-2', { helloDelay: 0, lockGrace: 5 });
  ch.emit('pusher:subscription_succeeded', { members: { a: {} }, myID: 'a', count: 1 });
  await space.lock('f');
  space.leave();
  assert.equal(unsubscribed, 'presence-space-2');
  assert.equal([...ch.handlers.values()].reduce((n, s) => n + s.size, 0), 0);

  // With a channel you passed in (Echo), leave tells the others.
  const e = entered({ a: {}, b: {} }, 'a');
  await e.space.lock('f');
  e.space.moveCursor(1, 1);
  e.space.leave();
  assert.deepEqual(e.ch.sentOf('client-spaces-unlock'), [{ id: 'f' }]);
  assert.deepEqual(e.ch.sentOf('client-spaces-cursor'), [{ hide: true }]);
});

test('enter refuses non-presence and presence-large channels', () => {
  const pusher = { subscribe: () => fakeChannel() };
  assert.throws(() => Space.enter(pusher, 'private-doc'), /not a presence channel/);
  assert.throws(() => Space.enter(pusher, 'presence-large-doc'), /not a presence channel/);
});

test('a channel that is already subscribed is picked up', () => {
  const ch = fakeChannel();
  ch.subscribed = true;
  ch.members = { members: { a: { name: 'A' } }, myID: 'a', count: 1 };
  const space = new Space(ch);
  assert.equal(space.ready, true);
  assert.equal(space.me.info.name, 'A');
});

test('actions before entering do nothing', async () => {
  const space = new Space(fakeChannel());
  space.setProfile({ color: 'red' });
  space.moveCursor(1, 1);
  await assert.rejects(space.lock('f'), /not entered/);
  assert.equal(space.me, null);
});
