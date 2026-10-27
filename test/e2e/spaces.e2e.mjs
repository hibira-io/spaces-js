// End-to-end test against a running Hibira (or any Pusher-compatible) server
// with client events enabled. Skipped unless SPACES_E2E_KEY is set:
//
//   SPACES_E2E_HOST=127.0.0.1 SPACES_E2E_PORT=8080 SPACES_E2E_TLS=0 \
//   SPACES_E2E_APP_ID=spaces SPACES_E2E_KEY=... SPACES_E2E_SECRET=... npm run test:e2e
import { test } from 'node:test';
import assert from 'node:assert/strict';
import PusherClient from 'pusher-js';
import PusherServer from 'pusher';
import { Space, LockedError } from '../../index.js';

const env = process.env;
const skip = !env.SPACES_E2E_KEY && 'set SPACES_E2E_KEY to run';
const host = env.SPACES_E2E_HOST ?? '127.0.0.1';
const port = Number(env.SPACES_E2E_PORT ?? 8080);
const tls = env.SPACES_E2E_TLS === '1';

const server = skip
  ? null
  : new PusherServer({ appId: env.SPACES_E2E_APP_ID, key: env.SPACES_E2E_KEY, secret: env.SPACES_E2E_SECRET, host, port: String(port), useTLS: tls });

function client(userId, name) {
  return new PusherClient(env.SPACES_E2E_KEY, {
    cluster: 'mt1',
    wsHost: host,
    wsPort: port,
    wssPort: port,
    forceTLS: tls,
    enabledTransports: ['ws'],
    channelAuthorization: {
      customHandler: ({ socketId, channelName }, cb) => {
        try {
          cb(null, server.authorizeChannel(socketId, channelName, { user_id: userId, user_info: { name } }));
        } catch (err) {
          cb(err, null);
        }
      },
    },
  });
}

const once = (space, ev, pred = () => true) =>
  new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`timeout waiting for ${ev}`)), 5000);
    const off = space.on(ev, (e) => {
      if (!pred(e)) return;
      clearTimeout(t);
      off();
      resolve(e);
    });
  });

test('space round trip on a real server', { skip }, async () => {
  const name = `presence-space-e2e${Date.now()}`;
  const conns = [];
  try {
    const pa = client('alice', 'Alice');
    conns.push(pa);
    const a = Space.enter(pa, name, { profile: { color: 'red' }, location: 'slide-1', helloDelay: 50 });
    await once(a, 'ready');
    const titleLock = await a.lock('title', { label: 'Title' });
    assert.equal(titleLock.mine, true);

    const bobSeen = once(a, 'members', (e) => e.updated?.id === 'bob' && e.updated.profile.color === 'blue');
    // Bob joins and learns Alice's profile, location and lock from her hello.
    const pb = client('bob', 'Bob');
    conns.push(pb);
    const b = Space.enter(pb, name, { profile: { color: 'blue' }, helloDelay: 50 });
    const aliceSeen = once(b, 'members', (e) => e.updated?.id === 'alice');
    await once(b, 'ready');
    await aliceSeen;
    const alice = b.member('alice');
    assert.deepEqual([alice.info.name, alice.profile.color, alice.location], ['Alice', 'red', 'slide-1']);
    assert.equal(b.lockOf('title').member.id, 'alice');
    await assert.rejects(b.lock('title'), LockedError);
    // ...and Alice gets Bob's.
    await bobSeen;

    // Follow: Bob follows Alice to the next slide.
    const followed = new Promise((resolve) => {
      const stop = b.follow('alice', (loc) => loc === 'slide-2' && (stop(), resolve(loc)));
    });
    a.setLocation('slide-2');
    assert.equal(await followed, 'slide-2');

    // Cursors arrive batched and in order.
    const xs = [];
    const done = once(b, 'cursor', (c) => c.x === 0.5 && !c.hidden);
    b.on('cursor', (c) => c.hidden || xs.push(c.x));
    for (const x of [0.1, 0.2, 0.3, 0.4, 0.5]) a.moveCursor(x, 0.5, { tool: 'pen' });
    const last = await done;
    assert.deepEqual(last.data, { tool: 'pen' });
    assert.deepEqual(xs, [0.1, 0.2, 0.3, 0.4, 0.5]);
    const hidden = once(b, 'cursor', (c) => c.hidden);
    a.hideCursor();
    await hidden;

    // Alice unlocks; Bob takes the lock and Alice sees it.
    const seen = once(a, 'lock', (e) => e.lock?.member.id === 'bob');
    a.unlock('title');
    await once(b, 'lock', (e) => e.id === 'title' && e.lock === null);
    await b.lock('title');
    await seen;

    // Bob leaves: his lock goes with him.
    const freed = once(a, 'lock', (e) => e.id === 'title' && e.lock === null);
    b.leave();
    await freed;
    assert.deepEqual(a.others, []);
    a.leave();
  } finally {
    for (const c of conns) c.disconnect();
  }
});
