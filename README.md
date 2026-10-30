# @hibira-io/spaces

[![npm](https://img.shields.io/npm/v/@hibira-io/spaces)](https://www.npmjs.com/package/@hibira-io/spaces)
[![ci](https://github.com/hibira-io/spaces-js/actions/workflows/ci.yml/badge.svg)](https://github.com/hibira-io/spaces-js/actions/workflows/ci.yml)

Multiplayer collaboration for [pusher-js](https://github.com/pusher/pusher-js)
and Laravel Echo on [Hibira](https://hibira.io): who's online (avatar stacks),
where everyone is (page, slide, field), live cursors, component locks so two
people don't edit the same field, and follow mode.

A space is a presence channel. The member list comes from the channel, and
everything else is a client event (`client-spaces-*`), so it works on any
Pusher-compatible server with client events turned on, with no extra server of
yours beyond the auth endpoint you already have.

Good fits: shared forms and back-office records, whiteboards and design tools,
slide decks, spreadsheets and tables, dashboards people look at together. It
does not merge document contents: pair it with your own saves, or with a CRDT
library such as Yjs for text.

## Install

```sh
npm install @hibira-io/spaces pusher-js
```

Turn on client events for the app in the Hibira portal (App → Settings).

## Usage

```js
import Pusher from 'pusher-js';
import { Space, LockedError } from '@hibira-io/spaces';

const pusher = new Pusher(key, {
  cluster: 'hkg', // required by pusher-js, unused once wsHost is set
  wsHost: 'ws.hibira.io',
  forceTLS: true,
  enabledTransports: ['ws', 'wss'],
  channelAuthorization: { endpoint: '/spaces/auth' },
});

const space = Space.enter(pusher, 'presence-space-doc-42', {
  profile: { color: '#e5484d' },
  location: { page: 'overview' },
});
```

### Who's here

```js
space.on('ready', ({ members }) => renderAvatars(members));
space.on('members', ({ members, joined, left }) => renderAvatars(members));

space.members;              // [{ id, info, profile, location, me }], you first
space.others;               // everyone but you
space.membersAt({ page: 'pricing' }); // who is on that page

space.setProfile({ status: 'away' });
space.setLocation({ page: 'pricing' });
```

`info` is the `user_info` your auth endpoint signs (name, avatar URL). `profile`
is what a member sets from the browser (color, status) and `location` is where
they are; both reach a newcomer within `helloDelay` ms of joining.

### Live cursors

```js
canvas.onpointermove = (e) => {
  const r = canvas.getBoundingClientRect();
  space.moveCursor((e.clientX - r.left) / r.width, (e.clientY - r.top) / r.height);
};
canvas.onpointerleave = () => space.hideCursor();

space.on('cursor', ({ id, member, x, y, hidden }) => {
  if (hidden) removeCursor(id);
  else drawCursor(id, member.info.name, member.profile.color, x, y);
});
```

Points are sent in one event every `cursorInterval` ms (200 by default) and
replayed on the other side at their original pace, so motion looks smooth at
five events a second. Use coordinates every member can map, such as fractions
of a shared canvas. A cursor that stays still for `cursorTimeout` ms (10 s)
is hidden.

### Component locks

```js
input.onfocus = async () => {
  try {
    await space.lock('invoice-7.amount', { label: 'Amount' });
  } catch (err) {
    if (err instanceof LockedError) {
      input.blur();
      toast(`${space.member(err.holder)?.info.name} is editing this`);
    }
  }
};
input.onblur = () => space.unlock('invoice-7.amount');

space.on('lock', ({ id, lock }) => markField(id, lock?.member)); // lock null = free
space.lockOf('invoice-7.amount'); // { id, member, attrs, mine } or null
```

`lock()` resolves once nobody contested it for `lockGrace` ms (300). When two
members ask at the same moment, every client picks the same winner: the
earlier request by the requester's clock, then the lower user id. A member's
locks go away when they leave or disconnect.

Locks are for the UI. The server does not enforce them, so if a wrong edit
would do harm, check in your backend too.

### Follow

```js
const stop = space.follow(presenterId, (location) => {
  if (location) goToSlide(location.slide);
});
```

The callback runs right away with the member's current location, then on every
change, and once with `null` when they leave.

### Leaving

```js
space.leave();
```

### Laravel Echo

```js
const space = new Space(Echo.join('doc.42').subscription, { profile: { color } });
```

## Auth endpoint

A space is a presence channel, so your auth endpoint signs the user's id and
public info. With the Node [`pusher`](https://www.npmjs.com/package/pusher)
package:

```js
app.post('/spaces/auth', (req, res) => {
  const { socket_id, channel_name } = req.body;
  if (!canOpen(req.user, channel_name)) return res.status(403).end();
  res.json(hibira.authorizeChannel(socket_id, channel_name, {
    user_id: String(req.user.id),
    user_info: { name: req.user.name, avatar: req.user.avatarUrl },
  }));
});
```

Laravel:

```php
// routes/channels.php
Broadcast::channel('doc.{id}', function ($user, $id) {
    return $user->can('view', Document::find($id))
        ? ['id' => $user->id, 'name' => $user->name, 'avatar' => $user->avatar_url]
        : false;
});
```

The same user in two tabs is one member (presence channels count users), so
their two cursors would share one id. Give each tab its own `user_id` (user id
plus a tab id) if that matters to you.

## Limits

| | |
|---|---|
| Members per space | presence channel limit (100 on Hibira) |
| Client events | 10 per second per connection on the server; the space sends at most `maxRate` (8) and merges waiting profile, location and cursor updates |
| Event size | 4 KB on the Free plan, 10 KB on Hobby and Starter, more on higher plans ([pricing](https://hibira.io/pricing)) |
| Billing | each client event counts as one message, however many members receive it |

A member moving the pointer all the time sends 5 events a second, about 18,000
an hour. Raise `cursorInterval` or only send cursors while the pointer is over
the shared area to use fewer messages.

## Events on the wire

All events are client events on the space's presence channel, so other SDKs
(Android, iOS, Unity) can read or send them too.

| Event | Data |
|---|---|
| `client-spaces-hello` | `{profile, location, locks: [{id, at, attrs}]}`, sent on entering and when someone joins |
| `client-spaces-profile` | `{profile}` |
| `client-spaces-location` | `{location}` |
| `client-spaces-cursor` | `{p: [[x, y, ms since first point], ...], data?}` or `{hide: true}` |
| `client-spaces-lock` | `{id, at, attrs}` (`at` = request time, ms since epoch) |
| `client-spaces-unlock` | `{id}` |

## Development

```sh
npm test         # unit tests
npm run test:e2e # against a running server, see test/e2e/spaces.e2e.mjs
```

## License

MIT
