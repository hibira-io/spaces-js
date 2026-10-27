import type Pusher from 'pusher-js';

export interface Member<Info = any, Profile = Record<string, any>, Location = any> {
  id: string;
  /** user_info signed by your auth endpoint. */
  info: Info | null;
  /** Set by the member with setProfile(). */
  profile: Profile;
  /** Set by the member with setLocation(). */
  location: Location | null;
  me: boolean;
}

export interface MembersEvent<Info = any, Profile = Record<string, any>, Location = any> {
  members: Member<Info, Profile, Location>[];
  joined?: Member<Info, Profile, Location>;
  left?: Member<Info, Profile, Location>;
  updated?: Member<Info, Profile, Location>;
}

export interface Cursor<Info = any, Profile = Record<string, any>, Location = any> {
  id: string;
  member: Member<Info, Profile, Location>;
  x: number;
  y: number;
  data: any;
}

export interface CursorEvent<Info = any, Profile = Record<string, any>, Location = any> {
  id: string;
  member: Member<Info, Profile, Location>;
  /** null when hidden. */
  x: number | null;
  y: number | null;
  data: any;
  hidden: boolean;
}

export interface Lock<Info = any, Profile = Record<string, any>, Location = any> {
  id: string;
  member: Member<Info, Profile, Location>;
  attrs: any;
  mine: boolean;
}

export interface LockEvent<Info = any, Profile = Record<string, any>, Location = any> {
  id: string;
  /** null when released. */
  lock: Lock<Info, Profile, Location> | null;
  previous: Lock<Info, Profile, Location> | null;
}

export interface SpaceEvents<Info = any, Profile = Record<string, any>, Location = any> {
  ready: { members: Member<Info, Profile, Location>[] };
  members: MembersEvent<Info, Profile, Location>;
  cursor: CursorEvent<Info, Profile, Location>;
  lock: LockEvent<Info, Profile, Location>;
  error: Error;
}

export interface SpaceOptions<Profile = Record<string, any>, Location = any> {
  /** Your profile (color, status...), merged with setProfile(). */
  profile?: Profile;
  /** Where you are (a page, a slide, a field...). */
  location?: Location;
  /** ms between cursor batches you send. Default 200. */
  cursorInterval?: number;
  /** ms after which a still cursor is hidden. Default 10000. */
  cursorTimeout?: number;
  /** Replay others' batched points at their original pace. Default true. */
  cursorReplay?: boolean;
  /** Points kept per batch, the latest ones. Default 20. */
  maxCursorPoints?: number;
  /** ms lock() waits for a competing request before it resolves. Default 300. */
  lockGrace?: number;
  /** Up to this many ms before greeting a new member. Default 300. */
  helloDelay?: number;
  /** Client events per second this space sends at most. Default 8. */
  maxRate?: number;
}

export class LockedError extends Error {
  readonly id: string;
  /** User id of the member holding the lock. */
  readonly holder: string;
}

export class Space<Info = any, Profile extends Record<string, any> = Record<string, any>, Location = any> {
  static enter<Info = any, Profile extends Record<string, any> = Record<string, any>, Location = any>(
    pusher: Pusher,
    channelName: string,
    options?: SpaceOptions<Profile, Location>,
  ): Space<Info, Profile, Location>;
  constructor(channel: any, options?: SpaceOptions<Profile, Location>);

  readonly channel: any;
  readonly name: string;
  myID: string | null;
  ready: boolean;

  readonly members: Member<Info, Profile, Location>[];
  readonly others: Member<Info, Profile, Location>[];
  readonly me: Member<Info, Profile, Location> | null;
  member(id: string): Member<Info, Profile, Location> | null;
  membersAt(location: Location): Member<Info, Profile, Location>[];

  setProfile(patch: Partial<Profile>): void;
  setLocation(location: Location | null): void;
  follow(id: string, fn: (location: Location | null, member: Member<Info, Profile, Location> | null) => void): () => void;

  moveCursor(x: number, y: number, data?: any): void;
  hideCursor(): void;
  readonly cursors: Cursor<Info, Profile, Location>[];

  lock(id: string, attrs?: any): Promise<Lock<Info, Profile, Location>>;
  unlock(id: string): void;
  lockOf(id: string): Lock<Info, Profile, Location> | null;
  readonly locks: Lock<Info, Profile, Location>[];

  on<K extends keyof SpaceEvents<Info, Profile, Location>>(event: K, fn: (e: SpaceEvents<Info, Profile, Location>[K]) => void): () => void;
  off<K extends keyof SpaceEvents<Info, Profile, Location>>(event: K, fn: (e: SpaceEvents<Info, Profile, Location>[K]) => void): void;
  leave(): void;
}

export default Space;
