export type PlaybackOwner = { claim(): Promise<boolean>; isOwner(): boolean; release(): void; dispose(): void };
const owners = new Map<symbol, () => void | Promise<unknown>>();
const callInterruptions = new Set<() => void | Promise<unknown>>();
let active: symbol | null = null, blocked = false, revision = 0;
async function pause(fn: () => void | Promise<unknown>) { try { await fn(); } catch {} }
export function registerPlayback(stop: () => void | Promise<unknown>): PlaybackOwner {
  const id = Symbol('media'); let disposed = false; owners.set(id, stop);
  return {
    async claim() {
      if (disposed || blocked) { await pause(stop); return false; }
      if (active === id) return true;
      const previous = active, request = ++revision; active = id;
      if (previous && owners.has(previous)) await pause(owners.get(previous)!);
      return !disposed && !blocked && active === id && revision === request;
    },
    isOwner: () => !disposed && !blocked && active === id,
    release() { if (active === id) { active = null; revision++; } },
    dispose() { disposed = true; owners.delete(id); if (active === id) { active = null; revision++; } void pause(stop); },
  };
}
export async function pauseAllMedia() { active = null; revision++; await Promise.all([...owners.values()].map(pause)); }
export function onMediaCallStarted(stop: () => void | Promise<unknown>) { callInterruptions.add(stop); return () => { callInterruptions.delete(stop); }; }
export async function setMediaCallActive(value: boolean) { blocked = value; if (value) await Promise.all([pauseAllMedia(), ...[...callInterruptions].map(pause)]); }
type NativeVideo = { pause(): void; addListener(name: 'playingChange', listener: (event: { isPlaying: boolean }) => void): { remove(): void } };
// Native controls and PiP can start playback without invoking a React handler.
export function ownVideoPlayer(player: NativeVideo) {
  const owner = registerPlayback(() => player.pause());
  const sub = player.addListener('playingChange', event => {
    if (event.isPlaying) void owner.claim().then(allowed => { if (!allowed) player.pause(); });
    else owner.release();
  });
  return { ...owner, dispose() { sub.remove(); owner.dispose(); } };
}
