// Shared cancellation for downloads, native decrypts and file actions.
type State = { generation: number; erased: boolean; tasks: Set<Promise<unknown>>; cancels: Map<() => unknown, number> };
const accounts = new Map<string, State>();
function stateFor(userId: string) {
  let state = accounts.get(userId);
  if (!state) { state = { generation: 0, erased: false, tasks: new Set(), cancels: new Map() }; accounts.set(userId, state); }
  return state;
}
export type MediaTask = { check(): void; wait<T>(promise: Promise<T>): Promise<T>; onCancel(fn: () => unknown): () => void };
export function registerMediaCancellation(userId: string, stop: () => unknown) {
  const state=stateFor(userId);state.cancels.set(stop,state.generation);
  if(state.erased)void Promise.resolve().then(stop);
  return()=>{state.cancels.delete(stop);};
}
export function runMediaTask<T>(userId: string, work: (task: MediaTask) => Promise<T>): Promise<T> {
  const state = stateFor(userId), generation = state.generation;
  const check = () => { if (state.erased || state.generation !== generation) throw Error('Работа с вложением отменена: сеанс завершён или аккаунт удалён'); };
  const owned = new Set<() => unknown>();
  const onCancel = (fn: () => unknown) => { owned.add(fn); state.cancels.set(fn, generation); return () => { owned.delete(fn); state.cancels.delete(fn); }; };
  const ticket: MediaTask = { check, onCancel, wait: promise => new Promise((resolve, reject) => {
    const unsubscribe = onCancel(() => reject(Error('Загрузка вложения отменена')));
    promise.then(value => { unsubscribe(); try { check(); resolve(value); } catch (error) { reject(error); } }, error => { unsubscribe(); reject(error); });
  }) };
  const promise = Promise.resolve().then(() => { check(); return work(ticket); });
  state.tasks.add(promise);
  const done = () => { state.tasks.delete(promise); for (const fn of owned) state.cancels.delete(fn); };
  void promise.then(done, done);
  return promise;
}
export async function cancelAccountMedia(userId: string, erase = false) {
  const state = stateFor(userId), generation = state.generation++;
  if (erase) state.erased = true;
  const tasks = [...state.tasks], callbacks = [...state.cancels].filter(([, owner]) => owner <= generation).map(([fn]) => fn);
  await Promise.allSettled(callbacks.map(fn => Promise.resolve().then(fn)));
  await Promise.allSettled(tasks);
}
