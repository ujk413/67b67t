// Persistent, origin-scoped storage for E2E keys.
//
// Keys live in IndexedDB, NOT localStorage. localStorage is synchronous, string
// only, and readable by any script that achieves XSS — a single injected snippet
// could exfiltrate the whole key backup. IndexedDB at least raises the bar and is
// the storage Signal Desktop itself uses for its ratchet/key records.
//
// This module is deliberately storage-only: no crypto, no network.

const DB_NAME = 'e2e-keys';
const DB_VERSION = 1;
const STORE = 'records';

let _dbPromise = null;

function openDb() {
  if (_dbPromise) return _dbPromise;
  _dbPromise = new Promise((resolve, reject) => {
    if (typeof indexedDB === 'undefined') {
      reject(new Error('IndexedDB unavailable'));
      return;
    }
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE)) {
        db.createObjectStore(STORE, { keyPath: 'id' });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
  return _dbPromise;
}

function tx(db, mode) {
  return db.transaction(STORE, mode).objectStore(STORE);
}

function reqToPromise(req) {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

// --- Generic key/value (record id = string) ---

export async function idbGet(id) {
  const db = await openDb();
  const row = await reqToPromise(tx(db, 'readonly').get(id));
  return row ? row.value : undefined;
}

export async function idbSet(id, value) {
  const db = await openDb();
  await reqToPromise(tx(db, 'readwrite').put({ id, value }));
}

export async function idbRemove(id) {
  const db = await openDb();
  await reqToPromise(tx(db, 'readwrite').delete(id));
}

export async function idbClearAll() {
  const db = await openDb();
  await reqToPromise(tx(db, 'readwrite').clear());
}

// --- Bulk record helpers used by the SignalProtocolStore ---
//
// libsignal addresses records by opaque string keys (identityKey<id>,
// preKey<id>, session<id>…). We namespace everything under a single prefix so
// wiping E2E state never touches unrelated app data.

const NS = 'sig:';

export async function recordGet(key, defaultValue) {
  const v = await idbGet(NS + key);
  return v === undefined ? defaultValue : v;
}

export async function recordPut(key, value) {
  await idbSet(NS + key, value);
}

export async function recordRemove(key) {
  await idbRemove(NS + key);
}

export async function recordKeys() {
  const db = await openDb();
  const keys = await reqToPromise(tx(db, 'readonly').getAllKeys());
  return keys.filter((k) => typeof k === 'string' && k.startsWith(NS)).map((k) => k.slice(NS.length));
}

// --- Whole-device backup (for the recovery-phrase flow) ---

export async function exportAll() {
  const db = await openDb();
  const rows = await reqToPromise(tx(db, 'readonly').getAll());
  const out = {};
  for (const row of rows) out[row.id] = row.value;
  return out;
}

export async function importAll(map) {
  const db = await openDb();
  const store = tx(db, 'readwrite');
  for (const [id, value] of Object.entries(map || {})) {
    store.put({ id, value });
  }
  await new Promise((resolve, reject) => {
    store.transaction.oncomplete = () => resolve();
    store.transaction.onerror = () => reject(store.transaction.error);
  });
}

export const SigStore = {
  get: recordGet,
  put: recordPut,
  remove: recordRemove,
  keys: recordKeys,
};
