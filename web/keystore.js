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
const erasedAccounts=new Set();
function erasedRecord(id){for(const user of erasedAccounts)if(id===NS+'backup:'+user||id===NS+'restore:'+user||id.startsWith(NS+'signal:'+user+':')||id.startsWith(NS+'gk:'+user+':'))return true;return false;}
function assertRecord(id){if(erasedRecord(id))throw Error('Аккаунт удалён с этого устройства');}
function recordOwner(id){const match=/^sig:(?:signal:([^:]+):|gk:([^:]+):|(?:backup|restore):([^:]+)$)/.exec(id);return match?.[1]||match?.[2]||match?.[3]||null;}
async function guardedRecord(mode,id,action){
 assertRecord(id);const db=await openDb(),user=recordOwner(id);
 return new Promise((resolve,reject)=>{
  const transaction=db.transaction(STORE,mode),records=transaction.objectStore(STORE);let value,error;
  const perform=()=>{const request=action(records);if(request)request.onsuccess=()=>{value=request.result;};};
  if(user){const check=records.get('sig:erased:'+user);check.onsuccess=()=>{if(check.result?.value){erasedAccounts.add(user);error=Error('Аккаунт удалён с этого устройства');transaction.abort();}else perform();};}else perform();
  transaction.oncomplete=()=>resolve(value);transaction.onerror=()=>reject(error||transaction.error);transaction.onabort=()=>reject(error||transaction.error||Error('Операция отменена'));
 });
}

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
  const row=await guardedRecord('readonly',id,store=>store.get(id));
  assertRecord(id);
  return row ? row.value : undefined;
}

export async function idbSet(id, value) {
  await guardedRecord('readwrite',id,store=>store.put({id,value}));
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
  async eraseAccount(userId){
    if(!userId)throw Error('Не указан аккаунт');erasedAccounts.add(String(userId));
    const db=await openDb();await new Promise((resolve,reject)=>{
      const transaction=db.transaction(STORE,'readwrite'),records=transaction.objectStore(STORE);
      records.put({id:NS+'erased:'+userId,value:true});
      for(const prefix of [NS+'signal:'+userId+':',NS+'gk:'+userId+':'])records.delete(IDBKeyRange.bound(prefix,prefix+'\uffff'));
      records.delete(NS+'backup:'+userId);records.delete(NS+'restore:'+userId);transaction.oncomplete=resolve;transaction.onerror=()=>reject(transaction.error);transaction.onabort=()=>reject(transaction.error||Error('Не удалось удалить ключи'));
    });
  },
  forAccount(userId) {
    const prefix = `signal:${userId}:`;
    return {
      get: (key, fallback) => recordGet(prefix + key, fallback),
      put: (key, value) => recordPut(prefix + key, value),
      remove: key => recordRemove(prefix + key),
      keys: async () => (await recordKeys()).filter(key => key.startsWith(prefix)).map(key => key.slice(prefix.length)),
    };
  },
};
