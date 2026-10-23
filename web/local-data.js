// Account-scoped, encrypted browser cache. Sessions and authentication tokens are never stored here.
const DB = '67chat-local-v1';
let opening;
const stores = new Map();
function database() {
  return opening ||= new Promise((resolve, reject) => {
    const req = indexedDB.open(DB, 1);
    req.onupgradeneeded = () => req.result.createObjectStore('records');
    req.onsuccess = () => resolve(req.result); req.onerror = () => { opening = null; reject(req.error); };
  });
}
async function access(mode, action) {
  const db = await database();
  return new Promise((resolve, reject) => {
    const tx = db.transaction('records', mode), store = tx.objectStore('records'); let value;
    const request = action(store); if (request) request.onsuccess = () => { value = request.result; };
    tx.oncomplete = () => resolve(value); tx.onerror = () => reject(tx.error); tx.onabort = () => reject(tx.error || new Error('Не удалось сохранить данные на устройстве'));
  });
}
export function createAccountStore(userId) {
  if (!userId) throw new Error('Сначала войдите в аккаунт');
  if(stores.has(userId))return stores.get(userId);
  const prefix = `user:${userId}:`; let keyPromise, writes = Promise.resolve();
  async function key() {
    return keyPromise ||= (async () => {
      const id = prefix + 'key', existing = await access('readonly', s => s.get(id)); if (existing) return existing;
      const created = await crypto.subtle.generateKey({ name: 'AES-GCM', length: 256 }, false, ['encrypt', 'decrypt']);
      try { await access('readwrite', s => s.add(created, id)); return created; }
      catch (error) { const winner = await access('readonly', s => s.get(id)); if (winner) return winner; throw error; }
    })();
  }
  async function unpack(value, id) {
    if (!value) return undefined;
    const data = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: value.iv, additionalData: new TextEncoder().encode(id) }, await key(), value.data);
    return JSON.parse(new TextDecoder().decode(data));
  }
  const store = {
    userId,
    async get(name, fallback = null) { await writes; const id = prefix + name; const result = await unpack(await access('readonly', s => s.get(id)), id); return result === undefined ? fallback : result; },
    set(name, value) {
      const task = writes.then(async () => {
        const id = prefix + name, iv = crypto.getRandomValues(new Uint8Array(12));
        const data = await crypto.subtle.encrypt({ name: 'AES-GCM', iv, additionalData: new TextEncoder().encode(id) }, await key(), new TextEncoder().encode(JSON.stringify(value)));
        await access('readwrite', s => s.put({ iv, data }, id));
      }); writes = task.catch(() => {}); return task;
    },
    remove(name) { const task = writes.then(() => access('readwrite', s => s.delete(prefix + name))); writes = task.catch(() => {}); return task; },
    setBlob(name, blob) {
      const task = writes.then(async () => {
        const id = prefix + name, iv = crypto.getRandomValues(new Uint8Array(12));
        const data = await crypto.subtle.encrypt({ name: 'AES-GCM', iv, additionalData: new TextEncoder().encode(id) }, await key(), await blob.arrayBuffer());
        await access('readwrite', s => s.put({ iv, data, mime: blob.type }, id));
      }); writes = task.catch(() => {}); return task;
    },
    async getBlob(name) { await writes; const id = prefix + name, value = await access('readonly', s => s.get(id)); if (!value) return null;
      const data = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: value.iv, additionalData: new TextEncoder().encode(id) }, await key(), value.data);
      return new Blob([data], { type: value.mime });
    },
    async entries(namePrefix) {
      await writes; const start = prefix + namePrefix, range = IDBKeyRange.bound(start, start + '\uffff');
      const db=await database(), rows=await new Promise((resolve,reject)=>{
        const tx=db.transaction('records','readonly'),result=[],cursor=tx.objectStore('records').openCursor(range);
        cursor.onsuccess=()=>{const row=cursor.result;if(row){result.push([row.key,row.value]);row.continue();}};
        tx.oncomplete=()=>resolve(result);tx.onerror=()=>reject(tx.error);
      });
      return Promise.all(rows.map(async ([id,value])=>[id.slice(prefix.length),await unpack(value,id)]));
    },
    async saveMessages(roomId, incoming) {
      // Serialize read/merge/write as one logical operation across network and realtime updates.
      const task = writes.then(async () => {
        const id = prefix + 'messages:' + roomId;
        const previous = await unpack(await access('readonly', s => s.get(id)), id) || [];
        const start=prefix+'hidden:',keys=await access('readonly',s=>s.getAllKeys(IDBKeyRange.bound(start,start+'\uffff')));
        const hidden=new Set(keys.map(key=>key.slice(start.length)));
        const byId = new Map([...previous, ...incoming].filter(message=>!hidden.has(message.id)).map(message => [message.id, message]));
        const values = [...byId.values()].sort(compareMessages), iv = crypto.getRandomValues(new Uint8Array(12));
        const data = await crypto.subtle.encrypt({ name: 'AES-GCM', iv, additionalData: new TextEncoder().encode(id) }, await key(), new TextEncoder().encode(JSON.stringify(values)));
        await access('readwrite', s => s.put({ iv, data }, id));
      }); writes = task.catch(() => {}); return task;
    },
    removeMessage(roomId, messageId) {
      const task=writes.then(async()=>{
        const id=prefix+'messages:'+roomId,previous=await unpack(await access('readonly',s=>s.get(id)),id)||[],iv=crypto.getRandomValues(new Uint8Array(12));
        const data=await crypto.subtle.encrypt({name:'AES-GCM',iv,additionalData:new TextEncoder().encode(id)},await key(),new TextEncoder().encode(JSON.stringify(previous.filter(m=>m.id!==messageId))));
        await access('readwrite',s=>s.put({iv,data},id));
      });writes=task.catch(()=>{});return task;
    },
    async clearHistory() { for (const kind of ['messages:','group-plain:'])for (const [name] of await store.entries(kind)) await store.remove(name); },
    flush: () => writes,
  };
  stores.set(userId,store);return store;
}
export function compareMessages(a, b) { return String(a.created_at).localeCompare(String(b.created_at)) || String(a.id).localeCompare(String(b.id)); }
export function mergeMessages(previous, incoming) {
  const byId = new Map(previous.map(m => [m.id, m]));
  for (const item of incoming) {
    if (item.client_message_id && !item.delivery) for (const [id, row] of byId) if (row.client_message_id === item.client_message_id && row.sender_id === item.sender_id) byId.delete(id);
    byId.set(item.id, { ...byId.get(item.id), ...item });
  }
  return [...byId.values()].sort(compareMessages);
}
export class DurableOutbox {
  constructor({ store, send, changed, report }) { Object.assign(this, { store, send, changed, report }); this.running = false; this.stopped = false; }
  async items() { return (await this.store.entries('outbox:')).map(([, value]) => value).sort(compareMessages); }
  async enqueue(item) { await this.store.set('outbox:' + item.id, item); this.changed(item); void this.flush(); }
  stop() { this.stopped = true; this.activeUpload?.abort(); }
  async retry(id) { const item = await this.store.get('outbox:' + id); if (item) { item.delivery = 'queued'; delete item.error; await this.store.set('outbox:' + id, item); this.changed(item); await this.flush(); } }
  async cancel(id) { const item = await this.store.get('outbox:' + id); if (item?.delivery === 'sending') throw new Error('Сообщение уже отправляется. Дождитесь подтверждения и удалите его.'); await this.store.remove('outbox:' + id); this.changed({ ...item, id, cancelled: true }); }
  async flush() {
    if (this.running || this.stopped || !navigator.onLine) return;
    this.running = true;
    try {
      for (const item of await this.items()) {
        if (this.stopped) break; if (item.delivery === 'failed') continue;
        if (!await this.store.get('outbox:' + item.id)) continue;
        item.delivery = 'sending'; await this.store.set('outbox:' + item.id, item); this.changed(item);
        try {
          const controller=new AbortController();this.activeUpload=controller;
          const message = await this.send(item,controller.signal);
          if (this.stopped) break;
          await this.store.saveMessages(item.room_id, [message]); await this.store.remove('outbox:' + item.id);if(item.upload?.blobKey)await this.store.remove(item.upload.blobKey); this.changed(item, message);
        } catch (error) {
          if (this.stopped) break;
          const transient = !error.status || error.status >= 500 || error.status === 429;
          item.delivery = transient ? 'queued' : 'failed'; item.error = error.message;
          await this.store.set('outbox:' + item.id, item); this.changed(item); if (transient) break;
        } finally {this.activeUpload=null;}
      }
    } catch (error) { this.report(error); } finally { this.running = false; }
  }
}
