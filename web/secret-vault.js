// This namespace is stored ONLY in encrypted account-local storage. Never export it.
const KEY = "secret-vault:v1",
  queues = new WeakMap(),
  epochs = new WeakMap();
const clone = (value) => JSON.parse(JSON.stringify(value));
function serialized(store, action) {
  const task = (queues.get(store) || Promise.resolve()).then(action);
  queues.set(
    store,
    task.catch(() => {}),
  );
  return task;
}
function clean(value) {
  const result = { v: 1, chats: {} };
  for (const [id, entry] of Object.entries(value?.chats || {}))
    if (entry.expiresAt > Date.now()) {
      entry.messages = entry.messages.filter((m) => m.expires_at > Date.now());
      result.chats[id] = entry;
    }
  return result;
}
export function eraseSecretVault(store) {
  // Fence old controllers synchronously, before React unmount or network logout.
  epochs.set(store, (epochs.get(store) || 0) + 1);
  return serialized(store, () => store.set(KEY, null));
}
export async function pruneSecretVault(store) {
  return serialized(store, async () => {
    const previous = await store.get(KEY, null),
      before = JSON.stringify(previous),
      next = clean(previous);
    if (JSON.stringify(next) !== before) await store.set(KEY, clone(next));
  });
}
export class SecretChatController {
  static async open(options) {
    const c = new SecretChatController();
    Object.assign(c, options);
    c.storeEpoch = epochs.get(c.store) || 0;
    c.closed = false;
    c.pending = Promise.resolve();
    const { chat, userId, deviceId } = c;
    if (!Number.isInteger(chat.ttl) || chat.ttl < 30 || chat.ttl > 86400)
      throw Error("Некорректный срок");
    const initiator = chat.initiator === userId;
    if (
      !(initiator || chat.recipient === userId) ||
      (initiator ? chat.initiator_device : chat.recipient_device) !== deviceId
    )
      throw Error("Некорректное устройство");
    c.peerDeviceId = initiator ? chat.recipient_device : chat.initiator_device;
    c.peerUserId = initiator ? chat.recipient : chat.initiator;
    c.id = chat.id + ":" + deviceId;
    c.session = new options.Session(chat.id, deviceId);
    c.guard();
    // Authorize the exact proven device and reject deleted/closed chats BEFORE showing local history.
    await c.inbox();
    c.guard();
    try {
      await serialized(c.store, async () => {
        c.guard();
        const vault = clean(await c.store.get(KEY, null));
        c.guard();
        const saved = vault.chats[c.id];
        const binding = [userId, deviceId, c.peerUserId, c.peerDeviceId].join(
          "|",
        );
        if (saved && saved.binding !== binding)
          throw Error("Некорректное устройство");
        if (saved) {
          await c.session.restore(saved.session);
          c.entry = saved;
        } else {
          if (Object.keys(vault.chats).length >= 100)
            throw Error("Начните новый секретный чат");
          await c.session.offer();
          c.entry = {
            binding,
            expiresAt: Date.now() + chat.ttl * 1000,
            messages: [],
            session: null,
          };
        }
        c.guard();
        c.entry.session = await c.session.snapshot();
        c.guard();
        vault.chats[c.id] = c.entry;
        await c.store.set(KEY, clone(vault));
        c.guard();
      });
      return c;
    } catch (error) {
      c.close();
      throw error;
    }
  }
  guard() {
    if (
      this.closed ||
      this.storeEpoch !== (epochs.get(this.store) || 0) ||
      !this.current()
    )
      throw Error("Сеанс изменился");
  }
  path(suffix = "") {
    return "/me/account/secret-chats/" + this.chat.id + suffix;
  }
  async inbox() {
    try {
      const rows = await this.request(
        this.path("/messages") +
          "?device_id=" +
          encodeURIComponent(this.deviceId),
      );
      this.guard();
      return rows;
    } catch (error) {
      this.guard();
      if (/secret_device_mismatch/.test(String(error))) await this.forget();
      throw error;
    }
  }
  history() {
    return this.entry.messages.filter((m) => m.expires_at > Date.now());
  }
  async forget() {
    await serialized(this.store, async () => {
      this.guard();
      const vault = clean(await this.store.get(KEY, null));
      this.guard();
      delete vault.chats[this.id];
      await this.store.set(KEY, clone(vault));
    });
    this.close();
  }
  operation(action, allowExpired = false) {
    const task = this.pending.then(async () => {
      this.guard();
      if (!allowExpired && this.entry.expiresAt <= Date.now()) {
        await this.forget();
        throw Error("Сообщение истекло");
      }
      return action();
    });
    this.pending = task.catch(() => {});
    return task;
  }
  async save(renew = false) {
    this.guard();
    if (this.entry.expiresAt <= Date.now()) {
      await this.forget();
      throw Error("Сообщение истекло");
    }
    const state = await this.session.snapshot();
    this.guard();
    if (renew) this.entry.expiresAt = Date.now() + this.chat.ttl * 1000;
    this.entry.session = state;
    this.entry.messages = this.history();
    await serialized(this.store, async () => {
      this.guard();
      const vault = clean(await this.store.get(KEY, null));
      this.guard();
      vault.chats[this.id] = this.entry;
      await this.store.set(KEY, clone(vault));
      this.guard();
    });
  }
  accept(peerKey) {
    return this.operation(async () => {
      if (this.session.peerPublicKey && this.session.peerPublicKey !== peerKey)
        throw Error("Код безопасности не совпадает");
      await this.session.accept(peerKey);
      this.guard();
      await this.save(true);
      return this.session.fingerprint;
    });
  }
  verify(code) {
    return this.operation(async () => {
      this.session.verify(code);
      await this.save(true);
    });
  }
  send(text) {
    return this.operation(async () => {
      if (
        typeof text !== "string" ||
        !text.trim() ||
        new TextEncoder().encode(text).length > 16000
      )
        throw Error("Некорректное сообщение");
      if (this.history().length >= 1024)
        throw Error("Начните новый секретный чат");
      // Probe also rejects remotely deleted chats before local encryption/display.
      await this.inbox();
      const expires_at = Date.now() + this.chat.ttl * 1000;
      const ciphertext = await this.session.encrypt(text, expires_at);
      this.guard();
      const nonce = Array.from(
        crypto.getRandomValues(new Uint8Array(16)),
        (b) => b.toString(16).padStart(2, "0"),
      ).join("");
      const result = await this.request(this.path("/messages"), {
        method: "POST",
        body: { device_id: this.deviceId, ciphertext, nonce },
      });
      this.guard();
      this.entry.messages.push({
        id: result.id,
        text,
        expires_at,
        direction: "out",
      });
      await this.save(true);
    });
  }
  refresh() {
    return this.operation(async () => {
      const rows = await this.inbox(),
        ids = [];
      for (const row of rows) {
        this.guard();
        if (row.sender_id !== this.peerUserId)
          throw Error("Некорректное устройство");
        if (!this.entry.messages.some((m) => m.id === row.id)) {
          if (this.history().length >= 1024)
            throw Error("Начните новый секретный чат");
          let decoded;
          try {
            decoded = await this.session.decrypt(
              row.ciphertext,
              this.peerDeviceId,
            );
          } catch (error) {
            this.guard();
            if (error?.code === "secret_expired") {
              ids.push(row.id);
              continue;
            }
            throw error;
          }
          this.guard();
          if (decoded.expires_at > Date.now() + this.chat.ttl * 1000 + 1000)
            throw Error("Некорректный срок");
          this.entry.messages.push({ ...decoded, id: row.id, direction: "in" });
        }
        ids.push(row.id);
      }
      // Durable history + replay fence must commit before deleting delivery ciphertext.
      await this.save(ids.length > 0);
      this.guard();
      if (ids.length)
        await this.request(this.path("/ack"), {
          method: "POST",
          body: { device_id: this.deviceId, ids },
        });
      this.guard();
      return this.history();
    });
  }
  prune() {
    return this.operation(async () => {
      if (this.entry.expiresAt <= Date.now()) {
        await this.forget();
      } else await this.save();
    }, true);
  }
  remove() {
    return this.operation(async () => {
      await this.request(this.path(), {
        method: "DELETE",
        body: { device_id: this.deviceId },
      });
      this.guard();
      await serialized(this.store, async () => {
        this.guard();
        const vault = clean(await this.store.get(KEY, null));
        this.guard();
        delete vault.chats[this.id];
        await this.store.set(KEY, clone(vault));
      });
      this.entry.messages = [];
      this.close();
    });
  }
  close() {
    this.closed = true;
    this.session.destroy();
    if (this.entry) {
      this.entry.messages = [];
      this.entry.session = null;
    }
  }
}
