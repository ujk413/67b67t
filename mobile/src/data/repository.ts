import * as SQLite from "expo-sqlite";
import * as SecureStore from "expo-secure-store";
import type { Message, Room } from "../types";
export type Draft = { text: string; reply?: Message | null };
export type Outgoing = {
  upload?: {uri:string;name:string;mime:string;prepared?:{uri:string;meta:Record<string,unknown>}};
  createdAt?: string;
  id: string;
  roomId: string;
  text: string;
  reply?: Message | null;
  body?: Record<string, unknown>;
  state: "queued" | "sending" | "failed";
  error?: string;
};
export function newId() {
  const b = new Uint8Array(16);
  crypto.getRandomValues(b);
  return Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
}
export class AccountRepository {
  private db: SQLite.SQLiteDatabase | null = null;
  private pending: Promise<void> | null = null;
  private closed = false;
  private closing: Promise<void> | null = null;
  private sequence: Promise<unknown> = Promise.resolve();
  private tombstones = new Set<string>();
  private access<T>(
    action: (db: SQLite.SQLiteDatabase) => Promise<T>,
  ): Promise<T> {
    const task = this.sequence.then(async () =>
      action(await this.connection()),
    );
    this.sequence = task.catch(() => {});
    return task;
  }
  private async transaction(
    db: SQLite.SQLiteDatabase,
    action: (db: SQLite.SQLiteDatabase) => Promise<void>,
  ) {
    await db.execAsync("BEGIN IMMEDIATE");
    try {
      await action(db);
      await db.execAsync("COMMIT");
    } catch (error) {
      await db.execAsync("ROLLBACK");
      throw error;
    }
  }
  constructor(readonly userId: string) {}
  async open() {
    if (this.closed) throw new Error("Хранилище закрыто");
    if (this.db) return;
    if (this.pending) return this.pending;
    this.pending = (async () => {
      const name = `account-${this.userId}`;
      let key = await SecureStore.getItemAsync(name);
      if (!key) {
        key = newId() + newId();
        await SecureStore.setItemAsync(name, key);
      }
      if (!/^[a-f0-9]{64}$/.test(key))
        throw new Error("Повреждён ключ локального хранилища");
      // This repository owns its statements and finalizes them before close.
      // Bulk-finalizing sqlite3_next_stmt also touches FTS5's internal cached statements.
      const db = await SQLite.openDatabaseAsync(`${name}.db`, {useNewConnection:true,finalizeUnusedStatementsBeforeClosing:false});
      await db.execAsync(`PRAGMA key="x'${key}'";`);
      const cipher = await db.getFirstAsync<{ cipher_version: string }>(
        "PRAGMA cipher_version",
      );
      if (!cipher?.cipher_version) {
        await db.closeAsync();
        throw new Error(
          "Установите новую APK-сборку с поддержкой шифрования хранилища",
        );
      }
      await db.execAsync(`PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS hidden_messages_local(id TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS messages(id TEXT PRIMARY KEY,room_id TEXT NOT NULL,created_at TEXT NOT NULL,data TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS messages_order ON messages(room_id,created_at,id);
        CREATE VIRTUAL TABLE IF NOT EXISTS search USING fts5(id UNINDEXED,room_id UNINDEXED,text,tokenize='unicode61');
        CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY,data TEXT NOT NULL);`);
      for(const row of await db.getAllAsync<{id:string}>('SELECT id FROM hidden_messages_local'))this.tombstones.add(row.id);
      if (this.closed) {
        await db.closeAsync();
        throw new Error("Хранилище закрыто");
      }
      this.db = db;
    })();
    try {
      await this.pending;
    } finally {
      this.pending = null;
    }
  }
  private async connection() {
    await this.open();
    if (!this.db || this.closed) throw new Error("Хранилище закрыто");
    return this.db;
  }
  async get<T>(key: string, fallback: T): Promise<T> {
    return this.access(async (db) => {
      const row = await db.getFirstAsync<{ value: string }>(
        "SELECT value FROM kv WHERE key=?",
        key,
      );
      return row ? JSON.parse(row.value) : fallback;
    });
  }
  async set(key: string, value: unknown) {
    return this.access(async (db) => {
      await db.runAsync(
        "INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)",
        key,
        JSON.stringify(value),
      );
    });
  }
  async rooms() {
    return this.get<Room[]>("rooms", []);
  }
  async saveRooms(rooms: Room[]) {
    return this.set("rooms", rooms);
  }
  async saveMessages(items: Message[]) {
    return this.access(async (db) => {
      await this.transaction(db, async (tx) => {
        for (const m of items) {
          if(this.tombstones.has(m.id))continue;
          if(await tx.getFirstAsync('SELECT id FROM hidden_messages_local WHERE id=?',m.id))continue;
          await tx.runAsync(
            "INSERT OR REPLACE INTO messages VALUES(?,?,?,?)",
            m.id,
            m.room_id,
            m.created_at,
            JSON.stringify(m),
          );
          await tx.runAsync("DELETE FROM search WHERE id=?", m.id);
          if (
            (m.text &&
              !m.encrypted_content &&
              !m.meta?.encrypted &&
              !m.meta?.e2e_group) ||
            m._decrypted
          )
            await tx.runAsync(
              "INSERT INTO search(id,room_id,text) VALUES(?,?,?)",
              m.id,
              m.room_id,
              m.text || "",
            );
        }
      });
    });
  }
  async messages(roomId: string, limit = 100) {
    return this.access(async (db) => {
      const rows = await db.getAllAsync<{ data: string }>(
        "SELECT data FROM messages WHERE room_id=? ORDER BY created_at DESC,id DESC LIMIT ?",
        roomId,
        limit,
      );
      return rows.reverse().map((x) => JSON.parse(x.data) as Message);
    });
  }
  async removeMessage(id: string) {
    return this.access(async (db) => {
      await this.transaction(db, async (tx) => {
        await tx.runAsync("DELETE FROM messages WHERE id=?", id);
        await tx.runAsync("DELETE FROM search WHERE id=?", id);
      });
    });
  }
  async hideMessage(id:string) {
    this.tombstones.add(id);
    return this.access(async db=>this.transaction(db,async tx=>{
      await tx.runAsync('INSERT OR IGNORE INTO hidden_messages_local(id)VALUES(?)',id);
      await tx.runAsync('DELETE FROM messages WHERE id=?',id);
      await tx.runAsync('DELETE FROM search WHERE id=?',id);
    }));
  }
  isHidden(id:string) {return this.tombstones.has(id);}
  async visibleMessages(items:Message[]) {
    return this.access(async db=>{
      const hidden=new Set((await db.getAllAsync<{id:string}>('SELECT id FROM hidden_messages_local')).map(row=>row.id));
      return items.filter(item=>!hidden.has(item.id) && !this.tombstones.has(item.id));
    });
  }
  async pruneRooms(ids: string[]) {
    return this.access(async (db) => {
      await this.transaction(db, async (tx) => {
        const known = await tx.getAllAsync<{ room_id: string }>(
          "SELECT DISTINCT room_id FROM messages",
        );
        for (const { room_id } of known)
          if (!ids.includes(room_id)) {
            await tx.runAsync("DELETE FROM messages WHERE room_id=?", room_id);
            await tx.runAsync("DELETE FROM search WHERE room_id=?", room_id);
          }
        const values = await tx.getAllAsync<{ key: string; value: string }>(
          "SELECT key,value FROM kv",
        );
        for (const row of values) {
          const prefix = row.key.split(":")[0];
          if (
            ["draft", "position", "scan", "e2e"].includes(prefix) &&
            !ids.includes(row.key.slice(prefix.length + 1))
          )
            await tx.runAsync("DELETE FROM kv WHERE key=?", row.key);
        }
        const outgoing = await tx.getAllAsync<{ id: string; data: string }>(
          "SELECT id,data FROM outbox",
        );
        for (const row of outgoing) {
          const item = JSON.parse(row.data) as Outgoing;
          if (!ids.includes(item.roomId)) {
            item.state = "failed";
            item.error = "Доступ к переписке отозван";
            delete item.reply;
            await tx.runAsync(
              "UPDATE outbox SET data=? WHERE id=?",
              JSON.stringify(item),
              row.id,
            );
          }
        }
      });
    });
  }
  async search(query: string, roomId?: string) {
    return this.access(async (db) => {
      const terms = query
        .trim()
        .split(/\s+/)
        .filter(Boolean)
        .map((x) => '"' + x.replace(/"/g, '""') + '"*')
        .join(" AND ");
      if (!terms) return [];
      const rows = await db.getAllAsync<{ data: string }>(
        `SELECT m.data FROM search JOIN messages m ON m.id=search.id WHERE search MATCH ? ${roomId ? "AND m.room_id=?" : ""} ORDER BY m.created_at DESC LIMIT 100`,
        ...(roomId ? [terms, roomId] : [terms]),
      );
      return rows.map((x) => JSON.parse(x.data) as Message);
    });
  }
  async outgoing() {
    return this.access(async (db) => {
      return (
        await db.getAllAsync<{ data: string }>(
          "SELECT data FROM outbox ORDER BY rowid",
        )
      ).map((x) => JSON.parse(x.data) as Outgoing);
    });
  }
  async putOutgoing(item: Outgoing) {
    return this.access(async (db) => {
      await db.runAsync(
        "INSERT OR REPLACE INTO outbox VALUES(?,?)",
        item.id,
        JSON.stringify(item),
      );
    });
  }
  async enqueueMedia(item:Outgoing) {
    return this.access(async db=>this.transaction(db,async tx=>{
      const delivered=await tx.getFirstAsync('SELECT id FROM messages WHERE json_extract(data,\'$.client_message_id\')=? LIMIT 1',item.id);
      const queued=await tx.getFirstAsync('SELECT id FROM outbox WHERE id=?',item.id);
      if(!delivered && !queued)await tx.runAsync('INSERT INTO outbox VALUES(?,?)',item.id,JSON.stringify(item));
      const key=`attachments:${item.roomId}`;
      const row=await tx.getFirstAsync<{value:string}>('SELECT value FROM kv WHERE key=?',key);
      if(row){const draft=JSON.parse(row.value);draft.items=(draft.items || []).filter((file:{id:string})=>file.id!==item.id);await tx.runAsync('UPDATE kv SET value=? WHERE key=?',JSON.stringify(draft),key);}
    }));
  }
  async removeOutgoing(id: string) {
    return this.access(async (db) => {
      await db.runAsync("DELETE FROM outbox WHERE id=?", id);
    });
  }
  async clearCache() {
    return this.access(async (db) => {
      await db.execAsync(
        "DELETE FROM messages;DELETE FROM search;DELETE FROM kv WHERE key LIKE 'scan:%' OR key LIKE 'group:%:plain:%';",
      );
      await db.execAsync("PRAGMA wal_checkpoint(TRUNCATE);VACUUM;");
    });
  }
  async clearAll() {
    return this.access(async (db) => {
      await db.execAsync(
        "DELETE FROM messages;DELETE FROM search;DELETE FROM outbox;DELETE FROM kv;",
      );
    });
  }
  async privateFiles() {
    return this.access(async (db) => {
      const rows = await db.getAllAsync<{ key: string; value: string }>(
        "SELECT key,value FROM kv WHERE key LIKE 'voice:%' OR key LIKE 'attachments:%'",
      );
      const drafts=rows.flatMap((row) => {
        const value = JSON.parse(row.value);
        return row.key.startsWith("voice:")
          ? value?.uri
            ? [value.uri]
            : []
          : (value?.items || []).map((item: { uri: string }) => item.uri);
      }) as string[];
      const queued=await db.getAllAsync<{data:string}>('SELECT data FROM outbox');
      return [...new Set([...drafts,...queued.flatMap(row=>{
        const upload=(JSON.parse(row.data) as Outgoing).upload;
        return upload?[upload.uri,...(upload.prepared?[upload.prepared.uri]:[])]:[];
      })])];
    });
  }
  async size() {
    return this.access(async (db) => {
      const count = await db.getFirstAsync<{ page_count: number }>(
          "PRAGMA page_count",
        ),
        size = await db.getFirstAsync<{ page_size: number }>(
          "PRAGMA page_size",
        );
      return (count?.page_count || 0) * (size?.page_size || 0);
    });
  }
  close(): Promise<void> {
    if(this.closing)return this.closing;
    this.closed = true;
    this.closing=(async()=>{
      await this.sequence;
      await this.pending?.catch(() => {});
      const db=this.db;
      this.db = null;
      await db?.closeAsync();
    })();return this.closing;
  }
  async destroy() {
    await this.close();
    await SQLite.deleteDatabaseAsync(`account-${this.userId}.db`);
    await SecureStore.deleteItemAsync(`account-${this.userId}`);
  }
}
