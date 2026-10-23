import { test } from "node:test";

import assert from "node:assert/strict";

import { SecretSession } from "../web/account-features.js";

import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import vm from "node:vm";

const require = createRequire(import.meta.url),
  nativeRequire = createRequire(
    new URL("../mobile/package.json", import.meta.url),
  ),
  ts = nativeRequire("typescript");

const native = {};
vm.runInNewContext(
  ts.transpileModule(
    readFileSync(
      new URL("../mobile/src/accountRegistry.ts", import.meta.url),
      "utf8",
    ),
    {
      compilerOptions: {
        module: ts.ModuleKind.CommonJS,
        target: ts.ScriptTarget.ES2022,
        esModuleInterop: true,
      },
    },
  ).outputText,
  {
    exports: native,
    crypto,
    TextEncoder,
    TextDecoder,
    btoa,
    atob,
    URL,
    Uint8Array,
    Uint32Array,
    Date,
    require: (name) =>
      name === "expo"
        ? { requireOptionalNativeModule: () => null }
        : name.startsWith("expo-")
          ? {}
          : name.includes("async-storage")
            ? { default: {} }
            : nativeRequire(name),
  },
);

const vaultModule = await import("../web/secret-vault.js").catch(() => ({}));

const chat = {
  id: "vault-chat",
  initiator: "alice",
  recipient: "bob",
  initiator_device: "a",
  recipient_device: "b",
  ttl: 30,
};

function store() {
  let value = null;
  return {
    get: async () => structuredClone(value),
    set: async (_key, next) => {
      value = structuredClone(next);
    },
    read: () => value,
  };
}

function server() {
  let rows = [],
    ack = 0,
    closed = false;
  return {
    request: async (path, { method = "GET", body } = {}) => {
      if (closed) throw Error("secret_device_mismatch");
      if (method === "GET") return rows;
      if (path.endsWith("/ack")) {
        ack++;
        rows = [];
        return { ok: true };
      }
      if (method === "DELETE") {
        closed = true;
        return { ok: true };
      }
      const row = {
        id: crypto.randomUUID(),
        sender_id: "alice",
        ciphertext: body.ciphertext,
      };
      rows.push(row);
      return row;
    },
    get ack() {
      return ack;
    },
    close: () => {
      closed = true;
    },
  };
}

async function controllers() {
  assert.equal(
    typeof vaultModule.SecretChatController,
    "function",
    "missing reusable encrypted vault controller",
  );
  const sa = store(),
    sb = store(),
    transport = server(),
    C = vaultModule.SecretChatController;
  const open = (userId, deviceId, storage) =>
    C.open({
      chat,
      userId,
      deviceId,
      store: storage,
      request: transport.request,
      Session: SecretSession,
      current: () => true,
    });
  const a = await open("alice", "a", sa),
    b = await open("bob", "b", sb);
  await a.accept(b.session.publicKey);
  await b.accept(a.session.publicKey);
  await a.verify(a.session.fingerprint);
  await b.verify(b.session.fingerprint);
  return { a, b, sa, sb, transport, open };
}

test("vault preserves both local histories and replay state across restart, ACKs only after storage", async () => {
  const { a, b, sa, sb, transport, open } = await controllers();
  await a.send("persist me");
  await b.refresh();
  assert.equal(transport.ack, 1);
  a.close();
  b.close();

  const resumed = await open("bob", "b", sb);
  assert.equal(resumed.history()[0].text, "persist me");
  assert.equal((await open("alice", "a", sa)).history()[0].text, "persist me");

  await assert.rejects(open("bob", "new-device", sb), /устройств/);

  transport.close();
  await assert.rejects(open("bob", "b", sb), /mismatch/);
  assert.equal(
    Object.keys(sb.read().chats).length,
    0,
    "remote deletion must erase the local secret once observed",
  );
});

test("vault removes expired secrets and explicit erasure destroys all device states", async () => {
  const { a, sa } = await controllers();
  await a.send("short lived");
  const before = Date.now;
  try {
    Date.now = () => before() + 31000;
    await a.prune();
    assert.equal(a.history().length, 0);
    assert.equal(Object.keys(sa.read().chats).length, 0);
  } finally {
    Date.now = before;
  }

  await vaultModule.eraseSecretVault(sa);
  assert.equal(sa.read(), null);
});
test("expired state cannot be renewed by sending before the expiry timer runs", async () => {
  const { a, sa } = await controllers(),
    before = Date.now;
  try {
    Date.now = () => before() + 31000;
    await assert.rejects(a.send("too late"), /истекло/);
    assert.equal(Object.keys(sa.read().chats).length, 0);
  } finally {
    Date.now = before;
  }
});
test("storage failure never ACKs delivery and retry commits the history once", async () => {
  const { a, b, sb, transport } = await controllers();
  await a.send("must commit before ack");
  const set = sb.set;
  sb.set = async () => {
    throw Error("disk full");
  };
  await assert.rejects(b.refresh(), /disk full/);
  assert.equal(transport.ack, 0);
  sb.set = set;
  await b.refresh();
  assert.equal(transport.ack, 1);
  assert.equal(b.history().length, 1);
});
test("authenticated expired delivery is discarded without blocking newer ciphertext", async () => {
  const { a, b, transport } = await controllers(),
    before = Date.now;
  const ciphertext = await a.session.encrypt("expired", Date.now() + 1000);
  await transport.request("/messages", {
    method: "POST",
    body: { ciphertext },
  });
  try {
    Date.now = () => before() + 2000;
    await a.send("current");
    await b.refresh();
    assert.deepEqual(
      b.history().map((m) => m.text),
      ["current"],
    );
    assert.equal(transport.ack, 1);
  } finally {
    Date.now = before;
  }
});
test("background locking during an encrypted write preserves the committed local history", async () => {
  const { a, sa, open } = await controllers();
  const write = sa.set;
  let release, started;
  const writing = new Promise((resolve) => {
    started = resolve;
  });
  sa.set = async (key, value) => {
    started();
    await new Promise((resolve) => {
      release = resolve;
    });
    return write(key, value);
  };
  const pending = a.send("durable during lock");
  await writing;
  a.close();
  release();
  await assert.rejects(pending, /Сеанс/);
  sa.set = write;
  assert.equal(
    (await open("alice", "a", sa)).history()[0]?.text,
    "durable during lock",
  );
});

test("explicit erase fences a still-mounted controller before the next React render", async () => {
  const { a, sa, open } = await controllers();
  await vaultModule.eraseSecretVault(sa);
  await assert.rejects(a.send("late after logout"), /Сеанс/);
  assert.equal(sa.read(), null);
  // Cached browser encrypted stores are reused on the next login. A fresh controller may use it.
  const fresh = await open("alice", "a", sa);
  assert.equal(fresh.history().length, 0);
});

test("late refresh cannot decrypt, persist or ACK after account scope changes", async () => {
  assert.equal(typeof vaultModule.SecretChatController, "function");
  let live = true,
    finish;
  const storage = store();
  const c = await vaultModule.SecretChatController.open({
    chat,
    userId: "alice",
    deviceId: "a",
    store: storage,
    Session: SecretSession,
    current: () => live,
    request: async () => [],
  });

  const before = structuredClone(storage.read());
  c.request = () =>
    new Promise((resolve) => {
      finish = resolve;
    });
  const pending = c.refresh();
  await Promise.resolve();
  live = false;
  finish([{ id: "late", ciphertext: "opaque" }]);
  await assert.rejects(pending, /Сеанс/);
  assert.deepEqual(storage.read(), before);
});

test("secret sessions resume locally with verification and replay fence, and reject another device", async () => {
  const a = new SecretSession("restart-chat", "a"),
    b = new SecretSession("restart-chat", "b");

  const ap = await a.offer(),
    bp = await b.offer();
  a.verify(await a.accept(bp));
  b.verify(await b.accept(ap));

  const ct = await a.encrypt("retained secret", Date.now() + 30000);
  await b.decrypt(ct, "a");

  assert.equal(
    typeof b.snapshot,
    "function",
    "encrypted storage needs a resumable device-scoped snapshot",
  );

  const saved = await b.snapshot();
  b.destroy();

  const restored = new SecretSession("restart-chat", "b");
  await restored.restore(saved);

  await assert.rejects(restored.decrypt(ct, "a"), /Повтор/);

  assert.equal(
    (
      await restored.decrypt(
        await a.encrypt("after restart", Date.now() + 30000),
        "a",
      )
    ).text,
    "after restart",
  );

  const wrong = new SecretSession("restart-chat", "new-device");
  await assert.rejects(wrong.restore(saved), /устройств/);

  await assert.rejects(
    restored.decrypt(
      await a.encrypt("wrong sender", Date.now() + 30000),
      "intruder",
    ),
    /устройств/,
  );
});

test("repeated offer is stable and accepting another peer invalidates manual verification", async () => {
  const a = new SecretSession("chat", "a"),
    b = new SecretSession("chat", "b"),
    c = new SecretSession("chat", "c");

  const ap = await a.offer();
  assert.equal(await a.offer(), ap);
  a.verify(await a.accept(await b.offer()));

  await a.accept(await c.offer());
  await assert.rejects(a.encrypt("no", Date.now() + 30000), /код безопасности/);
});

test("destroy during real pending key derivation is terminal for the controller crypto object", async () => {
  const storage = store(),
    controller = await vaultModule.SecretChatController.open({
      chat,
      userId: "alice",
      deviceId: "a",
      store: storage,
      request: async () => [],
      Session: SecretSession,
      current: () => true,
    });
  const peer = new SecretSession(chat.id, "b"),
    peerKey = await peer.offer(),
    derive = crypto.subtle.deriveKey.bind(crypto.subtle);
  let release, started;
  const waiting = new Promise((resolve) => {
    started = resolve;
  });
  crypto.subtle.deriveKey = async (...args) => {
    const key = await derive(...args);
    started();
    await new Promise((resolve) => {
      release = resolve;
    });
    return key;
  };
  try {
    const pending = controller.accept(peerKey);
    await waiting;
    controller.close();
    release();
    await assert.rejects(pending, /Чат закрыт|Сеанс/);
    assert.equal(controller.session.key, null);
    assert.equal(controller.session.pair, null);
    await assert.rejects(controller.session.offer(), /Чат закрыт/);
  } finally {
    crypto.subtle.deriveKey = derive;
  }
});

test("destroy during offer and restore cannot repopulate the private key pair", async () => {
  for (const operation of ["offer", "restore"]) {
    const source = new SecretSession("pending", "a");
    await source.offer();
    const snapshot = await source.snapshot(),
      session = new SecretSession("pending", "a");
    const method = operation === "offer" ? "generateKey" : "importKey",
      original = crypto.subtle[method].bind(crypto.subtle);
    let release, started;
    const waiting = new Promise((resolve) => {
      started = resolve;
    });
    crypto.subtle[method] = async (...args) => {
      const result = await original(...args);
      started();
      await new Promise((resolve) => {
        release = resolve;
      });
      return result;
    };
    try {
      const pending =
        operation === "offer" ? session.offer() : session.restore(snapshot);
      await waiting;
      session.destroy();
      release();
      await assert.rejects(pending, /Чат закрыт/);
      assert.equal(session.pair, null);
      assert.equal(session.key, null);
    } finally {
      crypto.subtle[method] = original;
    }
  }
});

test("native vault snapshots interoperate with browser, persist replay and reject another device", async () => {
  const a = new native.DeviceSecretSession("interop", "a"),
    b = new SecretSession("interop", "b");
  a.offer();
  const bp = await b.offer();
  a.verify(a.accept(bp));
  b.verify(await b.accept(a.publicKey));

  const ct = await b.encrypt("browser to native", Date.now() + 30000);
  assert.equal(a.decrypt(ct, "b").text, "browser to native");
  assert.equal(
    typeof a.snapshot,
    "function",
    "native encrypted history needs restart state",
  );

  const snapshot = a.snapshot(),
    resumed = new native.DeviceSecretSession("interop", "a");
  a.destroy();
  resumed.restore(snapshot);
  assert.throws(() => resumed.decrypt(ct, "b"), /Повтор/);

  assert.equal(
    (
      await b.decrypt(
        resumed.encrypt("native resumed", Date.now() + 30000),
        "a",
      )
    ).text,
    "native resumed",
  );

  assert.throws(
    () =>
      new native.DeviceSecretSession("interop", "new-device").restore(snapshot),
    /устройств/,
  );

  const wrongSender = await b.encrypt("bound sender", Date.now() + 30000);
  assert.throws(() => resumed.decrypt(wrongSender, "intruder"), /устройств/);
});
