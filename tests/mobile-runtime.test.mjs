import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const ts = require("../mobile/node_modules/typescript");
const appText = fs.readFileSync(
  new URL("../mobile/App.tsx", import.meta.url),
  "utf8",
);
const appSource = ts.createSourceFile(
  "App.tsx",
  appText,
  ts.ScriptTarget.Latest,
  true,
  ts.ScriptKind.TSX,
);
const app = appSource.statements.find(
  (n) => ts.isFunctionDeclaration(n) && n.name?.text === "App",
);

// Run the actual production callbacks with controllable native boundaries.
// No Expo runtime, network, real timers or duplicated connection implementation.
function evaluate(expression, scope) {
  const code = ts.transpileModule(
    `const callback = (${expression}); callback;`,
    {
      compilerOptions: {
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.CommonJS,
      },
    },
  ).outputText;
  return vm.runInNewContext(code, scope);
}

function effectContaining(marker, scope) {
  const statement = app.body.statements.find(
    (n) =>
      ts.isExpressionStatement(n) &&
      ts.isCallExpression(n.expression) &&
      n.expression.expression.getText(appSource) === "useEffect" &&
      n.expression.arguments[0].getText(appSource).includes(marker),
  );
  assert.ok(statement, `Missing production effect: ${marker}`);
  return evaluate(statement.expression.arguments[0].getText(appSource), scope);
}

function appFunction(name, scope) {
  const fn = app.body.statements.find(
    (n) => ts.isFunctionDeclaration(n) && n.name?.text === name,
  );
  assert.ok(fn, `Missing production function: ${name}`);
  return evaluate(fn.getText(appSource), scope);
}

function loadModule(relativePath, scope = {}, dependencies = {}) {
  const source = fs.readFileSync(
    new URL(relativePath, import.meta.url),
    "utf8",
  );
  const code = ts.transpileModule(source, {
    compilerOptions: {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.CommonJS,
      esModuleInterop: true,
    },
  }).outputText;
  const exports = {};
  vm.runInNewContext(code, {
    exports,
    AbortController, setTimeout, clearTimeout,
    ...scope,
    require(name) {
      assert.ok(name in dependencies, `Unexpected native import: ${name}`);
      return dependencies[name];
    },
  });
  return exports;
}

function connectionHarness({
  token = "current-token",
  background = false,
  calling = false,
} = {}) {
  const sockets = [],
    timers = new Map(),
    events = [];
  let serial = 0,
    now = 0,
    listener,
    resyncs = 0;
  class Socket {
    static CONNECTING = 0;
    static OPEN = 1;
    constructor(url) {
      this.url = url;
      this.readyState = 0;
      this.sent = [];
      sockets.push(this);
    }
    open(authenticate = true) {
      this.readyState = 1;
      this.onopen?.();
      if (authenticate) this.receive({ type: 'auth.ok' });
    }
    send(value) {
      this.sent.push(JSON.parse(value));
    }
    close() {
      this.readyState = 3;
      this.onclose?.();
    }
    receive(value) {
      this.onmessage?.({ data: JSON.stringify(value) });
    }
  }
  const scope = {
    token,
    baseUrl: "https://example.test/api",
    WebSocket: Socket,
    setConnection() {},
    Date: { now: () => now },
    Math,
    wsUrlFromApi: (value) => value.replace(/^http/, "ws") + "/ws",
    wsRef: { current: null },
    reconnectTimerRef: { current: null },
    activeRoomIdRef: { current: "room-a" },
    callActiveRef: { current: calling },
    onWsMessageRef: { current: async (value) => events.push(value) },
    resyncRef: {
      current: async () => {
        resyncs++;
      },
    },
    setTimeout(fn, delay) {
      const id = ++serial;
      timers.set(id, { fn, delay });
      return id;
    },
    clearTimeout(id) {
      timers.delete(id);
    },
    AppState: {
      currentState: background ? "background" : "active",
      addEventListener(_, fn) {
        listener = fn;
        return {
          remove() {
            listener = undefined;
          },
        };
      },
    },
  };
  const cleanup = loadModule(
    "../mobile/src/startConnection.ts",
    scope,
  ).startConnection(scope);
  return {
    scope,
    sockets,
    timers,
    events,
    cleanup,
    get resyncs() {
      return resyncs;
    },
    advance(ms) {
      now += ms;
    },
    state(value) {
      scope.AppState.currentState = value;
      listener?.(value);
    },
    retry() {
      assert.equal(timers.size, 1);
      const [id, { fn }] = timers.entries().next().value;
      timers.delete(id);
      fn();
    },
  };
}

test("WebSocket still reconnects after eleven minutes and rejoins the current room", () => {
  const h = connectionHarness();
  h.sockets[0].open();
  h.advance(11 * 60 * 1000);
  h.scope.activeRoomIdRef.current = "room-b";
  h.sockets[0].close();
  h.retry();
  h.sockets[1].open();
  assert.equal(h.sockets[1].url, "wss://example.test/api/ws");
  assert.equal(h.sockets[1].sent[0].token, "current-token");
  assert.equal(h.sockets[1].sent[1].room_id, "room-b");
  assert.equal(h.resyncs, 1);
  h.cleanup();
});

test("repeated foreground events do not create duplicate connecting or open sockets", () => {
  const h = connectionHarness();
  h.state("active");
  assert.equal(h.sockets.length, 1);
  h.sockets[0].open();
  h.state("active");
  h.state("active");
  assert.equal(h.sockets.length, 1);
  h.cleanup();
});

test("background pauses idle signaling and foreground reconnects and resynchronizes", () => {
  const h = connectionHarness();
  h.sockets[0].open();
  h.state("background");
  assert.equal(h.scope.wsRef.current, null);
  assert.equal(h.timers.size, 0);
  h.state("active");
  h.sockets[1].open();
  assert.equal(h.resyncs, 1);
  h.cleanup();
});

test("active or incoming calls keep signaling alive in the background", () => {
  const h = connectionHarness({ calling: true });
  h.sockets[0].open();
  h.state("background");
  assert.equal(h.sockets[0].readyState, 1);
  h.sockets[0].close();
  h.retry();
  assert.equal(h.sockets.length, 2);
  h.cleanup();
});

test("stale sockets cannot dispatch messages or schedule reconnects", () => {
  const h = connectionHarness();
  const old = h.sockets[0];
  old.open();
  h.state("background");
  h.state("active");
  h.sockets[1].open();
  old.receive({ type: "auth.required" });
  old.close();
  assert.equal(h.events.length, 0);
  assert.equal(h.timers.size, 0);
  h.sockets[1].receive({ type: "typing" });
  assert.equal(h.events.length, 1);
  h.cleanup();
});

test('initial auth.required is a handshake prompt; join waits for auth.ok', () => {
  const h = connectionHarness();
  h.sockets[0].open(false);
  assert.equal(h.sockets[0].sent.length, 1);
  h.sockets[0].receive({ type: 'auth.required' });
  assert.deepEqual(h.events, []);
  h.sockets[0].receive({ type: 'auth.ok' });
  assert.equal(h.sockets[0].sent[1].type, 'rooms.join');
  assert.equal(h.timers.size, 0);
  h.cleanup();
});
test('a stalled transport or authentication handshake closes and schedules reconnect', () => {
  const h = connectionHarness();
  const [id, deadline] = [...h.timers.entries()][0];
  assert.equal(deadline.delay, 15000);
  h.timers.delete(id); deadline.fn();
  assert.equal(h.sockets[0].readyState, 3);
  assert.equal(h.timers.size, 1);
  h.cleanup();
});
test('a network request that never completes aborts with a retryable timeout', async () => {
  const { requestWithTimeout } = loadModule('../mobile/src/requestTimeout.ts');
  let signal;
  await assert.rejects(requestWithTimeout(value => {
    signal = value;
    return new Promise(() => {});
  }, 5), error => error.code === 'ETIMEDOUT');
  assert.equal(signal.aborted, true);
  assert.equal(await requestWithTimeout(async () => 'ok', 100), 'ok');
});

test("effect cleanup cancels queued reconnects and removes the foreground listener", () => {
  const h = connectionHarness();
  h.sockets[0].open();
  h.sockets[0].close();
  assert.equal(h.timers.size, 1);
  h.cleanup();
  assert.equal(h.timers.size, 0);
  h.state("active");
  assert.equal(h.sockets.length, 1);
});

test("no authenticated socket is opened without a session or while initially backgrounded", () => {
  assert.equal(connectionHarness({ token: null }).sockets.length, 0);
  const h = connectionHarness({ background: true });
  assert.equal(h.sockets.length, 0);
  h.state("active");
  assert.equal(h.sockets.length, 1);
  h.cleanup();
});

test("401 handler clears the current session and ignores responses from an old session", () => {
  let handler,
    clears = 0,
    error;
  const cleanup = effectContaining("setUnauthorizedHandler", {
    tokenRef: { current: "new-token" },
    setUnauthorizedHandler(value) {
      handler = value;
    },
    clearSession: async () => {
      clears++;
    },
    setError(value) {
      error = value;
    },
  })();
  handler("old-token");
  assert.equal(clears, 0);
  handler("new-token");
  assert.equal(clears, 1);
  assert.match(error, /Сеанс завершён/);
  cleanup();
  assert.equal(handler, null);
});

test("API reports 401 once per token; an old failed request cannot suppress a new session failure", async () => {
  const apiModule = loadModule("../mobile/src/api.ts", {
    fetch: async () => ({
      ok: false,
      status: 401,
      text: async () => '{"error":"unauthorized"}',
    }),
  }, { './requestTimeout': loadModule('../mobile/src/requestTimeout.ts') });
  const tokens = [];
  apiModule.setUnauthorizedHandler((token) => tokens.push(token));
  for (const token of ["old-token", "old-token", "new-token", "new-token"]) {
    await assert.rejects(
      apiModule.api.me("https://example.test/api", token),
      /unauthorized/,
    );
  }
  assert.deepEqual(tokens, ["old-token", "new-token"]);
  apiModule.resetUnauthorizedGuard();
  await assert.rejects(
    apiModule.api.me("https://example.test/api", "new-token"),
  );
  assert.equal(tokens.length, 3);
});

function storageHarness(entries = {}, failWrite = false) {
  const values = new Map(Object.entries(entries));
  const module = loadModule(
    "../mobile/src/storage.ts",
    {},
    {
      "@react-native-async-storage/async-storage": {
        async getItem(key) {
          return values.get(key) ?? null;
        },
        async setItem(key, value) {
          if (failWrite) throw new Error("disk full");
          values.set(key, value);
        },
        async removeItem(key) {
          values.delete(key);
        },
      },
    },
  );
  return { values, ...module };
}

test("both old clients retain their session, with canonical token taking precedence", async () => {
  const h = storageHarness({ "auth.token": "legacy" });
  assert.equal(await h.restoreSessionToken(), "legacy");
  assert.equal(h.values.get("mobile.token"), "legacy");
  assert.equal(h.values.has("auth.token"), false);
  const canonical = storageHarness({
    "mobile.token": "current",
    "auth.token": "older",
  });
  assert.equal(await canonical.restoreSessionToken(), "current");
  await canonical.removeSessionTokens();
  assert.equal(await canonical.restoreSessionToken(), null);
});

test("failed migration keeps the legacy token so it can be retried without data loss", async () => {
  const h = storageHarness({ "auth.token": "legacy" }, true);
  await assert.rejects(h.restoreSessionToken(), /disk full/);
  assert.equal(h.values.get("auth.token"), "legacy");
});

function startupHarness(restoreSessionToken, bootstrap = async () => {}) {
  const state = { loading: true },
    tokenRef = { current: null };
  const environment = {
    storage: { get: async () => null },
    restoreSessionToken,
    tokenRef,
    bootstrap: (...args) => bootstrap(tokenRef, ...args),
    normalizeBaseUrl: (value) => value,
    DEFAULT_BASE_URL: "https://example.test/api",
    resetUnauthorizedGuard() {},
    errorText: (error) => error.message,
    setBaseUrl(value) {
      state.baseUrl = value;
    },
    setToken(value) {
      state.token = value;
    },
    setLoading(value) {
      state.loading = value;
    },
    setError(value) {
      state.error = value;
    },
    reconnectTimerRef: { current: null },
    wsRef: { current: null },
    audioRef: { current: null },
    recording: null,
  };
  loadModule(
    "../mobile/src/restoreSessionEffect.ts",
    environment,
  ).restoreSessionEffect(environment);
  return state;
}

test("restored token is available to the 401 handler before bootstrap can fail", async () => {
  const state = startupHarness(
    async () => "restored",
    async (tokenRef, baseUrl, token) => {
      assert.equal(tokenRef.current, token);
      assert.equal(token, "restored");
      assert.equal(baseUrl, "https://example.test/api");
    },
  );
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(state.token, "restored");
  assert.equal(state.loading, false);
});

test("storage errors leave the loading screen and expose a recoverable error", async () => {
  const state = startupHarness(async () => {
    throw new Error("disk unavailable");
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(state.loading, false);
  assert.equal(state.error, "disk unavailable");
});

test("a temporary bootstrap network error does not discard the restored session", async () => {
  const state = startupHarness(
    async () => "restored",
    async () => {
      throw new Error("offline");
    },
  );
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(state.token, "restored");
  assert.equal(state.error, "offline");
  assert.equal(state.loading, false);
});

test("logout immediately detaches signaling, cancels typing and deletes both session keys", async () => {
  const h = connectionHarness();
  h.sockets[0].open();
  const storage = storageHarness({
    "mobile.token": "current",
    "auth.token": "legacy",
  });
  const tokenRef = { current: "current" },
    typingTimersRef = { current: { peer: 12 } };
  const roomSecretCache = { current: new Map([["room-a", "secret"]]) };
  const clearedState = {};
  const scope = {
    endCall() { clearedState.callEnded = true; },
    ...h.scope,
    tokenRef,
    typingTimersRef,
    roomSecretCache,
    clearDerivedRoomKeys() {},
    local: { outbox: { current: { stop() {} } } },
    pushNotifications: { disconnect: async () => {} },
    storage: { del: async () => {} },
    removeSessionTokens: storage.removeSessionTokens,
  };
  for (const name of [
    "Token",
    "Me",
    "Rooms",
    "Messages",
    "ActiveRoomId",
    "ActiveRoomMembers",
    "TypingUsers",
  ]) {
    scope[`set${name}`] = (value) => {
      clearedState[name] = value;
    };
  }
  await appFunction("clearSession", scope)();
  assert.equal(clearedState.callEnded, true);
  assert.equal(tokenRef.current, null);
  assert.equal(h.scope.wsRef.current, null);
  assert.equal(h.timers.size, 0);
  assert.equal(roomSecretCache.current.size, 0);
  assert.equal(storage.values.size, 0);
  assert.equal(clearedState.Token, null);
  assert.equal(clearedState.Me, null);
  h.cleanup();
});
