import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { eraseSecretVault } from "../web/secret-vault.js";
const require = createRequire(import.meta.url),
  ts = require("../mobile/node_modules/typescript");
const source = readFileSync(
    new URL("../mobile/App.tsx", import.meta.url),
    "utf8",
  ),
  ast = ts.createSourceFile(
    "App.tsx",
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TSX,
  );
function callback(name, scope) {
  let found;
  function visit(node) {
    if (ts.isFunctionDeclaration(node) && node.name?.text === name)
      found = node;
    if (ts.isPropertyAssignment(node) && node.name.getText(ast) === name)
      found = node.initializer;
    if (ts.isJsxAttribute(node) && node.name.getText(ast) === name)
      found = node.initializer.expression;
    ts.forEachChild(node, visit);
  }
  visit(ast);
  assert.ok(found, name);
  return new Function(
    ...Object.keys(scope),
    "return (" +
      ts
        .transpileModule(found.getText(ast), {
          compilerOptions: { target: ts.ScriptTarget.ES2022 },
        })
        .outputText.trim()
        .replace(/;$/, "") +
      ");",
  )(...Object.values(scope));
}
function fixture() {
  const trace = [],
    repository = {
      closed: false,
      async set(key, value) {
        if (this.closed) throw Error("Хранилище закрыто");
        assert.equal(key, "secret-vault:v1");
        assert.equal(value, null);
        trace.push("erase");
      },
      async privateFiles() {
        if (this.closed) throw Error("Хранилище закрыто");
        return ["private-file"];
      },
      async destroy() {
        this.closed = true;
        trace.push("destroy");
      },
      async close() {
        this.closed = true;
        trace.push("close");
      },
    };
  const scope = {
    me: { id: "alice" },
    token: "token-a",
    baseUrl: "https://server/api",
    tokenRef: { current: "token-a" },
    sessionGenerationRef: { current: 1 },
    local: {
      repository: { current: repository },
      outbox: { current: { stop: () => trace.push("stop") } },
    },
    eraseSecretVault,
    endCall: () => trace.push("end-call"),
    removeIncomingAccountShares: (account) =>
      trace.push("shares-erased:" + account),
    stopExpressiveLiveLocations: () => {},
    cancelDecryptedMedia: async () => {},
    eraseDecryptedMedia: async () => trace.push("media-erased"),
    clearNativeAccount: async () => trace.push("native-erased"),
    clearPendingAccountRestore: async () => {},
    forgetDetachedLogins: async () => {},
    removePrivateMedia: async (files) => {
      assert.deepEqual(files, ["private-file"]);
      trace.push("private-files-erased");
    },
    pushNotifications: {
      disconnect: async () => trace.push("push-disconnected"),
    },
    wsRef: { current: { close: () => trace.push("ws-closed") } },
    reconnectTimerRef: { current: null },
    typingTimersRef: { current: {} },
    roomSecretCache: { current: new Map() },
    signalSession: { current: {} },
    clearDerivedRoomKeys: () => {},
    pendingNativeCall: { current: null },
    callAccountPrompt: { current: null },
    accountRegistry: {
      active: async () => "alice",
      remove: async () => trace.push("account-removed"),
    },
    removeSessionTokens: async () => trace.push("tokens-removed"),
    storage: { del: async () => {} },
    api: { logout: async () => trace.push("server-logout") },
  };
  for (const key of [
    "Composer",
    "Workspace",
    "Forwarding",
    "Editing",
    "AuthChallenge",
    "ViewerMessage",
    "TypingUsers",
    "Token",
    "Me",
    "Rooms",
    "Messages",
    "ActiveRoomId",
    "ActiveRoomMembers",
    "AutoAnswerId",
  ])
    scope["set" + key] = () => {};
  scope.clearSession = (...args) => callback("clearSession", scope)(...args);
  scope.logout = (...args) => callback("logout", scope)(...args);
  return { scope, trace, repository };
}
for (const name of ["onDeleted", "onClearData"])
  test(`actual native ${name} erases the vault before destroying the repository and finishes cleanup`, async () => {
    const { scope, trace, repository } = fixture();
    await callback(name, scope)();
    assert.ok(
      trace.indexOf("erase") < trace.indexOf("destroy"),
      trace.join(","),
    );
    assert.ok(trace.includes("shares-erased:alice"));
    assert.ok(trace.includes("push-disconnected"));
    assert.ok(trace.includes("tokens-removed"));
    assert.ok(trace.includes("account-removed"));
    assert.ok(trace.includes("private-files-erased"));
    assert.equal(repository.closed, true);
    assert.equal(scope.tokenRef.current, null);
    if (name === "onDeleted") assert.ok(trace.includes("native-erased"));
    else assert.ok(trace.includes("server-logout"));
  });
test("switching retained native accounts keeps incoming-share journals and secret vault", async () => {
  const { scope, trace } = fixture();
  await scope.clearSession(true);
  assert.equal(trace.includes("shares-erased:alice"), false);
  assert.equal(trace.includes("erase"), false);
});
test("React account-effect cleanup cannot close the repository before destructive cleanup reads private files", async () => {
  const { scope, repository, trace } = fixture();
  scope.setToken = (value) => {
    if (value === null) void repository.close();
  };
  await callback("onClearData", scope)();
  assert.ok(
    trace.indexOf("private-files-erased") < trace.indexOf("close"),
    trace.join(","),
  );
});
