import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { pruneSecretVault } from "../web/secret-vault.js";
import { SecretPeerSelector } from '../web/feature-selectors.js';
const require = createRequire(import.meta.url),
  ts = require("../mobile/node_modules/typescript");
function action(name, scope) {
  const source = readFileSync(
      new URL("../mobile/src/features/AccountFeatures.tsx", import.meta.url),
      "utf8",
    ),
    ast = ts.createSourceFile(
      "AccountFeatures.tsx",
      source,
      ts.ScriptTarget.Latest,
      true,
      ts.ScriptKind.TSX,
    ),
    component = ast.statements.find(
      (n) => ts.isFunctionDeclaration(n) && n.name?.text === "AccountFeatures",
    ),
    declaration = component.body.statements
      .flatMap((n) =>
        ts.isVariableStatement(n) ? [...n.declarationList.declarations] : [],
      )
      .find((d) => d.name.getText(ast) === name);
  return new Function(
    ...Object.keys(scope),
    "return " +
      ts.transpileModule("(" + declaration.initializer.getText(ast) + ")", {
        compilerOptions: { target: ts.ScriptTarget.ES2022 },
      }).outputText,
  )(...Object.values(scope));
}
test("background during secret device binding cannot create a late ephemeral key/session", async () => {
  let resolveDevice,
    created = 0;
  const scope = {
    hooks: {
      deviceId: () =>
        new Promise((resolve) => {
          resolveDevice = resolve;
        }),
    },
    protectSecretScreen: async () => {},
    secretController: { current: null },
    secretSelector: {current:new SecretPeerSelector(async()=>[],()=>true)},
    redrawSelector() {},
    secretGeneration: { current: 0 },
    foreground: { current: true },
    alive: { current: true },
    DeviceSecretSession: class {
      constructor() {
        created++;
      }
      offer() {}
    },
    secret: { current: null },
    setSecretChat(value) {
      assert.equal(value, null);
    },
    setMessages() {},
    setValues() {},
    set() {},
  };
  const pending = action("beginSecret", scope)({ id: "chat" });
  await Promise.resolve();
  scope.foreground.current = false;
  scope.secretGeneration.current++;
  resolveDevice("device");
  await pending;
  assert.equal(created, 0);
  assert.equal(scope.secret.current, null);
});
test("late secret inbox cannot publish plaintext after switching session", async () => {
  let finish,
    cleared = false;
  const controller = {
      refresh: () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
      history() {
        throw Error("late plaintext");
      },
    },
    scope = {
      secretController: { current: controller },
      secretGeneration: { current: 1 },
      foreground: { current: true },
      alive: { current: true },
      setMessages() {
        throw Error("late plaintext");
      },
      clearSecret() {
        cleared = true;
      },
    };
  const pending = action("refreshSecret", scope)();
  scope.secretController.current = null;
  scope.secretGeneration.current++;
  finish();
  await pending;
  assert.equal(cleared, false);
});
test("background secret cleanup erases drafts, peer/verification inputs and fingerprint", () => {
  let values = {
      secret_message: "private draft",
      secret_peer_key: "public",
      secret_verify: "code",
      fingerprint: "code",
      other: "keep",
    },
    destroyed = false;
  const scope = {
    secretController: { current: null },
    secretSelector: {current:new SecretPeerSelector(async()=>[],()=>true)},
    redrawSelector() {},
    secretGeneration: { current: 0 },
    secret: {
      current: {
        destroy() {
          destroyed = true;
        },
      },
    },
    setMessages() {},
    setSecretChat() {},
    setValues: (fn) => {
      values = fn(values);
    },
    protectSecretScreen: async () => {},
  };
  action("clearSecret", scope)();
  assert.equal(destroyed, true);
  assert.deepEqual(values, { other: "keep" });
});

test("native secret list timer prunes expired vault records without an open controller", async () => {
  const source = readFileSync(
      new URL("../mobile/src/features/AccountFeatures.tsx", import.meta.url),
      "utf8",
    ),
    ast = ts.createSourceFile(
      "AccountFeatures.tsx",
      source,
      ts.ScriptTarget.Latest,
      true,
      ts.ScriptKind.TSX,
    );
  let callback;
  function visit(n) {
    if (
      ts.isVariableDeclaration(n) &&
      n.name.getText(ast) === "timer" &&
      ts.isCallExpression(n.initializer) &&
      n.initializer.expression.getText(ast) === "setInterval"
    )
      callback = n.initializer.arguments[0];
    ts.forEachChild(n, visit);
  }
  visit(ast);
  assert.ok(callback);
  let value = {
    v: 1,
    chats: { old: { expiresAt: Date.now() - 1, messages: [] } },
  };
  const repository = {
      get: async () => value,
      set: async (_key, next) => {
        value = next;
      },
    },
    scope = {
      setMessages: () => {},
      secretController: { current: null },
      secretStore: { current: repository },
      pruneSecretVault,
      foreground: { current: true },
      alive: { current: true },
    };
  const tick = new Function(
    ...Object.keys(scope),
    "return " +
      ts.transpileModule("(" + callback.getText(ast) + ")", {
        compilerOptions: { target: ts.ScriptTarget.ES2022 },
      }).outputText,
  )(...Object.values(scope));
  await tick();
  assert.deepEqual(value.chats, {});
});
