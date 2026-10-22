import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import fs from 'node:fs';
import vm from 'node:vm';
import { webcrypto } from 'node:crypto';
const require = createRequire(new URL('../mobile/package.json', import.meta.url)), ts = require('typescript');
const nativeModule = { exports: {} };
vm.runInNewContext(ts.transpileModule(fs.readFileSync(new URL('../mobile/src/groupRatchet.ts', import.meta.url), 'utf8'), { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText, { module: nativeModule, exports: nativeModule.exports, require, crypto: webcrypto, Uint8Array, btoa, atob, setTimeout });
const mobile = nativeModule.exports, webModule = { exports: {} };
vm.runInNewContext(fs.readFileSync(new URL('../web/groupcrypto.js', import.meta.url), 'utf8'), { module: webModule, crypto: webcrypto, Uint8Array, TextEncoder, TextDecoder, btoa, atob });
const web = webModule.exports;
test('Android portable sender keys encrypt messages readable by the existing web protocol', async () => {
  const sender = await mobile.createSenderKey();
  const receiver = web.receiverStateFromDistribution(mobile.distributionFor(sender));
  const encrypted = await mobile.encryptWithState(sender, 'room', 'alice', 'Привет с Android 👋');
  assert.equal((await web.decryptWithState(receiver, 'room', 'alice', encrypted.envelope)).plaintext, 'Привет с Android 👋');
});
test('existing web sender keys including WebCrypto signatures decrypt on Android', async () => {
  let sender = await web.createSenderKey(); let receiver = mobile.receiverStateFromDistribution(web.distributionFor(sender)); const messages = [];
  for (const text of ['Первое', 'Второе', 'Третье']) { const result = await web.encryptWithState(sender, 'room', 'bob', text); sender = result.state; messages.push(result.envelope); }
  let result = await mobile.decryptWithState(receiver, 'room', 'bob', messages[2]); assert.equal(result.plaintext, 'Третье'); receiver = result.state;
  result = await mobile.decryptWithState(receiver, 'room', 'bob', messages[0]); assert.equal(result.plaintext, 'Первое');
  await assert.rejects(mobile.decryptWithState(result.state, 'room', 'bob', messages[0]), /message_key_consumed/);
});
test('Android does not advance a receiver for a forged envelope, room or far-future iteration', async () => {
  const sender = await web.createSenderKey(), receiver = mobile.receiverStateFromDistribution(web.distributionFor(sender)), initial = JSON.stringify(receiver);
  const { envelope } = await web.encryptWithState(sender, 'room', 'bob', 'Secret');
  await assert.rejects(mobile.decryptWithState(receiver, 'different-room', 'bob', envelope));
  await assert.rejects(mobile.decryptWithState(receiver, 'room', 'bob', { ...envelope, n: 1000000 }), /delta_too_large/);
  assert.equal(JSON.stringify(receiver), initial);
});
