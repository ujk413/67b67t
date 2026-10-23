import { test } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { verifyDeviceSignature } from "../api/account-integration.js";
const require = createRequire(import.meta.url),
  {
    Curve25519Wrapper,
  } = require("../mobile/node_modules/@privacyresearch/curve25519-typescript");
test("server accepts actual Java Curve25519.calculateSignature output and rejects changed message/key/signature", () => {
  const raw = "K0FtnYGcuS8/blZseSHqU5GnRlcEpmFCY4ktWS3vWSk=",
    message = Buffer.from(
      "NjdjaGF0LW5hdGl2ZS1kZXZpY2UtcHJvb2YtZml4dHVyZS12MQ==",
      "base64",
    ).toString(),
    signature =
      "TTLYlCkBiM7/xnCplloOIuuAdZXCykbe1wGBLsDaNRTZ4nU1BQKm5YCQHqtPmfU+N1uq4/DEqB6FNPNbd+oljA==";
  const serialized = Buffer.concat([
    Buffer.from([5]),
    Buffer.from(raw, "base64"),
  ]).toString("base64");
  assert.equal(verifyDeviceSignature(serialized, message, signature), true);
  assert.equal(
    verifyDeviceSignature(serialized, message + "changed", signature),
    false,
  );
  assert.equal(
    verifyDeviceSignature(
      serialized,
      message,
      Buffer.alloc(64).toString("base64"),
    ),
    false,
  );
  assert.equal(
    verifyDeviceSignature(
      Buffer.alloc(33).toString("base64"),
      message,
      signature,
    ),
    false,
  );
});
test("browser Curve25519Wrapper sign is verified with both serialized and raw identity keys", async () => {
  const c = await Curve25519Wrapper.create();
  for (const fill of [1, 17, 42, 99]) {
    const pair = c.keyPair(new Uint8Array(32).fill(fill).buffer),
      message = "server-nonce-user-session-device-" + fill,
      signature = Buffer.from(
        c.sign(pair.privKey, new TextEncoder().encode(message).buffer),
      ).toString("base64");
    assert.equal(
      verifyDeviceSignature(
        Buffer.from(pair.pubKey).toString("base64"),
        message,
        signature,
      ),
      true,
    );
    assert.equal(
      verifyDeviceSignature(
        Buffer.concat([Buffer.from([5]), Buffer.from(pair.pubKey)]).toString(
          "base64",
        ),
        message,
        signature,
      ),
      true,
    );
  }
});

test('web proof adapter signs the server-bound nonce with the current private key and refuses foreign account challenges',async()=>{const {createWebDeviceProof}=await import('../web/account-features.js'),{createHash}=await import('node:crypto'),curve=await Curve25519Wrapper.create(),pair=curve.keyPair(new Uint8Array(32).fill(81).buffer),publicKey=Buffer.concat([Buffer.from([5]),Buffer.from(pair.pubKey)]).toString('base64'),user='00000000-0000-4000-8000-000000000001';const e2e={address:{name:user},store:{get:async()=>pair},_getCurveWrapper:async()=>curve};const challenge=['67chat-device-proof-v1',user,'session','12',createHash('sha256').update(publicKey).digest('hex'),'a'.repeat(43)].join('\n');const proof=await createWebDeviceProof(async()=>({id:'proof',challenge}),e2e,'12',publicKey);assert.equal(verifyDeviceSignature(publicKey,challenge,proof.signature),true);await assert.rejects(createWebDeviceProof(async()=>({id:'proof',challenge:challenge.replace(user,'foreign-user')}),e2e,'12',publicKey),/Некорректное подтверждение/);});
