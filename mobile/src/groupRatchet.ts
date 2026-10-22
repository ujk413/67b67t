import { gcm } from "@noble/ciphers/aes";
import { utf8ToBytes, bytesToUtf8 } from "@noble/ciphers/utils";
import { hmac } from "@noble/hashes/hmac";
import { sha256 } from "@noble/hashes/sha2";
import { p256 } from "@noble/curves/nist";
export type SenderState = { keyId: string; genesis: string; chain: string; n: number; sign: { privJwk: { d: string; [key: string]: string }; pubRaw: string }; distributedTo: string[] };
export type ReceiverState = { keyId: string; chain: string; n: number; sigPub: string; skipped: Record<string, { mk: string; iv: string }> };
export type Distribution = { v: number; kid: string; chain: string; n: number; sig_pub: string };
export type Envelope = { v: number; kid: string; n: number; ct: string; sig: string };
export const MAX_DELTA = 5000;
const MAX_SKIPPED = 2000;
const b64 = (bytes: Uint8Array) => globalThis.btoa(Array.from(bytes, byte => String.fromCharCode(byte)).join(""));
const unb64 = (value: string) => Uint8Array.from(globalThis.atob(value.replace(/-/g, "+").replace(/_/g, "/")), c => c.charCodeAt(0));
const url64 = (bytes: Uint8Array) => b64(bytes).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value));
const aadOf = (room: string, sender: string, key: string, n: number) => utf8ToBytes(`${room}|${sender}|${key}|${n}`);
const concat = (a: Uint8Array, b: Uint8Array) => { const result = new Uint8Array(a.length + b.length); result.set(a); result.set(b, a.length); return result; };
function step(chain: Uint8Array) { return { mk: hmac(sha256, chain, Uint8Array.of(1)), next: hmac(sha256, chain, Uint8Array.of(2)), iv: hmac(sha256, chain, Uint8Array.of(3)).slice(0, 12) }; }
export async function createSenderKey(): Promise<SenderState> {
  const chain = crypto.getRandomValues(new Uint8Array(32)), secret = p256.utils.randomPrivateKey(), pub = p256.getPublicKey(secret, false);
  return { keyId: b64(crypto.getRandomValues(new Uint8Array(12))), genesis: b64(chain), chain: b64(chain), n: 0,
    sign: { privJwk: { kty: "EC", crv: "P-256", d: url64(secret), x: url64(pub.slice(1,33)), y: url64(pub.slice(33)) }, pubRaw: b64(pub) }, distributedTo: [] };
}
export function planSend(state: SenderState | null, members: string[], myId: string) {
  const membership = new Set(members.map(String)), rotate = !state || state.distributedTo.some(id => !membership.has(id));
  return { rotate, pending: members.map(String).filter(id => id !== myId && (rotate || !state!.distributedTo.includes(id))) };
}
export function distributionFor(state: SenderState): Distribution { return { v: 1, kid: state.keyId, chain: state.chain, n: state.n, sig_pub: state.sign.pubRaw }; }
export function receiverStateFromDistribution(value: Distribution): ReceiverState {
  if (value?.v !== 1 || !value.kid || !value.chain || !value.sig_pub || !Number.isInteger(value.n) || value.n < 0) throw Error("bad_distribution");
  return { keyId: value.kid, chain: value.chain, n: value.n, sigPub: value.sig_pub, skipped: {} };
}
export function ownReceiverState(state: SenderState): ReceiverState { return { keyId: state.keyId, chain: state.genesis, n: 0, sigPub: state.sign.pubRaw, skipped: {} }; }
export async function encryptWithState(state: SenderState, room: string, sender: string, text: string) {
  const next = clone(state), derived = step(unb64(next.chain)), aad = aadOf(room, sender, next.keyId, next.n), ct = gcm(derived.mk, derived.iv, aad).encrypt(utf8ToBytes(text));
  const signature = p256.sign(sha256(concat(aad, ct)), unb64(next.sign.privJwk.d), { prehash: false, lowS: false }).toCompactRawBytes();
  const envelope: Envelope = { v: 1, kid: next.keyId, n: next.n, ct: b64(ct), sig: b64(signature) }; next.chain = b64(derived.next); next.n++;
  return { envelope, state: next };
}
export async function decryptWithState(state: ReceiverState, room: string, sender: string, envelope: Envelope) {
  if (!envelope || envelope.v !== 1 || envelope.kid !== state.keyId) throw Error("unknown_key");
  const n = envelope.n; if (!Number.isInteger(n) || n < 0) throw Error("bad_iteration");
  const next = clone(state); let mk: Uint8Array, iv: Uint8Array;
  if (n < next.n) { const cached = next.skipped[n]; if (!cached) throw Error("message_key_consumed"); mk = unb64(cached.mk); iv = unb64(cached.iv); delete next.skipped[n]; }
  else {
    if (n - next.n > MAX_DELTA) throw Error("delta_too_large"); let chain: Uint8Array = unb64(next.chain);
    for (let i = next.n; i < n; i++) { const derived = step(chain); next.skipped[i] = { mk: b64(derived.mk), iv: b64(derived.iv) }; chain = derived.next; if (i % 100 === 99) await new Promise(resolve => setTimeout(resolve, 0)); }
    const derived = step(chain); mk = derived.mk; iv = derived.iv; next.chain = b64(derived.next); next.n = n + 1;
    for (const id of Object.keys(next.skipped).map(Number).sort((a,b) => a-b).slice(0, Math.max(0,Object.keys(next.skipped).length-MAX_SKIPPED))) delete next.skipped[id];
  }
  const ct = unb64(envelope.ct), aad = aadOf(room, sender, next.keyId, n);
  if (!p256.verify(unb64(envelope.sig), sha256(concat(aad, ct)), unb64(next.sigPub), { prehash: false, lowS: false, format: "compact" })) throw Error("bad_signature");
  return { plaintext: bytesToUtf8(gcm(mk, iv, aad).decrypt(ct)), state: next };
}
