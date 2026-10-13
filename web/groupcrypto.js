// Group E2E (Sender Keys) over WebCrypto.
// Browser: exposes window.GroupCrypto. Node (tests): module.exports.
//
// Each member owns one sender key per group: a symmetric chain (HMAC-SHA256 ratchet)
// plus an ECDSA P-256 signing key. Messages are AES-GCM encrypted with a per-message
// key, bound to room/sender/key/iteration via AAD, and signed by the sender. The
// key is handed to each other member through the existing pairwise Signal sessions.
//
// Trust model: no per-message forward secrecy on the device (the chain is stored
// locally, same boundary as the identity key). Rotation happens when a member leaves
// so a removed member cannot read later messages.
(function (root) {
  const cryptoApi = root.crypto || globalThis.crypto;
  const subtle = cryptoApi.subtle;
  const enc = new TextEncoder();
  const dec = new TextDecoder();
  const MAX_DELTA = 5000;   // refuse to ratchet further than this in one step
  const MAX_SKIPPED = 2000; // cached out-of-order message keys per sender

  const b64 = (u8) => {
    let s = '';
    for (const b of u8) s += String.fromCharCode(b);
    return btoa(s);
  };
  const unb64 = (s) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));
  const concat = (a, b) => {
    const out = new Uint8Array(a.length + b.length);
    out.set(a, 0);
    out.set(b, a.length);
    return out;
  };
  const clone = (v) => JSON.parse(JSON.stringify(v));

  async function hmac(keyBytes, data) {
    const k = await subtle.importKey('raw', keyBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
    return new Uint8Array(await subtle.sign('HMAC', k, data));
  }

  // One ratchet step: per-message key material and the next chain key.
  async function step(chain) {
    const mk = await hmac(chain, Uint8Array.of(0x01));
    const next = await hmac(chain, Uint8Array.of(0x02));
    const iv = (await hmac(chain, Uint8Array.of(0x03))).slice(0, 12);
    return { mk, iv, next };
  }

  const aesKey = (mk, usage) => subtle.importKey('raw', mk, { name: 'AES-GCM' }, false, [usage]);
  const aadOf = (roomId, senderId, keyId, n) => enc.encode(`${roomId}|${senderId}|${keyId}|${n}`);
  const ecdsa = { name: 'ECDSA', namedCurve: 'P-256' };
  const ecdsaHash = { name: 'ECDSA', hash: 'SHA-256' };

  async function signingKeyPair() {
    const pair = await subtle.generateKey(ecdsa, true, ['sign', 'verify']);
    return {
      privJwk: await subtle.exportKey('jwk', pair.privateKey),
      pubRaw: b64(new Uint8Array(await subtle.exportKey('raw', pair.publicKey)))
    };
  }

  // Sender-side state: what this device needs to keep sending into the group.
  async function createSenderKey() {
    const chain = cryptoApi.getRandomValues(new Uint8Array(32));
    const keyId = b64(cryptoApi.getRandomValues(new Uint8Array(12)));
    return {
      keyId,
      genesis: b64(chain),
      chain: b64(chain),
      n: 0,
      sign: await signingKeyPair(),
      distributedTo: []
    };
  }

  // Encrypt one message; returns the envelope and the updated sender state.
  async function encryptWithState(state, roomId, senderId, plaintext) {
    const rec = clone(state);
    const { mk, iv, next } = await step(unb64(rec.chain));
    const n = rec.n;
    const aad = aadOf(roomId, senderId, rec.keyId, n);
    const ct = new Uint8Array(await subtle.encrypt({ name: 'AES-GCM', iv, additionalData: aad }, await aesKey(mk, 'encrypt'), enc.encode(plaintext)));
    const priv = await subtle.importKey('jwk', rec.sign.privJwk, ecdsa, false, ['sign']);
    const sig = new Uint8Array(await subtle.sign(ecdsaHash, priv, concat(aad, ct)));
    rec.chain = b64(next);
    rec.n = n + 1;
    return { envelope: { v: 1, kid: rec.keyId, n, ct: b64(ct), sig: b64(sig) }, state: rec };
  }

  // What a member needs to read this sender from iteration `n` on.
  function distributionFor(state) {
    return { v: 1, kid: state.keyId, chain: state.chain, n: state.n, sig_pub: state.sign.pubRaw };
  }

  function receiverStateFromDistribution(d) {
    if (!d || d.v !== 1 || !d.kid || !d.chain || !d.sig_pub || !Number.isInteger(d.n) || d.n < 0) {
      throw new Error('bad_distribution');
    }
    return { keyId: d.kid, chain: d.chain, n: d.n, sigPub: d.sig_pub, skipped: {} };
  }

  // Our own messages can be re-read from the genesis chain (used after a reload).
  function ownReceiverState(state) {
    return { keyId: state.keyId, chain: state.genesis, n: 0, sigPub: state.sign.pubRaw, skipped: {} };
  }

  // Decrypt one envelope; returns plaintext and the new receiver state. The input
  // state is never mutated, so a forged envelope cannot advance the chain.
  async function decryptWithState(state, roomId, senderId, envelope) {
    if (!envelope || envelope.v !== 1 || envelope.kid !== state.keyId) throw new Error('unknown_key');
    const n = envelope.n;
    if (!Number.isInteger(n) || n < 0) throw new Error('bad_iteration');

    const rec = clone(state);
    let mk;
    let iv;
    if (n < rec.n) {
      const cached = rec.skipped[n];
      if (!cached) throw new Error('message_key_consumed');
      mk = unb64(cached.mk);
      iv = unb64(cached.iv);
      delete rec.skipped[n];
    } else {
      if (n - rec.n > MAX_DELTA) throw new Error('delta_too_large');
      let chain = unb64(rec.chain);
      for (let i = rec.n; i < n; i++) {
        const s = await step(chain);
        rec.skipped[i] = { mk: b64(s.mk), iv: b64(s.iv) };
        chain = s.next;
      }
      const s = await step(chain);
      mk = s.mk;
      iv = s.iv;
      rec.chain = b64(s.next);
      rec.n = n + 1;
      const keys = Object.keys(rec.skipped).map(Number).sort((a, b) => a - b);
      for (const k of keys.slice(0, Math.max(0, keys.length - MAX_SKIPPED))) delete rec.skipped[k];
    }

    const ct = unb64(envelope.ct);
    const aad = aadOf(roomId, senderId, rec.keyId, n);
    const pub = await subtle.importKey('raw', unb64(rec.sigPub), ecdsa, false, ['verify']);
    const ok = await subtle.verify(ecdsaHash, pub, unb64(envelope.sig), concat(aad, ct));
    if (!ok) throw new Error('bad_signature');
    const pt = await subtle.decrypt({ name: 'AES-GCM', iv, additionalData: aad }, await aesKey(mk, 'decrypt'), ct);
    return { plaintext: dec.decode(pt), state: rec };
  }

  // Decide what a sender does before encrypting. Rotate when a recipient who already
  // holds the key is no longer a member; then list the members who still need it.
  function planSend(state, members, myId) {
    const memberSet = new Set(members.map(String));
    const rotate = !state || state.distributedTo.some((id) => !memberSet.has(id));
    const distributed = rotate ? [] : state.distributedTo;
    const pending = members.map(String).filter((id) => id !== String(myId) && !distributed.includes(id));
    return { rotate, pending };
  }

  const api = {
    createSenderKey,
    planSend,
    encryptWithState,
    decryptWithState,
    distributionFor,
    receiverStateFromDistribution,
    ownReceiverState,
    MAX_DELTA
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.GroupCrypto = api;
})(typeof window !== 'undefined' ? window : globalThis);
