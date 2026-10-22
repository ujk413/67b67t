import { hmac } from '@noble/hashes/hmac';
import { sha256 } from '@noble/hashes/sha2';
import { nativeRoomKdf } from './nativeRoomKdf';

// PBKDF2-HMAC-SHA256, one 32-byte output block (RFC 8018, section 5.2).
// noble 1.x's asyncLoop yields only microtasks. Timers here also give Hermes
// a chance to process input and paint while retaining the existing key format.
export async function portableRoomPbkdf2(password: Uint8Array, salt: Uint8Array, iterations = 250000) {
  const prf = hmac.create(sha256, password);
  const first = new Uint8Array(salt.length + 4);
  first.set(salt); first[first.length - 1] = 1;
  let chain = prf.clone().update(first).digest();
  const output = chain.slice();
  let yieldedAt = Date.now();
  try {
    for (let iteration = 1; iteration < iterations; iteration++) {
      const next = prf.clone().update(chain).digest();
      chain.fill(0); chain = next;
      for (let index = 0; index < 32; index++) output[index] ^= chain[index];
      if (iteration % 512 === 0 && Date.now() - yieldedAt >= 8) {
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        yieldedAt = Date.now();
      }
    }
    return output;
  } finally {
    prf.destroy(); chain.fill(0); first.fill(0);
  }
}

export async function deriveRoomPbkdf2(password: Uint8Array, salt: Uint8Array) {
  const native = nativeRoomKdf(password, salt);
  if (native) return native;
  const subtle = (globalThis.crypto as any)?.subtle;
  if (subtle) {
    const key = await subtle.importKey('raw', password, 'PBKDF2', false, ['deriveBits']);
    return new Uint8Array(await subtle.deriveBits({ name: 'PBKDF2', hash: 'SHA-256', salt, iterations: 250000 }, key, 256));
  }
  return portableRoomPbkdf2(password, salt);
}
