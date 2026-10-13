import { gcm } from "@noble/ciphers/aes";
import { utf8ToBytes, bytesToUtf8 } from "@noble/ciphers/utils";
import { pbkdf2 } from "@noble/hashes/pbkdf2";
import { sha256 } from "@noble/hashes/sha2";

function bytesToBase64(bytes: Uint8Array) {
  let binary = "";
  for (let i = 0; i < bytes.length; i += 1) binary += String.fromCharCode(bytes[i]);
  return globalThis.btoa(binary);
}

function base64ToBytes(value: string) {
  const binary = globalThis.atob(value);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

function stableRoomPassphrase(roomId: string, roomSecret: string) {
  return bytesToBase64(sha256(utf8ToBytes(`room-passphrase-v2:${roomId}:${roomSecret}`)));
}

function deriveKey(roomId: string, roomSecret: string) {
  const passphrase = stableRoomPassphrase(roomId, roomSecret);
  return pbkdf2(sha256, utf8ToBytes(passphrase), utf8ToBytes(`room-v2:${roomId}:${roomSecret}`), {
    c: 250000,
    dkLen: 32,
  });
}

export async function encryptRoomText(roomId: string, roomSecret: string, text: string) {
  const iv = new Uint8Array(12);
  crypto.getRandomValues(iv);
  const key = deriveKey(roomId, roomSecret);
  const encrypted = gcm(key, iv).encrypt(utf8ToBytes(text));
  return {
    text: bytesToBase64(encrypted),
    meta: {
      encrypted: true,
      e2ee_v: 2,
      iv: bytesToBase64(iv),
    },
  };
}

export async function decryptRoomText(roomId: string, roomSecret: string, cipherText: string, ivBase64: string) {
  const key = deriveKey(roomId, roomSecret);
  const plain = gcm(key, base64ToBytes(ivBase64)).decrypt(base64ToBytes(cipherText));
  return bytesToUtf8(plain);
}
