function randomBase64(size = 32) {
  const bytes = new Uint8Array(size);
  crypto.getRandomValues(bytes);
  let binary = "";
  for (let i = 0; i < bytes.length; i += 1) binary += String.fromCharCode(bytes[i]);
  return globalThis.btoa(binary);
}

export class E2EEncryption {
  private initialized = false;
  private bundle: {
    identityKey: string;
    registrationId: number;
    signedPreKey: { keyId: number; publicKey: string; signature: string };
  } | null = null;
  // Pre-key ids must be unique per device (server ignores duplicates), so keep them increasing.
  private nextPreKeyId = Math.floor(Date.now() / 1000) % 2000000000;

  async initialize(_userId: string): Promise<void> {
    this.initialized = true;
  }

  // Fresh batch of one-time pre-keys with new ids, reusing the identity/signed pre-key of this session.
  async getPublicKeys(count = 20): Promise<{
    identityKey: string;
    registrationId: number;
    preKeys: Array<{ keyId: number; publicKey: string }>;
    signedPreKey: { keyId: number; publicKey: string; signature: string };
  }> {
    if (!this.initialized) throw new Error("E2E not initialized");
    if (!this.bundle) {
      this.bundle = {
        identityKey: randomBase64(32),
        registrationId: Math.floor(Math.random() * 1000000),
        signedPreKey: {
          keyId: 1,
          publicKey: randomBase64(32),
          signature: randomBase64(64),
        },
      };
    }
    const preKeys = Array.from({ length: count }, () => ({
      keyId: this.nextPreKeyId++,
      publicKey: randomBase64(32),
    }));
    return { ...this.bundle, preKeys };
  }

  async encrypt(_recipientId: string, _plaintext: string): Promise<{ type: number; body: string }> {
    throw new Error("Signal sessions are disabled in the Android client; room encryption is used.");
  }

  async decrypt(_senderId: string, _ciphertext: { type: number; body: string }): Promise<string> {
    throw new Error("Signal sessions are disabled in the Android client; room encryption is used.");
  }
}

export const e2e = new E2EEncryption();
