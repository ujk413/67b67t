// E2E Encryption using libsignal-protocol (Signal Protocol)
// This will be loaded via CDN in index.html

class E2EManager {
  constructor() {
    this.store = null;
    this.address = null;
    this.ready = false;
    this._curveWrapper = null;
  }

  useStore(userId, deviceId, backend) {
    this.store = new SignalProtocolStore(backend);
    this.address = new libsignal.SignalProtocolAddress(userId, parseInt(deviceId));
  }

  async init(userId, deviceId = '1', persistentStore) {
    // Initialize Signal Protocol Store (IndexedDB-backed when provided)
    this.store = new SignalProtocolStore(persistentStore);
    this.address = new libsignal.SignalProtocolAddress(userId, parseInt(deviceId));

    // Generate identity key pair
    const identityKeyPair = await libsignal.KeyHelper.generateIdentityKeyPair();
    await this.store.put('identityKey', identityKeyPair);

    // Generate registration ID
    const registrationId = libsignal.KeyHelper.generateRegistrationId();
    await this.store.put('registrationId', registrationId);

    // Generate pre-keys (100 one-time pre-keys).
    // This libsignal build only exposes generatePreKey (singular), so loop.
    const preKeys = [];
    for (let i = 0; i < 100; i++) {
      const preKey = await libsignal.KeyHelper.generatePreKey(i);
      await this.store.storePreKey(preKey.keyId, preKey.keyPair);
      preKeys.push(preKey);
    }

    // Generate signed pre-key
    const signedPreKey = await libsignal.KeyHelper.generateSignedPreKey(identityKeyPair, 1);
    await this.store.storeSignedPreKey(signedPreKey.keyId, signedPreKey.keyPair);
    // Remember the signature so pre-key replenishment can re-publish it unchanged.
    // Persisted (not just in memory) so it survives reloads and is part of backups.
    this._signedPreKeySignature = this.arrayBufferToBase64(signedPreKey.signature);
    await this.store.put('signedPreKeySignature1', this._signedPreKeySignature);
    // Monotonic pre-key id counter: ids must never be reused, because the server
    // keeps consumed rows and ignores re-uploads of an existing (device, key_id).
    await this.store.put('nextPreKeyId', 100);

    this.ready = true;

    return {
      identityKey: this.arrayBufferToBase64(identityKeyPair.pubKey),
      registrationId,
      preKeys: preKeys.map(pk => ({
        keyId: pk.keyId,
        publicKey: this.arrayBufferToBase64(pk.keyPair.pubKey)
      })),
      signedPreKey: {
        keyId: signedPreKey.keyId,
        publicKey: this.arrayBufferToBase64(signedPreKey.keyPair.pubKey),
        signature: this.arrayBufferToBase64(signedPreKey.signature)
      }
    };
  }

  // Generate a fresh batch of one-time pre-keys and return a bundle payload the
  // caller can POST to /e2e/keys. Reuses the existing identity key and signed
  // pre-key so no established session is disturbed.
  async replenishPreKeys(userId, deviceId = '1') {
    if (!this.ready) throw new Error('E2E not initialized');
    const identityKeyPair = await this.store.getIdentityKeyPair();
    if (!identityKeyPair) throw new Error('No identity key');
    const registrationId = (await this.store.get('registrationId')) ?? 0;
    const signedPreKey = await this.store.loadSignedPreKey(1);

    // Pick key ids from a persisted monotonic counter (never reuse an id, even if
    // the local pre-key was consumed and removed). Also stay above any id still in
    // the store, for stores created before the counter existed.
    const existing = await this.store._backend()?.keys?.() || [];
    let maxId = -1;
    for (const k of existing) {
      const m = /^preKey(\d+)$/.exec(k);
      if (m) maxId = Math.max(maxId, parseInt(m[1], 10));
    }
    const counter = Number(await this.store.get('nextPreKeyId', 0)) || 0;
    const start = Math.max(counter, maxId + 1);
    const BATCH = 100; // server accepts at most 100 pre-keys per upload
    const preKeys = [];
    for (let i = 0; i < BATCH; i++) {
      const preKey = await libsignal.KeyHelper.generatePreKey(start + i);
      await this.store.storePreKey(preKey.keyId, preKey.keyPair);
      preKeys.push(preKey);
    }
    await this.store.put('nextPreKeyId', start + BATCH);

    const signature = this._signedPreKeySignature
      || (await this.store.get('signedPreKeySignature1', '')) || '';
    this._signedPreKeySignature = signature;

    return {
      identityKey: this.arrayBufferToBase64(identityKeyPair.pubKey),
      registrationId,
      preKeys: preKeys.map(pk => ({
        keyId: pk.keyId,
        publicKey: this.arrayBufferToBase64(pk.keyPair.pubKey)
      })),
      signedPreKey: signedPreKey
        ? {
            keyId: 1,
            publicKey: this.arrayBufferToBase64(signedPreKey.pubKey),
            // The signature is issued once at init and re-sent verbatim; the
            // pre-keys are the only material that actually rotates here.
            signature
          }
        : null
    };
  }

  async encryptMessage(recipientId, recipientDeviceId, plaintext) {
    if (!this.ready) throw new Error('E2E not initialized');

    const recipientAddress = new libsignal.SignalProtocolAddress(recipientId + ':' + recipientDeviceId, parseInt(recipientDeviceId));
    const sessionCipher = new libsignal.SessionCipher(this.store, recipientAddress);

    const ciphertext = await sessionCipher.encrypt(this.stringToArrayBuffer(plaintext));

    return {
      type: ciphertext.type, // 1 = PreKeyWhisperMessage, 3 = WhisperMessage
      body: this.arrayBufferToBase64(ciphertext.body),
      registrationId: ciphertext.registrationId
    };
  }

  async decryptMessage(senderId, senderDeviceId, ciphertext) {
    if (!this.ready) throw new Error('E2E not initialized');

    const senderAddress = new libsignal.SignalProtocolAddress(senderId + ':' + senderDeviceId, parseInt(senderDeviceId));
    const sessionCipher = new libsignal.SessionCipher(this.store, senderAddress);

    let plaintext;
    if (ciphertext.type === 1) {
      // WhisperMessage (normal message)
      plaintext = await sessionCipher.decryptWhisperMessage(this.base64ToArrayBuffer(ciphertext.body), 'binary');
    } else if (ciphertext.type === 3) {
      // PreKeyWhisperMessage (initial message)
      plaintext = await sessionCipher.decryptPreKeyWhisperMessage(this.base64ToArrayBuffer(ciphertext.body), 'binary');
    } else {
      throw new Error('Unknown message type');
    }

    return this.arrayBufferToString(plaintext);
  }

  async buildSession(recipientId, recipientDeviceId, recipientBundle) {
    // Build session from recipient's bundle (identity key, signed pre-key, one-time pre-key)
    const recipientAddress = new libsignal.SignalProtocolAddress(recipientId + ':' + recipientDeviceId, parseInt(recipientDeviceId));
    const sessionBuilder = new libsignal.SessionBuilder(this.store, recipientAddress);

    // CRITICAL: verify the signed pre-key signature against the claimed identity
    // key BEFORE building the session. Without this check a malicious or buggy
    // server can hand out any identity key + signed pre-key pair it likes and
    // silently MITM the conversation. libsignal's processPreKey does not verify
    // the signature itself when the caller supplies a raw bundle.
    const identityKey = this.base64ToArrayBuffer(recipientBundle.identityKey);
    const signedPreKeyPublic = this.base64ToArrayBuffer(recipientBundle.signedPreKey.publicKey);
    const signedPreKeySignature = this.base64ToArrayBuffer(recipientBundle.signedPreKey.signature);

    const signatureValid = await this.verifySignedPreKey(
      identityKey,
      signedPreKeyPublic,
      signedPreKeySignature
    );
    if (!signatureValid) {
      throw new Error('Signed pre-key signature verification failed — possible MITM');
    }

    // The identity key must also be the one we already pinned for this peer.
    // First contact pins it; any later change of key is surfaced as untrusted.
    let firstContact = false;
    const known = await this.store.get('identityKey' + recipientId + ':' + recipientDeviceId);
    if (!known) {
      firstContact = true;
    } else if (!this.store.arrayBufferEquals(identityKey, known)) {
      const err = new Error('IDENTITY_KEY_CHANGED');
      err.code = 'IDENTITY_KEY_CHANGED';
      throw err;
    }

    await sessionBuilder.processPreKey({
      registrationId: recipientBundle.registrationId,
      identityKey,
      signedPreKey: {
        keyId: recipientBundle.signedPreKey.keyId,
        publicKey: signedPreKeyPublic,
        signature: signedPreKeySignature
      },
      preKey: recipientBundle.preKey ? {
        keyId: recipientBundle.preKey.keyId,
        publicKey: this.base64ToArrayBuffer(recipientBundle.preKey.publicKey)
      } : undefined
    });

    // Pin the verified identity key for future change detection.
    await this.store.saveIdentity(recipientId + ':' + recipientDeviceId, identityKey);

    return { firstContact };
  }

  // Verify a Curve25519/XEdDSA signed pre-key signature.
  //
  // libsignal's `Curve.verifySignature` wrapper is unreliable (its underlying
  // `verify()` returns true when a signature is INVALID — inverted semantics,
  // see @privacyresearch/curve25519-typescript curve-wrapper docs). We therefore
  // use `Curve25519Wrapper.signatureIsValid`, which has correct semantics and
  // expects: raw 32-byte Montgomery public key, the 33-byte serialized (0x05 ||)
  // signed pre-key as the signed message, and the 64-byte signature.
  async verifySignedPreKey(identityKey, signedPreKeyPublic, signature) {
    const wrapper = await this._getCurveWrapper();
    if (!wrapper) {
      // Fail closed: without a verifier we must not trust the bundle.
      throw new Error('Curve25519 verifier unavailable — cannot verify pre-key');
    }
    const rawIdentity = this.toRawMontgomery(identityKey);           // 32 bytes
    const signedMessage = this.serializePublicKey(signedPreKeyPublic); // 33 bytes, 0x05 prefix
    return wrapper.signatureIsValid(rawIdentity, signedMessage, signature);
  }

  async _getCurveWrapper() {
    if (this._curveWrapper) return this._curveWrapper;
    const Ctor =
      (typeof Curve25519Wrapper !== 'undefined' && Curve25519Wrapper) ||
      (typeof window !== 'undefined' && window.Curve25519Wrapper) ||
      null;
    if (!Ctor || typeof Ctor.create !== 'function') return null;
    this._curveWrapper = await Ctor.create();
    return this._curveWrapper;
  }

  // Normalize to the 32-byte raw Montgomery public key (drop the 0x05 type byte).
  toRawMontgomery(raw) {
    const bytes = new Uint8Array(raw);
    if (bytes.length === 33 && bytes[0] === 0x05) return bytes.slice(1).buffer;
    if (bytes.length === 33) return bytes.slice(1).buffer;
    return bytes.buffer;
  }

  serializePublicKey(raw) {
    const bytes = new Uint8Array(raw);
    if (bytes.length === 33 && bytes[0] === 0x05) return bytes.buffer;
    const out = new Uint8Array(33);
    out[0] = 0x05; // DJB_TYPE
    out.set(bytes.slice(0, 32), 1);
    return out.buffer;
  }

  // Helper functions
  arrayBufferToBase64(buffer) {
    if (typeof buffer === 'string') return btoa(buffer);
    const bytes = ArrayBuffer.isView(buffer) ? new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength) : new Uint8Array(buffer);
    let binary = '';
    for (let i = 0; i < bytes.byteLength; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
  }

  base64ToArrayBuffer(base64) {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes.buffer;
  }

  stringToArrayBuffer(str) {
    const encoder = new TextEncoder();
    return encoder.encode(str).buffer;
  }

  arrayBufferToString(buffer) {
    const decoder = new TextDecoder();
    return decoder.decode(buffer);
  }

  // Backup and Recovery
  async exportKeys() {
    if (!this.ready) throw new Error('E2E not initialized');

    // Export all keys from store
    const backup = {
      identityKey: await this.store.get('identityKey'),
      registrationId: await this.store.get('registrationId'),
      preKeys: {},
      signedPreKeys: {},
      sessions: {},
      // Group sender keys (gk:<user>:<room>:<suffix>); absent in older backups.
      groupKeys: {}
    };

    // Pre-keys: ids 0..99 from init(), plus any later replenished batches.
    const allKeys = [];
    const be0 = this.store._backend();
    if (be0 && typeof be0.keys === 'function') allKeys.push(...await be0.keys());
    else allKeys.push(...Object.keys(this.store.mem));
    const preKeyIds = new Set();
    for (let i = 0; i < 100; i++) preKeyIds.add(i);
    for (const k of allKeys) {
      const m = /^preKey(\d+)$/.exec(k);
      if (m) preKeyIds.add(parseInt(m[1], 10));
    }
    for (const i of preKeyIds) {
      const preKey = await this.store.loadPreKey(i);
      if (preKey) {
        backup.preKeys[i] = preKey;
      }
    }

    // Replenishment bookkeeping and pinned peer identities.
    backup.meta = {
      signedPreKeySignature: await this.store.get('signedPreKeySignature1', undefined),
      nextPreKeyId: await this.store.get('nextPreKeyId', undefined)
    };
    backup.peerIdentities = {};
    for (const k of allKeys) {
      if (k.startsWith('identityKey') && k !== 'identityKey') {
        backup.peerIdentities[k] = await this.store.get(k);
      }
    }

    // Group sender keys: our own chains and the inbound chains from other members.
    for (const k of allKeys) {
      if (k.startsWith('gk:')) backup.groupKeys[k] = await this.store.get(k);
    }

    // Signed pre-key (id 1 by convention).
    const signedPreKey = await this.store.loadSignedPreKey(1);
    if (signedPreKey) {
      backup.signedPreKeys[1] = signedPreKey;
    }

    // Sessions: enumerate the backing store and keep the ratchet records.
    const be = this.store._backend();
    if (be && typeof be.keys === 'function') {
      for (const key of await be.keys()) {
        if (key.startsWith('session')) {
          backup.sessions[key] = await be.get(key);
        }
      }
    } else {
      for (const key in this.store.mem) {
        if (key.startsWith('session')) backup.sessions[key] = this.store.mem[key];
      }
    }

    return backup;
  }

  async importKeys(backup) {
    // Reuse the currently active backend (IndexedDB) but clear prior state first.
    const be = this.store ? this.store._backend() : null;
    if (be && typeof be.keys === 'function') {
      // Keep the locally saved backup record (backup:<userId>) intact.
      for (const key of await be.keys()) {
        if (!key.startsWith('backup:')) await be.remove(key);
      }
    }
    this.store = new SignalProtocolStore(this.store ? this.store.persistent : null);

    await this.store.put('identityKey', backup.identityKey);
    await this.store.put('registrationId', backup.registrationId);

    // Import pre-keys
    for (const keyId in backup.preKeys) {
      await this.store.storePreKey(parseInt(keyId), backup.preKeys[keyId]);
    }

    // Import signed pre-keys
    for (const keyId in backup.signedPreKeys) {
      await this.store.storeSignedPreKey(parseInt(keyId), backup.signedPreKeys[keyId]);
    }

    // Import sessions
    for (const key in backup.sessions) {
      await this.store.put(key, backup.sessions[key]);
    }

    for (const key in (backup.groupKeys || {})) {
      await this.store.put(key, backup.groupKeys[key]);
    }

    // Replenishment bookkeeping and pinned peer identities (absent in old backups).
    const meta = backup.meta || {};
    if (meta.signedPreKeySignature) {
      await this.store.put('signedPreKeySignature1', meta.signedPreKeySignature);
      this._signedPreKeySignature = meta.signedPreKeySignature;
    }
    if (meta.nextPreKeyId != null) await this.store.put('nextPreKeyId', meta.nextPreKeyId);
    for (const key in (backup.peerIdentities || {})) {
      await this.store.put(key, backup.peerIdentities[key]);
    }

    this.ready = true;
  }

  async generateRecoveryPhrase() {
    // Export keys and convert to recovery phrase
    const backup = await this.exportKeys();
    const backupJson = JSON.stringify(backup);

    // Convert to base64
    const backupBase64 = btoa(backupJson);

    // Generate checksum
    const encoder = new TextEncoder();
    const data = encoder.encode(backupBase64);
    const hashBuffer = await crypto.subtle.digest('SHA-256', data);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const checksum = hashArray.slice(0, 4).map(b => b.toString(16).padStart(2, '0')).join('');

    return {
      phrase: backupBase64,
      checksum: checksum,
      timestamp: Date.now()
    };
  }

  async restoreFromRecoveryPhrase(phrase, checksum) {
    // Verify checksum
    const encoder = new TextEncoder();
    const data = encoder.encode(phrase);
    const hashBuffer = await crypto.subtle.digest('SHA-256', data);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const computedChecksum = hashArray.slice(0, 4).map(b => b.toString(16).padStart(2, '0')).join('');

    if (computedChecksum !== checksum) {
      throw new Error('Invalid recovery phrase: checksum mismatch');
    }

    // Decode and import
    const backupJson = atob(phrase);
    const backup = JSON.parse(backupJson);

    await this.importKeys(backup);
  }

  // Public half of the currently stored key material, in the shape POST /e2e/keys
  // expects (camelCase, like init()). Used to re-publish after restoring a backup.
  // Local one-time pre-keys that were already consumed are removed by libsignal,
  // so what remains in the store is what is still unused.
  async getPublicBundle(maxPreKeys = 100) {
    if (!this.ready) throw new Error('E2E not initialized');
    const identityKeyPair = await this.store.getIdentityKeyPair();
    if (!identityKeyPair) throw new Error('No identity key');
    const registrationId = (await this.store.get('registrationId')) ?? 0;
    const signedPreKey = await this.store.loadSignedPreKey(1);
    const signature = this._signedPreKeySignature
      || (await this.store.get('signedPreKeySignature1', '')) || '';

    const be = this.store._backend();
    const names = be && typeof be.keys === 'function' ? await be.keys() : Object.keys(this.store.mem);
    const ids = names
      .map(k => /^preKey(\d+)$/.exec(k))
      .filter(Boolean)
      .map(m => parseInt(m[1], 10))
      .sort((a, b) => a - b)
      .slice(0, maxPreKeys);
    const preKeys = [];
    for (const id of ids) {
      const pk = await this.store.loadPreKey(id);
      if (pk) preKeys.push({ keyId: id, publicKey: this.arrayBufferToBase64(pk.pubKey) });
    }

    return {
      identityKey: this.arrayBufferToBase64(identityKeyPair.pubKey),
      registrationId,
      preKeys,
      signedPreKey: signedPreKey
        ? { keyId: 1, publicKey: this.arrayBufferToBase64(signedPreKey.pubKey), signature }
        : null
    };
  }

  // --- Passphrase-encrypted key backup (file) ---
  //
  // Format (JSON, versioned): { format, v, kdf, hash, iter, salt, iv, ct, created }
  // Key = PBKDF2-SHA256(passphrase, salt, iter) -> AES-256-GCM. The plaintext is the
  // exportKeys() payload with binary fields tagged so they survive JSON.
  static get BACKUP_FORMAT() { return 'messenger67-e2e-backup'; }
  static get BACKUP_VERSION() { return 1; }
  static get BACKUP_PBKDF2_ITER() { return 310000; }
  static get BACKUP_MIN_ITER() { return 210000; }
  static get BACKUP_MAX_ITER() { return 5000000; }

  _backupReplacer(_key, value) {
    if (Object.prototype.toString.call(value) === '[object ArrayBuffer]') {
      return { __b64: this.arrayBufferToBase64(value) };
    }
    if (ArrayBuffer.isView(value)) {
      const u8 = new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
      return { __b64: this.arrayBufferToBase64(u8) };
    }
    return value;
  }

  _backupReviver(_key, value) {
    if (value && typeof value === 'object' && typeof value.__b64 === 'string' && Object.keys(value).length === 1) {
      return this.base64ToArrayBuffer(value.__b64);
    }
    return value;
  }

  async _deriveBackupKey(passphrase, salt, iterations) {
    const material = await crypto.subtle.importKey(
      'raw', new TextEncoder().encode(passphrase), 'PBKDF2', false, ['deriveKey']
    );
    return crypto.subtle.deriveKey(
      { name: 'PBKDF2', hash: 'SHA-256', salt, iterations },
      material,
      { name: 'AES-GCM', length: 256 },
      false,
      ['encrypt', 'decrypt']
    );
  }

  // Returns a JSON string ready to be saved as a file.
  async exportKeysBackup(passphrase, meta = {}) {
    if (!this.ready) throw new Error('E2E not initialized');
    if (typeof passphrase !== 'string' || passphrase.length < 8) {
      throw new Error('Passphrase must be at least 8 characters');
    }
    const iter = E2EManager.BACKUP_PBKDF2_ITER;
    const payload = {
      user_id: meta.userId ?? null,
      device_id: meta.deviceId ?? null,
      keys: await this.exportKeys()
    };
    const plaintext = new TextEncoder().encode(
      JSON.stringify(payload, (k, v) => this._backupReplacer(k, v))
    );

    const salt = crypto.getRandomValues(new Uint8Array(16));
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const key = await this._deriveBackupKey(passphrase, salt, iter);
    const aad = new TextEncoder().encode(`${E2EManager.BACKUP_FORMAT}|v${E2EManager.BACKUP_VERSION}|${iter}`);
    const ct = await crypto.subtle.encrypt({ name: 'AES-GCM', iv, additionalData: aad }, key, plaintext);

    return JSON.stringify({
      format: E2EManager.BACKUP_FORMAT,
      v: E2EManager.BACKUP_VERSION,
      kdf: 'PBKDF2',
      hash: 'SHA-256',
      iter,
      salt: this.arrayBufferToBase64(salt),
      iv: this.arrayBufferToBase64(iv),
      ct: this.arrayBufferToBase64(ct),
      created: new Date().toISOString()
    }, null, 2);
  }

  // Decrypts and installs a backup produced by exportKeysBackup().
  // `blob` is the file text (string) or the parsed object. Returns { userId, deviceId }.
  // Throws Error('BAD_FORMAT' | 'BAD_PASSPHRASE' | 'USER_MISMATCH') on failure and
  // leaves current keys untouched in that case.
  async importKeysBackup(blob, passphrase, expect = {}) {
    let env;
    try { env = typeof blob === 'string' ? JSON.parse(blob) : blob; } catch { throw new Error('BAD_FORMAT'); }
    if (!env || env.format !== E2EManager.BACKUP_FORMAT || env.v !== E2EManager.BACKUP_VERSION
        || env.kdf !== 'PBKDF2' || env.hash !== 'SHA-256') {
      throw new Error('BAD_FORMAT');
    }
    const iter = Number(env.iter);
    if (!Number.isInteger(iter) || iter < E2EManager.BACKUP_MIN_ITER || iter > E2EManager.BACKUP_MAX_ITER) {
      throw new Error('BAD_FORMAT');
    }
    if (typeof passphrase !== 'string' || !passphrase) throw new Error('BAD_PASSPHRASE');

    let payload;
    try {
      const salt = new Uint8Array(this.base64ToArrayBuffer(env.salt));
      const iv = new Uint8Array(this.base64ToArrayBuffer(env.iv));
      const ct = this.base64ToArrayBuffer(env.ct);
      const key = await this._deriveBackupKey(passphrase, salt, iter);
      const aad = new TextEncoder().encode(`${env.format}|v${env.v}|${iter}`);
      const plain = await crypto.subtle.decrypt({ name: 'AES-GCM', iv, additionalData: aad }, key, ct);
      payload = JSON.parse(new TextDecoder().decode(plain), (k, v) => this._backupReviver(k, v));
    } catch {
      // AES-GCM auth failure (wrong passphrase or tampered file) or malformed fields.
      throw new Error('BAD_PASSPHRASE');
    }

    const keys = payload?.keys;
    if (!keys || !keys.identityKey || keys.registrationId == null
        || !keys.preKeys || !keys.signedPreKeys || !keys.sessions) {
      throw new Error('BAD_FORMAT');
    }
    if (expect.userId != null && payload.user_id != null && String(payload.user_id) !== String(expect.userId)) {
      throw new Error('USER_MISMATCH');
    }

    await this.importKeys(keys);
    return { userId: payload.user_id ?? null, deviceId: payload.device_id ?? null };
  }

  // Persist the full key material to the IndexedDB keystore (not localStorage —
  // see keystore.js for why). Returns the backup object so callers can also push
  // an encrypted copy to the server if they choose.
  async saveToIndexedDB(userId) {
    const backup = await this.exportKeys();
    if (window.SigStore && typeof window.SigStore.put === 'function') {
      await window.SigStore.put('backup:' + userId, backup);
    }
    return backup;
  }

  async loadFromIndexedDB(userId) {
    // Live sessions are persisted after every ratchet operation. A backup can be older.
    if (this.store && await this.store.getIdentityKeyPair()) {
      this._signedPreKeySignature = await this.store.get('signedPreKeySignature1', '');
      this.ready = true;
      return true;
    }
    if (!window.SigStore || typeof window.SigStore.get !== 'function') return false;
    const backup = await window.SigStore.get('backup:' + userId, undefined);
    if (!backup) return false;
    await this.importKeys(backup);
    return true;
  }

  // Deprecated aliases kept so older callers keep working; both now route to
  // IndexedDB. Storing identity keys in localStorage is not acceptable.
  async saveToLocalStorage(userId) {
    return this.saveToIndexedDB(userId);
  }

  async loadFromLocalStorage(userId) {
    return this.loadFromIndexedDB(userId);
  }

  // Safety Numbers — a 60-digit fingerprint of both parties' identity keys.
  // Both sides must compute the identical number. We fork off Signal's scheme:
  // iterated SHA-512 over the ordered concatenation of both identity keys, then
  // the first 30 bytes rendered as decimal digits. The public identity keys are
  // passed in (not read from the store) so the same code runs on both devices.
  async computeSafetyNumber(myUserId, myIdentityKeyB64, theirUserId, theirIdentityKeyB64) {
    const myKey = this.base64ToArrayBuffer(myIdentityKeyB64);
    const theirKey = this.base64ToArrayBuffer(theirIdentityKeyB64);

    // Deterministic ordering so both sides hash the same bytes.
    const meFirst = String(myUserId) < String(theirUserId);
    const first = meFirst ? myKey : theirKey;
    const second = meFirst ? theirKey : myKey;

    const combined = new Uint8Array(
      new Uint8Array(first).length + new Uint8Array(second).length
    );
    combined.set(new Uint8Array(first), 0);
    combined.set(new Uint8Array(second), new Uint8Array(first).length);

    // Signal-style iterative hashing (matches the 5200-iteration fingerprint KDF).
    let hash = await crypto.subtle.digest('SHA-512', combined);
    const ITERATIONS = 5200;
    for (let i = 1; i < ITERATIONS; i++) {
      const salt = new Uint8Array(32);
      salt[0] = i & 0xff;
      salt[1] = (i >> 8) & 0xff;
      salt[2] = (i >> 16) & 0xff;
      salt[3] = (i >> 24) & 0xff;
      const input = new Uint8Array(hash.byteLength + salt.byteLength);
      input.set(new Uint8Array(hash), 0);
      input.set(salt, hash.byteLength);
      hash = await crypto.subtle.digest('SHA-512', input);
    }

    // Render first 30 bytes as 30 decimal values packed into 6 groups of 5.
    const bytes = new Uint8Array(hash).slice(0, 30);
    let digits = '';
    for (const b of bytes) digits += String(b).padStart(3, '0');
    const grouped = digits.match(/.{1,5}/g).join(' ');

    const qrData = `SAFETY:${meFirst ? myUserId : theirUserId}:${
      meFirst ? theirUserId : myUserId
    }:${this.arrayBufferToBase64(hash)}`;

    return { number: grouped, qrData, verified: false };
  }

  // Backwards-compatible shim: resolves the local identity key from the store
  // and delegates to computeSafetyNumber.
  async generateSafetyNumber(myUserId, theirUserId, theirIdentityKey) {
    if (!this.ready) throw new Error('E2E not initialized');
    const myPair = await this.store.getIdentityKeyPair();
    if (!myPair) throw new Error('No identity key');
    const mine = this.arrayBufferToBase64(myPair.pubKey);
    return this.computeSafetyNumber(myUserId, mine, theirUserId, theirIdentityKey);
  }

  async verifySafetyNumber(myUserId, theirUserId, theirIdentityKey, theirSafetyNumber) {
    const ourSafetyNumber = await this.generateSafetyNumber(
      myUserId, theirUserId, theirIdentityKey
    );
    return ourSafetyNumber.number === theirSafetyNumber;
  }
}

// Signal Protocol Store implementation.
//
// All records persist to IndexedDB via the keystore module, so identity keys
// and ratchet sessions survive reloads without ever touching localStorage.
// In-memory fallback is retained so the store still works in environments
// without IndexedDB (and before the keystore has finished loading).
class SignalProtocolStore {
  constructor(persistent) {
    this.mem = {};
    this.persistent = persistent && typeof persistent.get === 'function' ? persistent : null;
  }

  // Resolve the active backing functions (IndexedDB when available).
  _backend() {
    return this.persistent || window.SigStore || null;
  }

  async get(key, defaultValue) {
    const be = this._backend();
    if (be) {
      const v = await be.get(key, undefined);
      if (v !== undefined) return v;
      return defaultValue;
    }
    if (key in this.mem) return this.mem[key];
    return defaultValue;
  }

  async put(key, value) {
    this.mem[key] = value;
    const be = this._backend();
    if (be) await be.put(key, value);
  }

  async remove(key) {
    delete this.mem[key];
    const be = this._backend();
    if (be) await be.remove(key);
  }

  // Identity Key Store
  async getIdentityKeyPair() {
    return this.get('identityKey');
  }

  async getLocalRegistrationId() {
    return this.get('registrationId');
  }

  async isTrustedIdentity(identifier, identityKey, direction) {
    const trusted = await this.get('identityKey' + identifier);
    if (trusted === undefined) {
      return true;
    }
    return this.arrayBufferEquals(identityKey, trusted);
  }

  async saveIdentity(identifier, identityKey) {
    const existing = await this.get('identityKey' + identifier);
    if (existing && !this.arrayBufferEquals(identityKey, existing)) {
      return false;
    }
    await this.put('identityKey' + identifier, identityKey);
    return true;
  }

  // PreKey Store
  async loadPreKey(keyId) {
    const key = await this.get('preKey' + keyId);
    if (key) {
      return { pubKey: key.pubKey, privKey: key.privKey };
    }
    return undefined;
  }

  async storePreKey(keyId, keyPair) {
    await this.put('preKey' + keyId, keyPair);
  }

  async removePreKey(keyId) {
    await this.remove('preKey' + keyId);
  }

  // Signed PreKey Store
  async loadSignedPreKey(keyId) {
    const key = await this.get('signedPreKey' + keyId);
    if (key) {
      return { pubKey: key.pubKey, privKey: key.privKey };
    }
    return undefined;
  }

  async storeSignedPreKey(keyId, keyPair) {
    await this.put('signedPreKey' + keyId, keyPair);
  }

  async removeSignedPreKey(keyId) {
    await this.remove('signedPreKey' + keyId);
  }

  // Session Store
  async loadSession(identifier) {
    const session = await this.get('session' + identifier);
    return session || undefined;
  }

  async storeSession(identifier, record) {
    await this.put('session' + identifier, record);
  }

  async removeSession(identifier) {
    await this.remove('session' + identifier);
  }

  async removeAllSessions(identifier) {
    const be = this._backend();
    if (be && typeof be.keys === 'function') {
      for (const key of await be.keys()) {
        if (key.startsWith('session' + identifier)) await be.remove(key);
      }
    }
    for (const key in this.mem) {
      if (key.startsWith('session' + identifier)) delete this.mem[key];
    }
  }

  // Helper
  arrayBufferEquals(a, b) {
    if (a.byteLength !== b.byteLength) return false;
    const ua = new Uint8Array(a);
    const ub = new Uint8Array(b);
    for (let i = 0; i < ua.length; i++) {
      if (ua[i] !== ub[i]) return false;
    }
    return true;
  }
}

// Export for use in app
window.E2EManager = E2EManager;

// --- Media Encryption (E2E for files) ---
class MediaEncryption {
  constructor() {
    this.algorithm = { name: "AES-GCM", length: 256 };
  }

  async generateMediaKey() {
    return await crypto.subtle.generateKey(
      this.algorithm,
      true,
      ["encrypt", "decrypt"]
    );
  }

  async encryptFile(file) {
    // Generate random key for this file
    const key = await this.generateMediaKey();
    const iv = crypto.getRandomValues(new Uint8Array(12));

    // Read file as ArrayBuffer
    const fileData = await file.arrayBuffer();

    // Encrypt file data
    const encryptedData = await crypto.subtle.encrypt(
      { name: "AES-GCM", iv },
      key,
      fileData
    );

    // Export key for sharing
    const exportedKey = await crypto.subtle.exportKey("raw", key);

    return {
      encryptedData: new Uint8Array(encryptedData),
      key: new Uint8Array(exportedKey),
      iv: iv,
      originalName: file.name,
      originalType: file.type,
      originalSize: file.size
    };
  }

  async decryptFile(encryptedData, keyData, iv, mimeType) {
    // Import key
    const key = await crypto.subtle.importKey(
      "raw",
      keyData,
      this.algorithm,
      false,
      ["decrypt"]
    );

    // Decrypt
    const decryptedData = await crypto.subtle.decrypt(
      { name: "AES-GCM", iv },
      key,
      encryptedData
    );

    // Create blob
    return new Blob([decryptedData], { type: mimeType });
  }

  // Encrypt media key with E2E (for sharing with recipient)
  async encryptMediaKey(e2eManager, recipientId, mediaKey, iv) {
    const keyData = {
      key: this.arrayBufferToBase64(mediaKey),
      iv: this.arrayBufferToBase64(iv)
    };

    const encrypted = await e2eManager.encryptMessage(
      recipientId,
      '1',
      JSON.stringify(keyData)
    );

    return encrypted;
  }

  async decryptMediaKey(e2eManager, senderId, encryptedKeyData) {
    const plaintext = await e2eManager.decryptMessage(
      senderId,
      '1',
      encryptedKeyData
    );

    const keyData = JSON.parse(plaintext);
    return {
      key: this.base64ToArrayBuffer(keyData.key),
      iv: this.base64ToArrayBuffer(keyData.iv)
    };
  }

  arrayBufferToBase64(buffer) {
    const bytes = new Uint8Array(buffer);
    let binary = '';
    for (let i = 0; i < bytes.byteLength; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
  }

  base64ToArrayBuffer(base64) {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes.buffer;
  }
}

window.MediaEncryption = MediaEncryption;
