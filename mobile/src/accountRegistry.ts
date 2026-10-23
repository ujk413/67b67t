import * as SecureStore from "expo-secure-store";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { requireOptionalNativeModule } from "expo";
import { pbkdf2Async } from "@noble/hashes/pbkdf2";
import { sha256 } from "@noble/hashes/sha256";
import { hkdf } from "@noble/hashes/hkdf";
import { gcm } from "@noble/ciphers/aes";
import { x25519 } from "@noble/curves/ed25519";
export type RegisteredAccount = {
  id: string;
  baseUrl: string;
  userId: string;
  username: string;
};
const REGISTRY = "67chat.accounts.v1",
  ACTIVE = "67chat.active.v1";
const tokenKey = (id: string) => {
  if (!/^[A-Za-z0-9_-]{1,100}$/.test(id)) throw Error("Некорректный аккаунт");
  return "67chat.token." + id;
};
export class AccountRegistry {
  private sequence: Promise<unknown> = Promise.resolve();
  private serialized<T>(action: () => Promise<T>): Promise<T> {
    const task = this.sequence.then(action);
    this.sequence = task.catch(() => {});
    return task;
  }
  async list(): Promise<RegisteredAccount[]> {
    const value = await AsyncStorage.getItem(REGISTRY);
    if (!value) return [];
    const rows: unknown = JSON.parse(value);
    if (!Array.isArray(rows)) throw Error("Повреждён список аккаунтов");
    return rows.map((r: RegisteredAccount) => {
      tokenKey(r.id);
      if (
        typeof r.userId !== "string" ||
        typeof r.username !== "string" ||
        typeof r.baseUrl !== "string"
      )
        throw Error("Повреждён аккаунт");
      return {
        id: r.id,
        baseUrl: r.baseUrl,
        userId: r.userId,
        username: r.username,
      };
    });
  }
  async token(id: string) {
    return SecureStore.getItemAsync(tokenKey(id));
  }
  save(account: RegisteredAccount, token: string) {
    return this.serialized(async () => {
      tokenKey(account.id);
      const base = new URL(account.baseUrl);
      if (
        !["http:", "https:"].includes(base.protocol) ||
        base.username ||
        base.password ||
        base.search ||
        base.hash
      )
        throw Error("Некорректный сервер");
      if (!token) throw Error("Отсутствует сеанс");
      const clean = {
        id: account.id,
        baseUrl: account.baseUrl.replace(/\/$/, ""),
        userId: account.userId,
        username: account.username,
      };
      const all = await this.list();
      if (
        all.some(
          (a) =>
            a.id === clean.id &&
            (a.userId !== clean.userId || a.baseUrl !== clean.baseUrl),
        )
      )
        throw Error("Нельзя заменить владельца аккаунта");
      await SecureStore.setItemAsync(tokenKey(clean.id), token, {
        keychainAccessible: SecureStore.WHEN_UNLOCKED_THIS_DEVICE_ONLY,
      });
      await AsyncStorage.setItem(
        REGISTRY,
        JSON.stringify([...all.filter((a) => a.id !== clean.id), clean]),
      );
    });
  }
  activate(
    id: string,
    hooks: {
      close: () => Promise<void>;
      open: (account: RegisteredAccount, token: string) => Promise<void>;
    },
  ) {
    return this.serialized(async () => {
      const account = (await this.list()).find((a) => a.id === id),
        token = await this.token(id);
      if (!account || !token) throw Error("Сеанс аккаунта недоступен");
      await hooks.close();
      await AsyncStorage.removeItem(ACTIVE);
      await hooks.open(account, token);
      await AsyncStorage.setItem(ACTIVE, id);
      return account;
    });
  }
  async active() {
    return AsyncStorage.getItem(ACTIVE);
  }
  remove(id: string) {
    return this.serialized(async () => {
      await SecureStore.deleteItemAsync(tokenKey(id));
      const rows = await this.list();
      await AsyncStorage.setItem(
        REGISTRY,
        JSON.stringify(rows.filter((a) => a.id !== id)),
      );
      if ((await this.active()) === id) await AsyncStorage.removeItem(ACTIVE);
    });
  }
  /** Use this for repository/Signal/queue namespaces, including the server origin. */
  namespace(account: RegisteredAccount) {
    return Array.from(
      sha256(
        new TextEncoder().encode(
          new URL(account.baseUrl).origin + "|" + account.userId,
        ),
      ),
      (b) => b.toString(16).padStart(2, "0"),
    ).join("");
  }
}
export const accountRegistry = new AccountRegistry();
let installationSequence:Promise<unknown>=Promise.resolve();
/** Non-exportable per-installation revocation capability; tokens stay in SecureStore. */
export async function pushInstallationCapability(baseUrl?:string):Promise<{id:string;secret:string;origins:string[]}>{
 const task=installationSequence.then(async()=>{
  let id=await AsyncStorage.getItem('push.installation');const legacyInstallation=!!id;
  if(!id){id=Array.from(crypto.getRandomValues(new Uint8Array(16)),b=>b.toString(16).padStart(2,'0')).join('');await AsyncStorage.setItem('push.installation',id);}
  let secret=await SecureStore.getItemAsync('67chat.push-installation-secret.v1');
  if(!secret){secret=Array.from(crypto.getRandomValues(new Uint8Array(32)),b=>b.toString(16).padStart(2,'0')).join('');await SecureStore.setItemAsync('67chat.push-installation-secret.v1',secret,{keychainAccessible:SecureStore.WHEN_UNLOCKED_THIS_DEVICE_ONLY});}
  const origins=JSON.parse(await AsyncStorage.getItem('67chat.push-origins.v1')||'[]') as string[];
  if(legacyInstallation&&!origins.length){const prior=await AsyncStorage.getItem('mobile.baseUrl');if(prior){origins.push(prior);await AsyncStorage.setItem('67chat.push-origins.v1',JSON.stringify(origins));}}
  if(baseUrl&&!origins.includes(baseUrl)){origins.push(baseUrl);await AsyncStorage.setItem('67chat.push-origins.v1',JSON.stringify(origins));}
  return{id,secret,origins};
 });installationSequence=task.catch(()=>{});return task;
}
let pushTransportHooks:{pause?:()=>Promise<void>;resume?:()=>Promise<void>}={};
export function installPushTransportHooks(hooks:typeof pushTransportHooks){pushTransportHooks=hooks;}
export const mobileBackupCrypto = {
  random: (n: number) => crypto.getRandomValues(new Uint8Array(n)),
  derive: async (password: string, salt: Uint8Array, iter: number) =>
    new Uint8Array(
      await pbkdf2Async(sha256, new TextEncoder().encode(password), salt, {
        c: iter,
        dkLen: 32,
      }),
    ),
  encrypt: async (
    key: Uint8Array,
    iv: Uint8Array,
    aad: Uint8Array,
    data: Uint8Array,
  ) => new Uint8Array(gcm(key, iv, aad).encrypt(data)),
  decrypt: async (
    key: Uint8Array,
    iv: Uint8Array,
    aad: Uint8Array,
    data: Uint8Array,
  ) => new Uint8Array(gcm(key, iv, aad).decrypt(data)),
};
export type TransportPolicy = {
  mode: "direct" | "proxy" | "tor";
  host?: string;
  port?: number;
};
type Capabilities = {
  proxy?: boolean;
  tor?: boolean;
  failClosed?: boolean;
  dnsThroughProxy?: boolean;
  callsThroughProxy?: boolean;
  screenProtection?: boolean;
  globalNetworkLayer?: boolean;
  processRestartRequired?: boolean;
};
type SecurityNative = {
  capabilitiesAsync: () => Promise<Capabilities>;
  configureTransportAsync: (json: string) => Promise<void>;
  validateTransportAsync: (json:string) => Promise<void>;
  unregisterPushForTransportAsync:(policy:string,targets:string,installation:string,secret:string)=>Promise<void>;
  getTransportPolicyAsync: () => Promise<string>;
  restartTransportAsync: () => Promise<void>;
  assertCallsAllowed: () => void;
  setScreenProtectionAsync: (enabled: boolean) => Promise<void>;
};
const native = requireOptionalNativeModule<SecurityNative>("AccountSecurity");
let policy: TransportPolicy = { mode: "direct" };
let transportLoaded=false;
export async function transportCapabilities(): Promise<Capabilities> {
  return native?.capabilitiesAsync ? native.capabilitiesAsync() : {};
}
export async function configureTransport(value: TransportPolicy) {
  if (!["direct", "proxy", "tor"].includes(value.mode))
    throw Error("Неизвестный транспорт");
  const c=await transportCapabilities();
  if (value.mode !== "direct") {
    if (!native || !c[value.mode] || !c.failClosed || !c.dnsThroughProxy || !c.globalNetworkLayer)
      throw Error(
        "Эта APK не содержит fail-closed транспорт. Соединение не изменено.",
      );
    if (
      !value.host ||
      !Number.isInteger(value.port) ||
      value.port! < 1 ||
      value.port! > 65535
    )
      throw Error("Укажите адрес и порт прокси");
  }
  if(transportLoaded&&value.mode===policy.mode&&(value.mode==='direct'||value.host===policy.host&&value.port===policy.port))return;
  if(native){
    if(!c.processRestartRequired||!native.restartTransportAsync)throw Error('Нужна APK с полным перезапуском сетевого слоя');
    await native.validateTransportAsync(JSON.stringify(value));
    await pushTransportHooks.pause?.();
    try{
      const targets=[];
      for(const account of await accountRegistry.list()){const token=await accountRegistry.token(account.id);if(token)targets.push({baseUrl:account.baseUrl,token});}
      await unregisterPushForTransport(value,targets);
    }catch(error){await pushTransportHooks.resume?.().catch(()=>{});throw error;}
    await accountHooks.beforeTransportChange?.(value);
    await native.configureTransportAsync(JSON.stringify(value));
    policy=value;
    // Native persistence is authoritative and already blocks all new routes.
    // Restart even if the optional JS metadata write fails.
    try{await AsyncStorage.setItem("67chat.transport.v1",JSON.stringify(value));}finally{await native.restartTransportAsync();}
    return;
  }
  await AsyncStorage.setItem("67chat.transport.v1",JSON.stringify(value));
  policy = value;
}
export async function restoreTransport() {
  if(native?.getTransportPolicyAsync){
    const value=JSON.parse(await native.getTransportPolicyAsync()) as TransportPolicy;
    if(!['direct','proxy','tor'].includes(value.mode))throw Error('Сетевая политика повреждена. Прямое соединение заблокировано.');
    const c=await transportCapabilities();if(value.mode!=='direct'&&(!c[value.mode]||!c.failClosed||!c.dnsThroughProxy||!c.globalNetworkLayer))throw Error('Защищённый сетевой слой недоступен');
    policy=value;transportLoaded=true;return policy;
  }
  const saved = await AsyncStorage.getItem("67chat.transport.v1");
  if (saved) {
    policy = JSON.parse(saved);
    if(policy.mode!=='direct')throw Error('Обновите APK: защищённая сеть недоступна, прямой обход запрещён');
  }
  transportLoaded=true;return policy;
}
export function currentTransport() {
  return policy;
}
export async function unregisterPushForTransport(next:TransportPolicy,targets:{baseUrl:string;token:string}[]){
  if(next.mode==='direct')return;
  if(!native?.unregisterPushForTransportAsync)throw Error('Обновите APK для отзыва push через новый защищённый маршрут');
  const installation=await pushInstallationCapability();
  const rows=[...targets];
  for(const baseUrl of installation.origins)if(!rows.some(row=>row.baseUrl===baseUrl))rows.push({baseUrl,token:''});
  await native.unregisterPushForTransportAsync(JSON.stringify(next),JSON.stringify(rows),installation.id,installation.secret);
}
/** Uses the global native layer, including streaming/multipart and binary bodies. */
export async function transportFetch(
  url: string,
  options: RequestInit = {},
): Promise<Response> {
  if (policy.mode === "direct") return fetch(url, options);
  const c = await transportCapabilities();
  if (!native || !c[policy.mode] || !c.failClosed || !c.dnsThroughProxy || !c.globalNetworkLayer)
    throw Error("Прямой обход прокси запрещён");
  return fetch(url,options);
}
export async function assertCallsAllowed() {
  native?.assertCallsAllowed?.();
  if (policy.mode === "direct") return;
  const c = await transportCapabilities();
  if (!c.callsThroughProxy || !c.failClosed || !c.dnsThroughProxy)
    throw Error("Звонки отключены: прямой обход прокси запрещён");
}
export async function protectSecretScreen(enabled: boolean) {
  const capture = await import('expo-screen-capture');
  if(enabled)await capture.preventScreenCaptureAsync('67chat-secret');
  else await capture.allowScreenCaptureAsync('67chat-secret');
}
/** Snapshots must stay in encrypted device-local repository, outside all backups. */
export class DeviceSecretSession {
  private privateKey: Uint8Array | null = null;
  private key: Uint8Array | null = null;
  private seen = new Set<string>();
  private verified = false;
  peerPublicKey = "";
  publicKey = "";
  fingerprint = "";
  constructor(
    readonly chatId: string,
    readonly deviceId: string,
  ) {}
  offer() {
    if (!this.privateKey) {
      this.privateKey = crypto.getRandomValues(new Uint8Array(32));
      this.publicKey = b64(x25519.getPublicKey(this.privateKey));
    }
    return this.publicKey;
  }
  accept(peerKey: string) {
    this.verified = false;
    this.peerPublicKey = peerKey;
    this.offer();
    const peer = unb64(peerKey);
    if (peer.length !== 32) throw Error("Некорректный ключ устройства");
    const shared = x25519.getSharedSecret(this.privateKey!, peer);
    this.key = hkdf(
      sha256,
      shared,
      new TextEncoder().encode(this.chatId),
      new TextEncoder().encode("67chat-secret-v1"),
      32,
    );
    shared.fill(0);
    const sorted = [this.publicKey, peerKey].sort().join("|");
    this.fingerprint = Array.from(
      sha256(new TextEncoder().encode(this.chatId + "|" + sorted)),
      (b) => b.toString(16).padStart(2, "0"),
    )
      .join("")
      .match(/.{1,8}/g)!
      .join(" ");
    this.verified = false;
    return this.fingerprint;
  }
  verify(value: string) {
    if (value !== this.fingerprint || !this.key)
      throw Error("Код безопасности не совпадает");
    this.verified = true;
  }
  encrypt(text: string, expiresAt: number) {
    if (!this.key || !this.verified)
      throw Error("Сначала сравните код безопасности");
    if (
      !Number.isFinite(expiresAt) ||
      expiresAt <= Date.now() ||
      expiresAt > Date.now() + 86400000
    )
      throw Error("Некорректный срок");
    const iv = crypto.getRandomValues(new Uint8Array(12)),
      data = new TextEncoder().encode(
        JSON.stringify({
          text,
          expires_at: expiresAt,
          device_id: this.deviceId,
        }),
      );
    return b64(
      new TextEncoder().encode(
        JSON.stringify({
          v: 1,
          iv: b64(iv),
          ct: b64(
            gcm(this.key, iv, new TextEncoder().encode(this.chatId)).encrypt(
              data,
            ),
          ),
        }),
      ),
    );
  }
  decrypt(ciphertext: string, peerDeviceId?: string) {
    if (!this.key || !this.verified)
      throw Error("Сначала сравните код безопасности");
    const e = JSON.parse(new TextDecoder().decode(unb64(ciphertext)));
    if (e.v !== 1 || this.seen.has(e.iv)) throw Error("Повтор сообщения");
    const value = JSON.parse(
      new TextDecoder().decode(
        gcm(
          this.key,
          unb64(e.iv),
          new TextEncoder().encode(this.chatId),
        ).decrypt(unb64(e.ct)),
      ),
    );
    if (peerDeviceId && value.device_id !== peerDeviceId) throw Error("Некорректное устройство");
    if (typeof value.text !== "string" || !Number.isFinite(value.expires_at) || value.expires_at > Date.now()+86400000) throw Error("Некорректный срок");
    if (value.expires_at <= Date.now()) throw Object.assign(Error("Сообщение истекло"),{code:'secret_expired'});
    if (this.seen.size >= 1024) throw Error("Начните новый секретный чат");
    this.seen.add(e.iv);
    return value as { text: string; expires_at: number; device_id: string };
  }
  snapshot() {
    if(!this.privateKey)throw Error('Чат закрыт');
    return {v:1,chatId:this.chatId,deviceId:this.deviceId,privateKey:b64(this.privateKey),publicKey:this.publicKey,peerPublicKey:this.peerPublicKey,verified:this.verified,received:[...this.seen]};
  }
  restore(saved: ReturnType<DeviceSecretSession['snapshot']>) {
    if(saved?.v!==1||saved.chatId!==this.chatId||saved.deviceId!==this.deviceId)throw Error('Некорректное устройство');
    this.privateKey=unb64(saved.privateKey);
    if(this.privateKey.length!==32||b64(x25519.getPublicKey(this.privateKey))!==saved.publicKey)throw Error('Некорректный ключ устройства');
    this.publicKey=saved.publicKey;
    if(saved.peerPublicKey)this.accept(saved.peerPublicKey);
    this.seen=new Set(saved.received);
    if(saved.verified)this.verify(this.fingerprint);
  }
  destroy() {
    this.privateKey?.fill(0);
    this.key?.fill(0);
    this.privateKey = null;
    this.key = null;
    this.verified = false;
    this.seen.clear();
  }
}
const b64 = (bytes: Uint8Array) => {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
};
const unb64 = (s: string) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));
export type AccountNativeHooks = {
  beforeTransportChange: (policy:TransportPolicy) => Promise<void>;
  restoreSession: (
    credentials: RestoreCredentials,
  ) => Promise<Omit<RestoredAccountSession, "deviceId">>;
  onRestoredSession: (session: RestoredAccountSession) => Promise<void>;
  onRestoreFinished:()=>Promise<void>;
  exportKeys: () => Promise<Record<string, unknown>>;
  importKeys: (keys: Record<string, unknown>) => Promise<void>;
  switchAccount: (id: string) => Promise<void>;
  addAccount: () => Promise<void>;
  onDeleted: () => Promise<void>;
  deviceId: () => Promise<string>;
};
let accountHooks: Partial<AccountNativeHooks> = {};
export function installAccountNativeHooks(hooks: Partial<AccountNativeHooks>) {
  accountHooks = hooks;
}
const backupNative = requireOptionalNativeModule<{
  signalDeviceProofAsync: (user: string, challenge: string) => Promise<string>;
  signalClearAccountAsync: (user: string) => Promise<void>;
  signalPrepareBackupImportAsync: (
    user: string,
    keys: string,
  ) => Promise<string>;
  signalSignBackupProofAsync: (
    user: string,
    id: string,
    challenge: string,
  ) => Promise<string>;
  signalCommitBackupImportAsync: (user: string, id: string) => Promise<string>;
  signalFinishBackupImportAsync: (
    user: string,
    id: string,
    commit: boolean,
  ) => Promise<void>;
  signalRecoverBackupImportAsync: (
    user: string,
    commit: boolean,
  ) => Promise<void>;
  signalExportBackupKeysAsync: (user: string) => Promise<string>;
  signalImportBackupKeysAsync: (user: string, keys: string) => Promise<string>;
}>("RoomKey");
export function nativeBackupAvailable() {
  return Boolean(
    backupNative?.signalExportBackupKeysAsync &&
      backupNative?.signalImportBackupKeysAsync,
  );
}
export async function exportNativeBackupKeys(
  user: string,
): Promise<Record<string, unknown>> {
  if (!nativeBackupAvailable())
    throw Error("Установите APK с поддержкой совместимого бэкапа");
  return JSON.parse(await backupNative!.signalExportBackupKeysAsync(user));
}
export async function importNativeBackupKeys(
  user: string,
  keys: Record<string, unknown>,
): Promise<{ sessionsRebuilt: boolean; warning?: string }> {
  if (!nativeBackupAvailable())
    throw Error("Установите APK с поддержкой совместимого бэкапа");
  const encoded = JSON.stringify(keys, (_k, v) =>
    v instanceof ArrayBuffer
      ? { __b64: b64(new Uint8Array(v)) }
      : ArrayBuffer.isView(v)
        ? { __b64: b64(new Uint8Array(v.buffer, v.byteOffset, v.byteLength)) }
        : v,
  );
  return JSON.parse(
    await backupNative!.signalImportBackupKeysAsync(user, encoded),
  );
}
export function getAccountNativeHooks() {
  return accountHooks;
}
// Password restoration uses a fresh unbound bearer session; login must not bootstrap Signal.
export type RestoreCredentials = {
  password: string;
  totp?: string;
  backup_code?: string;
};
export type RestoredAccountSession = {
  user: { id: string; username: string };
  token: string;
  deviceId: string;
};
type RestoreRepository = {
  exportKeyState: () => Promise<Record<string, unknown>>;
  validateKeyState: (state: unknown) => unknown;
  replaceKeyState: (state: unknown) => Promise<void>;
  set: (key: string, value: unknown) => Promise<void>;
  get: <T>(key: string, fallback: T) => Promise<T>;
};
type RestoreRequest = (
  path: string,
  options: {
    method?: string;
    token: string;
    body?: unknown;
    bearerSession?: boolean;
  },
) => Promise<any>;
const RESTORE_PENDING = "67chat.restore.pending.v1",
  RESTORE_STATE = "account:restore:rollback";
const taggedKeys = (keys: Record<string, unknown>) =>
  JSON.stringify(keys, (_k, v) =>
    v instanceof ArrayBuffer
      ? { __b64: b64(new Uint8Array(v)) }
      : ArrayBuffer.isView(v)
        ? { __b64: b64(new Uint8Array(v.buffer, v.byteOffset, v.byteLength)) }
        : v,
  );
export async function signMobileDeviceProof(user: string, challenge: string) {
  if (!backupNative?.signalDeviceProofAsync)
    throw Error(
      "Обновите APK: требуется подтверждение владения ключом устройства",
    );
  return backupNative.signalDeviceProofAsync(user, challenge);
}
export async function clearNativeAccount(user: string) {
  if (!backupNative?.signalClearAccountAsync)
    throw Error("Обновите APK для полного удаления ключей аккаунта");
  await backupNative.signalClearAccountAsync(user);
}
export async function restoreAccountBackup(options: {
  baseUrl: string;
  user: { id: string; username: string };
  currentToken: string;
  currentDeviceId: string;
  keys: Record<string, unknown>;
  credentials: RestoreCredentials;
  repository: RestoreRepository;
  request: RestoreRequest;
  restoreSession: (
    credentials: RestoreCredentials,
  ) => Promise<Omit<RestoredAccountSession, "deviceId">>;
  onRestoredSession: (session: RestoredAccountSession) => Promise<void>;
}) {
  const { repository, keys, user } = options;
  if (
    !backupNative?.signalPrepareBackupImportAsync ||
    !backupNative.signalSignBackupProofAsync ||
    !backupNative.signalCommitBackupImportAsync ||
    !backupNative.signalFinishBackupImportAsync ||
    !backupNative.signalRecoverBackupImportAsync
  )
    throw Error("Установите APK с безопасным восстановлением резервной копии");
  if (await SecureStore.getItemAsync(RESTORE_PENDING))
    throw Error("Сначала завершите восстановление предыдущей попытки");
  if (!repository?.replaceKeyState)
    throw Error("Эта APK не поддерживает безопасный откат ключей групп");
  if (keys.mobile_key_state != null)
    repository.validateKeyState(keys.mobile_key_state);
  const original = await repository.exportKeyState();
  repository.validateKeyState(original);
  await repository.set(RESTORE_STATE, original);
  const stage = JSON.parse(
    await backupNative.signalPrepareBackupImportAsync(
      user.id,
      taggedKeys(keys),
    ),
  ) as {
    id: string;
    bundle: {
      identityKey: string;
      registrationId: number;
      signedPreKey: { keyId: number; publicKey: string; signature: string };
      preKeys: { keyId: number; publicKey: string }[];
    };
  };
  let pending: any = null,
    switched = false;
  try {
    const session = await options.restoreSession(options.credentials);
    if (
      !session.token ||
      session.token === options.currentToken ||
      session.user.id !== user.id
    )
      throw Error(
        "Для восстановления нужен новый сеанс этого аккаунта без публикации ключей",
      );
    const deviceId = String(
      2 + (crypto.getRandomValues(new Uint32Array(1))[0] % 2147483645),
    );
    pending = {
      baseUrl: options.baseUrl,
      user,
      oldToken: options.currentToken,
      oldDeviceId: options.currentDeviceId,
      token: session.token,
      deviceId,
      stageId: stage.id,
      restoreId: null,
      phase: "prepared",
    };
    await SecureStore.setItemAsync(RESTORE_PENDING, JSON.stringify(pending), {
      keychainAccessible: SecureStore.WHEN_UNLOCKED_THIS_DEVICE_ONLY,
    });
    const call = (path: string, body?: unknown) =>
      options.request(path, { method: "POST", token: session.token, body });
    const preflight = await call("/me/account/backup/preflight", {
      device_id: deviceId,
      identity_key: stage.bundle.identityKey,
    });
    pending.restoreId = preflight.restore_id;
    await SecureStore.setItemAsync(RESTORE_PENDING, JSON.stringify(pending));
    const challenge = await call("/e2e/device-proof", {
      device_id: deviceId,
      identity_key: stage.bundle.identityKey,
    });
    const signature = await backupNative.signalSignBackupProofAsync(
      user.id,
      stage.id,
      challenge.challenge,
    );
    await call("/e2e/keys", {
      device_id: deviceId,
      identity_key: stage.bundle.identityKey,
      registration_id: stage.bundle.registrationId,
      signed_pre_key: {
        key_id: stage.bundle.signedPreKey.keyId,
        public_key: stage.bundle.signedPreKey.publicKey,
        signature: stage.bundle.signedPreKey.signature,
      },
      pre_keys: stage.bundle.preKeys
        .slice(0, 100)
        .map((k) => ({ key_id: k.keyId, public_key: k.publicKey })),
      device_proof: { id: challenge.id, signature },
    });
    pending.phase = "committing";
    await SecureStore.setItemAsync(RESTORE_PENDING, JSON.stringify(pending));
    const result = JSON.parse(
      await backupNative.signalCommitBackupImportAsync(user.id, stage.id),
    );
    if (keys.mobile_key_state != null)
      await repository.replaceKeyState(keys.mobile_key_state);
    switched = true; // A throwing hook may already have persisted credentials.
    await options.onRestoredSession({
      user: session.user,
      token: session.token,
      deviceId,
    });
    const committed = {...pending,phase:"committed"};
    await SecureStore.setItemAsync(RESTORE_PENDING, JSON.stringify(committed));
    pending=committed;
    await backupNative.signalFinishBackupImportAsync(user.id, stage.id, true);
    await call("/me/account/backup/finalize", {});
    await repository.set(RESTORE_STATE, null);
    await SecureStore.deleteItemAsync(RESTORE_PENDING);
    return result as { sessionsRebuilt: boolean; warning?: string };
  } catch (error) {
    // Commit is durable once the journal says committed; network cleanup can retry at boot.
    if (pending?.phase === "committed"){
      await backupNative.signalRecoverBackupImportAsync?.(user.id,true);
      return{sessionsRebuilt:true,warning:"Ключи восстановлены. Завершение синхронизации повторится при следующем подключении."};
    }
    await backupNative.signalFinishBackupImportAsync(user.id, stage.id, false);
    await repository.replaceKeyState(original);
    if (switched)
      await options.onRestoredSession({
        user,
        token: options.currentToken,
        deviceId: options.currentDeviceId,
      });
    if (pending) {
      try {
        if (pending.restoreId)
          await options.request("/me/account/backup/abort", {
            method: "POST",
            token: options.currentToken,
            body: { restore_id: pending.restoreId },
          });
        else
          await options.request("/auth/logout", {
            method: "POST",
            token: pending.token,
            body: {},
          });
        await SecureStore.deleteItemAsync(RESTORE_PENDING);
      } catch {
        throw Error(
          "Восстановление отменено, прежние ключи сохранены. Подключитесь к серверу для отзыва незавершённого сеанса.",
        );
      }
    }
    await repository.set(RESTORE_STATE, null);
    throw error;
  }
}
export async function recoverPendingAccountRestore(options: {
  baseUrl: string;
  userId: string;
  repository: RestoreRepository;
  request: RestoreRequest;
  onRestoredSession: (session: RestoredAccountSession) => Promise<void>;
}) {
  const raw = await SecureStore.getItemAsync(RESTORE_PENDING);
  if (!raw) return;
  const p = JSON.parse(raw);
  if (p.user.id !== options.userId || p.baseUrl !== options.baseUrl)
    throw Error("Незавершённое восстановление принадлежит другому аккаунту");
  if (!backupNative?.signalRecoverBackupImportAsync)
    throw Error("Обновите APK для восстановления незавершённой операции");
  const committed = p.phase === "committed";
  await backupNative.signalRecoverBackupImportAsync(p.user.id, committed);
  if (committed) {
    await options.onRestoredSession({
      user: p.user,
      token: p.token,
      deviceId: p.deviceId,
    });
    try {
      await options.request("/me/account/backup/finalize", {
        method: "POST",
        token: p.token,
        body: {},
      });
    } catch {
      // The new keys and credentials are durable. Offline cleanup must not
      // prevent using them; the retained journal retries finalization at boot.
      return;
    }
  } else {
    const original = await options.repository.get<Record<
      string,
      unknown
    > | null>(RESTORE_STATE, null);
    if (original) await options.repository.replaceKeyState(original);
    await options.onRestoredSession({
      user: p.user,
      token: p.oldToken,
      deviceId: p.oldDeviceId,
    });
    if (p.restoreId)
      await options.request("/me/account/backup/abort", {
        method: "POST",
        token: p.oldToken,
        body: { restore_id: p.restoreId },
      });
    else
      await options.request("/auth/logout", {
        method: "POST",
        token: p.token,
        body: {},
      });
  }
  await options.repository.set(RESTORE_STATE, null);
  await SecureStore.deleteItemAsync(RESTORE_PENDING);
}

export async function pendingAccountRestoreOwner(){const raw=await SecureStore.getItemAsync(RESTORE_PENDING);if(!raw)return null;const value=JSON.parse(raw);return{userId:String(value.user.id),username:String(value.user.username),baseUrl:String(value.baseUrl)};}

export async function clearPendingAccountRestore(userId:string){
  const raw=await SecureStore.getItemAsync(RESTORE_PENDING);if(!raw)return;
  let owner:string|undefined;try{owner=JSON.parse(raw)?.user?.id;}catch{await SecureStore.deleteItemAsync(RESTORE_PENDING);return;}
  if(owner===userId)await SecureStore.deleteItemAsync(RESTORE_PENDING);
}
