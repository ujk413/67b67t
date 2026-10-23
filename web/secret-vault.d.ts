export interface SecretStore {
  get<T>(key: string, fallback: T): Promise<T>;
  set(key: string, value: unknown): Promise<unknown>;
}
export interface SecretMessage {
  id: string;
  text: string;
  expires_at: number;
  direction: "in" | "out";
  device_id?: string;
}
export interface SecretCryptoSession {
  publicKey: string;
  peerPublicKey?: string;
  fingerprint: string;
  offer(): string | Promise<string>;
  accept(key: string): string | Promise<string>;
  verify(code: string): void;
  encrypt(text: string, expiresAt: number): string | Promise<string>;
  decrypt(
    ciphertext: string,
    peerDeviceId?: string,
  ):
    | { text: string; expires_at: number; device_id: string }
    | Promise<{ text: string; expires_at: number; device_id: string }>;
  snapshot(): any | Promise<any>;
  restore(value: any): void | Promise<void>;
  destroy(): void;
}
export function eraseSecretVault(store: SecretStore): Promise<void>;
export function pruneSecretVault(store: SecretStore): Promise<void>;
export class SecretChatController {
  static open(options: {
    chat: Record<string, any>;
    userId: string;
    deviceId: string;
    store: SecretStore;
    Session: new (chatId: string, deviceId: string) => SecretCryptoSession;
    current: () => boolean;
    request: (
      path: string,
      options?: { method?: string; body?: unknown },
    ) => Promise<any>;
  }): Promise<SecretChatController>;
  readonly session: SecretCryptoSession;
  readonly closed: boolean;
  history(): SecretMessage[];
  accept(key: string): Promise<string>;
  verify(code: string): Promise<void>;
  send(text: string): Promise<void>;
  refresh(): Promise<SecretMessage[]>;
  inbox(): Promise<any[]>;
  prune(): Promise<void>;
  remove(): Promise<void>;
  close(): void;
}
