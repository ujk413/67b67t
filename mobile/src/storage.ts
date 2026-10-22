import AsyncStorage from "@react-native-async-storage/async-storage";

export const storage = {
  get: (key: string) => AsyncStorage.getItem(key),
  set: (key: string, value: string) => AsyncStorage.setItem(key, value),
  del: (key: string) => AsyncStorage.removeItem(key),
};

export const STORAGE_KEYS = {
  baseUrl: "mobile.baseUrl",
  token: "mobile.token",
};

const LEGACY_TOKEN_KEY = "auth.token";

// Both released clients used these keys. Prefer the canonical session and only
// remove the legacy value once its replacement has been persisted successfully.
export async function restoreSessionToken(): Promise<string | null> {
  const token = await storage.get(STORAGE_KEYS.token);
  if (token) return token;
  const legacyToken = await storage.get(LEGACY_TOKEN_KEY);
  if (!legacyToken) return null;
  await storage.set(STORAGE_KEYS.token, legacyToken);
  await storage.del(LEGACY_TOKEN_KEY);
  return legacyToken;
}

export async function removeSessionTokens(): Promise<void> {
  await Promise.all([
    storage.del(STORAGE_KEYS.token),
    storage.del(LEGACY_TOKEN_KEY),
  ]);
}
