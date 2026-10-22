// App lock: biometrics or the device passcode before the messenger opens.
//
// Telegram's "passcode lock" equivalent. The setting lives in the app's own
// storage (not SecureStore, because it is a preference, not a secret), and the
// gate is checked when the app starts and when it returns from the background
// after a grace period. Unlocking uses whatever the device offers —
// fingerprint, face or PIN — through expo-local-authentication, so this app
// never stores a credential of its own.

import { AppState, Platform } from "react-native";
import * as LocalAuthentication from "expo-local-authentication";
import { STORAGE_KEYS, storage } from "./storage";

const LOCK_KEY = "appLock.enabled";
/** Coming back from the background within this window does not re-prompt. */
const BACKGROUND_GRACE_MS = 60_000;

export type LockState = {
  enabled: boolean;
  available: boolean;
  biometry: LocalAuthentication.AuthenticationType[];
};

export async function readLockPreference(): Promise<boolean> {
  try {
    return (await storage.get(LOCK_KEY)) === "1";
  } catch {
    return false;
  }
}

export async function setLockPreference(enabled: boolean): Promise<void> {
  await storage.set(LOCK_KEY, enabled ? "1" : "0");
}

/** Whether the device can authenticate at all (biometry or passcode). */
export async function describeLock(): Promise<LockState> {
  if (Platform.OS === "web") return { enabled: false, available: false, biometry: [] };
  const [enabled, hasHardware, enrolled, biometry] = await Promise.all([
    readLockPreference(),
    LocalAuthentication.hasHardwareAsync().catch(() => false),
    LocalAuthentication.isEnrolledAsync().catch(() => false),
    LocalAuthentication.supportedAuthenticationTypesAsync().catch(() => []),
  ]);
  return { enabled, available: hasHardware && enrolled, biometry };
}

export function lockLabel(biometry: LocalAuthentication.AuthenticationType[]) {
  if (biometry.includes(LocalAuthentication.AuthenticationType.FACIAL_RECOGNITION)) {
    return "Разблокировать по лицу";
  }
  if (biometry.includes(LocalAuthentication.AuthenticationType.FINGERPRINT)) {
    return "Разблокировать отпечатком";
  }
  return "Разблокировать";
}

/**
 * Ask the device to authenticate. Returns true only on a real success; a
 * cancelled prompt keeps the app locked.
 */
export async function unlockApp(reason = "Разблокируйте 67chat"): Promise<boolean> {
  try {
    const result = await LocalAuthentication.authenticateAsync({
      promptMessage: reason,
      // Fall back to the device PIN/pattern, like Telegram does.
      disableDeviceFallback: false,
      cancelLabel: "Отмена",
    });
    return result.success === true;
  } catch {
    return false;
  }
}

/**
 * Re-lock after the app has been in the background long enough to matter.
 * Returns an unsubscribe function.
 */
export function watchBackground(onRelock: () => void) {
  let hiddenAt: number | null = null;
  const subscription = AppState.addEventListener("change", (next) => {
    if (next === "background" || next === "inactive") {
      hiddenAt = Date.now();
      return;
    }
    if (next === "active" && hiddenAt !== null) {
      const away = Date.now() - hiddenAt;
      hiddenAt = null;
      if (away > BACKGROUND_GRACE_MS) onRelock();
    }
  });
  return () => subscription.remove();
}

export { STORAGE_KEYS };
