import { requireOptionalNativeModule } from 'expo';
import { bytesToUtf8 } from '@noble/ciphers/utils';
import { Platform } from 'react-native';

const nativeModule = requireOptionalNativeModule<{
  deriveAsync(password: string, saltBase64: string): Promise<string>;
}>('RoomKey');

export function nativeRoomKdf(password: Uint8Array, salt: Uint8Array): Promise<Uint8Array> | null {
  // PBKDF2WithHmacSHA256 is available from Android API 26. Keep the
  // existing portable path for the app's supported Android 7 devices.
  if (!nativeModule || Platform.OS !== 'android' || Number(Platform.Version) < 26) return null;
  let binary = '';
  for (const byte of salt) binary += String.fromCharCode(byte);
  return nativeModule.deriveAsync(bytesToUtf8(password), globalThis.btoa(binary)).then(encoded => {
    const decoded = globalThis.atob(encoded);
    if (decoded.length !== 32) throw new Error('Некорректный ключ переписки');
    return Uint8Array.from(decoded, char => char.charCodeAt(0));
  });
}
