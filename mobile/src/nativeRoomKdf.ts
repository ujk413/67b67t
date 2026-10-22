// Browser/Node fallback. Metro selects .native.ts for the installed mobile app.
export function nativeRoomKdf(_password: Uint8Array, _salt: Uint8Array): Promise<Uint8Array> | null {
  return null;
}
