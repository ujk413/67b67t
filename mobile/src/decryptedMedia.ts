import { requireOptionalNativeModule } from "expo";
import * as Files from "expo-file-system/legacy";
import { sha256 } from "@noble/hashes/sha256";
import { bytesToHex, utf8ToBytes } from "@noble/hashes/utils";
import type { Message } from "./types";
import { trimMediaCache } from "./mediaFiles";
import { cancelAccountMedia, runMediaTask } from "./mediaLifecycle";
const native = requireOptionalNativeModule<{ decryptFileAsync(input: string, output: string, key: string, iv: string): Promise<string> }>("RoomKey");
const pending = new Map<string, Promise<string>>();
const progressListeners = new Map<string, Set<(value: number) => void>>();
let serial = 0;
export const cancelDecryptedMedia = (userId: string) => cancelAccountMedia(userId);
export async function eraseDecryptedMedia(userId: string) {
  if (!/^[a-f0-9-]{36}$/i.test(userId)) throw Error('Некорректный аккаунт');
  await cancelAccountMedia(userId, true);
  for (const path of [`decoded-${userId}/`, `media-downloads/${userId}/`])
    await Files.deleteAsync(`${Files.cacheDirectory}${path}`, { idempotent: true });
}
export async function resolveMediaSource(message: Message, baseUrl: string, userId: string, headers: Record<string, string>, decode: (message: Message) => Promise<Message>, onProgress?: (value: number) => void) {
  const info = message.meta?.media_encryption as { name: string; mime: string; key_message: Partial<Message> } | undefined;
  const remote = `${baseUrl}/media/${message.media_id}`;
  if (!info) return runMediaTask(userId, async () => ({ uri: remote, mime: message.media_mime || "application/octet-stream", name: String(message.meta?.filename || "67chat-media") }));
  if (!native) throw new Error("Для просмотра защищённого вложения установите новую APK 67chat");
  const origin = bytesToHex(sha256(utf8ToBytes(baseUrl))).slice(0, 24), id = `${userId}:${origin}:${message.media_id}`;
  if (onProgress) { let listeners = progressListeners.get(id); if (!listeners) { listeners = new Set(); progressListeners.set(id, listeners); } listeners.add(onProgress); }
  let task = pending.get(id);
  if (!task) {
    task = runMediaTask(userId, async ticket => {
      const directory = `${Files.cacheDirectory}decoded-${userId}/`;
      const ext = ({ "image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "video/mp4": "mp4", "video/webm": "webm", "audio/mp4": "m4a", "application/pdf": "pdf" } as Record<string, string>)[info.mime] || "bin";
      // Publish only complete decrypts; legacy cache entries could be partial.
      const output = `${directory}${origin}-${message.media_id}.verified.${ext}`;
      const temporary = `${output}.${++serial}.part`, input = `${temporary}.encrypted`;
      let unpause = () => {};
      try {
        const cached = await Files.getInfoAsync(output); ticket.check(); if (cached.exists) return output;
        await trimMediaCache((message.media_size || 0) * 2); ticket.check();
        await Files.makeDirectoryAsync(directory, { intermediates: true }); ticket.check();
        const decoded = await decode({ ...info.key_message, id: message.id + "-file-key", type: "text", room_id: message.room_id, sender_id: message.sender_id } as Message); ticket.check();
        let secret: { key: string; iv: string }; try { secret = JSON.parse(decoded.text || ""); } catch { throw new Error("Ключ вложения пока недоступен. Обновите переписку."); }
        const download = Files.createDownloadResumable(remote, input, { headers }, event => {
          if (event.totalBytesExpectedToWrite > 0) for (const listener of progressListeners.get(id) || []) listener(Math.min(1, event.totalBytesWritten / event.totalBytesExpectedToWrite));
        });
        unpause = ticket.onCancel(() => download.pauseAsync().catch(() => {}));
        const result = await ticket.wait(download.downloadAsync()); ticket.check();
        if (result?.status !== 200) throw new Error("Не удалось загрузить вложение");
        // Await the native writer even after cancellation so erasure can drain it.
        await native!.decryptFileAsync(input, temporary, secret.key, secret.iv); ticket.check();
        await Files.moveAsync({ from: temporary, to: output }); ticket.check();
        return output;
      } finally {
        unpause();
        await Promise.all([input, temporary].map(uri => Files.deleteAsync(uri, { idempotent: true }).catch(() => {})));
      }
    });
    pending.set(id, task);
    const done = () => { if (pending.get(id) === task) pending.delete(id); };
    void task.then(done, done);
  }
  try { return { uri: await task, mime: info.mime, name: info.name }; }
  finally { if (onProgress) { const listeners = progressListeners.get(id); listeners?.delete(onProgress); if (!listeners?.size) progressListeners.delete(id); } }
}
