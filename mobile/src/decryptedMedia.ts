import { requireOptionalNativeModule } from "expo";
import * as Files from "expo-file-system/legacy";
import type { Message } from "./types";
import { trimMediaCache } from "./mediaFiles";
const native = requireOptionalNativeModule<{ decryptFileAsync(input: string, output: string, key: string, iv: string): Promise<string> }>("RoomKey");
const pending = new Map<string, Promise<string>>();
export async function resolveMediaSource(message: Message, baseUrl: string, userId: string, headers: Record<string, string>, decode: (message: Message) => Promise<Message>) {
  const info = message.meta?.media_encryption as { name: string; mime: string; key_message: Partial<Message> } | undefined;
  const remote = `${baseUrl}/media/${message.media_id}`;
  if (!info) return { uri: remote, mime: message.media_mime || "application/octet-stream", name: String(message.meta?.filename || "67chat-media") };
  if (!native) throw new Error("Для просмотра защищённого вложения установите новую APK 67chat");
  const id = `${userId}:${message.media_id}`;
  let task = pending.get(id);
  if (!task) {
    task = (async () => {
      const directory = `${Files.cacheDirectory}decoded-${userId}/`;
      const ext = ({ "image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "video/mp4": "mp4", "video/webm": "webm", "audio/mp4": "m4a", "application/pdf": "pdf" } as Record<string, string>)[info.mime] || "bin";
      const output = `${directory}${message.media_id}.${ext}`;
      if ((await Files.getInfoAsync(output)).exists) return output;
      await trimMediaCache((message.media_size || 0) * 2);
      await Files.makeDirectoryAsync(directory, { intermediates: true });
      const decoded = await decode({ ...info.key_message, id: message.id + "-file-key", type: "text", room_id: message.room_id, sender_id: message.sender_id } as Message);
      let secret: { key: string; iv: string }; try { secret = JSON.parse(decoded.text || ""); } catch { throw new Error("Ключ вложения пока недоступен. Обновите переписку."); }
      const input = `${directory}${message.media_id}.encrypted`;
      try {
        const result = await Files.downloadAsync(remote, input, { headers });
        if (result.status !== 200) throw new Error("Не удалось загрузить вложение");
        return await native!.decryptFileAsync(input, output, secret.key, secret.iv);
      } finally { await Files.deleteAsync(input, { idempotent: true }).catch(() => {}); }
    })().finally(() => pending.delete(id));
    pending.set(id, task);
  }
  return { uri: await task, mime: info.mime, name: info.name };
}
