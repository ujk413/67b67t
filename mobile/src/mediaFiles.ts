import * as Files from "expo-file-system/legacy";
import * as Sharing from "expo-sharing";
import { storage } from "./storage";
import type { AccountRepository } from "./data/repository";
export async function removePrivateMedia(uris: string[]) {
  const directory = Files.documentDirectory;
  if (!directory) return;
  for (const uri of new Set(uris)) {
    if (!uri.startsWith(directory)) continue;
    const path = uri.slice(directory.length);
    if (
      /^voice-[a-f0-9]{32}\.m4a$/.test(path) ||
      /^pending-media\/[a-f0-9]{32}-[a-zA-Z0-9._-]+$/.test(path) ||
      /^draft-media\/[a-f0-9-]{36}\/(voice-[a-f0-9]{32}\.m4a|[a-f0-9]{32}-[a-zA-Z0-9._-]+)$/.test(path)
    )
      await Files.deleteAsync(uri, { idempotent: true });
  }
}
export async function openMediaFile(
  url: string,
  headers: Record<string, string>,
  mime: string,
  userId = "shared",
) {
  const extensions: Record<string, string> = {
    "application/pdf": "pdf",
    "text/plain": "txt",
    "application/zip": "zip",
    "application/msword": "doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
      "docx",
  };
  const name =
    url
      .split("/")
      .at(-1)
      ?.replace(/[^a-zA-Z0-9_-]/g, "") || "file";
  const directory = `${Files.cacheDirectory}media-downloads/${userId}/`;
  await trimMediaCache();
  await Files.makeDirectoryAsync(directory, { intermediates: true });
  const result = await Files.downloadAsync(
    url,
    `${directory}${name}.${extensions[mime] || "bin"}`,
    { headers },
  );
  if (result.status < 200 || result.status >= 300)
    throw new Error("Не удалось загрузить файл");
  if (!(await Sharing.isAvailableAsync()))
    throw new Error("Просмотр файлов недоступен на устройстве");
  await Sharing.shareAsync(result.uri, {
    mimeType: mime,
    dialogTitle: "Открыть файл",
  });
}

async function filesIn(directory: string, depth = 0): Promise<{ uri: string; size: number; modified: number }[]> {
  const info = await Files.getInfoAsync(directory); if (!info.exists || !info.isDirectory || depth > 4) return [];
  const names = (await Files.readDirectoryAsync(directory)).filter(name => name !== "." && name !== ".." && !name.includes("/"));
  const result = [];
  for (const name of names) {
    const uri = directory.replace(/\/?$/, "/") + name, value = await Files.getInfoAsync(uri);
    if (!value.exists) continue;
    if (value.isDirectory) result.push(...await filesIn(uri, depth + 1));
    else result.push({ uri, size: value.size || 0, modified: value.modificationTime || 0 });
  }
  return result;
}
async function cachedMedia() {
  if (!Files.cacheDirectory) return [];
  const roots = (await Files.readDirectoryAsync(Files.cacheDirectory)).filter(name => name === "media-downloads" || name === "encrypted-uploads" || /^decoded-[a-f0-9-]{36}$/.test(name));
  return (await Promise.all(roots.map(name => filesIn(Files.cacheDirectory! + name + "/")))).flat();
}
export async function mediaUsage(repository: AccountRepository) {
  const refs = new Set(await repository.privateFiles());
  const own = await filesIn(`${Files.documentDirectory}draft-media/${repository.userId}/`);
  for (const file of own) if (!refs.has(file.uri)) await Files.deleteAsync(file.uri, { idempotent: true });
  let drafts = 0; for (const uri of refs) { const info = await Files.getInfoAsync(uri); if (info.exists && !info.isDirectory) drafts += info.size || 0; }
  const cached = (await cachedMedia()).reduce((sum, file) => sum + file.size, 0);
  return { cached, drafts, database: await repository.size(), total: cached + drafts + await repository.size() };
}
export async function clearMediaCache() { for (const file of await cachedMedia()) await Files.deleteAsync(file.uri, { idempotent: true }); }
export async function trimMediaCache(reserve = 0) {
  const configured = Number(await storage.get("media.cacheLimitMB")) || 256, limit = Math.max(0, configured * 1048576 - reserve);
  const files = (await cachedMedia()).sort((a, b) => a.modified - b.modified); let bytes = files.reduce((sum, file) => sum + file.size, 0);
  for (const file of files) { if (bytes <= limit) break; await Files.deleteAsync(file.uri, { idempotent: true }); bytes -= file.size; }
}
