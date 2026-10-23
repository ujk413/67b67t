import * as Files from "expo-file-system/legacy";
import * as Sharing from "expo-sharing";
import * as IntentLauncher from "expo-intent-launcher";
import { Platform } from "react-native";
import { runMediaTask, type MediaTask } from "./mediaLifecycle";
import { storage } from "./storage";
import type { AccountRepository } from "./data/repository";
const ACCOUNT_MEDIA_ID=/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const PRIVATE_DRAFT_PATH=/^draft-media\/[a-f0-9-]{36}\/(voice-[a-f0-9]{32}\.m4a|[a-f0-9]{32}-[a-zA-Z0-9._-]+|[a-f0-9]{32}\.shared|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}_(?:[0-9]|10)\.shared(?:\.importing)?)$/;
export async function removePrivateMedia(uris: string[],ownerId?:string) {
  const directory=Files.documentDirectory;if(!directory)return;
  if(ownerId!==undefined&&!ACCOUNT_MEDIA_ID.test(ownerId))throw Error('Некорректный аккаунт очистки файлов');
  const candidates=new Set(uris);
  if(ownerId){
    // Only enumerate one validated account directory, never recurse. Orphan staging
    // copies may predate outbox registration and therefore have no repository reference.
    const own=`${directory}draft-media/${ownerId}/`,info=await Files.getInfoAsync(own);
    if(info.exists&&info.isDirectory)for(const name of await Files.readDirectoryAsync(own)){
      if(!PRIVATE_DRAFT_PATH.test(`draft-media/${ownerId}/${name}`))continue;
      const file=await Files.getInfoAsync(own+name);if(file.exists&&!file.isDirectory)candidates.add(own+name);
    }
  }
  for(const uri of candidates){
    if(!uri.startsWith(directory))continue;
    const path=uri.slice(directory.length);
    if(ownerId&&path.startsWith('draft-media/')&&!path.startsWith(`draft-media/${ownerId}/`))continue;
    if(/^voice-[a-f0-9]{32}\.m4a$/.test(path)||/^pending-media\/[a-f0-9]{32}-[a-zA-Z0-9._-]+$/.test(path)||PRIVATE_DRAFT_PATH.test(path))await Files.deleteAsync(uri,{idempotent:true});
  }
}
async function launchLocalFile(uri: string, mime: string, task: MediaTask, share: boolean, isCurrent?: () => boolean) {
  const check=()=>{task.check();if(isCurrent?.()===false)throw Error("Просмотр вложения закрыт");};
  check();
  if (Platform.OS === "android" && !share) {
    const content = await Files.getContentUriAsync(uri); check();
    await IntentLauncher.startActivityAsync("android.intent.action.VIEW", { data: content, type: mime, flags: 1 });
  } else {
    const available = await Sharing.isAvailableAsync(); check();
    if (!available) throw Error("Просмотр файлов недоступен на устройстве");
    await Sharing.shareAsync(uri, { mimeType: mime, dialogTitle: share ? "Поделиться" : "Открыть файл" });
  }
}
export function openLocalMediaFile(uri: string, mime: string, userId: string, share = false, isCurrent?: () => boolean) {
  return runMediaTask(userId, task => launchLocalFile(uri, mime, task, share, isCurrent));
}
export function openMediaFile(url: string, headers: Record<string, string>, mime: string, userId = "shared", share = false, isCurrent?: () => boolean) {
  return runMediaTask(userId, async task => {
    const check=()=>{task.check();if(isCurrent?.()===false)throw Error("Просмотр вложения закрыт");};
    const extensions: Record<string, string> = { "application/pdf": "pdf", "text/plain": "txt", "application/zip": "zip", "application/msword": "doc", "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx" };
    const name = url.split("/").at(-1)?.replace(/[^a-zA-Z0-9_-]/g, "") || "file";
    const directory = `${Files.cacheDirectory}media-downloads/${userId}/`;
    const target = `${directory}${name}.${extensions[mime] || "bin"}`;
    await trimMediaCache(); check();
    await Files.makeDirectoryAsync(directory, { intermediates: true }); check();
    const download = Files.createDownloadResumable(url, target, { headers });
    const unsubscribe = task.onCancel(() => download.pauseAsync().catch(() => {}));
    try {
      const result = await task.wait(download.downloadAsync()); check();
      if (!result || result.status < 200 || result.status >= 300) throw Error("Не удалось загрузить файл");
      await launchLocalFile(result.uri, mime, task, share, isCurrent);
    } catch (error) { await Files.deleteAsync(target, { idempotent: true }).catch(() => {}); throw error; }
    finally { unsubscribe(); }
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
