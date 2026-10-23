// In-app update for the sideloaded Android build.
//
// Android never installs an APK silently outside of device-owner/root setups,
// so the flow is: ask the server for the newest build, download it, then hand
// the file to the system installer. The user still confirms the install.
//
// Why this is safe to do from our own server: Android verifies the APK
// signature at install time and refuses any package that is not signed with
// the key the installed app was signed with. A tampered download therefore
// fails closed. The size check below exists so a truncated transfer does not
// reach the installer at all.
//
// The manifest is public (no session needed) so an update can be offered even
// when the stored token has expired.

import { Platform } from "react-native";
import Constants from "expo-constants";
import * as Application from "expo-application";
import * as IntentLauncher from "expo-intent-launcher";
import * as Files from "expo-file-system/legacy";
import { requireOptionalNativeModule } from "expo";

const updater = requireOptionalNativeModule<{
  downloadApkAsync(url:string,identity:string,size:number,hash:string,id:string):Promise<string>;
  cancelApkDownload(id:string):void;
  apkSha256Async(uri:string):Promise<string>;
  addListener(event:string,callback:(event:{id:string;bytes:number;total:number})=>void):{remove():void};
}>("RoomKey");

export type AppRelease = {
  version: string;
  versionCode: number;
  path: string;
  size: number;
  sha256: string;
  notes: string;
  publishedAt: string;
  mandatory: boolean;
  minSupportedVersionCode: number;
};

export type UpdateCheck =
  | { status: "unsupported" }
  | { status: "unavailable"; current: number }
  | { status: "up-to-date"; current: number }
  | { status: "available"; release: AppRelease; current: number; url: string };

const ANDROID_PACKAGE_MIME = "application/vnd.android.package-archive";
const FLAG_GRANT_READ_URI_PERMISSION = 1;
const APK_FILE = "67chat-update.apk";

/** Build number of the running binary, or null when it cannot be determined. */
export function currentVersionCode(): number | null {
  // Expo Go reports the host's build number, not the messenger's. The installed
  // binary is authoritative; app config may describe a different JS build.
  if (Constants.executionEnvironment === "storeClient") return null;
  const fromBuild = Number(Application.nativeBuildVersion);
  if (Number.isInteger(fromBuild) && fromBuild > 0) return fromBuild;
  // Expo Go / web have no native build number; treat them as not updatable.
  return null;
}

export function currentVersionName(): string {
  return String(Application.nativeApplicationVersion || "");
}

/**
 * Ask the server which build is current. Never throws: any network or parse
 * failure is distinct from a successful check, so it can be retried after reconnect.
 */
export async function checkForUpdate(baseUrl: string): Promise<UpdateCheck> {
  if (Platform.OS !== "android") return { status: "unsupported" };
  const current = currentVersionCode();
  if (current === null) return { status: "unsupported" };

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 8000);
  try {
    const res = await fetch(`${baseUrl}/app/version`, {
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
    // 204 = nothing published yet.
    if (res.status === 204) return { status: "up-to-date", current };
    if (!res.ok) return { status: "unavailable", current };
    const release = (await res.json()) as Partial<AppRelease>;
    const versionCode = Number(release.versionCode);
    if (!Number.isInteger(versionCode) || versionCode <= 0) return { status: "unavailable", current };
    if (versionCode <= current) {
      return { status: "up-to-date", current };
    }
    if (typeof release.path !== "string" || !/^\/(?!\/)[^\s?#]*$/.test(release.path)) {
      return { status: "unavailable", current };
    }
    return {
      status: "available",
      current,
      url: `${baseUrl}${release.path}`,
      release: {
        version: String(release.version || ""),
        versionCode,
        path: release.path,
        size: Number(release.size) || 0,
        sha256: String(release.sha256 || ""),
        notes: String(release.notes || ""),
        publishedAt: String(release.publishedAt || ""),
        mandatory: release.mandatory === true,
        minSupportedVersionCode: Number(release.minSupportedVersionCode) || 0,
      },
    };
  } catch {
    return { status: "unavailable", current };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Download the APK to the cache directory. Returns the local file URI.
 * `onProgress` receives 0..1 (or -1 when the server sent no Content-Length).
 */
export async function downloadRelease(
  url: string,
  onProgress?: (fraction: number) => void,
  signal?: AbortSignal,
  release?: AppRelease,
): Promise<string> {
  if(!updater?.downloadApkAsync)throw Error('native_update_unavailable');
  if(!release || !/^[a-f0-9]{64}$/i.test(release.sha256) || !(release.size>0))throw Error('invalid_release');
  const id=`${Date.now()}-${Math.random()}`;
  const subscription=updater.addListener('apkProgress',event=>{
    if(event.id===id && !signal?.aborted)onProgress?.(event.total>0?event.bytes/event.total:-1);
  });
  const cancel=()=>updater.cancelApkDownload(id);
  signal?.addEventListener('abort',cancel,{once:true});
  try {
    if(signal?.aborted)throw Error('download_cancelled');
    const result = await updater.downloadApkAsync(url,String(release.versionCode),release.size,release.sha256,id);
    if(signal?.aborted)throw Error('download_cancelled');
    return result;
  } catch(error) {
    const message=error instanceof Error?error.message:String(error);
    throw Error(message.match(/checksum_mismatch|incomplete_download|download_cancelled|invalid_release|invalid_download_range|download_http_\d+/)?.[0] || message);
  } finally {signal?.removeEventListener('abort',cancel);subscription.remove();}
}

/** Re-check even a previously downloaded APK before every installer handoff. */
export async function verifyRelease(uri:string,release:AppRelease,signal?:AbortSignal):Promise<void> {
  if(signal?.aborted)throw Error('download_cancelled');
  if(!/^[a-f0-9]{64}$/i.test(release.sha256) || !updater?.apkSha256Async)throw Error('invalid_release');
  const actual=await updater.apkSha256Async(uri);
  if(signal?.aborted)throw Error('download_cancelled');
  if(actual.toLowerCase()!==release.sha256.toLowerCase())throw Error('checksum_mismatch');
}

/** Delete a downloaded APK (after install, or when the user dismisses). */
export async function discardDownload(uri?: string | null) {
  const target = uri || `${Files.cacheDirectory}${APK_FILE}`;
  try {
    await Files.deleteAsync(target, { idempotent: true });
  } catch {
    // Best effort: the cache directory is cleaned by the OS anyway.
  }
}

/**
 * Hand the APK to the system installer. Rejects with `install_permission_required`
 * when the user has not allowed this app to install unknown apps yet.
 */
export async function installRelease(fileUri: string,signal?:AbortSignal): Promise<void> {
  const contentUri = await Files.getContentUriAsync(fileUri);
  if(signal?.aborted)throw Error('install_cancelled');
  await IntentLauncher.startActivityAsync("android.intent.action.VIEW", {
    data: contentUri,
    type: ANDROID_PACKAGE_MIME,
    flags: FLAG_GRANT_READ_URI_PERMISSION,
  });
}

/** Size of the downloaded file, used to reject a truncated transfer. */
export async function downloadedSize(fileUri: string): Promise<number> {
  const info = await Files.getInfoAsync(fileUri);
  return info.exists && typeof info.size === "number" ? info.size : 0;
}

/** Opens the OS screen where the user allows installs from this app. */
export async function openInstallPermissionSettings(): Promise<void> {
  await IntentLauncher.startActivityAsync(
    "android.settings.MANAGE_UNKNOWN_APP_SOURCES",
    { data: `package:${Application.applicationId || Constants.expoConfig?.android?.package || ""}` },
  );
}
