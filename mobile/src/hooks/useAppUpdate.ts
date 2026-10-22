import { useCallback, useEffect, useRef, useState } from "react";
import { AppState, Platform } from "react-native";
import {
  checkForUpdate,
  currentVersionCode,
  discardDownload,
  downloadRelease,
  downloadedSize,
  installRelease,
  openInstallPermissionSettings,
  type AppRelease,
} from "../updates";

export type UpdateState =
  | { phase: "idle" }
  | { phase: "available"; release: AppRelease; url: string }
  | { phase: "downloading"; release: AppRelease; progress: number }
  | { phase: "ready"; release: AppRelease; fileUri: string }
  | { phase: "installing"; release: AppRelease; fileUri: string }
  | { phase: "error"; release: AppRelease; message: string; needsPermission: boolean };

const CHECK_THROTTLE_MS = 60 * 60 * 1000; // at most once an hour per app run
const SIZE_TOLERANCE = 4096; // bytes; the server size is authoritative

/**
 * Watches for a newer Android build.
 *
 * Checks once on mount and again whenever the app returns to the foreground,
 * throttled so switching apps does not hammer the endpoint. The prompt is
 * dismissible unless the release is marked mandatory.
 */
export function useAppUpdate(baseUrl: string, enabled: boolean) {
  const [state, setState] = useState<UpdateState>({ phase: "idle" });
  const [dismissed, setDismissed] = useState<number | null>(null);
  const lastCheck = useRef(0);
  const checking = useRef(false);

  const check = useCallback(
    async (force = false) => {
      if (!enabled || Platform.OS !== "android") return;
      if (currentVersionCode() === null) return;
      if (checking.current) return;
      if (!force && Date.now() - lastCheck.current < CHECK_THROTTLE_MS) return;
      checking.current = true;
      lastCheck.current = Date.now();
      try {
        const result = await checkForUpdate(baseUrl);
        if (result.status !== "available") return;
        // Never downgrade a visible prompt or interrupt an in-flight download.
        setState((prev) =>
          prev.phase === "idle" || prev.phase === "available"
            ? { phase: "available", release: result.release, url: result.url }
            : prev,
        );
      } finally {
        checking.current = false;
      }
    },
    [baseUrl, enabled],
  );

  useEffect(() => {
    void check();
    const subscription = AppState.addEventListener("change", (next) => {
      if (next === "active") void check();
    });
    return () => subscription.remove();
  }, [check]);

  const install = useCallback(async (release: AppRelease, fileUri: string) => {
    setState({ phase: "installing", release, fileUri });
    try {
      await installRelease(fileUri);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      setState({
        phase: "error",
        release,
        message,
        needsPermission: /permission|unknown|denied/i.test(message),
      });
    }
  }, []);

  const start = useCallback(async () => {
    if (state.phase !== "available") return;
    const { release, url } = state;
    setState({ phase: "downloading", release, progress: 0 });
    try {
      const fileUri = await downloadRelease(url, (fraction) =>
        setState((prev) =>
          prev.phase === "downloading"
            ? { ...prev, progress: fraction }
            : prev,
        ),
      );
      // Reject a truncated transfer before showing the installer: installing a
      // half-written APK fails with a confusing system error.
      if (release.size > 0) {
        const actual = await downloadedSize(fileUri);
        if (Math.abs(actual - release.size) > SIZE_TOLERANCE) {
          await discardDownload(fileUri);
          throw new Error("incomplete_download");
        }
      }
      setState({ phase: "ready", release, fileUri });
      await install(release, fileUri);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      setState({
        phase: "error",
        release,
        message,
        needsPermission: /permission|unknown|denied/i.test(message),
      });
    }
  }, [state, install]);

  const retryInstall = useCallback(async () => {
    if (state.phase !== "ready" && state.phase !== "error") return;
    if (!("fileUri" in state) || !state.fileUri) return;
    await install(state.release, state.fileUri);
  }, [state, install]);

  const dismiss = useCallback(() => {
    if (state.phase === "available") setDismissed(state.release.versionCode);
  }, [state]);

  const openPermissionSettings = useCallback(async () => {
    try {
      await openInstallPermissionSettings();
    } catch {
      // Older builds may not expose the settings intent; the user can open
      // Android settings manually.
    }
  }, []);

  const visible =
    state.phase !== "idle" &&
    (state.phase !== "available" || dismissed !== state.release.versionCode);

  return {
    state,
    visible,
    start,
    dismiss,
    retryInstall,
    openPermissionSettings,
    recheck: () => check(true),
  };
}
