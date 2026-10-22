import type { User } from "./types";
type Ref<T> = { current: T };
type Environment = {
  restoreSessionToken: () => Promise<string | null>;
  storage: { get: (key: string) => Promise<string | null> };
  normalizeBaseUrl: (base: string) => string;
  DEFAULT_BASE_URL: string;
  setBaseUrl: (base: string) => void;
  setMe: (user: User | null) => void;
  tokenRef: Ref<string | null>;
  resetUnauthorizedGuard: () => void;
  setToken: (token: string | null) => void;
  bootstrap: (base: string, token: string) => Promise<void>;
  setError: (error: string) => void;
  errorText: (error: unknown) => string;
  setLoading: (value: boolean) => void;
  reconnectTimerRef: Ref<ReturnType<typeof setTimeout> | null>;
  wsRef: Ref<WebSocket | null>;
  audioRef: Ref<{ unloadAsync: () => Promise<unknown> } | null>;
  recording: { stopAndUnloadAsync: () => Promise<unknown> } | null;
};
export function restoreSessionEffect(environment: Environment) {
  const {
    restoreSessionToken,
    storage,
    normalizeBaseUrl,
    DEFAULT_BASE_URL,
    setBaseUrl,
    setMe,
    tokenRef,
    resetUnauthorizedGuard,
    setToken,
    bootstrap,
    setError,
    errorText,
    setLoading,
    reconnectTimerRef,
    wsRef,
    audioRef,
    recording,
  } = environment;

  let mounted = true;
  (async () => {
    try {
      const storedToken = await restoreSessionToken();
      if (!mounted) return;
      const nextBaseUrl = normalizeBaseUrl(DEFAULT_BASE_URL);
      setBaseUrl(nextBaseUrl);
      if (storedToken) {
        const cachedUser = await storage.get("mobile.user");
        let cached=false;
        if (cachedUser)try {const user=JSON.parse(cachedUser);if(user?.id){setMe(user);cached=true;}}catch{}
        tokenRef.current = storedToken;
        resetUnauthorizedGuard();
        setToken(storedToken);
        if(cached)setLoading(false);
        await bootstrap(nextBaseUrl, storedToken);
      }
    } catch (e) {
      if (mounted) setError(errorText(e));
    } finally {
      if (mounted) setLoading(false);
    }
  })();

  return () => {
    mounted = false;
    reconnectTimerRef.current && clearTimeout(reconnectTimerRef.current);
    wsRef.current?.close();
    audioRef.current?.unloadAsync().catch(() => undefined);
    recording?.stopAndUnloadAsync().catch(() => undefined);
  };
}
