type Ref<T> = { current: T };
type Environment = {
  token: string | null;
  baseUrl: string;
  wsRef: Ref<WebSocket | null>;
  reconnectTimerRef: Ref<ReturnType<typeof setTimeout> | null>;
  callActiveRef: Ref<boolean>;
  activeRoomIdRef: Ref<string | null>;
  resyncRef: Ref<(() => Promise<void>) | null>;
  onWsMessageRef: Ref<((data: any) => Promise<void>) | null>;
  AppState: {
    currentState: string;
    addEventListener: (
      event: "change",
      callback: (state: string) => void,
    ) => { remove: () => void };
  };
  WebSocket: typeof WebSocket;
  wsUrlFromApi: (base: string) => string;
  setConnection: (status: string) => void;
  onAuthenticated?: () => void;
};
export function startConnection(environment: Environment) {
  const {
    token,
    baseUrl,
    wsRef,
    reconnectTimerRef,
    callActiveRef,
    activeRoomIdRef,
    resyncRef,
    onWsMessageRef,
    AppState,
    WebSocket,
    wsUrlFromApi,
    setConnection,onAuthenticated,
  } = environment;

  if (!token) {
    reconnectTimerRef.current && clearTimeout(reconnectTimerRef.current);
    reconnectTimerRef.current = null;
    const stale = wsRef.current;
    wsRef.current = null;
    stale?.close();
    return;
  }

  let cancelled = false;
  let attempt = 0;
  let firstOpen = true;
  let handshakeTimer: ReturnType<typeof setTimeout> | null = null;
  const clearHandshake = () => {
    if (handshakeTimer) clearTimeout(handshakeTimer);
    handshakeTimer = null;
  };

  const clearTimer = () => {
    if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
    reconnectTimerRef.current = null;
  };
  // Detach first so the old socket's onclose cannot schedule a reconnect.
  const closeSocket = () => {
    clearHandshake();
    const current = wsRef.current;
    wsRef.current = null;
    try {
      current?.close();
    } catch {}
  };
  // Sockets are paused in the background unless a call needs signaling.
  const shouldPause = () =>
    AppState.currentState === "background" && !callActiveRef.current;

  const connect = () => {
    clearTimer();
    if (cancelled || shouldPause()) return;
    const existing = wsRef.current;
    if (
      existing &&
      (existing.readyState === WebSocket.CONNECTING ||
        existing.readyState === WebSocket.OPEN)
    )
      return;

    const ws = new WebSocket(wsUrlFromApi(baseUrl));
    setConnection("connecting");
    wsRef.current = ws;
    let authenticated = false;
    handshakeTimer = setTimeout(() => {
      if (wsRef.current === ws && !authenticated) ws.close();
    }, 15000);

    ws.onopen = () => {
      if (wsRef.current !== ws) {
        ws.close();
        return;
      }
      ws.send(JSON.stringify({ type: "auth", token }));
    };

    ws.onmessage = (event) => {
      if (wsRef.current !== ws) return;
      let data: any;
      try {
        data = JSON.parse(event.data);
      } catch {
        return;
      }
      // Older servers ask for auth before processing the token sent on open.
      if (data.type === 'auth.required' && !authenticated) return;
      if (data.type === 'auth.ok') {
        if (authenticated) return;
        authenticated = true;
        clearHandshake();
        const needsResync = !firstOpen;
        firstOpen = false;
        attempt = 0;
        setConnection('connected');
        const roomId = activeRoomIdRef.current;
        if (roomId) ws.send(JSON.stringify({ type: 'rooms.join', room_id: roomId }));
        onAuthenticated?.();
        if (needsResync) void resyncRef.current?.();
        return;
      }
      void onWsMessageRef.current?.(data);
    };

    ws.onclose = () => {
      if (wsRef.current !== ws) return;
      clearHandshake();
      wsRef.current = null;
      setConnection("offline");
      if (cancelled || shouldPause()) return;
      const delay = Math.min(30000, 1000 * 2 ** attempt);
      attempt += 1;
      reconnectTimerRef.current = setTimeout(
        connect,
        Math.round(delay * (0.75 + Math.random() * 0.5)),
      );
    };
    ws.onerror = () => {
      if (wsRef.current === ws) ws.close();
    };
  };

  const sub = AppState.addEventListener("change", (state) => {
    if (state === "active") {
      attempt = 0;
      connect();
    } else if (state === "background" && !callActiveRef.current) {
      clearTimer();
      closeSocket();
    }
  });

  connect();
  return () => {
    cancelled = true;
    sub.remove();
    clearTimer();
    closeSocket();
  };
}
