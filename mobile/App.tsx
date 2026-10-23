import { protectedUploader } from "./src/protectedUpload";
import { nativeCalls } from "./src/nativeCalls";
import { GroupEncryption } from "./src/groupEncryption";
import { getMobileDeviceId } from "./src/crypto";
import { openMediaFile, removePrivateMedia } from "./src/mediaFiles";
import { userError } from "./src/errors";
import { MediaViewer } from "./src/components/MediaViewer";
import { Icon } from "./src/components/Icon";
import { restoreSessionEffect } from "./src/restoreSessionEffect";
import { startConnection } from "./src/startConnection";
import { SafeAreaProvider, SafeAreaView } from "react-native-safe-area-context";
import { MediaSession, MediaDecoder, useMediaHeaders } from "./src/MediaSession";
import { useChatPresence } from "./src/hooks/useChatPresence";
import { useAccountData } from "./src/hooks/useAccountData";
import { mobileApi } from "./src/mobileApi";
import { mergeMessages } from "./src/data/messages";
import { Workspace, MessageActions } from "./src/screens/Workspace";
import { AppNavigation } from "./src/components/AppNavigation";
import { ConnectionBanner } from "./src/components/ConnectionBanner";
import { UpdateBanner } from "./src/components/UpdateBanner";
import { useAppUpdate } from "./src/hooks/useAppUpdate";
import { beginCall as beginAudioCall, endCall as endAudioCall } from "./src/audioSession";
import { describeLock, lockLabel, unlockApp, watchBackground, setLockPreference } from "./src/appLock";
import { currentVersionName } from "./src/updates";
import { LockScreen } from "./src/components/LockScreen";
import { ThemeProvider, useTheme } from "./src/ThemeProvider";
import "react-native-get-random-values";
import { StatusBar } from "expo-status-bar";
import { Audio } from "expo-av";
import { ResizeMode, Video } from './src/components/Video';
import * as DocumentPicker from "expo-document-picker";
import * as ImagePicker from "expo-image-picker";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ActivityIndicator,
  Alert,
  AppState,
  FlatList,
  Image,
  BackHandler,
  KeyboardAvoidingView,
  Linking,
  Modal,
  Platform,
  Pressable,
  RefreshControl,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import { api, request, resetUnauthorizedGuard, setUnauthorizedHandler } from "./src/api";
import {
  DEFAULT_BASE_URL,
  mediaThumbUrl,
  mediaUrl,
  normalizeBaseUrl,
  wsUrlFromApi,
} from "./src/config";
import { displayName, roomPreview, roomTitle, shortTime } from "./src/format";
import {
  STORAGE_KEYS,
  storage,
  restoreSessionToken,
  removeSessionTokens,
} from "./src/storage";
import type { Message, Room, User } from "./src/types";
import { e2e } from "./src/crypto";
import { pushNotifications } from "./src/notifications";
import { WebRTCCall } from "./src/webrtc";
import {
  decryptRoomText,
  encryptRoomText,
  clearDerivedRoomKeys,
} from "./src/roomCrypto";
import { RTCView } from "react-native-webrtc";
import { telegramColors } from "./src/theme";
import { AuthScreen } from "./src/screens/AuthScreen";
import { ChatListScreen } from "./src/screens/ChatListScreen";
import { ChatScreen } from "./src/screens/ChatScreen";
import { ProfileScreen } from "./src/screens/ProfileScreen";

type AuthMode = "login" | "register";
type ReactionTarget = {
  id: string;
  reactions?: Record<string, string[]> | null;
} | null;

const REACTION_SET = ["👍", "❤️", "😂", "🔥", "😮", "👎"];
const TYPING_TTL_MS = 8000;
const TYPING_SEND_INTERVAL_MS = 3000;

export default function Root() {
  return (
    <SafeAreaProvider>
      <ThemeProvider>
        <App />
      </ThemeProvider>
    </SafeAreaProvider>
  );
}

function App() {
  const styles = useStyles();
  const { colors: telegramColors } = useTheme();

  const theme = useTheme();
  const [workspace, setWorkspace] = useState<string | null>(null);
  const [searchMatches, setSearchMatches] = useState<{
    roomId: string;
    ids: string[];
    index: number;
  } | null>(null);
  const [connection, setConnection] = useState("connecting");
  const [selectionReset, setSelectionReset] = useState(0);
  const [actionMessages, setActionMessages] = useState<Message[]>([]);
  const [editing, setEditing] = useState<Message | null>(null);
  const [focusId, setFocusId] = useState<string | null>(null);
  const [forwarding, setForwarding] = useState<Message[]>([]);
  const [baseUrl, setBaseUrl] = useState(DEFAULT_BASE_URL);
  const [token, setToken] = useState<string | null>(null);
  const [me, setMe] = useState<User | null>(null);
  const [rooms, setRooms] = useState<Room[]>([]);
  const [activeRoomId, setActiveRoomId] = useState<string | null>(null);
  const [activeRoomMembers, setActiveRoomMembers] = useState<User[]>([]);
  const [messages, setMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState(true);
  const [authBusy, setAuthBusy] = useState(false);
  const [refreshingRooms, setRefreshingRooms] = useState(false);
  const [refreshingMessages, setRefreshingMessages] = useState(false);
  const [busyUpload, setBusyUpload] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [composer, setComposer] = useState("");
  // Who is typing, keyed by roomId:userId. Entries expire after TYPING_TTL_MS if no update arrives.
  const [typingUsers, setTypingUsers] = useState<
    Record<string, { username: string; roomId: string }>
  >({});
  const typingTimersRef = useRef<Record<string, ReturnType<typeof setTimeout>>>(
    {},
  );
  const typingSentAtRef = useRef(0);
  const [authMode, setAuthMode] = useState<AuthMode>("login");
  const [profileOpen, setProfileOpen] = useState(false);
  const [profileName, setProfileName] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [reactionTarget, setReactionTarget] = useState<ReactionTarget>(null);
  const [viewerMessage, setViewerMessage] = useState<Message | null>(null);
  const [recording, setRecording] = useState<Audio.Recording | null>(null);
  const [playingId, setPlayingId] = useState<string | null>(null);
  const [incomingCall, setIncomingCall] = useState<{
    callId?: string;
    roomId: string;
    callerId: string;
    callerName: string;
    isVideo: boolean;
    offer: any;
  } | null>(null);
  const [activeCall, setActiveCall] = useState<WebRTCCall | null>(null);
  const [callState, setCallState] = useState<
    "idle" | "calling" | "ringing" | "connected"
  >("idle");
  const [localStream, setLocalStream] = useState<any>(null);
  const [remoteStream, setRemoteStream] = useState<any>(null);
  const [speakerEnabled, setSpeakerEnabled] = useState(false);
  const [autoAnswerId, setAutoAnswerId] = useState<string | null>(null);
  const [isAudioEnabled, setIsAudioEnabled] = useState(true);
  const [isVideoEnabled, setIsVideoEnabled] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const audioRef = useRef<Audio.Sound | null>(null);
  const roomSecretCache = useRef<Map<string, string>>(new Map());
  // Latest-value refs so the long-lived WebSocket/AppState callbacks never see stale state.
  const tokenRef = useRef<string | null>(null);
  const activeRoomIdRef = useRef<string | null>(null);
  const callActiveRef = useRef(false);
  const onWsMessageRef = useRef<((data: any) => Promise<void>) | null>(null);
  const resyncRef = useRef<(() => Promise<void>) | null>(null);
  useEffect(() => {
    void pushNotifications
      .setBadgeCount(
        rooms
          .filter((r) => !r.preferences?.muted)
          .reduce((sum, r) => sum + (r.unread_count || 0), 0),
      )
      .catch(() => {});
  }, [rooms]);
  const groupEncryption = useMemo(() => me?.id && token ? new GroupEncryption(me.id, baseUrl, token, () => local.repository.current) : null, [me?.id, token, baseUrl]);
  async function encodeText(roomId: string, text: string) {
    if (rooms.find(room => room.id === roomId)?.kind === 'group' && groupEncryption && await groupEncryption.enabled(roomId)) return groupEncryption.encrypt(roomId, text);
    const secret = await getRoomSecret(roomId); if (!secret) throw Error('Ключ переписки недоступен');
    return {type:'text', ...(await encryptRoomText(roomId,secret,text))};
  }
  const appUpdate = useAppUpdate(baseUrl, true);
  // --- App lock -------------------------------------------------------------
  const [lockEnabled, setLockEnabled] = useState(false);
  const [lockReady,setLockReady] = useState(false);
  const [unlocked, setUnlocked] = useState(true);
  const [unlocking, setUnlocking] = useState(false);
  const [lockError, setLockError] = useState<string | null>(null);
  const [lockButtonLabel, setLockButtonLabel] = useState("Разблокировать");

  useEffect(() => {
    let live = true;
    void (async () => {
      const state = await describeLock();
      if (!live) return;
      setLockEnabled(state.enabled);
      setLockButtonLabel(lockLabel(state.biometry));
      // Locked on a cold start; a session already open in this run stays open.
      if (state.enabled) setUnlocked(false);
      setLockReady(true);
    })();
    return () => {
      live = false;
    };
  }, []);

  const tryUnlock = useCallback(async () => {
    if (unlocking) return;
    setUnlocking(true);
    setLockError(null);
    try {
      if (await unlockApp()) {
        setUnlocked(true);
      } else {
        setLockError("Не удалось подтвердить. Попробуйте ещё раз.");
      }
    } finally {
      setUnlocking(false);
    }
  }, [unlocking]);

  useEffect(() => {
    if (!lockEnabled) return;
    return watchBackground(() => setUnlocked(false));
  }, [lockEnabled]);

  const local = useAccountData({
    me,
    token,
    baseUrl,
    activeRoomId,
    decode: prepareMessage,
    encode: encodeText,
    onMessages: setMessages,
    onRooms: setRooms,
    onError: setError,
    onRevoke: () => {
      setActiveRoomId(null);
      setActiveRoomMembers([]);
      setError("Доступ к переписке прекращён");
    },
    getSecret: getRoomSecret,
  });
  useEffect(() => {
    if (!activeRoomId || !local.ready) return;
    let active = true;
    void local.repository.current
      ?.get<{ text: string; reply?: Message | null }>(`draft:${activeRoomId}`, {
        text: "",
      })
      .then((draft) => {
        if (active) {
          setComposer(draft.text);
          local.setReply(draft.reply || null);
        }
      })
      .catch((e) => setError(String(e)));
    return () => {
      active = false;
    };
  }, [activeRoomId, local.ready]);
  useEffect(() => {
    if (local.ready)
      void local.repository.current?.saveRooms(rooms).catch(() => {});
  }, [rooms, local.ready]);
  useEffect(() => {
    if (!token || !me) return;
    let stopped = false,
      dispose: (() => void) | undefined;
    pushNotifications.onMessage((data) => {
      if (data.room_id)
        void mobileApi(baseUrl, token)
          .room(data.room_id)
          .then((room) => {
            setWorkspace(null);
            setProfileOpen(false);
            return openRoom(room);
          })
          .catch((e) => setError(String(e)));
    });
    void pushNotifications
      .connect(baseUrl, token, () => activeRoomIdRef.current)
      .then((fn) => {
        if (stopped) fn();
        else dispose = fn;
      })
      .catch((e) => setError(String(e)));
    return () => {
      stopped = true;
      dispose?.();
    };
  }, [token, me?.id]);
  useEffect(() => {
    if (token) void pushNotifications.refresh().catch(() => {});
  }, [activeRoomId, token]);
  function changeComposer(text: string) {
    setComposer(text);
    notifyTyping(text.length > 0);
    if (activeRoomId)
      void local.repository.current
        ?.set(`draft:${activeRoomId}`, { text, reply: local.reply })
        .catch((e) => setError(String(e)));
  }
  tokenRef.current = token;
  activeRoomIdRef.current = activeRoomId;
  callActiveRef.current =
    !!activeCall || !!incomingCall || callState !== "idle";

  useEffect(() => {
    // Ignore late failures from a previous session when another user has logged in.
    setUnauthorizedHandler((failedToken) => {
      if (failedToken !== tokenRef.current) return;
      void clearSession();
      setError("Сеанс завершён. Войдите снова.");
    });
    return () => setUnauthorizedHandler(null);
  }, []);

  useEffect(
    () =>
      restoreSessionEffect({
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
      }),
    [],
  );

  useEffect(() => {
    setProfileName(me?.display_name || "");
  }, [me?.display_name]);

  useEffect(
    () =>
      startConnection({
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
        setConnection,
      }),
    [baseUrl, token],
  );

  onWsMessageRef.current = handleWsMessage;
  resyncRef.current = resyncAfterReconnect;

  async function resyncAfterReconnect() {
    if (!tokenRef.current) return;
    await local.sync();
    try {
      const list = await api.rooms(baseUrl, tokenRef.current);
      setRooms(await prepareRooms(list));
      if (
        activeRoomIdRef.current &&
        !list.some((room) => room.id === activeRoomIdRef.current)
      ) {
        setActiveRoomId(null);
        setMessages([]);
      }
    } catch {}
    await replenishPrekeys(tokenRef.current);
  }

  async function replenishPrekeys(
    activeToken: string,
    userId = me?.id,
    nextBaseUrl = baseUrl,
  ) {
    if (!userId) return;
    try {
      const deviceId = await getMobileDeviceId(userId);
      const { unused } = await api.e2ePrekeyCount(
        nextBaseUrl,
        activeToken,
        deviceId,
      );
      e2e.configure(nextBaseUrl, activeToken);
      await e2e.initialize(userId);
      if (unused >= 10) return;
      await api.uploadE2EKeys(
        nextBaseUrl,
        activeToken,
        deviceId,
        await e2e.getPublicKeys(20),
      );
    } catch (e) {
      console.error("Failed to replenish pre-keys:", errorText(e));
    }
  }

  async function handleWsMessage(data: any) {
    const receivingToken = token;
    if (data.type === "auth.required" || data.type === 'auth.error') {
      await clearSession();
      setError("Сеанс завершён. Войдите снова.");
      return;
    }
    if (data.type === "rooms.update" && Array.isArray(data.rooms)) {
      setRooms(await prepareRooms(data.rooms, baseUrl, token));
      return;
    }
    if (data.type === "message.new" || data.type === "message.update") {
      const incoming = await prepareMessage(
        data.message as Message,
        baseUrl,
        token,
      );
      if (tokenRef.current !== receivingToken) return;
      const repository=local.repository.current;
      await repository?.saveMessages([incoming]);
      if(tokenRef.current!==receivingToken || repository!==local.repository.current)return;
      if(repository && !(await repository.visibleMessages([incoming])).length)return;
      if(repository?.isHidden(incoming.id))return;
      setRooms((prev) =>
        reorderRooms(
          updateRoomPreview(prev, incoming, me?.id || null),
          incoming.room_id,
        ),
      );
      if (incoming.room_id === activeRoomIdRef.current && tokenRef.current) {
        setMessages((prev) => mergeMessages(prev, [incoming]).filter(message=>!repository?.isHidden(message.id)));
        try {
          // Read receipts are sent by the visible-message callback.
        } catch {}
      }
      return;
    }
    if (data.type === 'sender_key.available' && data.room_id) {
      try { await groupEncryption?.inbox(data.room_id); if (data.room_id === activeRoomIdRef.current) await local.open(rooms.find(room => room.id === data.room_id)!); } catch {}
      return;
    }
    if (data.type === 'room.pin' && data.room_id) {
      setRooms(prev=>prev.map(room=>room.id===data.room_id?{...room,pinned_message_id:data.message_id || null}:room));
      return;
    }
    if ((data.type === "message.delete" || data.type === "message.hide") && data.message_id) {
      void local.repository.current
        ?.hideMessage(data.message_id)
        .catch(() => {});
      setMessages((prev) => prev.filter((item) => item.id !== data.message_id));
      return;
    }
    if (
      data.type === "reaction.add" &&
      data.message_id &&
      data.emoji &&
      data.user_id
    ) {
      setMessages((prev) =>
        prev.map((item) =>
          item.id === data.message_id
            ? withReaction(item, data.emoji, data.user_id, true)
            : item,
        ),
      );
      return;
    }
    if (
      data.type === "reaction.remove" &&
      data.message_id &&
      data.emoji &&
      data.user_id
    ) {
      setMessages((prev) =>
        prev.map((item) =>
          item.id === data.message_id
            ? withReaction(item, data.emoji, data.user_id, false)
            : item,
        ),
      );
      return;
    }
    if (
      data.type === "messages.read" &&
      Array.isArray(data.message_ids) &&
      data.user_id
    ) {
      setMessages((prev) =>
        prev.map((item) =>
          data.message_ids.includes(item.id)
            ? withRead(item, data.user_id)
            : item,
        ),
      );
      return;
    }
    if (data.type === "typing" && data.room_id && data.user_id) {
      const key = `${data.room_id}:${data.user_id}`;
      clearTimeout(typingTimersRef.current[key]);
      delete typingTimersRef.current[key];
      if (data.is_typing) {
        setTypingUsers((prev) => ({
          ...prev,
          [key]: { username: data.username || "Кто-то", roomId: data.room_id },
        }));
        typingTimersRef.current[key] = setTimeout(() => {
          delete typingTimersRef.current[key];
          setTypingUsers((prev) => {
            const next = { ...prev };
            delete next[key];
            return next;
          });
        }, TYPING_TTL_MS);
      } else {
        setTypingUsers((prev) => {
          const next = { ...prev };
          delete next[key];
          return next;
        });
      }
    }
    if (data.type === 'call.created') { callIdRef.current = data.call_id; return; }
    if (data.type === "call.offer") {
      if (data.call_id && (incomingCall?.callId === data.call_id || callIdRef.current === data.call_id)) return;
      if (activeCallRef.current || incomingCall) { wsRef.current?.send(JSON.stringify({ type: "call.hangup", room_id: data.room_id, to_user_id: data.from_user_id, reason: "busy" })); return; }
      callIdRef.current=data.call_id || null; callRoomRef.current=data.room_id;
      pendingCallIce.current.push(...(data.candidates || []).map((candidate:any)=>({roomId:data.room_id,candidate})));
      armCallTimeout();
      setIncomingCall({
        callId: data.call_id,
        roomId: data.room_id,
        callerId: data.from_user_id,
        callerName: data.from_username || "Абонент",
        isVideo: data.is_video || false,
        offer: data.offer || data.sdp,
      });
      return;
    }
    if (data.type === "call.answer" && activeCallRef.current && data.room_id === callRoomRef.current) {
      try {
        await activeCallRef.current.handleAnswer(data.answer || data.sdp);
      } catch (error) {
        console.error("Failed to handle answer:", error);
      }
      return;
    }
    if (
      (data.type === "call.ice" || data.type === "call.ice_candidate") &&
      data.room_id
    ) {
      try {
        if (activeCallRef.current && callRoomRef.current === data.room_id) await activeCallRef.current.addIceCandidate(data.candidate || data.ice);
        else { pendingCallIce.current.push({ roomId: data.room_id, candidate: data.candidate || data.ice }); if (pendingCallIce.current.length > 128) pendingCallIce.current.shift(); }
      } catch (error) {
        console.error("Failed to add ICE candidate:", error);
      }
      return;
    }
    if ((data.type === "call.hangup" || data.type === "call.end") && (!data.room_id || data.room_id === callRoomRef.current || data.room_id === incomingCall?.roomId)) {
      endCall(false);
      return;
    }
  }

  async function bootstrap(nextBaseUrl: string, nextToken: string) {
    const [user, roomList] = await Promise.all([
      api.me(nextBaseUrl, nextToken),
      api.rooms(nextBaseUrl, nextToken),
    ]);
    if(tokenRef.current!==nextToken)return;
    await storage.set("mobile.user", JSON.stringify(user));
    setMe(user);
    setRooms(roomList);
    // Initial navigation must not wait for every preview's key derivation or prekeys.
    void prepareRooms(roomList, nextBaseUrl, nextToken).then(prepared => {
      if (tokenRef.current === nextToken) setRooms(prepared);
    }).catch(() => {});
    void replenishPrekeys(nextToken, user.id, nextBaseUrl);
  }

  async function clearSession() {
    endCall();
    tokenRef.current = null;
    local.outbox.current?.stop();
    if (token) void pushNotifications.disconnect(baseUrl, token);
    const socket = wsRef.current;
    wsRef.current = null;
    socket?.close();
    if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
    reconnectTimerRef.current = null;
    Object.values(typingTimersRef.current).forEach(clearTimeout);
    typingTimersRef.current = {};
    setTypingUsers({});
    roomSecretCache.current.clear();
    clearDerivedRoomKeys();
    setToken(null);
    setMe(null);
    setRooms([]);
    setMessages([]);
    setActiveRoomId(null);
    setActiveRoomMembers([]);
    await removeSessionTokens();
    await storage.del("mobile.user");
  }

  async function persistSession(nextBaseUrl: string, nextToken: string, user?: User) {
    resetUnauthorizedGuard();
    tokenRef.current = nextToken;
    setBaseUrl(nextBaseUrl);
    setToken(nextToken);
    try {await storage.set(STORAGE_KEYS.token,nextToken);}catch{setError('Вход выполнен. Не удалось сохранить сеанс на телефоне: после перезапуска потребуется войти снова.');}
    if (user) {
      setMe(user);
      await storage.set("mobile.user", JSON.stringify(user)).catch(()=>{});
    }
    try {
      await bootstrap(nextBaseUrl, nextToken);
    } catch (e) {
      // The account and session already exist. Keep the signed-in state and allow refresh.
      setError(`Вход выполнен. Не удалось обновить данные: ${userError(e)}`);
    }
  }

  async function submitAuth() {
    if (authBusy) return;
    setAuthBusy(true);
    setError(null);
    const nextBaseUrl = normalizeBaseUrl(DEFAULT_BASE_URL);
    try {
      const result =
        authMode === "register"
          ? await api.mobileRegister(nextBaseUrl, username.trim(), password)
          : await api.mobileLogin(nextBaseUrl, username.trim(), password);
      if (!result.token) throw new Error("Server did not return a token");
      await persistSession(nextBaseUrl, result.token, result.user);
      setPassword("");
      setUsername("");
    } catch (e) {
      setError(errorText(e));
    } finally {
      setAuthBusy(false);
    }
  }

  async function refreshRooms() {
    if (!token) return;
    setRefreshingRooms(true);
    setError(null);
    try {
      const [roomList, user] = await Promise.all([
        api.rooms(baseUrl, token),
        api.me(baseUrl, token),
      ]);
      setRooms(await prepareRooms(roomList, baseUrl, token));
      setMe(user);
    } catch (e) {
      setError(errorText(e));
    } finally {
      setRefreshingRooms(false);
    }
  }

  async function openRoom(room: Room) {
    setSearchMatches(null);
    setActiveRoomMembers([]);
    if (!token) return;
    activeRoomIdRef.current = room.id;
    setActiveRoomId(room.id);
    setMessages([]);
    setComposer("");
    setEditing(null);
    setFocusId(null);
    setError(null);
    setRooms((prev) =>
      prev.some((r) => r.id === room.id) ? prev : [room, ...prev],
    );
    await local.open(room);
    try {
      const members = await api.roomMembers(baseUrl, token, room.id);
      if (activeRoomIdRef.current === room.id) setActiveRoomMembers(members);
    } catch {}
    if (wsRef.current?.readyState === WebSocket.OPEN)
      wsRef.current.send(
        JSON.stringify({ type: "rooms.join", room_id: room.id }),
      );
  }
  async function refreshMessages() {
    if (activeRoom) await local.open(activeRoom);
  }

  async function getRoomSecret(
    roomId: string,
    nextBaseUrl = baseUrl,
    nextToken = token,
  ) {
    if (!nextToken) return null;
    const cached = roomSecretCache.current.get(roomId);
    if (cached) return cached;
    const data = await api.roomKey(nextBaseUrl, nextToken, roomId);
    roomSecretCache.current.set(roomId, data.key);
    return data.key;
  }

  async function prepareMessage(
    message: Message,
    nextBaseUrl = baseUrl,
    nextToken = token,
  ): Promise<Message> {
    const preview = message.meta?.reply_preview as
      | { text?: string; meta?: { iv?: string } }
      | undefined;
    if (preview?.text && preview.meta?.iv && message.reply_to) {
      try {
        const secret = await getRoomSecret(
          message.room_id,
          nextBaseUrl,
          nextToken,
        );
        if (secret)
          message = {
            ...message,
            reply_to: {
              ...message.reply_to,
              text: await decryptRoomText(
                message.room_id,
                secret,
                preview.text,
                preview.meta.iv,
              ),
            },
          };
      } catch {}
    }
    const protectedPreview=message.meta?.reply_preview as Partial<Message> | undefined;
    if(protectedPreview?.meta?.e2e_group && message.reply_to) {
      const reply=await prepareMessage({...protectedPreview,id:message.id+'-reply',room_id:message.room_id,sender_id:message.sender_id,type:'text'} as Message,nextBaseUrl,nextToken);
      message={...message,reply_to:{...message.reply_to,text:reply.text}};
    }
    const caption=message.meta?.caption_message as Partial<Message> | undefined;
    if(message.type!=='text' && caption) {
      const decoded=await prepareMessage({...caption,id:message.id+'-caption',room_id:message.room_id,sender_id:message.sender_id,type:'text'} as Message,nextBaseUrl,nextToken);
      return {...message,text:decoded.text,_decrypted:decoded._decrypted};
    }
    if (message.type !== "text") return message;
    if (message.meta?.e2e_group) {
      try { if (!groupEncryption) throw Error('Ключи ещё загружаются'); return {...message, text: await groupEncryption.decrypt(message), _decrypted:true}; }
      catch (error) { return {...message,text:errorText(error),_decrypted:false}; }
    }
    if (
      message.meta?.encrypted &&
      message.meta?.e2ee_v === 2 &&
      message.text &&
      typeof message.meta.iv === "string"
    ) {
      try {
        const secret = await getRoomSecret(
          message.room_id,
          nextBaseUrl,
          nextToken,
        );
        if (!secret)
          return {
            ...message,
            text: "Не удалось расшифровать сообщение. Откройте веб-версию.",
          };
        return {
          ...message,
          text: await decryptRoomText(
            message.room_id,
            secret,
            message.text,
            message.meta.iv,
          ),
          _decrypted: true,
        };
      } catch {
        return {
          ...message,
          text: "Не удалось расшифровать сообщение. Откройте веб-версию.",
        };
      }
    }
    if (message.encrypted_content && message.sender_id !== me?.id) {
      try {
        const decrypted = await e2e.decrypt(
          message.sender_id,
          JSON.parse(message.encrypted_content),
        );
        return { ...message, text: decrypted, _decrypted: true };
      } catch {
        return {
          ...message,
          text: "Не удалось расшифровать сообщение. Откройте веб-версию.",
        };
      }
    }
    if (message.encrypted_content && !message._decrypted)
      return {
        ...message,
        text: "Сообщение со сквозным шифрованием. Откройте веб-версию.",
      };
    return message;
  }

  async function prepareMessages(
    items: Message[],
    nextBaseUrl = baseUrl,
    nextToken = token,
  ) {
    return Promise.all(
      items.map((item) => prepareMessage(item, nextBaseUrl, nextToken)),
    );
  }

  async function prepareRooms(
    items: Room[],
    nextBaseUrl = baseUrl,
    nextToken = token,
  ) {
    return Promise.all(
      items.map(async (room) => {
        if (room.last_msg_type !== "text") return room;
        if (
          !(
            room.last_msg_meta?.encrypted &&
            room.last_msg_meta?.e2ee_v === 2 &&
            room.last_msg_text &&
            typeof room.last_msg_meta.iv === "string"
          )
        ) {
          return room;
        }
        try {
          const secret = await getRoomSecret(room.id, nextBaseUrl, nextToken);
          if (!secret) return room;
          const text = await decryptRoomText(
            room.id,
            secret,
            room.last_msg_text,
            room.last_msg_meta.iv,
          );
          return { ...room, last_msg_text: text };
        } catch {
          return { ...room, last_msg_text: null };
        }
      }),
    );
  }

  async function saveProfile() {
    if (!token || !me) return;
    try {
      await api.updateMe(baseUrl, token, profileName.trim() || null);
      if (newPassword.trim())
        await api.changePassword(baseUrl, token, newPassword);
      const user = await api.me(baseUrl, token);
      setMe(user);
      setNewPassword("");
      setProfileOpen(false);
    } catch (e) {
      setError(errorText(e));
    }
  }

  async function pickAvatar() {
    if (!token) return;
    const result = await ImagePicker.launchImageLibraryAsync({
      mediaTypes: ["images"],
      quality: 0.9,
    });
    if (result.canceled || !result.assets?.[0]) return;
    try {
      setBusyUpload(true);
      const asset = result.assets[0];
      await api.uploadMyAvatar(baseUrl, token, {
        uri: asset.uri,
        name: asset.fileName || "avatar.jpg",
        type: asset.mimeType || "image/jpeg",
      });
      const user = await api.me(baseUrl, token);
      setMe(user);
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusyUpload(false);
    }
  }

  function notifyTyping(isTyping: boolean) {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== 1 || !activeRoomId) return;
    const now = Date.now();
    if (isTyping && now - typingSentAtRef.current < TYPING_SEND_INTERVAL_MS)
      return;
    typingSentAtRef.current = isTyping ? now : 0;
    try {
      ws.send(
        JSON.stringify({
          type: "typing",
          room_id: activeRoomId,
          is_typing: isTyping,
        }),
      );
    } catch {}
  }

  async function sendCurrentMessage() {
    if (!token || !activeRoomId || !composer.trim()) return;
    const text = composer.trim(),
      sendingRoomId = activeRoomId,
      sendingReply = local.reply;
    try {
      if (editing) {
        const encrypted = await encodeText(activeRoomId, text);
        const sent = await mobileApi(baseUrl, token).edit(
          activeRoomId,
          editing.id,
          encrypted,
        );
        const message = { ...sent, text, _decrypted: true };
        await local.repository.current?.saveMessages([message]);
        if (activeRoomIdRef.current === sendingRoomId) {
          setMessages((prev) => mergeMessages(prev, [message]));
          setEditing(null);
        }
      } else await local.send(text, undefined, sendingRoomId, sendingReply);
      if (activeRoomIdRef.current === sendingRoomId) {
        setComposer("");
        notifyTyping(false);
        setError(null);
      }
    } catch (e) {
      setError(errorText(e));
    }
  }

  async function sendPickedUpload(
    kind: "image" | "video" | "voice" | "video_note" | "media",
    file: { uri: string; name: string; type: string },
    duration?: number,
  ) {
    if (!token || !activeRoomId) return;
    const roomId=activeRoomId;
    const sendingToken=token;
    try {
      setBusyUpload(true);
      const uploaded = await protectedUploader(baseUrl,token,encodeText)(roomId,{...file,mime:file.type},()=>{}).promise;
      if(tokenRef.current!==sendingToken)throw Error('Сеанс изменился. Вложение сохранено для повторной отправки.');
      await local.send(
        "",
        {
          type:
            kind === "voice"
              ? "voice"
              : kind === "video_note"
                ? "video_note"
                : "media",
          media_id: uploaded.id,
          text: "",
          meta: {...uploaded.meta,...(duration ? {duration_ms:duration} : {})},
        },
        roomId,
      );
    } catch (e) {
      setError(errorText(e));
      throw e;
    } finally {
      setBusyUpload(false);
    }
  }

  async function pickImage() {
    const result = await ImagePicker.launchImageLibraryAsync({
      mediaTypes: ["images"],
      quality: 0.9,
    });
    if (result.canceled || !result.assets?.[0]) return;
    const asset = result.assets[0];
    await sendPickedUpload("image", {
      uri: asset.uri,
      name: asset.fileName || "image.jpg",
      type: asset.mimeType || "image/jpeg",
    });
  }

  async function pickVideo() {
    const result = await ImagePicker.launchImageLibraryAsync({
      mediaTypes: ["videos"],
      quality: 0.9,
    });
    if (result.canceled || !result.assets?.[0]) return;
    const asset = result.assets[0];
    await sendPickedUpload("video", {
      uri: asset.uri,
      name: asset.fileName || "video.mp4",
      type: asset.mimeType || "video/mp4",
    });
  }

  async function pickVideoNote() {
    const result = await ImagePicker.launchCameraAsync({
      mediaTypes: ["videos"],
      videoMaxDuration: 60,
      quality: 0.8,
    });
    if (result.canceled || !result.assets?.[0]) return;
    const asset = result.assets[0];
    await sendPickedUpload("video_note", {
      uri: asset.uri,
      name: asset.fileName || "circle.mp4",
      type: asset.mimeType || "video/mp4",
    });
  }

  async function pickFile() {
    const result = await DocumentPicker.getDocumentAsync({ multiple: false });
    if (result.canceled || !result.assets?.[0]) return;
    const asset = result.assets[0];
    await sendPickedUpload("media", {
      uri: asset.uri,
      name: asset.name,
      type: asset.mimeType || "application/octet-stream",
    });
  }

  async function toggleRecording() {
    if (recording) {
      const current = recording;
      setRecording(null);
      try {
        await current.stopAndUnloadAsync();
        const uri = current.getURI();
        if (uri) {
          await sendPickedUpload("voice", {
            uri,
            name: Platform.OS === "ios" ? "voice.caf" : "voice.m4a",
            type: Platform.OS === "ios" ? "audio/x-caf" : "audio/mp4",
          });
        }
      } catch (e) {
        setError(errorText(e));
      }
      return;
    }

    try {
      const perm = await Audio.requestPermissionsAsync();
      if (!perm.granted) {
        Alert.alert("Microphone", "Microphone permission is required.");
        return;
      }
      await Audio.setAudioModeAsync({
        allowsRecordingIOS: true,
        playsInSilentModeIOS: true,
      });
      const rec = new Audio.Recording();
      await rec.prepareToRecordAsync(
        Audio.RecordingOptionsPresets.HIGH_QUALITY,
      );
      await rec.startAsync();
      setRecording(rec);
    } catch (e) {
      setError(errorText(e));
    }
  }

  async function playVoice(message: Message) {
    const url = mediaUrl(baseUrl, message.media_id);
    if (!url) return;
    try {
      if (audioRef.current) {
        await audioRef.current.unloadAsync();
        audioRef.current = null;
      }
      if (playingId === message.id) {
        setPlayingId(null);
        return;
      }
      setPlayingId(message.id);
      await Audio.setAudioModeAsync({ playsInSilentModeIOS: true });
      const { sound } = await Audio.Sound.createAsync(
        { uri: url, headers: { Authorization: `Bearer ${token}` } },
        { shouldPlay: true },
      );
      audioRef.current = sound;
      sound.setOnPlaybackStatusUpdate((status) => {
        if (!status.isLoaded || !status.didJustFinish) return;
        setPlayingId(null);
        sound.unloadAsync().catch(() => undefined);
      });
    } catch (e) {
      setPlayingId(null);
      setError(errorText(e));
    }
  }

  function openMedia(message: Message) {
    setViewerMessage(message);
  }

  function closeMedia() {
    setViewerMessage(null);
  }

  async function toggleReaction(emoji: string) {
    if (!token || !reactionTarget) return;
    const existing = reactionTarget.reactions?.[emoji] || [];
    try {
      if (existing.includes(me?.id || "")) {
        await api.removeReaction(baseUrl, token, reactionTarget.id, emoji);
        setMessages((prev) =>
          prev.map((item) =>
            item.id === reactionTarget.id
              ? withReaction(item, emoji, me?.id || "", false)
              : item,
          ),
        );
      } else {
        await api.addReaction(baseUrl, token, reactionTarget.id, emoji);
        setMessages((prev) =>
          prev.map((item) =>
            item.id === reactionTarget.id
              ? withReaction(item, emoji, me?.id || "", true)
              : item,
          ),
        );
      }
      setReactionTarget(null);
    } catch (e) {
      setError(errorText(e));
    }
  }

  async function logout() {
    const previousToken = token;
    await clearSession();
    if (previousToken) void api.logout(baseUrl, previousToken).catch(() => {});
  }

  const [folders, setFolders] = useState<{ id: string; title: string; room_ids: string[] }[]>([]);
  const [selectedFolder, setSelectedFolder] = useState<string | null>(null);
  useEffect(() => {
    if (!token || !me) { setFolders([]); setSelectedFolder(null); return; }
    let live = true;
    const load = () => void mobileApi(baseUrl, token).folders().then(items => { if (live && tokenRef.current === token) { setFolders(items); void local.repository.current?.set("folders", items); } }).catch(async () => { const cached = await local.repository.current?.get<typeof folders>("folders", []); if (live && cached) setFolders(cached); });
    load(); const sub = AppState.addEventListener("change", state => { if (state === "active") load(); });
    return () => { live = false; sub.remove(); };
  }, [token, me?.id, local.ready, refreshingRooms]);
  const folderFilter = folders.find(folder => folder.id === selectedFolder);

  const activeCallRef = useRef<WebRTCCall | null>(null);
  const callRoomRef = useRef<string | null>(null);
  const callIdRef = useRef<string | null>(null);
  useEffect(() => {
    if (!token || !me) return;
    let live=true;
    const check=async()=>{try {const pending=await nativeCalls.pending();if(!pending || !live)return;
      const data=await request<any>(baseUrl,`/calls/${pending.call_id}`,{token}); if(!live)return;
      await onWsMessageRef.current?.(data); if(pending.action==='answer')setAutoAnswerId(pending.call_id);
    }catch(error){if(live)setError('Звонок уже завершён или соединение недоступно');}};
    void check();const sub=AppState.addEventListener('change',state=>{if(state==='active')void check();});const action=nativeCalls.listen(()=>void check());
    return ()=>{live=false;sub.remove();action?.remove();};
  },[token,me?.id,baseUrl]);
  useEffect(()=>{if(autoAnswerId && incomingCall?.callId===autoAnswerId && connection==='connected'){setAutoAnswerId(null);void answerCall();}},[autoAnswerId,incomingCall,connection]);
  const pendingCallIce = useRef<{ roomId: string; candidate: any }[]>([]);
  const callTimeout = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => { if (callTimeout.current) clearTimeout(callTimeout.current); void activeCallRef.current?.end(); }, []);
  function armCallTimeout() { if (callTimeout.current) clearTimeout(callTimeout.current); callTimeout.current = setTimeout(() => { setError("Абонент не ответил"); endCall(); }, 45000); }
  function clearCallTimeout() { if (callTimeout.current) clearTimeout(callTimeout.current); callTimeout.current = null; }

  async function startCall(isVideo: boolean) {
    if (!activeRoomId || !token || activeCallRef.current || incomingCall) return;
    if(connection!=="connected"){setError("Дождитесь подключения для звонка");return;}
    const roomId = activeRoomId; callRoomRef.current = roomId; pendingCallIce.current = [];
    setIsAudioEnabled(true);
    try {
      const ice = await api
        .iceServers(baseUrl, token)
        .then((r) => r.iceServers)
        .catch(() => undefined);
      const call = new WebRTCCall(
        { audio: true, video: isVideo },
        {
          onLocalStream: (stream) => setLocalStream(stream),
          onRemoteStream: (stream) => setRemoteStream(stream),
          onIceCandidate: (candidate) => {
            wsRef.current?.send(
              JSON.stringify({
                type: "call.ice",
                room_id: roomId,
                candidate,
              }),
            );
          },
          onConnectionStateChange: (state) => {
            if (state === "connected") { clearCallTimeout(); setCallState("connected"); }
            if (state === "failed" || state === "closed") endCall();
          },
          onError: (error) => {
            console.error("WebRTC error:", error);
            setError(error.message);
            endCall();
          },
        },
        true,
        ice,
      );

      activeCallRef.current = call;
      await beginAudioCall();
      await call.start();
      await nativeCalls.start(isVideo); setSpeakerEnabled(isVideo); await nativeCalls.speaker(isVideo);
      for (const ice of pendingCallIce.current.splice(0)) if (ice.roomId === roomId) await call.addIceCandidate(ice.candidate);
      armCallTimeout();
      setActiveCall(call);
      setCallState("calling");
      setIsVideoEnabled(isVideo);

      const offer = call.getLocalDescription();
      wsRef.current?.send(
        JSON.stringify({
          type: "call.offer",
          room_id: roomId,
          offer,
          sdp: offer,
          is_video: isVideo,
        }),
      );
    } catch (error) {
      console.error("Failed to start call:", error);
      setError("Не удалось начать звонок. Проверьте разрешения микрофона и камеры.");
      endCall();
    }
  }

  async function answerCall() {
    if (!incomingCall || !token) return;
    if(connection!=='connected'){setError('Подключаемся к звонку. Нажмите ответить после восстановления соединения.');return;}

    const roomId = incomingCall.roomId; callRoomRef.current = roomId;
    setIsAudioEnabled(true);
    try {
      const ice = await api
        .iceServers(baseUrl, token)
        .then((r) => r.iceServers)
        .catch(() => undefined);
      const call = new WebRTCCall(
        { audio: true, video: incomingCall.isVideo },
        {
          onLocalStream: (stream) => setLocalStream(stream),
          onRemoteStream: (stream) => setRemoteStream(stream),
          onIceCandidate: (candidate) => {
            wsRef.current?.send(
              JSON.stringify({
                type: "call.ice",
                room_id: incomingCall.roomId,
                candidate,
              }),
            );
          },
          onConnectionStateChange: (state) => {
            if (state === "connected") { clearCallTimeout(); setCallState("connected"); }
            if (state === "failed" || state === "closed") endCall();
          },
          onError: (error) => {
            console.error("WebRTC error:", error);
            setError(error.message);
            endCall();
          },
        },
        false,
        ice,
      );

      activeCallRef.current = call;
      await call.start();
      await nativeCalls.start(incomingCall.isVideo); setSpeakerEnabled(incomingCall.isVideo); await nativeCalls.speaker(incomingCall.isVideo);
      if(incomingCall.callId)await nativeCalls.clear(incomingCall.callId);
      const answer = await call.handleOffer(incomingCall.offer);
      setActiveCall(call);
      for (const ice of pendingCallIce.current.splice(0)) if (ice.roomId === roomId) await call.addIceCandidate(ice.candidate);
      setCallState("calling"); armCallTimeout();
      setIsVideoEnabled(incomingCall.isVideo);
      void mobileApi(baseUrl, token).room(incomingCall.roomId).then(openRoom).catch(() => {});
      setIncomingCall(null);

      wsRef.current?.send(
        JSON.stringify({
          type: "call.answer",
          room_id: incomingCall.roomId,
          answer,
          sdp: answer,
        }),
      );
    } catch (error) {
      console.error("Failed to answer call:", error);
      setError("Не удалось ответить на звонок. Проверьте разрешения микрофона и камеры.");
      void activeCallRef.current?.end(); activeCallRef.current = null;
      rejectCall();
    }
  }

  function rejectCall() {
    if (!incomingCall) return;
    if(incomingCall.callId)void nativeCalls.clear(incomingCall.callId);
    clearCallTimeout();callRoomRef.current=null;callIdRef.current=null;
    wsRef.current?.send(
      JSON.stringify({
        type: "call.hangup",
        room_id: incomingCall.roomId,
      }),
    );
    setIncomingCall(null);
  }

  function endCall(signal = true) {
    const call = activeCallRef.current, roomId = callRoomRef.current;
    activeCallRef.current = null; callRoomRef.current = null; pendingCallIce.current = []; clearCallTimeout();
    if(callIdRef.current)void nativeCalls.clear(callIdRef.current);callIdRef.current=null;void nativeCalls.stop();
    if (call) void call.end();
    if(signal && roomId && wsRef.current?.readyState===WebSocket.OPEN)wsRef.current.send(JSON.stringify({type:'call.hangup',room_id:roomId}));
    // Every exit path lands here: hang up, decline, network drop, timeout.
    // Restoring media routing is what stops audio sounding "like a call".
    void endAudioCall();
    setActiveCall(null); setLocalStream(null); setRemoteStream(null); setCallState("idle"); setIncomingCall(null);
  }

  function toggleAudio() {
    if (activeCall) {
      const newState = !isAudioEnabled;
      activeCall.toggleAudio(newState);
      setIsAudioEnabled(newState);
    }
  }

  function toggleVideo() {
    if (activeCall) {
      const newState = !isVideoEnabled;
      activeCall.toggleVideo(newState);
      setIsVideoEnabled(newState);
    }
  }

  async function switchCamera() {
    if (activeCall) {
      try {
        await activeCall.switchCamera();
      } catch (error) {
        console.error("Failed to switch camera:", error);
      }
    }
  }

  const activeRoom = rooms.find((room) => room.id === activeRoomId) || null;
  const peer = useChatPresence(
    baseUrl,
    token,
    activeRoom?.kind === "dm" ? activeRoom.other_user_id : null,
  );
  const participants: User[] = [...activeRoomMembers, ...(me ? [me] : [])];
  if (activeRoom?.kind === "dm" && activeRoom.other_user_id)
    participants.push(
      peer || {
        id: activeRoom.other_user_id,
        username: activeRoom.other_username || "",
        display_name: activeRoom.other_display_name,
        avatar_id: activeRoom.other_avatar_id,
      },
    );
  const typingNames = Object.values(typingUsers)
    .filter((u) => u.roomId === activeRoomId)
    .map((u) => u.username);

  if (loading) {
    return (
      <SafeAreaView style={styles.center}>
        <StatusBar style={theme.dark ? "light" : "dark"} />
        <ActivityIndicator size="large" color={telegramColors.misc.progress} />
      </SafeAreaView>
    );
  }

  // App lock: gate the whole UI until the device authenticates. Shown before
  // the login screen so a locked app never reveals cached chats either.
  if (!lockReady) return <SafeAreaView style={styles.screen}><ActivityIndicator /></SafeAreaView>;
  if (lockEnabled && !unlocked) {
    return (
      <>
        <StatusBar style={theme.dark ? "light" : "dark"} />
        <LockScreen
          label={lockButtonLabel}
          busy={unlocking}
          error={lockError}
          onUnlock={tryUnlock}
        />
      </>
    );
  }

  if (!token || !me) {
    return (
      <>
        <StatusBar style={theme.dark ? "light" : "dark"} />
        {appUpdate.visible && <UpdateBanner state={appUpdate.state} onUpdate={appUpdate.start} onDismiss={appUpdate.dismiss} onRetryInstall={appUpdate.retryInstall} onOpenPermissionSettings={appUpdate.openPermissionSettings} />}
        <AuthScreen
          authMode={authMode}
          username={username}
          password={password}
          error={error}
          busy={authBusy}
          onChangeMode={setAuthMode}
          onChangeUsername={setUsername}
          onChangePassword={setPassword}
          onSubmit={submitAuth}
        />
      </>
    );
  }

  const nativeWorkspace =
    !!workspace &&
    workspace !== "attachments" && workspace !== "circle" &&
    !workspace.startsWith("room:");
  const renderWorkspace = () => (
    <Workspace
      lockEnabled={lockEnabled}
      onChangeLock={async()=>{
        const lock=await describeLock();
        if(!lock.available)throw Error('Сначала настройте код блокировки или биометрию в Android');
        if(!await unlockApp('Подтвердите изменение блокировки 67chat'))throw Error('Изменение отменено');
        await setLockPreference(!lockEnabled);setLockEnabled(!lockEnabled);setUnlocked(true);
      }}
      onCheckUpdates={appUpdate.recheck}
      updateLabel={`${currentVersionName()} · ${{checking:'Проверка…','up-to-date':'Установлена последняя версия',unavailable:'Не удалось проверить. Повторите при подключении к сети',available:'Доступно обновление',unsupported:'Обновление недоступно для этой сборки'}[appUpdate.checkStatus] || 'Проверка при запуске'}`}
      uploader={protectedUploader(baseUrl,token,encodeText)}
      decode={prepareMessage}
      embedded={nativeWorkspace}
      onMode={setWorkspace}
      mode={workspace}
      onClose={() => setWorkspace(null)}
      baseUrl={baseUrl}
      token={token}
      user={me}
      rooms={rooms}
      room={activeRoom}
      repository={local.repository.current}
      onOpenSearch={async (room, ids, index) => {
        setWorkspace(null);
        await openRoom(room);
        await local.jump(ids[index]);
        setFocusId(ids[index]);
        setSearchMatches({ roomId: room.id, ids, index });
      }}
      onOpen={async (room, id) => {
        setWorkspace(null);
        await openRoom(room);
        if (id) {
          await local.jump(id);
          setFocusId(id);
        }
      }}
      onOpenMedia={(message) => {
        setWorkspace(null);
        openMedia(message);
      }}
      onClearData={async () => {
        local.outbox.current?.stop();
        await removePrivateMedia(
          (await local.repository.current?.privateFiles()) || [],
        );
        await local.repository.current?.destroy();
        setWorkspace(null);
        await logout();
      }}
      onProfile={() => {
        setWorkspace(null);
        setProfileName(me.display_name || "");
        setProfileOpen(true);
      }}
      onRefresh={refreshRooms}
      onError={setError}
      onLogout={logout}
      onPickImage={pickImage}
      onPickVideo={pickVideo}
      onPickFile={pickFile}
      onPickVideoNote={pickVideoNote}
      forwarding={forwarding}
      enqueueForward={async (room, message) => {
        const sendingToken=token;
        let body: Record<string, unknown>;
        if (message.type === "text") {
          body = await encodeText(room.id,message.text || "");
        } else
          body = {
            type: message.type,
            media_id: message.media_id,
            text: message.text || "",
            meta: { ...message.meta, caption_message: undefined, forwarded: true },
          };
        const protectedFile=message.meta?.media_encryption as {key_message:Partial<Message>} | undefined;
        if(protectedFile && message.type!=='text') {
          const key=await prepareMessage({...protectedFile.key_message,id:message.id+'-key',room_id:message.room_id,sender_id:message.sender_id,type:'text'} as Message);
          if(!key._decrypted)throw Error('Не удалось получить ключ вложения');
          body.meta={...(body.meta as any),caption_message:undefined,media_encryption:{...protectedFile,key_message:await encodeText(room.id,key.text || '')}};
        }
        if(tokenRef.current!==sendingToken)throw Error('Сеанс изменился');
        await local.send(message.text || "", body, room.id, null);
      }}
      onForwarded={() => setForwarding([])}
      onSendMedia={async (text, body, roomId) => {
        if(tokenRef.current!==token)throw Error('Сеанс изменился. Вложения сохранены.');
        await local.send(text, body, roomId);
      }}
    />
  );
  const renderProfile = () => (
    <ProfileScreen
      user={me}
      baseUrl={baseUrl}
      profileName={profileName}
      newPassword={newPassword}
      busyUpload={busyUpload}
      onClose={() => setProfileOpen(false)}
      onChangeName={setProfileName}
      onChangePassword={setNewPassword}
      onSave={saveProfile}
      onPickAvatar={pickAvatar}
      mediaUrl={mediaUrl}
    />
  );

  return (
    <MediaDecoder.Provider value={{baseUrl,userId:me.id,decode:prepareMessage}}><MediaSession.Provider value={{ Authorization: `Bearer ${token}` }}>
      <SafeAreaView style={styles.screen}>
        <StatusBar style={theme.dark ? "light" : "dark"} />
        <ConnectionBanner
          status={connection}
          error={error}
          onDismiss={() => setError(null)}
        />
        {appUpdate.visible && (
          <UpdateBanner
            state={appUpdate.state}
            onUpdate={appUpdate.start}
            onDismiss={appUpdate.dismiss}
            onRetryInstall={appUpdate.retryInstall}
            onOpenPermissionSettings={appUpdate.openPermissionSettings}
          />
        )}
        <KeyboardAvoidingView
          style={styles.flex}
          behavior={Platform.OS === "ios" ? "padding" : "height"}
        >
          <AppNavigation
            roomId={activeRoomId}
            workspace={nativeWorkspace ? renderWorkspace : undefined}
            profile={profileOpen ? renderProfile : undefined}
            onCloseWorkspace={() => setWorkspace(null)}
            onCloseProfile={() => setProfileOpen(false)}
            onBack={() => setActiveRoomId(null)}
            list={() => (
              <ChatListScreen
                user={me}
                folders={folders}
                selectedFolder={selectedFolder}
                onFolder={setSelectedFolder}
                onFolders={() => setWorkspace("folders")}
                onSaved={() => { void mobileApi(baseUrl, token).saved().then(async ({ id }) => { await refreshRooms(); await openRoom(await mobileApi(baseUrl, token).room(id)); }).catch(e => setError(userError(e))); }}
                rooms={rooms
                  .filter(
                    (r) => !r.preferences?.hidden && !r.preferences?.archived && (!folderFilter || folderFilter.room_ids.includes(r.id)),
                  )
                  .sort(
                    (a, b) =>
                      Number(!!b.preferences?.pinned) -
                      Number(!!a.preferences?.pinned),
                  )}
                baseUrl={baseUrl}
                refreshing={refreshingRooms}
                onRefresh={refreshRooms}
                onProfilePress={() => {
                  setWorkspace("settings");
                }}
                onRoomPress={openRoom}
                onLogout={logout}
                archivedCount={
                  rooms.filter(
                    (room) =>
                      room.preferences?.archived && !room.preferences?.hidden,
                  ).length
                }
                onCompose={() => setWorkspace("new")}
                onArchive={() => setWorkspace("archive")}
                onSearch={() => setWorkspace("search")}
                onRoomMenu={(room) => {
                  setRooms((prev) => prev);
                  setWorkspace(`room:${room.id}`);
                }}
                mediaUrl={mediaUrl}
              />
            )}
            chat={() =>
              activeRoom ? (
                <ChatScreen
                  onUnpin={activeRoom.kind==='dm' || ['owner','admin'].includes(activeRoom.my_role || '') ? ()=>{void request(baseUrl,`/rooms/${activeRoom.id}/pin`,{token,method:'PUT',body:{message_id:null}}).catch(e=>setError(userError(e)));}:undefined}
                  room={activeRoom}
                  messages={messages}
                  meId={me.id}
                  baseUrl={baseUrl}
                  composer={composer}
                  typingNames={typingNames}
                  participants={[
                    ...new Map(
                      participants.map((user) => [user.id, user]),
                    ).values(),
                  ]}
                  peer={peer}
                  connection={connection}
                  refreshing={refreshingMessages}
                  recording={!!recording}
                  playingId={playingId}
                  disabled={
                    busyUpload ||
                    (activeRoom.kind === "channel" &&
                      !(
                        ["owner", "admin"].includes(activeRoom.my_role || "") ||
                        activeRoom.created_by === me.id
                      ))
                  }
                  onBack={() => setActiveRoomId(null)}
                  onRefresh={refreshMessages}
                  onChangeComposer={changeComposer}
                  onSend={sendCurrentMessage}
                  onAttach={() => setWorkspace("attachments")}
                  onVideoNote={() => setWorkspace("circle")}
                  onVoice={toggleRecording}
                  onVoiceRecorded={async (uri, duration) => {
                    await sendPickedUpload("voice", {
                      uri,
                      name: "voice.m4a",
                      type: "audio/mp4",
                    }, duration);
                  }}
                  onAudioCall={
                    activeRoom.kind === "dm" && !activeRoom.is_saved
                      ? () => startCall(false)
                      : undefined
                  }
                  onVideoCall={
                    activeRoom.kind === "dm" && !activeRoom.is_saved ? () => startCall(true) : undefined
                  }
                  onPlayVoice={playVoice}
                  onReact={(message) =>
                    setReactionTarget({
                      id: message.id,
                      reactions: message.reactions || null,
                    })
                  }
                  onOpenMedia={openMedia}
                  mediaUrl={mediaUrl}
                  mediaThumbUrl={mediaThumbUrl}
                  loading={local.loading}
                  loadingOlder={local.loadingOlder}
                  hasOlder={!!local.history?.has_older}
                  hasNewer={!!local.history?.has_newer}
                  onNewer={() => {
                    const last = messages.filter((m) => !m.delivery).at(-1);
                    if (last)
                      void local
                        .newer(last.id)
                        .catch((e) => setError(String(e)));
                  }}
                  firstUnreadId={local.history?.first_unread_id || null}
                  focusId={focusId}
                  repository={local.repository.current}
                  onOlder={() => {
                    const first = messages.find((m) => !m.delivery);
                    if (first) void local.older(first.id);
                  }}
                  onVisible={(id) => {
                    void mobileApi(baseUrl, token)
                      .read(activeRoomId!, id)
                      .catch(() => {});
                  }}
                  onUsername={(username) => {
                    void mobileApi(baseUrl, token)
                      .users(username)
                      .then((users) => {
                        const user = users.find(
                          (u) =>
                            u.username.toLowerCase() === username.toLowerCase(),
                        );
                        if (user)
                          return mobileApi(baseUrl, token)
                            .createRoom("dm", [user.username])
                            .then(({ id }) =>
                              mobileApi(baseUrl, token).room(id),
                            )
                            .then(openRoom);
                        setError("Пользователь не найден");
                      })
                      .catch((e) => setError(String(e)));
                  }}
                  onDetails={() => setWorkspace("details")}
                  onSearch={() => setWorkspace("search-room")}
                  searchMatches={
                    searchMatches?.roomId === activeRoomId
                      ? {
                          index: searchMatches.index,
                          total: searchMatches.ids.length,
                        }
                      : undefined
                  }
                  onCloseSearch={() => setSearchMatches(null)}
                  onSearchNavigate={(delta) => {
                    if (!searchMatches) return;
                    const next = Math.max(
                      0,
                      Math.min(
                        searchMatches.ids.length - 1,
                        searchMatches.index + delta,
                      ),
                    );
                    const id = searchMatches.ids[next];
                    void local
                      .jump(id)
                      .then(() => {
                        if (activeRoomIdRef.current === searchMatches.roomId) {
                          setFocusId(id);
                          setSearchMatches({ ...searchMatches, index: next });
                        }
                      })
                      .catch((e) => setError(String(e)));
                  }}
                  selectionReset={selectionReset}
                  onActions={(items) => setActionMessages(items)}
                  onReply={(message) => {
                    local.setReply(message);
                    void local.repository.current?.set(
                      `draft:${activeRoomId}`,
                      { text: composer, reply: message },
                    );
                  }}
                  reply={editing || local.reply}
                  editing={!!editing}
                  onCancelReply={() => {
                    local.setReply(null);
                    setEditing(null);
                    void local.repository.current?.set(
                      `draft:${activeRoomId}`,
                      { text: composer },
                    );
                  }}
                  onJump={(id) => {
                    void local
                      .jump(id)
                      .then(() => setFocusId(id))
                      .catch((e) => setError(String(e)));
                  }}
                  onRetry={(id) => {
                    void local.retry(id);
                  }}
                  onCancelSend={(id) => {
                    void local.cancel(id);
                  }}
                />
              ) : null
            }
          />
        </KeyboardAvoidingView>

        {!nativeWorkspace && renderWorkspace()}

        <MessageActions
          onPin={activeRoom?.kind==='dm' || ['owner','admin'].includes(activeRoom?.my_role || '') ? async(message)=>{
            await request(baseUrl,`/rooms/${message.room_id}/pin`,{token,method:'PUT',body:{message_id:message.id}});
            setRooms(prev=>prev.map(room=>room.id===message.room_id?{...room,pinned_message_id:message.id}:room));
          }:undefined}
          onHide={async(items)=>{
            for(const message of items){
              await request(baseUrl,`/rooms/${message.room_id}/messages/${message.id}/hide`,{token,method:'DELETE'});
              await local.repository.current?.hideMessage(message.id);
              setMessages(prev=>prev.filter(m=>m.id!==message.id));
            }
          }}
          messages={actionMessages}
          meId={me.id}
          onSelectMore={() => setActionMessages([])}
          onClose={() => {
            setActionMessages([]);
            setSelectionReset((n) => n + 1);
          }}
          onReply={(m) => {
            local.setReply(m);
            void local.repository.current?.set(`draft:${activeRoomId}`, {
              text: composer,
              reply: m,
            });
          }}
          onReact={(m) =>
            setReactionTarget({ id: m.id, reactions: m.reactions || null })
          }
          onEdit={(m) => {
            setEditing(m);
            setComposer(m.text || "");
          }}
          onForward={(items) => {
            setForwarding(items);
            setWorkspace("forward");
          }}
          onDelete={async (items) => {
            for (const m of items) {
              await mobileApi(baseUrl, token).remove(m.room_id, m.id);
              await local.repository.current?.removeMessage(m.id);
            }
            setMessages((prev) =>
              prev.filter((m) => !items.some((x) => x.id === m.id)),
            );
          }}
          onError={setError}
        />

        <ReactionModal
          open={!!reactionTarget}
          onClose={() => setReactionTarget(null)}
          onPick={toggleReaction}
        />

        <MediaViewer
          messages={messages.some(m => m.id === viewerMessage?.id) ? messages : viewerMessage ? [viewerMessage] : []}
          userId={me.id}
          decode={prepareMessage}
          onChange={setViewerMessage}
          message={viewerMessage}
          baseUrl={baseUrl}
          onClose={closeMedia}
        />

        {incomingCall && (
          <Modal visible transparent animationType="fade">
            <View style={styles.callModalOverlay}>
              <View style={styles.callModalCard}>
                <Text style={styles.callModalTitle}>Входящий вызов</Text>
                <Text style={styles.callModalName}>
                  {incomingCall.callerName}
                </Text>
                <Text style={styles.callModalType}>
                  {incomingCall.isVideo ? "Видеовызов" : "Аудиовызов"}
                </Text>
                <View style={styles.callModalButtons}>
                  <Pressable
                    style={[styles.callButton, styles.callButtonAccept]}
                    onPress={answerCall}
                  >
                    <Text style={styles.callButtonText}>✓ Ответить</Text>
                  </Pressable>
                  <Pressable
                    style={[styles.callButton, styles.callButtonReject]}
                    onPress={rejectCall}
                  >
                    <Text style={styles.callButtonText}>✕ Отклонить</Text>
                  </Pressable>
                </View>
              </View>
            </View>
          </Modal>
        )}

        {callState !== "idle" && (
          <Modal visible animationType="slide">
            <View style={styles.callScreen}>
              {remoteStream && (
                <RTCView
                  streamURL={remoteStream.toURL()}
                  style={styles.remoteVideo}
                  objectFit="cover"
                />
              )}
              {localStream && isVideoEnabled && (
                <RTCView
                  streamURL={localStream.toURL()}
                  style={styles.localVideo}
                  objectFit="cover"
                  mirror
                />
              )}
              <View style={styles.callOverlay}>
                <Text style={styles.callStatus}>
                  {callState === "calling"
                    ? "Звоним..."
                    : callState === "ringing"
                      ? "Звонит..."
                      : "Подключено"}
                </Text>
                <View style={styles.callControls}>
                  <Pressable
                    style={styles.callControlButton}
                    onPress={toggleAudio}
                  >
                    <Icon
                      name="mic"
                      color={
                        isAudioEnabled
                          ? telegramColors.text.primary
                          : telegramColors.misc.error
                      }
                    />
                  </Pressable>
                  <Pressable style={styles.callControlButton} accessibilityLabel={speakerEnabled ? 'Выключить громкую связь' : 'Громкая связь'} onPress={()=>{const value=!speakerEnabled;setSpeakerEnabled(value);void nativeCalls.speaker(value)?.catch(()=>setError('Не удалось переключить звук'));}}>
                    <Icon name="speaker" color={speakerEnabled ? telegramColors.text.blue : telegramColors.text.primary} />
                  </Pressable>
                  {!!localStream?.getVideoTracks().length && (
                    <>
                      <Pressable
                        style={styles.callControlButton}
                        onPress={toggleVideo}
                      >
                        <Icon name="video" />
                      </Pressable>
                      <Pressable
                        style={styles.callControlButton}
                        onPress={switchCamera}
                      >
                        <Icon name="camera-flip" />
                      </Pressable>
                    </>
                  )}
                  <Pressable
                    style={[styles.callControlButton, styles.callEndButton]}
                    onPress={() => endCall()}
                  >
                    <Text style={styles.callControlIcon}>✕</Text>
                  </Pressable>
                </View>
              </View>
            </View>
          </Modal>
        )}
      </SafeAreaView>
    </MediaSession.Provider></MediaDecoder.Provider>
  );
}

function ReactionModal({
  open,
  onClose,
  onPick,
}: {
  open: boolean;
  onClose: () => void;
  onPick: (emoji: string) => void;
}) {
  const styles = useStyles();
  const { colors: telegramColors } = useTheme();

  return (
    <Modal
      transparent
      visible={open}
      animationType="fade"
      onRequestClose={onClose}
    >
      <Pressable style={styles.modalBackdrop} onPress={onClose}>
        <View style={styles.reactionModal}>
          {REACTION_SET.map((emoji) => (
            <Pressable
              key={emoji}
              style={styles.reactionPick}
              onPress={() => onPick(emoji)}
            >
              <Text style={styles.reactionPickText}>{emoji}</Text>
            </Pressable>
          ))}
        </View>
      </Pressable>
    </Modal>
  );
}

function MediaViewerModal({
  open,
  message,
  baseUrl,
  onClose,
  onPlayVoice,
}: {
  open: boolean;
  message: Message | null;
  baseUrl: string;
  onClose: () => void;
  onPlayVoice: (message: Message) => void;
}) {
  const headers = useMediaHeaders();
  const styles = useStyles();
  const { colors: telegramColors } = useTheme();

  const url = message ? mediaUrl(baseUrl, message.media_id) : null;
  const mime = message?.media_mime || "";
  const isImage =
    !!message && message.type === "media" && mime.startsWith("image/");
  const isVideo =
    !!message && message.type === "media" && mime.startsWith("video/");
  const isVoice = !!message && message.type === "voice";
  const isVideoNote = !!message && message.type === "video_note";
  const isFile = !!message && !isImage && !isVideo && !isVoice && !isVideoNote;

  useEffect(() => {
    if (!open) return;
    const sub = BackHandler.addEventListener("hardwareBackPress", () => {
      onClose();
      return true;
    });
    return () => sub.remove();
  }, [open, onClose]);

  if (!message) return null;

  return (
    <Modal visible={open} animationType="fade" onRequestClose={onClose}>
      <View style={styles.viewerScreen}>
        <Pressable style={styles.viewerClose} onPress={onClose}>
          <Text style={styles.viewerCloseText}>Закрыть</Text>
        </Pressable>
        <View style={styles.viewerBody}>
          {isImage && url ? (
            <Image
              source={{ uri: url, headers }}
              style={styles.viewerImage}
              resizeMode="contain"
            />
          ) : null}
          {isVideo && url ? (
            <Video
              source={{ uri: url, headers }}
              style={styles.viewerVideo}
              useNativeControls
              resizeMode={ResizeMode.CONTAIN}
            />
          ) : null}
          {isVoice && url ? (
            <Pressable
              style={styles.viewerVoice}
              onPress={() => onPlayVoice(message)}
            >
              <Text style={styles.viewerVoiceText}>
                Воспроизвести
                {message.duration_sec
                  ? ` (${Math.round(message.duration_sec)}с)`
                  : ""}
              </Text>
            </Pressable>
          ) : null}
          {isVideoNote && url ? (
            <Video
              source={{ uri: url, headers }}
              style={styles.viewerVideoNote}
              useNativeControls
              resizeMode={ResizeMode.CONTAIN}
              isLooping
            />
          ) : null}
          {isFile && url ? (
            <Pressable
              style={styles.viewerFile}
              onPress={() => {
                void openMediaFile(
                  url,
                  headers,
                  message?.media_mime || "application/octet-stream",
                ).catch((e) => Alert.alert("Файл", String(e.message || e)));
              }}
            >
              <Text style={styles.viewerFileTitle}>
                {message.text || "Открыть файл"}
              </Text>
              <Text style={styles.viewerFileSubtitle}>{mime || "file"}</Text>
            </Pressable>
          ) : null}
        </View>
      </View>
    </Modal>
  );
}


function reorderRooms(rooms: Room[], roomId: string) {
  const idx = rooms.findIndex((room) => room.id === roomId);
  if (idx <= 0) return rooms;
  const next = rooms.slice();
  const [room] = next.splice(idx, 1);
  next.unshift(room);
  return next;
}

function updateRoomPreview(
  rooms: Room[],
  message: Message,
  meId: string | null,
) {
  return rooms.map((room) =>
    room.id !== message.room_id
      ? room
      : {
          ...room,
          last_msg_text: message.text || null,
          last_msg_type: message.type,
          last_msg_sender_id: message.sender_id,
          last_msg_sender_username: message.sender_username || null,
          last_msg_sender_name: message.sender_display_name || null,
          last_msg_at: message.created_at,
          unread_count:
            message.sender_id === meId
              ? 0
              : Math.max(1, Number(room.unread_count || 0)),
        },
  );
}

function upsertMessage(messages: Message[], incoming: Message) {
  const idx = messages.findIndex((item) => item.id === incoming.id);
  if (idx === -1) return [...messages, incoming];
  const next = messages.slice();
  next[idx] = { ...next[idx], ...incoming };
  return next;
}

function withReaction(
  message: Message,
  emoji: string,
  userId: string,
  add: boolean,
): Message {
  const prev = { ...(message.reactions || {}) };
  const users = new Set(prev[emoji] || []);
  if (add) users.add(userId);
  else users.delete(userId);
  if (users.size) prev[emoji] = [...users];
  else delete prev[emoji];
  return { ...message, reactions: prev };
}

function withRead(message: Message, userId: string): Message {
  const users = new Set(message.read_by || []);
  users.add(userId);
  return { ...message, read_by: [...users] };
}

function errorText(value: unknown) { return userError(value); }

const useStyles = () => {
  const { colors: telegramColors } = useTheme();
  return StyleSheet.create({
    flex: { flex: 1 },
    screen: { flex: 1, backgroundColor: telegramColors.windowBackground },
    center: {
      flex: 1,
      alignItems: "center",
      justifyContent: "center",
      backgroundColor: telegramColors.windowBackground,
    },

    modalBackdrop: {
      flex: 1,
      backgroundColor: telegramColors.misc.backdrop,
      justifyContent: "center",
      alignItems: "center",
    },
    reactionModal: {
      flexDirection: "row",
      flexWrap: "wrap",
      gap: 10,
      padding: 18,
      backgroundColor: telegramColors.composer.background,
      borderRadius: 20,
    },
    reactionPick: {
      width: 52,
      height: 52,
      borderRadius: 16,
      alignItems: "center",
      justifyContent: "center",
      backgroundColor: telegramColors.windowBackgroundGray,
    },
    reactionPickText: { fontSize: 24 },

    viewerScreen: {
      flex: 1,
      backgroundColor: telegramColors.misc.mediaOverlay,
      paddingTop: 20,
    },
    viewerClose: {
      alignSelf: "flex-end",
      marginHorizontal: 16,
      marginBottom: 12,
      paddingHorizontal: 14,
      paddingVertical: 10,
      borderRadius: 12,
      backgroundColor: telegramColors.misc.mediaOverlayControl,
    },
    viewerCloseText: {
      color: telegramColors.misc.mediaOverlayText,
      fontWeight: "700",
    },
    viewerBody: {
      flex: 1,
      alignItems: "center",
      justifyContent: "center",
      padding: 18,
    },
    viewerImage: { flex: 1, width: "100%", height: "100%" },
    viewerVideo: { width: "100%", height: "100%" },
    viewerVideoNote: {
      width: 240,
      height: 240,
      borderRadius: 120,
      overflow: "hidden",
    },
    viewerVoice: {
      paddingHorizontal: 18,
      paddingVertical: 14,
      borderRadius: 14,
      backgroundColor: telegramColors.misc.mediaOverlayControl,
    },
    viewerVoiceText: {
      color: telegramColors.misc.mediaOverlayText,
      fontWeight: "700",
    },
    viewerFile: {
      borderWidth: 1,
      borderColor: telegramColors.misc.mediaOverlayControlBorder,
      backgroundColor: telegramColors.misc.mediaOverlayControl,
      borderRadius: 16,
      padding: 16,
      width: "100%",
    },
    viewerFileTitle: {
      color: telegramColors.misc.mediaOverlayText,
      fontWeight: "700",
      fontSize: 16,
    },
    viewerFileSubtitle: { color: telegramColors.text.gray2, marginTop: 4 },

    callModalOverlay: {
      flex: 1,
      backgroundColor: telegramColors.misc.mediaOverlay,
      justifyContent: "center",
      alignItems: "center",
      padding: 20,
    },
    callModalCard: {
      width: "100%",
      maxWidth: 320,
      backgroundColor: telegramColors.composer.background,
      borderRadius: 24,
      padding: 24,
      alignItems: "center",
      gap: 16,
    },
    callModalTitle: {
      fontSize: 18,
      fontWeight: "700",
      color: telegramColors.text.gray5,
    },
    callModalName: {
      fontSize: 24,
      fontWeight: "800",
      color: telegramColors.text.primary,
    },
    callModalType: { fontSize: 16, color: telegramColors.text.blue },
    callModalButtons: { flexDirection: "row", gap: 12, marginTop: 8 },
    callButton: {
      flex: 1,
      paddingVertical: 14,
      borderRadius: 14,
      alignItems: "center",
      justifyContent: "center",
    },
    callButtonAccept: { backgroundColor: telegramColors.misc.callAccept },
    callButtonReject: { backgroundColor: telegramColors.misc.callReject },
    callButtonText: {
      color: telegramColors.text.primary,
      fontWeight: "700",
      fontSize: 16,
    },

    callScreen: {
      flex: 1,
      backgroundColor: telegramColors.misc.callBackground,
    },
    remoteVideo: {
      flex: 1,
      width: "100%",
      height: "100%",
      backgroundColor: telegramColors.misc.callBackground,
    },
    localVideo: {
      position: "absolute",
      top: 60,
      right: 20,
      width: 120,
      height: 160,
      borderRadius: 12,
      backgroundColor: telegramColors.misc.callLocalVideo,
    },
    callOverlay: {
      position: "absolute",
      top: 0,
      left: 0,
      right: 0,
      bottom: 0,
      justifyContent: "space-between",
      padding: 20,
    },
    callStatus: {
      fontSize: 18,
      fontWeight: "700",
      color: telegramColors.misc.mediaOverlayText,
      textAlign: "center",
      marginTop: 40,
    },
    callControls: {
      flexDirection: "row",
      justifyContent: "center",
      gap: 16,
      marginBottom: 40,
    },
    callControlButton: {
      width: 60,
      height: 60,
      borderRadius: 30,
      backgroundColor: telegramColors.misc.callControl,
      alignItems: "center",
      justifyContent: "center",
    },
    callEndButton: { backgroundColor: telegramColors.misc.callEnd },
    callControlIcon: { fontSize: 24 },
  });
};
