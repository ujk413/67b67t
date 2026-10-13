import { StatusBar } from "expo-status-bar";
import { Audio, ResizeMode, Video } from "expo-av";
import * as DocumentPicker from "expo-document-picker";
import * as ImagePicker from "expo-image-picker";
import { useEffect, useMemo, useRef, useState } from "react";
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
  SafeAreaView,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import { api, resetUnauthorizedGuard, setUnauthorizedHandler } from "./src/api";
import { DEFAULT_BASE_URL, mediaThumbUrl, mediaUrl, normalizeBaseUrl, wsUrlFromApi } from "./src/config";
import { displayName, roomPreview, roomTitle, shortTime } from "./src/format";
import { STORAGE_KEYS, storage } from "./src/storage";
import type { Message, Room, User } from "./src/types";
import { e2e } from "./src/crypto";
import { pushNotifications } from "./src/notifications";
import { WebRTCCall } from "./src/webrtc";
import { decryptRoomText, encryptRoomText } from "./src/roomCrypto";
import { RTCView } from "react-native-webrtc";

type AuthMode = "login" | "register";
type ReactionTarget = { id: string; reactions?: Record<string, string[]> | null } | null;

const REACTION_SET = ["👍", "❤️", "😂", "🔥", "😮", "👎"];
const TYPING_TTL_MS = 8000;
const TYPING_SEND_INTERVAL_MS = 3000;

export default function App() {
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
  const [typingUsers, setTypingUsers] = useState<Record<string, { username: string; roomId: string }>>({});
  const typingTimersRef = useRef<Record<string, ReturnType<typeof setTimeout>>>({});
  const typingSentAtRef = useRef(0);
  const [authMode, setAuthMode] = useState<AuthMode>("login");
  const [profileOpen, setProfileOpen] = useState(false);
  const [profileName, setProfileName] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [reactionTarget, setReactionTarget] = useState<ReactionTarget>(null);
  const [viewerMessage, setViewerMessage] = useState<Message | null>(null);
  const [recording, setRecording] = useState<Audio.Recording | null>(null);
  const [playingId, setPlayingId] = useState<string | null>(null);
  const [incomingCall, setIncomingCall] = useState<{ roomId: string; callerId: string; callerName: string; isVideo: boolean; offer: any } | null>(null);
  const [activeCall, setActiveCall] = useState<WebRTCCall | null>(null);
  const [callState, setCallState] = useState<'idle' | 'calling' | 'ringing' | 'connected'>('idle');
  const [localStream, setLocalStream] = useState<any>(null);
  const [remoteStream, setRemoteStream] = useState<any>(null);
  const [isAudioEnabled, setIsAudioEnabled] = useState(true);
  const [isVideoEnabled, setIsVideoEnabled] = useState(true);
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
  tokenRef.current = token;
  activeRoomIdRef.current = activeRoomId;
  callActiveRef.current = !!activeCall || !!incomingCall || callState !== "idle";

  useEffect(() => {
    // Global 401: api.ts fires this once; clear the stored session and fall back to the login screen.
    setUnauthorizedHandler((failedToken) => {
      if (tokenRef.current && failedToken !== tokenRef.current) return;
      void clearSession();
      setError("Session expired. Please log in again.");
    });
    return () => setUnauthorizedHandler(null);
  }, []);

  useEffect(() => {
    let mounted = true;
    (async () => {
      const storedToken = await storage.get(STORAGE_KEYS.token);
      if (!mounted) return;
      const nextBaseUrl = normalizeBaseUrl(DEFAULT_BASE_URL);
      setBaseUrl(nextBaseUrl);
      if (storedToken) {
        setToken(storedToken);
        try {
          await bootstrap(nextBaseUrl, storedToken);
        } catch (e) {
          await clearSession();
          setError(errorText(e));
        }
      }
      setLoading(false);
    })();

    return () => {
      mounted = false;
      reconnectTimerRef.current && clearTimeout(reconnectTimerRef.current);
      wsRef.current?.close();
      audioRef.current?.unloadAsync().catch(() => undefined);
      recording?.stopAndUnloadAsync().catch(() => undefined);
    };
  }, []);

  useEffect(() => {
    setProfileName(me?.display_name || "");
  }, [me?.display_name]);

  useEffect(() => {
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

    const clearTimer = () => {
      if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    };
    // Detach first so the old socket's onclose cannot schedule a reconnect.
    const closeSocket = () => {
      const current = wsRef.current;
      wsRef.current = null;
      try {
        current?.close();
      } catch {}
    };
    // Sockets are paused in the background unless a call needs signaling.
    const shouldPause = () => AppState.currentState === "background" && !callActiveRef.current;

    const connect = () => {
      clearTimer();
      if (cancelled || shouldPause()) return;
      const existing = wsRef.current;
      if (existing && (existing.readyState === WebSocket.CONNECTING || existing.readyState === WebSocket.OPEN)) return;

      const ws = new WebSocket(wsUrlFromApi(baseUrl));
      wsRef.current = ws;

      ws.onopen = () => {
        if (wsRef.current !== ws) {
          ws.close();
          return;
        }
        const needsResync = !firstOpen;
        firstOpen = false;
        attempt = 0;
        ws.send(JSON.stringify({ type: "auth", token }));
        const roomId = activeRoomIdRef.current;
        if (roomId) ws.send(JSON.stringify({ type: "rooms.join", room_id: roomId }));
        // Fetch messages/unread counters missed while disconnected or backgrounded.
        if (needsResync) void resyncRef.current?.();
      };

      ws.onmessage = (event) => {
        if (wsRef.current !== ws) return;
        let data: any;
        try {
          data = JSON.parse(event.data);
        } catch {
          return;
        }
        void onWsMessageRef.current?.(data);
      };

      ws.onclose = () => {
        if (wsRef.current !== ws) return;
        wsRef.current = null;
        if (cancelled || shouldPause()) return;
        const delay = Math.min(30000, 1000 * 2 ** attempt);
        attempt += 1;
        reconnectTimerRef.current = setTimeout(connect, Math.round(delay * (0.75 + Math.random() * 0.5)));
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
  }, [baseUrl, token]);

  onWsMessageRef.current = handleWsMessage;
  resyncRef.current = resyncAfterReconnect;

  async function resyncAfterReconnect() {
    const activeToken = tokenRef.current;
    if (!activeToken) return;
    try {
      const roomList = await api.rooms(baseUrl, activeToken);
      const prepared = await prepareRooms(roomList, baseUrl, activeToken);
      const roomId = activeRoomIdRef.current;
      if (!roomId) {
        setRooms(prepared);
      } else {
        const roomMessages = await api.roomMessages(baseUrl, activeToken, roomId);
        const preparedMessages = await prepareMessages(roomMessages, baseUrl, activeToken);
        if (activeRoomIdRef.current === roomId) {
          setMessages(preparedMessages);
          await api.markRead(baseUrl, activeToken, roomId);
          setRooms(prepared.map((item) => item.id === roomId ? { ...item, unread_count: 0 } : item));
        } else {
          setRooms(prepared);
        }
      }
    } catch (e) {
      console.error("Resync failed:", errorText(e));
    }
    await replenishPrekeys(activeToken);
  }

  // Keep the server-side one-time pre-key pool topped up (server reports the unused count).
  async function replenishPrekeys(activeToken: string, userId = me?.id, nextBaseUrl = baseUrl) {
    if (!userId) return;
    try {
      const deviceId = await getMobileDeviceId(userId);
      const { unused } = await api.e2ePrekeyCount(nextBaseUrl, activeToken, deviceId);
      if (unused >= 10) return;
      await e2e.initialize(userId);
      await api.uploadE2EKeys(nextBaseUrl, activeToken, deviceId, await e2e.getPublicKeys(20));
    } catch (e) {
      console.error("Failed to replenish pre-keys:", errorText(e));
    }
  }

  async function handleWsMessage(data: any) {
    if (data.type === "auth.required") {
      await clearSession();
      setError("Session expired");
      return;
    }
    if (data.type === "rooms.update" && Array.isArray(data.rooms)) {
      setRooms(await prepareRooms(data.rooms, baseUrl, token));
      return;
    }
    if (data.type === "message.new") {
      const incoming = await prepareMessage(data.message as Message, baseUrl, token);

      setRooms((prev) => reorderRooms(updateRoomPreview(prev, incoming, me?.id || null), incoming.room_id));
      if (incoming.room_id === activeRoomId && token) {
        setMessages((prev) => upsertMessage(prev, incoming));
        try {
          await api.markRead(baseUrl, token, incoming.room_id);
        } catch {}
      }
      return;
    }
    if (data.type === "message.delete" && data.message_id) {
      setMessages((prev) => prev.filter((item) => item.id !== data.message_id));
      return;
    }
    if (data.type === "reaction.add" && data.message_id && data.emoji && data.user_id) {
      setMessages((prev) => prev.map((item) => item.id === data.message_id ? withReaction(item, data.emoji, data.user_id, true) : item));
      return;
    }
    if (data.type === "reaction.remove" && data.message_id && data.emoji && data.user_id) {
      setMessages((prev) => prev.map((item) => item.id === data.message_id ? withReaction(item, data.emoji, data.user_id, false) : item));
      return;
    }
    if (data.type === "messages.read" && Array.isArray(data.message_ids) && data.user_id) {
      setMessages((prev) => prev.map((item) => data.message_ids.includes(item.id) ? withRead(item, data.user_id) : item));
      return;
    }

    // WebRTC call signaling
    if (data.type === "typing" && data.room_id && data.user_id) {
      const key = `${data.room_id}:${data.user_id}`;
      clearTimeout(typingTimersRef.current[key]);
      delete typingTimersRef.current[key];
      if (data.is_typing) {
        setTypingUsers((prev) => ({ ...prev, [key]: { username: data.username || "Кто-то", roomId: data.room_id } }));
        typingTimersRef.current[key] = setTimeout(() => {
          delete typingTimersRef.current[key];
          setTypingUsers((prev) => { const next = { ...prev }; delete next[key]; return next; });
        }, TYPING_TTL_MS);
      } else {
        setTypingUsers((prev) => { const next = { ...prev }; delete next[key]; return next; });
      }
    }
    if (data.type === "call.offer") {
      setIncomingCall({
        roomId: data.room_id,
        callerId: data.from_user_id,
        callerName: data.from_username || 'Unknown',
        isVideo: data.is_video || false,
        offer: data.offer || data.sdp,
      });
      return;
    }
    if (data.type === "call.answer" && activeCall) {
      try {
        await activeCall.handleAnswer(data.answer || data.sdp);
      } catch (error) {
        console.error('Failed to handle answer:', error);
      }
      return;
    }
    if ((data.type === "call.ice" || data.type === "call.ice_candidate") && activeCall) {
      try {
        await activeCall.addIceCandidate(data.candidate || data.ice);
      } catch (error) {
        console.error('Failed to add ICE candidate:', error);
      }
      return;
    }
    if (data.type === "call.hangup" || data.type === "call.end") {
      endCall();
      return;
    }
  }

  async function bootstrap(nextBaseUrl: string, nextToken: string) {
    const [user, roomList] = await Promise.all([api.me(nextBaseUrl, nextToken), api.rooms(nextBaseUrl, nextToken)]);
    setMe(user);
    setRooms(await prepareRooms(roomList, nextBaseUrl, nextToken));

    // Initialize E2E encryption
    // Uploads a first batch for a new device, tops up when the server reports < 10 unused.
    await replenishPrekeys(nextToken, user.id, nextBaseUrl);

    // Initialize push notifications
    try {
      const fcmToken = await pushNotifications.initialize();
      if (fcmToken) {
        // Register FCM token with server
        await api.registerFCMToken(nextBaseUrl, nextToken, fcmToken);
      }

      // Handle notification taps
      pushNotifications.onMessage((data) => {
        if (data.room_id) {
          openRoom(data.room_id);
        }
      });
    } catch (error) {
      console.error('Failed to initialize push notifications:', error);
    }
  }

  async function clearSession() {
    setToken(null);
    setMe(null);
    setRooms([]);
    setMessages([]);
    setActiveRoomId(null);
    setActiveRoomMembers([]);
    await storage.del(STORAGE_KEYS.token);
  }

  async function persistSession(nextBaseUrl: string, nextToken: string) {
    resetUnauthorizedGuard();
    await storage.set(STORAGE_KEYS.token, nextToken);
    setBaseUrl(nextBaseUrl);
    setToken(nextToken);
    await bootstrap(nextBaseUrl, nextToken);
  }

  async function submitAuth() {
    setAuthBusy(true);
    setError(null);
    const nextBaseUrl = normalizeBaseUrl(DEFAULT_BASE_URL);
    try {
      const result = authMode === "register"
        ? await api.mobileRegister(nextBaseUrl, username.trim(), password)
        : await api.mobileLogin(nextBaseUrl, username.trim(), password);
      if (!result.token) throw new Error("Server did not return a token");
      await persistSession(nextBaseUrl, result.token);
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
      const [roomList, user] = await Promise.all([api.rooms(baseUrl, token), api.me(baseUrl, token)]);
      setRooms(await prepareRooms(roomList, baseUrl, token));
      setMe(user);
    } catch (e) {
      setError(errorText(e));
    } finally {
      setRefreshingRooms(false);
    }
  }

  async function openRoom(room: Room) {
    if (!token) return;
    setActiveRoomId(room.id);
    setError(null);
    try {
      const [roomMessages, members] = await Promise.all([
        api.roomMessages(baseUrl, token, room.id),
        api.roomMembers(baseUrl, token, room.id).catch(() => []),
      ]);
      setMessages(await prepareMessages(roomMessages, baseUrl, token));
      setActiveRoomMembers(members);
      await api.markRead(baseUrl, token, room.id);
      if (wsRef.current?.readyState === WebSocket.OPEN) {
        wsRef.current.send(JSON.stringify({ type: "rooms.join", room_id: room.id }));
      }
      setRooms((prev) => prev.map((item) => item.id === room.id ? { ...item, unread_count: 0 } : item));
    } catch (e) {
      setError(errorText(e));
    }
  }

  async function refreshMessages() {
    if (!token || !activeRoomId) return;
    setRefreshingMessages(true);
    try {
      const roomMessages = await api.roomMessages(baseUrl, token, activeRoomId);
      setMessages(await prepareMessages(roomMessages, baseUrl, token));
      await api.markRead(baseUrl, token, activeRoomId);
    } catch (e) {
      setError(errorText(e));
    } finally {
      setRefreshingMessages(false);
    }
  }

  async function getRoomSecret(roomId: string, nextBaseUrl = baseUrl, nextToken = token) {
    if (!nextToken) return null;
    const cached = roomSecretCache.current.get(roomId);
    if (cached) return cached;
    const data = await api.roomKey(nextBaseUrl, nextToken, roomId);
    roomSecretCache.current.set(roomId, data.key);
    return data.key;
  }

  async function prepareMessage(message: Message, nextBaseUrl = baseUrl, nextToken = token): Promise<Message> {
    if (message.type !== "text") return message;
    // Group Sender Keys messages need the web client; the mobile app has no group key store.
    if (message.meta?.e2e_group) return { ...message, text: "🔒 Сообщение в группе со сквозным шифрованием. Откройте веб-версию.", _decrypted: false };
    if (message.meta?.encrypted && message.meta?.e2ee_v === 2 && message.text && typeof message.meta.iv === "string") {
      try {
        const secret = await getRoomSecret(message.room_id, nextBaseUrl, nextToken);
        if (!secret) return { ...message, text: "Encrypted message" };
        return {
          ...message,
          text: await decryptRoomText(message.room_id, secret, message.text, message.meta.iv),
          _decrypted: true,
        };
      } catch {
        return { ...message, text: "Encrypted message" };
      }
    }
    if (message.encrypted_content && message.sender_id !== me?.id) {
      try {
        const decrypted = await e2e.decrypt(message.sender_id, JSON.parse(message.encrypted_content));
        return { ...message, text: decrypted, _decrypted: true };
      } catch {
        return { ...message, text: "Encrypted message" };
      }
    }
    return message;
  }

  async function prepareMessages(items: Message[], nextBaseUrl = baseUrl, nextToken = token) {
    return Promise.all(items.map((item) => prepareMessage(item, nextBaseUrl, nextToken)));
  }

  async function prepareRooms(items: Room[], nextBaseUrl = baseUrl, nextToken = token) {
    return Promise.all(items.map(async (room) => {
      if (room.last_msg_type !== "text") return room;
      if (!(room.last_msg_meta?.encrypted && room.last_msg_meta?.e2ee_v === 2 && room.last_msg_text && typeof room.last_msg_meta.iv === "string")) {
        return room;
      }
      try {
        const secret = await getRoomSecret(room.id, nextBaseUrl, nextToken);
        if (!secret) return room;
        const text = await decryptRoomText(room.id, secret, room.last_msg_text, room.last_msg_meta.iv);
        return { ...room, last_msg_text: text };
      } catch {
        return { ...room, last_msg_text: null };
      }
    }));
  }

  async function saveProfile() {
    if (!token || !me) return;
    try {
      await api.updateMe(baseUrl, token, profileName.trim() || null);
      if (newPassword.trim()) await api.changePassword(baseUrl, token, newPassword);
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
    const result = await ImagePicker.launchImageLibraryAsync({ mediaTypes: ["images"], quality: 0.9 });
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

  // Sends "typing" to the room, at most once per TYPING_SEND_INTERVAL_MS while the composer has text.
  function notifyTyping(isTyping: boolean) {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== 1 || !activeRoomId) return;
    const now = Date.now();
    if (isTyping && now - typingSentAtRef.current < TYPING_SEND_INTERVAL_MS) return;
    typingSentAtRef.current = isTyping ? now : 0;
    try { ws.send(JSON.stringify({ type: "typing", room_id: activeRoomId, is_typing: isTyping })); } catch {}
  }

  async function sendCurrentMessage() {
    if (!token || !activeRoomId) return;
    const text = composer.trim();
    if (!text) return;
    const groupE2e = activeRoom?.kind === "group" ? await api.roomE2e(baseUrl, token, activeRoomId).catch(() => null) : null;
    if (groupE2e?.enabled) {
      setError("В этой группе включено сквозное шифрование. Отправка с телефона пока не поддерживается, используйте веб-версию.");
      return;
    }
    setComposer("");
    notifyTyping(false);
    setError(null);
    try {
      const secret = await getRoomSecret(activeRoomId);
      const encrypted = secret ? await encryptRoomText(activeRoomId, secret, text) : null;
      const sent = encrypted
        ? await api.sendMediaMessage(baseUrl, token, activeRoomId, {
          type: "text",
          text: encrypted.text,
          meta: encrypted.meta,
        })
        : await api.sendMessage(baseUrl, token, activeRoomId, text);
      const display = { ...sent, text, _decrypted: !!encrypted };
      setMessages((prev) => upsertMessage(prev, display));
      setRooms((prev) => reorderRooms(updateRoomPreview(prev, display, me?.id || null), activeRoomId));
    } catch (e) {
      setComposer(text);
      setError(errorText(e));
    }
  }

  async function sendPickedUpload(kind: "image" | "video" | "voice" | "video_note" | "media", file: { uri: string; name: string; type: string }) {
    if (!token || !activeRoomId) return;
    try {
      setBusyUpload(true);
      const uploaded = await api.upload(baseUrl, token, kind, file);
      const sent = await api.sendMediaMessage(baseUrl, token, activeRoomId, {
        type: kind === "voice" ? "voice" : kind === "video_note" ? "video_note" : "media",
        text: "",
        media_id: uploaded.id,
      });
      setMessages((prev) => upsertMessage(prev, sent));
      setRooms((prev) => reorderRooms(updateRoomPreview(prev, sent, me?.id || null), activeRoomId));
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusyUpload(false);
    }
  }

  async function pickImage() {
    const result = await ImagePicker.launchImageLibraryAsync({ mediaTypes: ["images"], quality: 0.9 });
    if (result.canceled || !result.assets?.[0]) return;
    const asset = result.assets[0];
    await sendPickedUpload("image", { uri: asset.uri, name: asset.fileName || "image.jpg", type: asset.mimeType || "image/jpeg" });
  }

  async function pickVideo() {
    const result = await ImagePicker.launchImageLibraryAsync({ mediaTypes: ["videos"], quality: 0.9 });
    if (result.canceled || !result.assets?.[0]) return;
    const asset = result.assets[0];
    await sendPickedUpload("video", { uri: asset.uri, name: asset.fileName || "video.mp4", type: asset.mimeType || "video/mp4" });
  }

  async function pickVideoNote() {
    const result = await ImagePicker.launchCameraAsync({ mediaTypes: ["videos"], videoMaxDuration: 60, quality: 0.8 });
    if (result.canceled || !result.assets?.[0]) return;
    const asset = result.assets[0];
    await sendPickedUpload("video_note", { uri: asset.uri, name: asset.fileName || "circle.mp4", type: asset.mimeType || "video/mp4" });
  }

  async function pickFile() {
    const result = await DocumentPicker.getDocumentAsync({ multiple: false });
    if (result.canceled || !result.assets?.[0]) return;
    const asset = result.assets[0];
    await sendPickedUpload("media", { uri: asset.uri, name: asset.name, type: asset.mimeType || "application/octet-stream" });
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
      await Audio.setAudioModeAsync({ allowsRecordingIOS: true, playsInSilentModeIOS: true });
      const rec = new Audio.Recording();
      await rec.prepareToRecordAsync(Audio.RecordingOptionsPresets.HIGH_QUALITY);
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
      const { sound } = await Audio.Sound.createAsync({ uri: url }, { shouldPlay: true });
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
        setMessages((prev) => prev.map((item) => item.id === reactionTarget.id ? withReaction(item, emoji, me?.id || "", false) : item));
      } else {
        await api.addReaction(baseUrl, token, reactionTarget.id, emoji);
        setMessages((prev) => prev.map((item) => item.id === reactionTarget.id ? withReaction(item, emoji, me?.id || "", true) : item));
      }
      setReactionTarget(null);
    } catch (e) {
      setError(errorText(e));
    }
  }

  async function logout() {
    if (token) {
      try {
        await api.logout(baseUrl, token);
      } catch {}
    }
    await clearSession();
  }

  async function startCall(isVideo: boolean) {
    if (!activeRoomId || !token) return;

    try {
      const ice = await api.iceServers(baseUrl, token).then((r) => r.iceServers).catch(() => undefined);
      const call = new WebRTCCall(
        { audio: true, video: isVideo },
        {
          onLocalStream: (stream) => setLocalStream(stream),
          onRemoteStream: (stream) => setRemoteStream(stream),
          onIceCandidate: (candidate) => {
            wsRef.current?.send(JSON.stringify({
              type: 'call.ice',
              room_id: activeRoomId,
              candidate
            }));
          },
          onConnectionStateChange: (state) => {
            if (state === 'connected') setCallState('connected');
            if (state === 'failed' || state === 'closed') endCall();
          },
          onError: (error) => {
            console.error('WebRTC error:', error);
            setError(error.message);
            endCall();
          }
        },
        true,
        ice
      );

      await call.start();
      setActiveCall(call);
      setCallState('calling');
      setIsVideoEnabled(isVideo);

      const offer = call.getLocalDescription();
      wsRef.current?.send(JSON.stringify({
        type: 'call.offer',
        room_id: activeRoomId,
        offer,
        is_video: isVideo
      }));
    } catch (error) {
      console.error('Failed to start call:', error);
      setError('Failed to start call');
      endCall();
    }
  }

  async function answerCall() {
    if (!incomingCall || !token) return;

    try {
      const ice = await api.iceServers(baseUrl, token).then((r) => r.iceServers).catch(() => undefined);
      const call = new WebRTCCall(
        { audio: true, video: incomingCall.isVideo },
        {
          onLocalStream: (stream) => setLocalStream(stream),
          onRemoteStream: (stream) => setRemoteStream(stream),
          onIceCandidate: (candidate) => {
            wsRef.current?.send(JSON.stringify({
              type: 'call.ice',
              room_id: incomingCall.roomId,
              candidate
            }));
          },
          onConnectionStateChange: (state) => {
            if (state === 'connected') setCallState('connected');
            if (state === 'failed' || state === 'closed') endCall();
          },
          onError: (error) => {
            console.error('WebRTC error:', error);
            setError(error.message);
            endCall();
          }
        },
        false,
        ice
      );

      await call.start();
      const answer = await call.handleOffer(incomingCall.offer);
      setActiveCall(call);
      setCallState('connected');
      setIsVideoEnabled(incomingCall.isVideo);
      setActiveRoomId(incomingCall.roomId);
      setIncomingCall(null);

      wsRef.current?.send(JSON.stringify({
        type: 'call.answer',
        room_id: incomingCall.roomId,
        answer
      }));
    } catch (error) {
      console.error('Failed to answer call:', error);
      setError('Failed to answer call');
      rejectCall();
    }
  }

  function rejectCall() {
    if (!incomingCall) return;
    wsRef.current?.send(JSON.stringify({
      type: 'call.hangup',
      room_id: incomingCall.roomId
    }));
    setIncomingCall(null);
  }

  function endCall() {
    if (activeCall) {
      activeCall.end();
      if (activeRoomId) {
        wsRef.current?.send(JSON.stringify({
          type: 'call.hangup',
          room_id: activeRoomId
        }));
      }
    }
    setActiveCall(null);
    setLocalStream(null);
    setRemoteStream(null);
    setCallState('idle');
    setIncomingCall(null);
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
        console.error('Failed to switch camera:', error);
      }
    }
  }

  const activeRoom = rooms.find((room) => room.id === activeRoomId) || null;
  const typingNames = Object.values(typingUsers)
    .filter((u) => u.roomId === activeRoomId)
    .map((u) => u.username);
  const roomSubtitle = useMemo(() => {
    if (!activeRoom) return "";
    if (activeRoom.kind === "dm") return activeRoomMembers[0] ? `@${activeRoomMembers.find((item) => item.id !== me?.id)?.username || activeRoom.other_username || ""}` : "dialog";
    return activeRoomMembers.length ? `${activeRoomMembers.length} members` : activeRoom.kind;
  }, [activeRoom, activeRoomMembers, me?.id]);

  if (loading) {
    return <SafeAreaView style={styles.center}><StatusBar style="light" /><ActivityIndicator size="large" color="#63a4ff" /></SafeAreaView>;
  }

  if (!token || !me) {
    return (
      <SafeAreaView style={styles.screen}>
        <StatusBar style="light" />
        <View style={styles.authCard}>
          <Text style={styles.brand}>Messenger67</Text>
          <Text style={styles.caption}>Native Android client with chats, media, voice, reactions and profile controls.</Text>
          <View style={styles.switchRow}>
            <ModeButton label="Login" active={authMode === "login"} onPress={() => setAuthMode("login")} />
            <ModeButton label="Register" active={authMode === "register"} onPress={() => setAuthMode("register")} />
          </View>
          <TextInput style={styles.input} value={username} onChangeText={setUsername} autoCapitalize="none" autoCorrect={false} placeholder="Username" placeholderTextColor="#6b7f99" />
          <TextInput style={styles.input} value={password} onChangeText={setPassword} secureTextEntry placeholder="Password" placeholderTextColor="#6b7f99" />
          {error ? <Text style={styles.error}>{error}</Text> : null}
          <Pressable style={[styles.primaryButton, authBusy && styles.buttonDisabled]} onPress={submitAuth} disabled={authBusy}>
            <Text style={styles.primaryButtonText}>{authBusy ? "Please wait..." : authMode === "login" ? "Login" : "Create account"}</Text>
          </Pressable>
        </View>
      </SafeAreaView>
    );
  }

  return (
    <SafeAreaView style={styles.screen}>
      <StatusBar style="light" />
      <KeyboardAvoidingView style={styles.flex} behavior={Platform.OS === "ios" ? "padding" : undefined}>
        {!activeRoom ? (
          <View style={styles.flex}>
            <View style={styles.header}>
              <Pressable onPress={() => setProfileOpen(true)} style={styles.profileChip}>
                <Avatar user={me} baseUrl={baseUrl} size={42} />
                <View style={styles.headerMain}>
                  <Text style={styles.headerTitle}>{displayName(me)}</Text>
                  <Text style={styles.headerSubtitle}>@{me.username}</Text>
                </View>
              </Pressable>
              <View style={styles.headerActions}>
                <GhostButton label="Refresh" onPress={refreshRooms} />
                <GhostButton label="Logout" onPress={logout} />
              </View>
            </View>
            {error ? <Text style={styles.errorBar}>{error}</Text> : null}
            <FlatList
              data={rooms}
              keyExtractor={(item) => item.id}
              refreshControl={<RefreshControl refreshing={refreshingRooms} onRefresh={refreshRooms} tintColor="#63a4ff" />}
              contentContainerStyle={rooms.length ? styles.listContent : styles.emptyListContent}
              ListEmptyComponent={<EmptyState title="No chats yet" text="Open the web client or ask someone to write to this account." />}
              renderItem={({ item }) => (
                <Pressable style={styles.roomItem} onPress={() => openRoom(item)}>
                  <Avatar room={item} meId={me.id} baseUrl={baseUrl} size={48} />
                  <View style={styles.roomMain}>
                    <View style={styles.roomRow}>
                      <Text style={styles.roomTitle} numberOfLines={1}>{roomTitle(item, me.id)}</Text>
                      <Text style={styles.roomTime}>{shortTime(item.last_msg_at)}</Text>
                    </View>
                    <View style={styles.roomRow}>
                      <Text style={styles.roomPreview} numberOfLines={2}>{roomPreview(item, me.id)}</Text>
                      {!!item.unread_count && <View style={styles.badge}><Text style={styles.badgeText}>{item.unread_count}</Text></View>}
                    </View>
                  </View>
                </Pressable>
              )}
            />
          </View>
        ) : (
          <View style={styles.flex}>
            <View style={styles.header}>
              <GhostButton label="Back" onPress={() => setActiveRoomId(null)} />
              <View style={styles.headerRoom}>
                <Text style={styles.headerTitle} numberOfLines={1}>{roomTitle(activeRoom, me.id)}</Text>
                <Text style={styles.headerSubtitle}>{roomSubtitle}</Text>
              </View>
              {activeRoom?.kind === 'dm' && (
                <View style={styles.callButtons}>
                  <Pressable style={styles.callIconButton} onPress={() => startCall(false)}>
                    <Text style={styles.callIcon}>📞</Text>
                  </Pressable>
                  <Pressable style={styles.callIconButton} onPress={() => startCall(true)}>
                    <Text style={styles.callIcon}>📹</Text>
                  </Pressable>
                </View>
              )}
              <GhostButton label="Refresh" onPress={refreshMessages} />
            </View>
            {error ? <Text style={styles.errorBar}>{error}</Text> : null}
            <FlatList
              data={messages}
              keyExtractor={(item) => item.id}
              refreshControl={<RefreshControl refreshing={refreshingMessages} onRefresh={refreshMessages} tintColor="#63a4ff" />}
              contentContainerStyle={messages.length ? styles.messagesContent : styles.emptyListContent}
              ListEmptyComponent={<EmptyState title="No messages" text="Send the first message from here." />}
              renderItem={({ item }) => (
                <MessageCard
                  item={item}
                  meId={me.id}
                  baseUrl={baseUrl}
                  playing={playingId === item.id}
                  onPlayVoice={() => playVoice(item)}
                  onReact={() => setReactionTarget({ id: item.id, reactions: item.reactions })}
                  onOpenMedia={openMedia}
                />
              )}
            />
            <View style={styles.attachRow}>
              <SmallAction label="Photo" onPress={pickImage} disabled={busyUpload} />
              <SmallAction label="Video" onPress={pickVideo} disabled={busyUpload} />
              <SmallAction label="File" onPress={pickFile} disabled={busyUpload} />
              <SmallAction label={recording ? "Stop" : "Voice"} onPress={toggleRecording} disabled={busyUpload} active={!!recording} />
              <SmallAction label="Circle" onPress={pickVideoNote} disabled={busyUpload} />
            </View>
            {typingNames.length > 0 ? <Text style={styles.typingText}>{typingNames.join(", ")} печатает…</Text> : null}
            <View style={styles.composer}>
              <TextInput
                style={styles.composerInput}
                value={composer}
                onChangeText={(value) => { setComposer(value); notifyTyping(value.length > 0); }}
                placeholder="Message"
                placeholderTextColor="#6b7f99"
                multiline
              />
              <Pressable style={[styles.primaryButtonSmall, busyUpload && styles.buttonDisabled]} onPress={sendCurrentMessage} disabled={busyUpload}>
                <Text style={styles.primaryButtonText}>{busyUpload ? "..." : "Send"}</Text>
              </Pressable>
            </View>
          </View>
        )}
      </KeyboardAvoidingView>
      <ProfileModal
        open={profileOpen}
        me={me}
        baseUrl={baseUrl}
        profileName={profileName}
        newPassword={newPassword}
        busyUpload={busyUpload}
        onClose={() => setProfileOpen(false)}
        onChangeName={setProfileName}
        onChangePassword={setNewPassword}
        onSave={saveProfile}
        onPickAvatar={pickAvatar}
      />
      <ReactionModal open={!!reactionTarget} onClose={() => setReactionTarget(null)} onPick={toggleReaction} />
      <MediaViewerModal
        open={!!viewerMessage}
        message={viewerMessage}
        baseUrl={baseUrl}
        onClose={closeMedia}
        onPlayVoice={playVoice}
      />

      {/* Incoming Call Modal */}
      {incomingCall && (
        <Modal visible transparent animationType="fade">
          <View style={styles.callModalOverlay}>
            <View style={styles.callModalCard}>
              <Text style={styles.callModalTitle}>Incoming Call</Text>
              <Text style={styles.callModalName}>{incomingCall.callerName}</Text>
              <Text style={styles.callModalType}>{incomingCall.isVideo ? 'Video Call' : 'Voice Call'}</Text>
              <View style={styles.callModalButtons}>
                <Pressable style={[styles.callButton, styles.callButtonAccept]} onPress={answerCall}>
                  <Text style={styles.callButtonText}>✓ Answer</Text>
                </Pressable>
                <Pressable style={[styles.callButton, styles.callButtonReject]} onPress={rejectCall}>
                  <Text style={styles.callButtonText}>✕ Reject</Text>
                </Pressable>
              </View>
            </View>
          </View>
        </Modal>
      )}

      {/* Active Call Modal */}
      {callState !== 'idle' && (
        <Modal visible animationType="slide">
          <View style={styles.callScreen}>
            {remoteStream && (
              <RTCView streamURL={remoteStream.toURL()} style={styles.remoteVideo} objectFit="cover" />
            )}
            {localStream && isVideoEnabled && (
              <RTCView streamURL={localStream.toURL()} style={styles.localVideo} objectFit="cover" mirror />
            )}
            <View style={styles.callOverlay}>
              <Text style={styles.callStatus}>
                {callState === 'calling' ? 'Calling...' : callState === 'ringing' ? 'Ringing...' : 'Connected'}
              </Text>
              <View style={styles.callControls}>
                <Pressable style={styles.callControlButton} onPress={toggleAudio}>
                  <Text style={styles.callControlIcon}>{isAudioEnabled ? '🔊' : '🔇'}</Text>
                </Pressable>
                {isVideoEnabled && (
                  <>
                    <Pressable style={styles.callControlButton} onPress={toggleVideo}>
                      <Text style={styles.callControlIcon}>📹</Text>
                    </Pressable>
                    <Pressable style={styles.callControlButton} onPress={switchCamera}>
                      <Text style={styles.callControlIcon}>🔄</Text>
                    </Pressable>
                  </>
                )}
                <Pressable style={[styles.callControlButton, styles.callEndButton]} onPress={endCall}>
                  <Text style={styles.callControlIcon}>✕</Text>
                </Pressable>
              </View>
            </View>
          </View>
        </Modal>
      )}
    </SafeAreaView>
  );
}

function ModeButton({ label, active, onPress }: { label: string; active: boolean; onPress: () => void }) {
  return <Pressable style={[styles.modeButton, active && styles.modeButtonActive]} onPress={onPress}><Text style={[styles.modeButtonText, active && styles.modeButtonTextActive]}>{label}</Text></Pressable>;
}

function GhostButton({ label, onPress }: { label: string; onPress: () => void }) {
  return <Pressable onPress={onPress} style={styles.ghostButton}><Text style={styles.ghostButtonText}>{label}</Text></Pressable>;
}

function SmallAction({ label, onPress, disabled, active }: { label: string; onPress: () => void; disabled?: boolean; active?: boolean }) {
  return <Pressable onPress={onPress} disabled={disabled} style={[styles.smallAction, active && styles.smallActionActive, disabled && styles.buttonDisabled]}><Text style={styles.smallActionText}>{label}</Text></Pressable>;
}

function EmptyState({ title, text }: { title: string; text: string }) {
  return <View style={styles.emptyState}><Text style={styles.emptyTitle}>{title}</Text><Text style={styles.emptyText}>{text}</Text></View>;
}

function Avatar({ user, room, meId, baseUrl, size }: { user?: User | null; room?: Room | null; meId?: string | null; baseUrl: string; size: number }) {
  const mediaId = user?.avatar_id || room?.avatar_id || room?.other_avatar_id || null;
  const title = user ? displayName(user) : room ? roomTitle(room, meId) : "?";
  const url = mediaUrl(baseUrl, mediaId);
  if (url) return <Image source={{ uri: url }} style={[styles.avatar, { width: size, height: size, borderRadius: size / 2 }]} />;
  return <View style={[styles.roomAvatar, { width: size, height: size, borderRadius: size / 2 }]}><Text style={styles.roomAvatarText}>{title.slice(0, 1).toUpperCase()}</Text></View>;
}

function MessageCard({
  item, meId, baseUrl, playing, onPlayVoice, onReact, onOpenMedia,
}: {
  item: Message;
  meId: string;
  baseUrl: string;
  playing: boolean;
  onPlayVoice: () => void;
  onReact: () => void;
  onOpenMedia: (message: Message) => void;
}) {
  const own = item.sender_id === meId;
  const read = own && item.read_by && item.read_by.length > 1;
  return (
    <View style={[styles.messageWrap, own ? styles.messageOwnWrap : styles.messageOtherWrap]}>
      <View style={[styles.messageBubble, own ? styles.messageOwn : styles.messageOther]}>
        <Text style={[styles.messageSender, own && styles.messageSenderOwn]}>{own ? "You" : item.sender_display_name || item.sender_username || "User"}</Text>
        {item.reply_to?.text ? <Text style={styles.replyText} numberOfLines={2}>Reply: {item.reply_to.text}</Text> : null}
        <MessageBody item={item} baseUrl={baseUrl} playing={playing} onPlayVoice={onPlayVoice} onOpenMedia={onOpenMedia} />
        <View style={styles.messageMetaRow}>
          <Text style={[styles.messageTime, own && styles.messageTimeOwn]}>{shortTime(item.created_at)}{read ? "  Read" : ""}</Text>
          <Pressable onPress={onReact}><Text style={styles.reactLink}>React</Text></Pressable>
        </View>
        <ReactionBar reactions={item.reactions} />
      </View>
    </View>
  );
}

function MessageBody({ item, baseUrl, playing, onPlayVoice, onOpenMedia }: { item: Message; baseUrl: string; playing: boolean; onPlayVoice: () => void; onOpenMedia: (message: Message) => void }) {
  const url = mediaUrl(baseUrl, item.media_id);
  const thumb = mediaThumbUrl(baseUrl, item.media_id) || url;
  if (item.type === "text") return <Text style={styles.messageText}>{item.text || ""}</Text>;
  if (item.type === "voice") {
    return <Pressable style={styles.voiceBubble} onPress={onPlayVoice}><Text style={styles.voiceText}>{playing ? "Stop voice" : `Play voice${item.duration_sec ? ` (${Math.round(item.duration_sec)}s)` : ""}`}</Text></Pressable>;
  }
  if (item.type === "video_note" && url) {
    return <Pressable onPress={() => onOpenMedia(item)}><Video source={{ uri: url }} style={styles.circleVideo} useNativeControls resizeMode={ResizeMode.COVER} isLooping /></Pressable>;
  }
  if (item.type === "media" && url) {
    if ((item.media_mime || "").startsWith("image/")) {
      return <Pressable onPress={() => onOpenMedia(item)}><Image source={{ uri: thumb || url }} style={styles.mediaImage} resizeMode="cover" /></Pressable>;
    }
    if ((item.media_mime || "").startsWith("video/")) {
      return <Pressable onPress={() => onOpenMedia(item)}><Video source={{ uri: url }} style={styles.mediaVideo} useNativeControls resizeMode={ResizeMode.CONTAIN} /></Pressable>;
    }
    return <Pressable onPress={() => onOpenMedia(item)} style={styles.fileCard}><Text style={styles.fileTitle}>{item.text || "Open file"}</Text><Text style={styles.fileSubtitle}>{item.media_mime || "file"}</Text></Pressable>;
  }
  return <Text style={styles.messageText}>[{item.type}]</Text>;
}

function ReactionBar({ reactions }: { reactions?: Record<string, string[]> | null }) {
  const items = Object.entries(reactions || {}).filter(([, users]) => users.length);
  if (!items.length) return null;
  return <View style={styles.reactionRow}>{items.map(([emoji, users]) => <View key={emoji} style={styles.reactionChip}><Text style={styles.reactionText}>{emoji} {users.length}</Text></View>)}</View>;
}

function ProfileModal(props: {
  open: boolean;
  me: User;
  baseUrl: string;
  profileName: string;
  newPassword: string;
  busyUpload: boolean;
  onClose: () => void;
  onChangeName: (value: string) => void;
  onChangePassword: (value: string) => void;
  onSave: () => void;
  onPickAvatar: () => void;
}) {
  return (
    <Modal transparent visible={props.open} animationType="slide" onRequestClose={props.onClose}>
      <View style={styles.modalBackdrop}>
        <View style={styles.modalCard}>
          <ScrollView contentContainerStyle={styles.modalContent}>
            <Text style={styles.modalTitle}>Profile</Text>
            <Avatar user={props.me} baseUrl={props.baseUrl} size={72} />
            <GhostButton label={props.busyUpload ? "Uploading..." : "Change avatar"} onPress={props.onPickAvatar} />
            <TextInput style={styles.input} value={props.profileName} onChangeText={props.onChangeName} placeholder="Display name" placeholderTextColor="#6b7f99" />
            <TextInput style={styles.input} value={props.newPassword} onChangeText={props.onChangePassword} secureTextEntry placeholder="New password" placeholderTextColor="#6b7f99" />
            <Text style={styles.profileMeta}>@{props.me.username}</Text>
            <View style={styles.modalButtons}>
              <GhostButton label="Close" onPress={props.onClose} />
              <Pressable style={styles.primaryButtonInline} onPress={props.onSave}><Text style={styles.primaryButtonText}>Save</Text></Pressable>
            </View>
          </ScrollView>
        </View>
      </View>
    </Modal>
  );
}

function ReactionModal({ open, onClose, onPick }: { open: boolean; onClose: () => void; onPick: (emoji: string) => void }) {
  return (
    <Modal transparent visible={open} animationType="fade" onRequestClose={onClose}>
      <Pressable style={styles.modalBackdrop} onPress={onClose}>
        <View style={styles.reactionModal}>
          {REACTION_SET.map((emoji) => <Pressable key={emoji} style={styles.reactionPick} onPress={() => onPick(emoji)}><Text style={styles.reactionPickText}>{emoji}</Text></Pressable>)}
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
  const url = message ? mediaUrl(baseUrl, message.media_id) : null;
  const thumb = message ? mediaThumbUrl(baseUrl, message.media_id) : null;
  const mime = message?.media_mime || "";
  const isImage = !!message && message.type === "media" && mime.startsWith("image/");
  const isVideo = !!message && message.type === "media" && mime.startsWith("video/");
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
        <Pressable style={styles.viewerClose} onPress={onClose}><Text style={styles.viewerCloseText}>Close</Text></Pressable>
        <View style={styles.viewerBody}>
          {isImage && url ? (
            <Image source={{ uri: url }} style={styles.viewerImage} resizeMode="contain" />
          ) : null}
          {isVideo && url ? (
            <Video source={{ uri: url }} style={styles.viewerVideo} useNativeControls resizeMode={ResizeMode.CONTAIN} />
          ) : null}
          {isVoice && url ? (
            <Pressable style={styles.viewerVoice} onPress={() => onPlayVoice(message)}>
              <Text style={styles.viewerVoiceText}>Play voice{message.duration_sec ? ` (${Math.round(message.duration_sec)}s)` : ""}</Text>
            </Pressable>
          ) : null}
          {isVideoNote && url ? (
            <Video source={{ uri: url }} style={styles.viewerVideoNote} useNativeControls resizeMode={ResizeMode.CONTAIN} isLooping />
          ) : null}
          {isFile && url ? (
            <Pressable style={styles.viewerFile} onPress={() => Linking.openURL(url)}>
              <Text style={styles.viewerFileTitle}>{message.text || "Open file"}</Text>
              <Text style={styles.viewerFileSubtitle}>{mime || "file"}</Text>
            </Pressable>
          ) : null}
        </View>
      </View>
    </Modal>
  );
}

async function getMobileDeviceId(userId: string) {
  const key = `mobile.e2eDeviceId.${userId}`;
  const existing = await storage.get(key);
  if (existing) return existing;
  const generated = `mobile-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  await storage.set(key, generated);
  return generated;
}

function reorderRooms(rooms: Room[], roomId: string) {
  const idx = rooms.findIndex((room) => room.id === roomId);
  if (idx <= 0) return rooms;
  const next = rooms.slice();
  const [room] = next.splice(idx, 1);
  next.unshift(room);
  return next;
}

function updateRoomPreview(rooms: Room[], message: Message, meId: string | null) {
  return rooms.map((room) => room.id !== message.room_id ? room : {
    ...room,
    last_msg_text: message.text || null,
    last_msg_type: message.type,
    last_msg_sender_id: message.sender_id,
    last_msg_sender_username: message.sender_username || null,
    last_msg_sender_name: message.sender_display_name || null,
    last_msg_at: message.created_at,
    unread_count: message.sender_id === meId ? 0 : Math.max(1, Number(room.unread_count || 0)),
  });
}

function upsertMessage(messages: Message[], incoming: Message) {
  const idx = messages.findIndex((item) => item.id === incoming.id);
  if (idx === -1) return [...messages, incoming];
  const next = messages.slice();
  next[idx] = { ...next[idx], ...incoming };
  return next;
}

function withReaction(message: Message, emoji: string, userId: string, add: boolean): Message {
  const prev = { ...(message.reactions || {}) };
  const users = new Set(prev[emoji] || []);
  if (add) users.add(userId); else users.delete(userId);
  if (users.size) prev[emoji] = [...users]; else delete prev[emoji];
  return { ...message, reactions: prev };
}

function withRead(message: Message, userId: string): Message {
  const users = new Set(message.read_by || []);
  users.add(userId);
  return { ...message, read_by: [...users] };
}

function errorText(value: unknown) {
  const text = value instanceof Error ? value.message : "Unknown error";
  switch (text) {
    case "bad_credentials": return "Wrong username or password";
    case "username_taken": return "Username is already taken";
    case "username_too_short": return "Username is too short";
    case "username_too_long": return "Username is too long";
    case "username_invalid": return "Username may contain only latin letters, digits and underscore";
    case "password_too_short": return "Password is too short";
    case "captcha_failed": return "Registration requires captcha on this server";
    case "account_banned": return "This account is banned";
    default: return text;
  }
}

const styles = StyleSheet.create({
  flex: { flex: 1 },
  screen: { flex: 1, backgroundColor: "#09111b" },
  center: { flex: 1, alignItems: "center", justifyContent: "center", backgroundColor: "#09111b" },
  authCard: { margin: 20, marginTop: 60, padding: 20, borderRadius: 24, backgroundColor: "#0f1826", borderWidth: 1, borderColor: "#1c2a3e", gap: 12 },
  brand: { fontSize: 28, fontWeight: "800", color: "#eef5ff" },
  caption: { fontSize: 14, lineHeight: 20, color: "#9db1ca", marginBottom: 8 },
  switchRow: { flexDirection: "row", gap: 8, marginBottom: 4 },
  modeButton: { flex: 1, borderRadius: 12, borderWidth: 1, borderColor: "#223249", backgroundColor: "#121d2c", paddingVertical: 10, alignItems: "center" },
  modeButtonActive: { backgroundColor: "rgba(99,164,255,.15)", borderColor: "rgba(99,164,255,.35)" },
  modeButtonText: { color: "#9db1ca", fontWeight: "700" },
  modeButtonTextActive: { color: "#eef5ff" },
  input: { width: "100%", borderWidth: 1, borderColor: "rgba(255,255,255,.08)", backgroundColor: "rgba(0,0,0,.22)", borderRadius: 14, paddingHorizontal: 14, paddingVertical: 12, fontSize: 16, color: "#eef5ff" },
  error: { color: "#ff7d98", fontSize: 14 },
  errorBar: { color: "#ffc0cc", backgroundColor: "rgba(255,77,109,.12)", marginHorizontal: 16, marginTop: 8, borderRadius: 12, paddingHorizontal: 12, paddingVertical: 10 },
  primaryButton: { backgroundColor: "rgba(99,164,255,.17)", borderRadius: 14, paddingVertical: 14, alignItems: "center", borderWidth: 1, borderColor: "rgba(99,164,255,.35)" },
  primaryButtonSmall: { backgroundColor: "rgba(99,164,255,.17)", borderRadius: 14, paddingHorizontal: 16, paddingVertical: 14, alignItems: "center", justifyContent: "center", borderWidth: 1, borderColor: "rgba(99,164,255,.35)" },
  primaryButtonInline: { backgroundColor: "rgba(99,164,255,.17)", borderRadius: 14, paddingHorizontal: 16, paddingVertical: 12, alignItems: "center", justifyContent: "center", borderWidth: 1, borderColor: "rgba(99,164,255,.35)" },
  primaryButtonText: { color: "#fff", fontWeight: "700", fontSize: 15 },
  buttonDisabled: { opacity: 0.6 },
  header: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 10, paddingHorizontal: 16, paddingVertical: 14, borderBottomWidth: 1, borderBottomColor: "#182638", backgroundColor: "#0e1622" },
  profileChip: { flex: 1, flexDirection: "row", alignItems: "center", gap: 12 },
  headerMain: { flex: 1 },
  headerActions: { flexDirection: "row", gap: 8 },
  headerRoom: { flex: 1 },
  headerTitle: { fontSize: 20, fontWeight: "800", color: "#eef5ff" },
  headerSubtitle: { fontSize: 13, color: "#9db1ca", marginTop: 2 },
  ghostButton: { borderWidth: 1, borderColor: "rgba(255,255,255,.08)", borderRadius: 12, paddingHorizontal: 12, paddingVertical: 10, backgroundColor: "rgba(255,255,255,.04)" },
  ghostButtonText: { color: "#eef5ff", fontWeight: "700" },
  listContent: { padding: 12, gap: 10 },
  emptyListContent: { flexGrow: 1, justifyContent: "center", padding: 24 },
  emptyState: { alignItems: "center", gap: 8 },
  emptyTitle: { fontSize: 22, fontWeight: "800", color: "#eef5ff" },
  emptyText: { fontSize: 14, textAlign: "center", lineHeight: 20, color: "#9db1ca" },
  roomItem: { flexDirection: "row", gap: 12, padding: 14, borderRadius: 18, backgroundColor: "rgba(0,0,0,.18)", borderWidth: 1, borderColor: "rgba(255,255,255,.06)" },
  roomAvatar: { backgroundColor: "rgba(99,164,255,.15)", alignItems: "center", justifyContent: "center", borderWidth: 1, borderColor: "rgba(99,164,255,.25)" },
  roomAvatarText: { color: "#eef5ff", fontWeight: "800", fontSize: 18 },
  avatar: { backgroundColor: "#1a2638" },
  roomMain: { flex: 1, gap: 6 },
  roomRow: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 8 },
  roomTitle: { flex: 1, fontSize: 16, fontWeight: "700", color: "#eef5ff" },
  roomPreview: { flex: 1, fontSize: 13, color: "#9db1ca" },
  roomTime: { fontSize: 12, color: "#9db1ca" },
  badge: { minWidth: 22, height: 22, borderRadius: 11, backgroundColor: "#63a4ff", alignItems: "center", justifyContent: "center", paddingHorizontal: 6 },
  badgeText: { color: "#fff", fontSize: 12, fontWeight: "800" },
  messagesContent: { padding: 12, gap: 10 },
  messageWrap: { width: "100%" },
  messageOwnWrap: { alignItems: "flex-end" },
  messageOtherWrap: { alignItems: "flex-start" },
  messageBubble: { maxWidth: "88%", borderRadius: 18, paddingHorizontal: 14, paddingVertical: 10, gap: 6 },
  messageOwn: { backgroundColor: "rgba(99,164,255,.12)", borderWidth: 1, borderColor: "rgba(99,164,255,.20)" },
  messageOther: { backgroundColor: "rgba(0,0,0,.22)", borderWidth: 1, borderColor: "rgba(255,255,255,.06)" },
  messageSender: { fontSize: 12, fontWeight: "700", color: "#63a4ff" },
  messageSenderOwn: { color: "#a9d0ff" },
  replyText: { fontSize: 12, color: "#8fb0d3" },
  messageText: { fontSize: 16, color: "#eef5ff" },
  messageMetaRow: { flexDirection: "row", justifyContent: "space-between", alignItems: "center", gap: 12 },
  messageTime: { fontSize: 11, color: "#9db1ca" },
  messageTimeOwn: { color: "#9db1ca" },
  reactLink: { color: "#7eb7ff", fontSize: 12, fontWeight: "700" },
  reactionRow: { flexDirection: "row", flexWrap: "wrap", gap: 6 },
  reactionChip: { paddingHorizontal: 8, paddingVertical: 4, borderRadius: 999, backgroundColor: "rgba(255,255,255,.06)" },
  reactionText: { color: "#eef5ff", fontSize: 12 },
  mediaImage: { width: 220, height: 220, borderRadius: 16, backgroundColor: "#122034" },
  mediaVideo: { width: 240, height: 180, borderRadius: 16, backgroundColor: "#122034" },
  circleVideo: { width: 180, height: 180, borderRadius: 90, overflow: "hidden", backgroundColor: "#122034" },
  fileCard: { borderWidth: 1, borderColor: "rgba(255,255,255,.08)", backgroundColor: "rgba(255,255,255,.04)", borderRadius: 14, padding: 12 },
  fileTitle: { color: "#eef5ff", fontWeight: "700" },
  fileSubtitle: { color: "#9db1ca", marginTop: 4 },
  voiceBubble: { paddingHorizontal: 12, paddingVertical: 12, borderRadius: 14, backgroundColor: "rgba(255,255,255,.05)" },
  voiceText: { color: "#eef5ff", fontWeight: "700" },
  attachRow: { flexDirection: "row", flexWrap: "wrap", gap: 8, paddingHorizontal: 12, paddingTop: 10, paddingBottom: 4, borderTopWidth: 1, borderTopColor: "#182638", backgroundColor: "#0e1622" },
  smallAction: { paddingHorizontal: 12, paddingVertical: 10, borderRadius: 12, backgroundColor: "rgba(255,255,255,.04)", borderWidth: 1, borderColor: "rgba(255,255,255,.08)" },
  smallActionActive: { backgroundColor: "rgba(255,77,109,.16)", borderColor: "rgba(255,77,109,.25)" },
  smallActionText: { color: "#eef5ff", fontWeight: "700", fontSize: 12 },
  composer: { flexDirection: "row", gap: 10, padding: 12, backgroundColor: "#0e1622" },
  typingText: { color: "#8ca3bf", fontSize: 12, paddingHorizontal: 14, paddingBottom: 4 },
  composerInput: { flex: 1, minHeight: 52, maxHeight: 120, borderWidth: 1, borderColor: "rgba(255,255,255,.08)", backgroundColor: "rgba(0,0,0,.22)", borderRadius: 16, paddingHorizontal: 14, paddingVertical: 12, fontSize: 16, color: "#eef5ff" },
  modalBackdrop: { flex: 1, backgroundColor: "rgba(0,0,0,.52)", justifyContent: "center", padding: 18 },
  modalCard: { borderRadius: 24, backgroundColor: "#0f1826", borderWidth: 1, borderColor: "#1c2a3e", maxHeight: "88%" },
  modalContent: { alignItems: "center", gap: 12, padding: 20 },
  modalTitle: { fontSize: 22, fontWeight: "800", color: "#eef5ff" },
  profileMeta: { color: "#9db1ca" },
  modalButtons: { width: "100%", flexDirection: "row", justifyContent: "space-between", marginTop: 4 },
  reactionModal: { alignSelf: "center", flexDirection: "row", flexWrap: "wrap", gap: 10, padding: 18, backgroundColor: "#0f1826", borderRadius: 20, borderWidth: 1, borderColor: "#1c2a3e" },
  reactionPick: { width: 52, height: 52, borderRadius: 16, alignItems: "center", justifyContent: "center", backgroundColor: "rgba(255,255,255,.05)" },
  reactionPickText: { fontSize: 24 },
  viewerScreen: { flex: 1, backgroundColor: "#09111b", paddingTop: 20 },
  viewerClose: { alignSelf: "flex-end", marginHorizontal: 16, marginBottom: 12, paddingHorizontal: 14, paddingVertical: 10, borderRadius: 12, backgroundColor: "rgba(255,255,255,.08)" },
  viewerCloseText: { color: "#eef5ff", fontWeight: "700" },
  viewerBody: { flex: 1, alignItems: "center", justifyContent: "center", padding: 18 },
  viewerImage: { flex: 1, width: "100%", height: "100%", backgroundColor: "#09111b" },
  viewerVideo: { width: "100%", height: "100%", backgroundColor: "#09111b" },
  viewerVideoNote: { width: 240, height: 240, borderRadius: 120, overflow: "hidden", backgroundColor: "#09111b" },
  viewerVoice: { paddingHorizontal: 18, paddingVertical: 14, borderRadius: 14, backgroundColor: "rgba(255,255,255,.08)" },
  viewerVoiceText: { color: "#eef5ff", fontWeight: "700" },
  viewerFile: { borderWidth: 1, borderColor: "rgba(255,255,255,.08)", backgroundColor: "rgba(255,255,255,.04)", borderRadius: 16, padding: 16, width: "100%" },
  viewerFileTitle: { color: "#eef5ff", fontWeight: "700", fontSize: 16 },
  viewerFileSubtitle: { color: "#9db1ca", marginTop: 4 },

  // Call UI styles
  callButtons: { flexDirection: "row", gap: 8 },
  callIconButton: { width: 40, height: 40, borderRadius: 20, backgroundColor: "rgba(99,164,255,.15)", alignItems: "center", justifyContent: "center", borderWidth: 1, borderColor: "rgba(99,164,255,.25)" },
  callIcon: { fontSize: 20 },
  callModalOverlay: { flex: 1, backgroundColor: "rgba(0,0,0,.85)", justifyContent: "center", alignItems: "center", padding: 20 },
  callModalCard: { width: "100%", maxWidth: 320, backgroundColor: "#0f1826", borderRadius: 24, padding: 24, borderWidth: 1, borderColor: "#1c2a3e", alignItems: "center", gap: 16 },
  callModalTitle: { fontSize: 18, fontWeight: "700", color: "#9db1ca" },
  callModalName: { fontSize: 24, fontWeight: "800", color: "#eef5ff" },
  callModalType: { fontSize: 16, color: "#63a4ff" },
  callModalButtons: { flexDirection: "row", gap: 12, marginTop: 8 },
  callButton: { flex: 1, paddingVertical: 14, borderRadius: 14, alignItems: "center", justifyContent: "center", borderWidth: 1 },
  callButtonAccept: { backgroundColor: "rgba(76,217,100,.15)", borderColor: "rgba(76,217,100,.35)" },
  callButtonReject: { backgroundColor: "rgba(255,77,109,.15)", borderColor: "rgba(255,77,109,.35)" },
  callButtonText: { color: "#eef5ff", fontWeight: "700", fontSize: 16 },
  callScreen: { flex: 1, backgroundColor: "#000" },
  remoteVideo: { flex: 1, width: "100%", height: "100%", backgroundColor: "#000" },
  localVideo: { position: "absolute", top: 60, right: 20, width: 120, height: 160, borderRadius: 12, backgroundColor: "#1a1a1a", borderWidth: 2, borderColor: "#333" },
  callOverlay: { position: "absolute", top: 0, left: 0, right: 0, bottom: 0, justifyContent: "space-between", padding: 20 },
  callStatus: { fontSize: 18, fontWeight: "700", color: "#fff", textAlign: "center", marginTop: 40, textShadowColor: "rgba(0,0,0,.8)", textShadowOffset: { width: 0, height: 1 }, textShadowRadius: 4 },
  callControls: { flexDirection: "row", justifyContent: "center", gap: 16, marginBottom: 40 },
  callControlButton: { width: 60, height: 60, borderRadius: 30, backgroundColor: "rgba(255,255,255,.2)", alignItems: "center", justifyContent: "center", borderWidth: 1, borderColor: "rgba(255,255,255,.3)" },
  callEndButton: { backgroundColor: "rgba(255,77,109,.8)", borderColor: "rgba(255,77,109,1)" },
  callControlIcon: { fontSize: 24 },
});
