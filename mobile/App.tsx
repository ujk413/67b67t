import "react-native-get-random-values";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  ActivityIndicator,
  Alert,
  AppState,
  BackHandler,
  Image,
  KeyboardAvoidingView,
  Linking,
  Modal,
  Platform,
  Pressable,
  SafeAreaView,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import { StatusBar } from "expo-status-bar";
import { Audio, Video, ResizeMode } from "expo-av";
import * as ImagePicker from "expo-image-picker";
import * as DocumentPicker from "expo-document-picker";
import { RTCView } from "react-native-webrtc";

import { api } from "./src/api";
import { displayName, roomTitle, roomPreview, shortTime } from "./src/format";
import { storage } from "./src/storage";
import { e2e } from "./src/crypto";
import { pushNotifications } from "./src/notifications";
import { WebRTCCall } from "./src/webrtc";
import { encryptRoomText, decryptRoomText } from "./src/roomCrypto";
import type { Message, Room, User } from "./src/types";
import { DEFAULT_BASE_URL, normalizeBaseUrl } from "./src/config";
import { telegramColors } from "./src/theme";

// Screens
import { AuthScreen } from "./src/screens/AuthScreen";
import { ChatListScreen } from "./src/screens/ChatListScreen";
import { ChatScreen } from "./src/screens/ChatScreen";
import { ProfileScreen } from "./src/screens/ProfileScreen";

const STORAGE_KEYS = { token: "auth.token" };
const REACTION_SET = ["👍", "❤️", "😂", "🔥", "😮", "👎"];
const TYPING_TTL_MS = 8000;
const TYPING_SEND_INTERVAL_MS = 3000;

function mediaUrl(baseUrl: string, mediaId: string | null | undefined) {
  return mediaId ? `${baseUrl}/media/${mediaId}` : null;
}

function mediaThumbUrl(baseUrl: string, mediaId: string | null | undefined) {
  return mediaId ? `${baseUrl}/media/${mediaId}/thumb` : null;
}

export default function App() {
  const [baseUrl, setBaseUrl] = useState(normalizeBaseUrl(DEFAULT_BASE_URL));
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
  const [typingUsers, setTypingUsers] = useState<Record<string, { username: string; roomId: string }>>({});
  const [authMode, setAuthMode] = useState<"login" | "register">("login");
  const [profileOpen, setProfileOpen] = useState(false);
  const [profileName, setProfileName] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [reactionTarget, setReactionTarget] = useState<{ id: string; reactions: Record<string, string[]> | null } | null>(null);
  const [viewerMessage, setViewerMessage] = useState<Message | null>(null);
  const [recording, setRecording] = useState<Audio.Recording | null>(null);
  const [playingId, setPlayingId] = useState<string | null>(null);

  const [incomingCall, setIncomingCall] = useState<{
    roomId: string;
    callerId: string;
    callerName: string;
    isVideo: boolean;
    offer: any;
  } | null>(null);
  const [activeCall, setActiveCall] = useState<WebRTCCall | null>(null);
  const [callState, setCallState] = useState<"idle" | "calling" | "ringing" | "connected">("idle");
  const [localStream, setLocalStream] = useState<any>(null);
  const [remoteStream, setRemoteStream] = useState<any>(null);
  const [isAudioEnabled, setIsAudioEnabled] = useState(true);
  const [isVideoEnabled, setIsVideoEnabled] = useState(false);

  const wsRef = useRef<WebSocket | null>(null);
  const onWsMessageRef = useRef<((data: any) => void) | null>(null);
  const resyncRef = useRef<(() => Promise<void>) | null>(null);
  const tokenRef = useRef<string | null>(null);
  const activeRoomIdRef = useRef<string | null>(null);
  const reconnectTimerRef = useRef<NodeJS.Timeout | null>(null);
  const roomSecretCache = useRef(new Map<string, string>());
  const audioRef = useRef<Audio.Sound | null>(null);
  const typingTimersRef = useRef<Record<string, NodeJS.Timeout>>({});
  const typingSentAtRef = useRef(0);
  const unauthorizedGuardRef = useRef(0);
  const callRef = useRef<WebRTCCall | null>(null);
  const callActiveRef = useRef(false);

  tokenRef.current = token;
  activeRoomIdRef.current = activeRoomId;
  callRef.current = activeCall;
  callActiveRef.current = callState !== "idle";

  useEffect(() => {
    const load = async () => {
      const savedToken = await storage.get(STORAGE_KEYS.token);
      if (savedToken) {
        try {
          setBaseUrl(normalizeBaseUrl(DEFAULT_BASE_URL));
          setToken(savedToken);
          await bootstrap(normalizeBaseUrl(DEFAULT_BASE_URL), savedToken);
        } catch (e) {
          console.error("Bootstrap failed:", errorText(e));
          await storage.del(STORAGE_KEYS.token);
        }
      }
      setLoading(false);
    };
    load();
  }, []);

  function resetUnauthorizedGuard() {
    unauthorizedGuardRef.current = Date.now();
  }

  function shouldPause() {
    return !tokenRef.current || Date.now() - unauthorizedGuardRef.current > 600000;
  }

  useEffect(() => {
    if (!token) return;
    resetUnauthorizedGuard();
    let cancelled = false;
    let attempt = 0;

    const connect = () => {
      if (cancelled || shouldPause()) return;
      const ws = new WebSocket(`${baseUrl.replace(/^http/, "ws")}/ws`);
      wsRef.current = ws;

      ws.onopen = () => {
        attempt = 0;
        clearTimeout(reconnectTimerRef.current!);
        reconnectTimerRef.current = null;
        if (wsRef.current !== ws || !tokenRef.current) return;
        try {
          ws.send(JSON.stringify({ type: "auth", token: tokenRef.current }));
          if (activeRoomIdRef.current) {
            ws.send(JSON.stringify({ type: "rooms.join", room_id: activeRoomIdRef.current }));
          }
          resyncRef.current?.();
        } catch {}
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          onWsMessageRef.current?.(data);
        } catch {}
      };

      const clearTimer = () => {
        if (reconnectTimerRef.current) {
          clearTimeout(reconnectTimerRef.current);
          reconnectTimerRef.current = null;
        }
      };

      const closeSocket = () => {
        try {
          ws.close();
        } catch {}
      };

      ws.onerror = () => {};

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
        if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
        try {
          wsRef.current?.close();
        } catch {}
      }
    });

    connect();
    return () => {
      cancelled = true;
      sub.remove();
      if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
      try {
        wsRef.current?.close();
      } catch {}
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
          setRooms(prepared.map((item) => (item.id === roomId ? { ...item, unread_count: 0 } : item)));
        } else {
          setRooms(prepared);
        }
      }
    } catch (e) {
      console.error("Resync failed:", errorText(e));
    }
    await replenishPrekeys(activeToken);
  }

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
      setMessages((prev) =>
        prev.map((item) => (item.id === data.message_id ? withReaction(item, data.emoji, data.user_id, true) : item))
      );
      return;
    }
    if (data.type === "reaction.remove" && data.message_id && data.emoji && data.user_id) {
      setMessages((prev) =>
        prev.map((item) => (item.id === data.message_id ? withReaction(item, data.emoji, data.user_id, false) : item))
      );
      return;
    }
    if (data.type === "messages.read" && Array.isArray(data.message_ids) && data.user_id) {
      setMessages((prev) =>
        prev.map((item) => (data.message_ids.includes(item.id) ? withRead(item, data.user_id) : item))
      );
      return;
    }
    if (data.type === "typing" && data.room_id && data.user_id) {
      const key = `${data.room_id}:${data.user_id}`;
      clearTimeout(typingTimersRef.current[key]);
      delete typingTimersRef.current[key];
      if (data.is_typing) {
        setTypingUsers((prev) => ({ ...prev, [key]: { username: data.username || "Кто-то", roomId: data.room_id } }));
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
    if (data.type === "call.offer") {
      setIncomingCall({
        roomId: data.room_id,
        callerId: data.from_user_id,
        callerName: data.from_username || "Unknown",
        isVideo: data.is_video || false,
        offer: data.offer || data.sdp,
      });
      return;
    }
    if (data.type === "call.answer" && activeCall) {
      try {
        await activeCall.handleAnswer(data.answer || data.sdp);
      } catch (error) {
        console.error("Failed to handle answer:", error);
      }
      return;
    }
    if ((data.type === "call.ice" || data.type === "call.ice_candidate") && activeCall) {
      try {
        await activeCall.addIceCandidate(data.candidate || data.ice);
      } catch (error) {
        console.error("Failed to add ICE candidate:", error);
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
    await replenishPrekeys(nextToken, user.id, nextBaseUrl);
    try {
      const fcmToken = await pushNotifications.initialize();
      if (fcmToken) {
        await api.registerFCMToken(nextBaseUrl, nextToken, fcmToken);
      }
      pushNotifications.onMessage((data: any) => {
        if (data.room_id) {
          openRoom(data.room_id);
        }
      });
    } catch (error) {
      console.error("Failed to initialize push notifications:", error);
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
      const result =
        authMode === "register"
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
      setRooms((prev) => prev.map((item) => (item.id === room.id ? { ...item, unread_count: 0 } : item)));
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
    if (message.meta?.e2e_group)
      return { ...message, text: "🔒 Сообщение в группе со сквозным шифрованием. Откройте веб-версию.", _decrypted: false };
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
    return Promise.all(
      items.map(async (room) => {
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
      })
    );
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

  function notifyTyping(isTyping: boolean) {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== 1 || !activeRoomId) return;
    const now = Date.now();
    if (isTyping && now - typingSentAtRef.current < TYPING_SEND_INTERVAL_MS) return;
    typingSentAtRef.current = isTyping ? now : 0;
    try {
      ws.send(JSON.stringify({ type: "typing", room_id: activeRoomId, is_typing: isTyping }));
    } catch {}
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

  async function sendPickedUpload(
    kind: "image" | "video" | "voice" | "video_note" | "media",
    file: { uri: string; name: string; type: string }
  ) {
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
    await sendPickedUpload("image", {
      uri: asset.uri,
      name: asset.fileName || "image.jpg",
      type: asset.mimeType || "image/jpeg",
    });
  }

  async function pickVideo() {
    const result = await ImagePicker.launchImageLibraryAsync({ mediaTypes: ["videos"], quality: 0.9 });
    if (result.canceled || !result.assets?.[0]) return;
    const asset = result.assets[0];
    await sendPickedUpload("video", {
      uri: asset.uri,
      name: asset.fileName || "video.mp4",
      type: asset.mimeType || "video/mp4",
    });
  }

  async function pickVideoNote() {
    const result = await ImagePicker.launchCameraAsync({ mediaTypes: ["videos"], videoMaxDuration: 60, quality: 0.8 });
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
        setMessages((prev) =>
          prev.map((item) => (item.id === reactionTarget.id ? withReaction(item, emoji, me?.id || "", false) : item))
        );
      } else {
        await api.addReaction(baseUrl, token, reactionTarget.id, emoji);
        setMessages((prev) =>
          prev.map((item) => (item.id === reactionTarget.id ? withReaction(item, emoji, me?.id || "", true) : item))
        );
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
                room_id: activeRoomId,
                candidate,
              })
            );
          },
          onConnectionStateChange: (state) => {
            if (state === "connected") setCallState("connected");
            if (state === "failed" || state === "closed") endCall();
          },
          onError: (error) => {
            console.error("WebRTC error:", error);
            setError(error.message);
            endCall();
          },
        },
        true,
        ice
      );

      await call.start();
      setActiveCall(call);
      setCallState("calling");
      setIsVideoEnabled(isVideo);

      const offer = call.getLocalDescription();
      wsRef.current?.send(
        JSON.stringify({
          type: "call.offer",
          room_id: activeRoomId,
          offer,
          is_video: isVideo,
        })
      );
    } catch (error) {
      console.error("Failed to start call:", error);
      setError("Failed to start call");
      endCall();
    }
  }

  async function answerCall() {
    if (!incomingCall || !token) return;

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
              })
            );
          },
          onConnectionStateChange: (state) => {
            if (state === "connected") setCallState("connected");
            if (state === "failed" || state === "closed") endCall();
          },
          onError: (error) => {
            console.error("WebRTC error:", error);
            setError(error.message);
            endCall();
          },
        },
        false,
        ice
      );

      await call.start();
      const answer = await call.handleOffer(incomingCall.offer);
      setActiveCall(call);
      setCallState("connected");
      setIsVideoEnabled(incomingCall.isVideo);
      setActiveRoomId(incomingCall.roomId);
      setIncomingCall(null);

      wsRef.current?.send(
        JSON.stringify({
          type: "call.answer",
          room_id: incomingCall.roomId,
          answer,
        })
      );
    } catch (error) {
      console.error("Failed to answer call:", error);
      setError("Failed to answer call");
      rejectCall();
    }
  }

  function rejectCall() {
    if (!incomingCall) return;
    wsRef.current?.send(
      JSON.stringify({
        type: "call.hangup",
        room_id: incomingCall.roomId,
      })
    );
    setIncomingCall(null);
  }

  function endCall() {
    if (activeCall) {
      activeCall.end();
      if (activeRoomId) {
        wsRef.current?.send(
          JSON.stringify({
            type: "call.hangup",
            room_id: activeRoomId,
          })
        );
      }
    }
    setActiveCall(null);
    setLocalStream(null);
    setRemoteStream(null);
    setCallState("idle");
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
        console.error("Failed to switch camera:", error);
      }
    }
  }

  const activeRoom = rooms.find((room) => room.id === activeRoomId) || null;
  const typingNames = Object.values(typingUsers)
    .filter((u) => u.roomId === activeRoomId)
    .map((u) => u.username);

  if (loading) {
    return (
      <SafeAreaView style={styles.center}>
        <StatusBar style="light" />
        <ActivityIndicator size="large" color={telegramColors.misc.progress} />
      </SafeAreaView>
    );
  }

  if (!token || !me) {
    return (
      <>
        <StatusBar style="light" />
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

  return (
    <SafeAreaView style={styles.screen}>
      <StatusBar style="light" />
      <KeyboardAvoidingView style={styles.flex} behavior={Platform.OS === "ios" ? "padding" : undefined}>
        {!activeRoom ? (
          <ChatListScreen
            user={me}
            rooms={rooms}
            baseUrl={baseUrl}
            refreshing={refreshingRooms}
            onRefresh={refreshRooms}
            onProfilePress={() => {
              setProfileName(me.display_name || "");
              setProfileOpen(true);
            }}
            onRoomPress={openRoom}
            mediaUrl={mediaUrl}
          />
        ) : (
          <ChatScreen
            room={activeRoom}
            messages={messages}
            meId={me.id}
            baseUrl={baseUrl}
            composer={composer}
            typingNames={typingNames}
            refreshing={refreshingMessages}
            recording={!!recording}
            playingId={playingId}
            disabled={busyUpload}
            onBack={() => setActiveRoomId(null)}
            onRefresh={refreshMessages}
            onChangeComposer={(text) => {
              setComposer(text);
              notifyTyping(text.length > 0);
            }}
            onSend={sendCurrentMessage}
            onAttach={pickFile}
            onVoice={toggleRecording}
            onAudioCall={activeRoom.kind === "dm" ? () => startCall(false) : undefined}
            onVideoCall={activeRoom.kind === "dm" ? () => startCall(true) : undefined}
            onPlayVoice={playVoice}
            onReact={(message) => setReactionTarget({ id: message.id, reactions: message.reactions || null })}
            onOpenMedia={openMedia}
            mediaUrl={mediaUrl}
            mediaThumbUrl={mediaThumbUrl}
          />
        )}
      </KeyboardAvoidingView>

      <ProfileScreen
        visible={profileOpen}
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

      <ReactionModal
        open={!!reactionTarget}
        onClose={() => setReactionTarget(null)}
        onPick={toggleReaction}
      />

      <MediaViewerModal
        open={!!viewerMessage}
        message={viewerMessage}
        baseUrl={baseUrl}
        onClose={closeMedia}
        onPlayVoice={playVoice}
      />

      {incomingCall && (
        <Modal visible transparent animationType="fade">
          <View style={styles.callModalOverlay}>
            <View style={styles.callModalCard}>
              <Text style={styles.callModalTitle}>Входящий вызов</Text>
              <Text style={styles.callModalName}>{incomingCall.callerName}</Text>
              <Text style={styles.callModalType}>{incomingCall.isVideo ? "Видеовызов" : "Аудиовызов"}</Text>
              <View style={styles.callModalButtons}>
                <Pressable style={[styles.callButton, styles.callButtonAccept]} onPress={answerCall}>
                  <Text style={styles.callButtonText}>✓ Ответить</Text>
                </Pressable>
                <Pressable style={[styles.callButton, styles.callButtonReject]} onPress={rejectCall}>
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
              <RTCView streamURL={remoteStream.toURL()} style={styles.remoteVideo} objectFit="cover" />
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
                {callState === "calling" ? "Звоним..." : callState === "ringing" ? "Звонит..." : "Подключено"}
              </Text>
              <View style={styles.callControls}>
                <Pressable style={styles.callControlButton} onPress={toggleAudio}>
                  <Text style={styles.callControlIcon}>{isAudioEnabled ? "🔊" : "🔇"}</Text>
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

function ReactionModal({ open, onClose, onPick }: { open: boolean; onClose: () => void; onPick: (emoji: string) => void }) {
  return (
    <Modal transparent visible={open} animationType="fade" onRequestClose={onClose}>
      <Pressable style={styles.modalBackdrop} onPress={onClose}>
        <View style={styles.reactionModal}>
          {REACTION_SET.map((emoji) => (
            <Pressable key={emoji} style={styles.reactionPick} onPress={() => onPick(emoji)}>
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
  const url = message ? mediaUrl(baseUrl, message.media_id) : null;
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
        <Pressable style={styles.viewerClose} onPress={onClose}>
          <Text style={styles.viewerCloseText}>Закрыть</Text>
        </Pressable>
        <View style={styles.viewerBody}>
          {isImage && url ? <Image source={{ uri: url }} style={styles.viewerImage} resizeMode="contain" /> : null}
          {isVideo && url ? (
            <Video source={{ uri: url }} style={styles.viewerVideo} useNativeControls resizeMode={ResizeMode.CONTAIN} />
          ) : null}
          {isVoice && url ? (
            <Pressable style={styles.viewerVoice} onPress={() => onPlayVoice(message)}>
              <Text style={styles.viewerVoiceText}>
                Воспроизвести{message.duration_sec ? ` (${Math.round(message.duration_sec)}с)` : ""}
              </Text>
            </Pressable>
          ) : null}
          {isVideoNote && url ? (
            <Video
              source={{ uri: url }}
              style={styles.viewerVideoNote}
              useNativeControls
              resizeMode={ResizeMode.CONTAIN}
              isLooping
            />
          ) : null}
          {isFile && url ? (
            <Pressable style={styles.viewerFile} onPress={() => Linking.openURL(url)}>
              <Text style={styles.viewerFileTitle}>{message.text || "Открыть файл"}</Text>
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
          unread_count: message.sender_id === meId ? 0 : Math.max(1, Number(room.unread_count || 0)),
        }
  );
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

function errorText(value: unknown) {
  const text = value instanceof Error ? value.message : "Unknown error";
  switch (text) {
    case "bad_credentials":
      return "Неверное имя пользователя или пароль";
    case "username_taken":
      return "Имя пользователя уже занято";
    case "username_too_short":
      return "Слишком короткое имя";
    case "username_too_long":
      return "Слишком длинное имя";
    case "username_invalid":
      return "Имя может содержать только латинские буквы, цифры и подчёркивание";
    case "password_too_short":
      return "Слишком короткий пароль";
    case "captcha_failed":
      return "Регистрация на этом сервере требует captcha";
    case "account_banned":
      return "Аккаунт заблокирован";
    default:
      return text;
  }
}

const styles = StyleSheet.create({
  flex: { flex: 1 },
  screen: { flex: 1, backgroundColor: telegramColors.windowBackground },
  center: { flex: 1, alignItems: "center", justifyContent: "center", backgroundColor: telegramColors.windowBackground },

  modalBackdrop: {
    flex: 1,
    backgroundColor: "rgba(0,0,0,0.5)",
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
    backgroundColor: "rgba(255,255,255,0.05)",
  },
  reactionPickText: { fontSize: 24 },

  viewerScreen: { flex: 1, backgroundColor: telegramColors.windowBackground, paddingTop: 20 },
  viewerClose: {
    alignSelf: "flex-end",
    marginHorizontal: 16,
    marginBottom: 12,
    paddingHorizontal: 14,
    paddingVertical: 10,
    borderRadius: 12,
    backgroundColor: "rgba(255,255,255,0.08)",
  },
  viewerCloseText: { color: telegramColors.text.primary, fontWeight: "700" },
  viewerBody: { flex: 1, alignItems: "center", justifyContent: "center", padding: 18 },
  viewerImage: { flex: 1, width: "100%", height: "100%", backgroundColor: telegramColors.windowBackground },
  viewerVideo: { width: "100%", height: "100%", backgroundColor: telegramColors.windowBackground },
  viewerVideoNote: {
    width: 240,
    height: 240,
    borderRadius: 120,
    overflow: "hidden",
    backgroundColor: telegramColors.windowBackground,
  },
  viewerVoice: { paddingHorizontal: 18, paddingVertical: 14, borderRadius: 14, backgroundColor: "rgba(255,255,255,0.08)" },
  viewerVoiceText: { color: telegramColors.text.primary, fontWeight: "700" },
  viewerFile: {
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.08)",
    backgroundColor: "rgba(255,255,255,0.04)",
    borderRadius: 16,
    padding: 16,
    width: "100%",
  },
  viewerFileTitle: { color: telegramColors.text.primary, fontWeight: "700", fontSize: 16 },
  viewerFileSubtitle: { color: telegramColors.text.gray5, marginTop: 4 },

  callModalOverlay: {
    flex: 1,
    backgroundColor: "rgba(0,0,0,0.85)",
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
  callModalTitle: { fontSize: 18, fontWeight: "700", color: telegramColors.text.gray5 },
  callModalName: { fontSize: 24, fontWeight: "800", color: telegramColors.text.primary },
  callModalType: { fontSize: 16, color: telegramColors.text.blue },
  callModalButtons: { flexDirection: "row", gap: 12, marginTop: 8 },
  callButton: {
    flex: 1,
    paddingVertical: 14,
    borderRadius: 14,
    alignItems: "center",
    justifyContent: "center",
  },
  callButtonAccept: { backgroundColor: "rgba(76,217,100,0.2)" },
  callButtonReject: { backgroundColor: "rgba(255,77,109,0.2)" },
  callButtonText: { color: telegramColors.text.primary, fontWeight: "700", fontSize: 16 },

  callScreen: { flex: 1, backgroundColor: "#000" },
  remoteVideo: { flex: 1, width: "100%", height: "100%", backgroundColor: "#000" },
  localVideo: {
    position: "absolute",
    top: 60,
    right: 20,
    width: 120,
    height: 160,
    borderRadius: 12,
    backgroundColor: "#1a1a1a",
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
    color: "#fff",
    textAlign: "center",
    marginTop: 40,
  },
  callControls: { flexDirection: "row", justifyContent: "center", gap: 16, marginBottom: 40 },
  callControlButton: {
    width: 60,
    height: 60,
    borderRadius: 30,
    backgroundColor: "rgba(255,255,255,0.2)",
    alignItems: "center",
    justifyContent: "center",
  },
  callEndButton: { backgroundColor: "rgba(255,77,109,0.8)" },
  callControlIcon: { fontSize: 24 },
});
