import {t as uiText} from "../web/i18n.js";
import { protectedUploader } from "./src/protectedUpload";
import * as FileSystem from 'expo-file-system/legacy';
import { newId,AccountRepository } from './src/data/repository';
import {mediaDetails} from '../web/media-details.js';
import {FeatureProvider} from './src/features/FeatureProvider';
import {eraseSecretVault,pruneSecretVault} from '../web/secret-vault.js';
import {createSessionScope} from '../web/session-scope.js';
import type {FeatureContext} from './src/features/types';
import {resolveMediaSource, cancelDecryptedMedia, eraseDecryptedMedia} from './src/decryptedMedia';
import {pauseAllMedia} from './src/playbackOwnership';
import {ShareInConfirm,updateUnreadWidget,removeIncomingAccountShares} from './src/features/shareIn';
import {stopExpressiveLiveLocations} from './src/features/ExpressiveFeatures';
import * as SecureStore from 'expo-secure-store';
import {AuthChallenge,type AccountChallenge} from './src/features/AuthChallenge';
import {QrLogin} from './src/features/QrLogin';
import {retryDetachedLogins,forgetDetachedLogins} from './src/features/loginLifecycle';
import {parse67Link,type DeepLink} from '../web/deep-links.js';
import {accountRegistry,installAccountNativeHooks,clearNativeAccount,recoverPendingAccountRestore,pendingAccountRestoreOwner,clearPendingAccountRestore,restoreTransport,assertCallsAllowed,unregisterPushForTransport,type RestoredAccountSession} from './src/accountRegistry';
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
import {addNetworkStateListener} from 'expo-network';
import { useAccountData } from "./src/hooks/useAccountData";
import {useDraftSync} from './src/hooks/useDraftSync';
import { mobileApi } from "./src/mobileApi";
import { mergeDisplayMessages as mergeMessages } from "./src/data/messages";
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
import { E2EEncryption } from "./src/crypto";
import { pushNotifications } from "./src/notifications";
import {qualityStorageKey,setAccountDownloadPolicy} from './src/features/qualityPolicy';
import type {QualityDownloadPolicy} from '../web/quality-policy.js';
import { WebRTCCall,type CallEvents } from "./src/webrtc";
import {
  decryptRoomText,
  encryptRoomText,
  clearDerivedRoomKeys,
} from "./src/roomCrypto";
import { RTCView } from "@livekit/react-native-webrtc";
import { telegramColors } from "./src/theme";
import { AuthScreen } from "./src/screens/AuthScreen";
import { ChatListScreen } from "./src/screens/ChatListScreen";
import { ChatScreen } from "./src/screens/ChatScreen";
import { ProfileScreen } from "./src/screens/ProfileScreen";

// Durable credentials boundary; this helper never starts network/Signal bootstrap.
async function persistRestoredCredentials(base:string,session:RestoredAccountSession){
 await storage.set('mobile.e2eDeviceId.'+session.user.id,session.deviceId);
 const registered={id:session.user.id,baseUrl:base,userId:session.user.id,username:session.user.username};
 await accountRegistry.save(registered,session.token);await accountRegistry.activate(registered.id,{close:async()=>{},open:async()=>{}});
 await storage.set('mobile.user',JSON.stringify(session.user));await removeSessionTokens();
}
async function restoreRegisteredSessionToken(){
  await restoreTransport();
  await retryDetachedLogins();
 const pending=await pendingAccountRestoreOwner();
 if(pending){if(normalizeBaseUrl(pending.baseUrl)!==normalizeBaseUrl(DEFAULT_BASE_URL))throw Error('Незавершённое восстановление относится к другому серверу');const repo=new AccountRepository(pending.userId);await repo.open();try{await recoverPendingAccountRestore({baseUrl:pending.baseUrl,userId:pending.userId,repository:repo,request:(path,options)=>request(pending.baseUrl,path,options),onRestoredSession:session=>persistRestoredCredentials(pending.baseUrl,session)});}finally{await repo.close();}}
 const active=await accountRegistry.active();if(active){const registered=(await accountRegistry.list()).find(row=>row.id===active);if(registered&&normalizeBaseUrl(registered.baseUrl)===normalizeBaseUrl(DEFAULT_BASE_URL)){const saved=await accountRegistry.token(active);if(saved)return saved;}}
 return restoreSessionToken();
}
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
  const [pendingLink,setPendingLink]=useState<DeepLink|null>(null),[featureTarget,setFeatureTarget]=useState<Message|null>(null);
  const [forwarding, setForwarding] = useState<Message[]>([]);
  const forwardOperation=useRef(newId());
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
  const [authChallenge,setAuthChallenge]=useState<AccountChallenge|null>(null);
  const [qrLogin,setQrLogin]=useState(false);
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
  const [callNetwork,setCallNetwork]=useState(''),[callDuration,setCallDuration]=useState(0),[sharingScreen,setSharingScreen]=useState(false);
  const callStartedAt=useRef<number|null>(null),callGeneration=useRef(0);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const audioRef = useRef<Audio.Sound | null>(null);
  const roomSecretCache = useRef<Map<string, string>>(new Map());
  const pendingRestoredSession=useRef<RestoredAccountSession|null>(null);
  const signalSession=useRef<{token:string;engine:E2EEncryption}|null>(null);
  // Latest-value refs so the long-lived WebSocket/AppState callbacks never see stale state.
  const tokenRef = useRef<string | null>(null);
  // Saved accounts reuse their bearer. Every close/open needs a distinct lifetime.
  const sessionGenerationRef = useRef(0);
  // Only opening changes the repository effect's lifetime. Closing fences work
  // immediately, but keeps that effect alive until secret erasure has finished.
  const [sessionGeneration,setSessionGeneration] = useState(0);
  function activateNativeSession(nextToken: string | null) {
    sessionGenerationRef.current++;
    tokenRef.current = nextToken;
    setSessionGeneration(sessionGenerationRef.current);
    setToken(nextToken);
  }
  const qualityAccountRef=useRef<string|null>(null);
  const pendingNotification=useRef<{room_id:string;recipient_id:string;text?:string}|null>(null);
  const pendingNativeCall=useRef<{call_id:string;recipient_id:string;action?:string;expires_at?:number|string}|null>(null);
  const callAccountPrompt=useRef<string|null>(null);
  const activeRoomIdRef = useRef<string | null>(null);
  const callActiveRef = useRef(false);
  const onWsMessageRef = useRef<((data: any) => Promise<void>) | null>(null);
  const resyncRef = useRef<(() => Promise<void>) | null>(null);
  useEffect(() => {
    updateUnreadWidget(rooms.reduce((sum,room)=>sum+(room.unread_count||0),0));
    void pushNotifications
      .setBadgeCount(
        rooms
          .filter((r) => !r.preferences?.muted)
          .reduce((sum, r) => sum + (r.unread_count || 0), 0),
      )
      .catch(() => {});
  }, [rooms]);
  async function encodeText(roomId: string, text: string) {
    const sendingToken=token,sendingGeneration=sessionGeneration;
    const guard=()=>{if(!sendingToken||tokenRef.current!==sendingToken||sessionGenerationRef.current!==sendingGeneration)throw Error('Сеанс изменился');};guard();
    if(rooms.find(room=>room.id===roomId)?.kind==='group'&&groupEncryption){const enabled=await groupEncryption.enabled(roomId);guard();if(enabled){const body=await groupEncryption.encrypt(roomId,text);guard();return body;}}
    const secret = await getRoomSecret(roomId); if (!secret) throw Error('Ключ переписки недоступен');
    guard();const body=await encryptRoomText(roomId,secret,text);guard();return {type:'text',...body};
  }
  const appUpdate = useAppUpdate(baseUrl, true);
  // --- App lock -------------------------------------------------------------
  const [lockEnabled, setLockEnabled] = useState(false);
  const [lockReady,setLockReady] = useState(false);
  const [unlocked, setUnlocked] = useState(true);
  const [unlocking, setUnlocking] = useState(false);
  const [lockError, setLockError] = useState<string | null>(null);
  const [lockButtonLabel, setLockButtonLabel] = useState(uiText("Разблокировать"));

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
        setLockError(uiText("Не удалось подтвердить. Попробуйте ещё раз."));
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
    activationGeneration:sessionGeneration,
    upload: protectedUploader(baseUrl,token || '',encodeText,me?.id),
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
      setError(uiText("Доступ к переписке прекращён"));
    },
    getSecret: getRoomSecret,
  });
  const groupEncryption = useMemo(() => {
    if (!me?.id || !token) return null;
    // The render that opens an activation can still see the previous store.
    // Bind only a repository published by this activation's account-data effect.
    let owner:AccountRepository|null=local.repositoryGeneration.current===sessionGeneration&&local.repository.current?.userId===me.id?local.repository.current:null;
    return new GroupEncryption(me.id,baseUrl,token,()=>{
      if(tokenRef.current!==token||sessionGenerationRef.current!==sessionGeneration)return null;
      if(local.repositoryGeneration.current!==sessionGeneration)return null;
      const current=local.repository.current;
      if(!current||current.userId!==me.id)return null;
      if(!owner)owner=current;
      return owner===current?owner:null;
    });
  }, [me?.id,token,baseUrl,sessionGeneration]);
  useEffect(()=>{const repository=local.repository.current;if(local.ready&&repository)void pruneSecretVault(repository).catch(()=>{});},[local.ready,me?.id]);
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
      .catch((e) => setError(userError(e)));
    return () => {
      active = false;
    };
  }, [activeRoomId, local.ready]);
  const draftPreviews=useDraftSync({repository:local.repository.current,ready:local.ready,baseUrl,token,userId:me?.id||null,roomId:activeRoomId,text:composer,reply:local.reply,encode:encodeText,decode:prepareMessage,apply:(room,draft)=>{if(activeRoomIdRef.current===room){setComposer(draft.text);local.setReply(draft.reply||null);}}});
  useEffect(()=>{const receive=(url:string|null)=>{const target=parse67Link(url);if(target)setPendingLink(target);};void Linking.getInitialURL().then(receive);const sub=Linking.addEventListener('url',event=>receive(event.url));return()=>sub.remove();},[]);
  useEffect(()=>{if(!pendingLink||!token||!me||!local.ready)return;let alive=true;const savedToken=token;void(async()=>{
    let roomId:string,messageId:string|undefined;
    if(pendingLink.type==='action'){if(pendingLink.action==='compose'){setWorkspace('new');setPendingLink(null);return;}roomId=(await mobileApi(baseUrl,token).saved()).id;}
    else if(pendingLink.type==='chat'){roomId=pendingLink.roomId;messageId=pendingLink.messageId;}
    else if(pendingLink.type==='profile'){const users=await mobileApi(baseUrl,token).users(pendingLink.username),user=users.find(row=>row.username.toLowerCase()===pendingLink.username.toLowerCase());if(!user)throw Error('Пользователь не найден');roomId=(await mobileApi(baseUrl,token).createRoom('dm',[user.username])).id;}
    else {const result=await request<{id:string}>(baseUrl,'/invites/'+encodeURIComponent(pendingLink.token)+'/join',{token,method:'POST',body:{}});roomId=result.id;}
    const room=await mobileApi(baseUrl,token).room(roomId);if(!alive||tokenRef.current!==savedToken)return;setWorkspace(null);await openRoom(room);if(messageId){await local.jump(messageId);if(alive)setFocusId(messageId);}if(alive)setPendingLink(null);
  })().catch(e=>{if(alive){setError((e as {status?:number}).status===403||(e as {status?:number}).status===404?uiText("Нет доступа к этой переписке, сообщение удалено или приглашение истекло."):userError(e));setPendingLink(null);}});return()=>{alive=false;};},[pendingLink,token,me?.id,local.ready]);
  useEffect(()=>{
    if(!me||!token)return;
    installAccountNativeHooks({
      deviceId:()=>getMobileDeviceId(me.id),
      restoreSession:async credentials=>{
        if(!credentials.password)throw Error('Введите пароль аккаунта для нового сеанса восстановления');
        let deviceKey=await SecureStore.getItemAsync('67chat.login-device');if(!deviceKey){deviceKey=newId()+newId();await SecureStore.setItemAsync('67chat.login-device',deviceKey);}
        let result=await request<{user?:User;token?:string;challenge?:AccountChallenge}>(baseUrl,'/auth/login',{method:'POST',bearerSession:true,body:{username:me.username,password:credentials.password,totp:credentials.totp,backup_code:credentials.backup_code,device_key:deviceKey,device_name:uiText("67chat Android — восстановление")}});
        if(result.challenge){await request(baseUrl,'/me/account/challenges/'+result.challenge.id+'/approve',{method:'POST',token,body:{}});result=await request(baseUrl,'/auth/account-challenge/complete',{method:'POST',bearerSession:true,body:{id:result.challenge.id,secret:result.challenge.secret,code:credentials.totp,backup_code:credentials.backup_code}});}
        if(!result.token||result.user?.id!==me.id)throw Error('Новый сеанс восстановления не подтверждён');
        return{user:result.user,token:result.token};
      },
      onRestoredSession:async session=>{if(session.user.id!==me.id)throw Error('Аккаунт восстановления не совпадает');await persistRestoredCredentials(baseUrl,session);pendingRestoredSession.current=session;},
      onRestoreFinished:async()=>{const restored=pendingRestoredSession.current;if(!restored)return;pendingRestoredSession.current=null;activateNativeSession(restored.token);local.outbox.current?.stop();wsRef.current?.close();wsRef.current=null;signalSession.current=null;roomSecretCache.current.clear();clearDerivedRoomKeys();resetUnauthorizedGuard();setMe(restored.user);await bootstrap(baseUrl,restored.token,false);},
      addAccount:()=>clearSession(true),
      switchAccount:async id=>{await accountRegistry.activate(id,{close:()=>clearSession(true),open:async(account,saved)=>{if(normalizeBaseUrl(account.baseUrl)!==normalizeBaseUrl(DEFAULT_BASE_URL))throw Error('Этот клиент поддерживает аккаунты сервера 67chat.ru');const user=await api.me(account.baseUrl,saved);if(user.id!==account.userId)throw Error('Аккаунт не совпадает с сохранённым сеансом');await persistSession(account.baseUrl,saved,user,false);}});},
      beforeTransportChange:async next=>{
        local.outbox.current?.stop();endCall();wsRef.current?.close();wsRef.current=null;
      },
      onDeleted:async()=>{local.outbox.current?.stop();await eraseDecryptedMedia(me.id);await clearNativeAccount(me.id);await clearPendingAccountRestore(me.id);await forgetDetachedLogins(me.id);await storage.del("mobile.e2eDeviceId."+me.id);await clearSession(false,true);},
    });return()=>installAccountNativeHooks({});
  },[me?.id,token,baseUrl]);
  useEffect(() => {
    if (local.ready)
      void local.repository.current?.saveRooms(rooms).catch(() => {});
  }, [rooms, local.ready]);
  useEffect(() => {
    if (!token || !me) return;
    let stopped = false,
      dispose: (() => void) | undefined;
    const receive=(data:{room_id?:string;recipient_id?:string})=>{
      if(stopped||tokenRef.current!==token||!data.room_id)return;
      if(data.recipient_id&&data.recipient_id!==me.id){void requestNotificationAccount({room_id:data.room_id,recipient_id:data.recipient_id});return;}
      void mobileApi(baseUrl,token).room(data.room_id).then(room=>{
        if(stopped||tokenRef.current!==token)return;
        setWorkspace(null);setProfileOpen(false);return openRoom(room);
      }).catch(e=>{if(!stopped&&tokenRef.current===token)setError(userError(e));});
    };
    pushNotifications.onMessage(receive);
    const pending=pendingNotification.current;
    if(pending?.recipient_id===me.id&&!pending.text){pendingNotification.current=null;receive(pending);}
    void pushNotifications
      .connect(baseUrl, token, () => activeRoomIdRef.current, me.id)
      .then((fn) => {
        if (stopped) fn();
        else dispose = fn;
      })
      .catch((e) => setError(userError(e)));
    return () => {
      stopped = true;
      dispose?.();
    };
  }, [token, me?.id,baseUrl]);
  useEffect(() => {
    if (token) void pushNotifications.refresh().catch(() => {});
  }, [activeRoomId, token]);
  useEffect(()=>{
    if(!token||!me)return;
    let live=true;const account=me.id;
    if(qualityAccountRef.current!==account){qualityAccountRef.current=account;theme.setLanguage('ru');theme.setWallpaper('plain');theme.setAccent('emerald');}
    void request<{language:'ru'|'en';wallpaper:'plain'|'dots'|'mesh';accent:'emerald'|'blue'|'violet';download:QualityDownloadPolicy;quiet:unknown;cache_max_mb:number}>(baseUrl,'/me/quality',{token}).then(async prefs=>{
      if(!live||tokenRef.current!==token)return;
      await Promise.all([
        setAccountDownloadPolicy(account,prefs.download),
        storage.set(qualityStorageKey(account,'quiet'),JSON.stringify(prefs.quiet)),
        storage.set(qualityStorageKey(account,'cache'),String(prefs.cache_max_mb)),
        storage.set('media.cacheLimitMB',String(prefs.cache_max_mb)),
      ]);
      if(live&&tokenRef.current===token){theme.setLanguage(prefs.language);theme.setWallpaper(prefs.wallpaper);theme.setAccent(prefs.accent);}
    }).catch(error=>{if(live)setError(userError(error));});
    return()=>{live=false;};
  },[token,me?.id,baseUrl]);
  useEffect(()=>{
    if(!token||!me||!local.ready||lockEnabled&&!unlocked){pushNotifications.onQuickReply(null);return;}
    const account=me.id,session=token;
    const reply=async(item:{roomId:string;recipientId:string;text:string})=>{
      if(tokenRef.current!==session)throw Error('Сеанс изменился');
      if(item.recipientId!==account){await requestNotificationAccount({room_id:item.roomId,recipient_id:item.recipientId,text:item.text});return;}
      await mobileApi(baseUrl,session).room(item.roomId);
      if(tokenRef.current!==session)throw Error('Сеанс изменился');
      await local.send(item.text,undefined,item.roomId,undefined);
    };
    pushNotifications.onQuickReply(reply);
    const pending=pendingNotification.current;
    if(pending?.recipient_id===account&&pending.text){pendingNotification.current=null;void reply({roomId:pending.room_id,recipientId:account,text:pending.text}).catch(error=>{if(tokenRef.current===session){pendingNotification.current=pending;setError(userError(error));}});}
    return()=>pushNotifications.onQuickReply(null);
  },[token,me?.id,local.ready,lockEnabled,unlocked,baseUrl]);
  async function requestNotificationAccount(item:{room_id:string;recipient_id:string;text?:string}){
    const notificationSession=tokenRef.current;
    const accounts=await accountRegistry.list(),target=accounts.find(row=>row.userId===item.recipient_id&&normalizeBaseUrl(row.baseUrl)===normalizeBaseUrl(baseUrl));
    if(!notificationSession||tokenRef.current!==notificationSession)return;
    if(!target){Alert.alert(uiText("Другой аккаунт"),uiText("Войдите в аккаунт, которому адресовано уведомление."));return;}
    Alert.alert(item.text?uiText("Переключиться и отправить ответ?"):uiText("Переключить аккаунт?"),uiText("Уведомление адресовано @{0}. {1}",target.username,item.text?uiText("После переключения ваш ответ будет отправлен из этого аккаунта."):uiText("Открыть его аккаунт?")),[
      {text:uiText("Отмена"),style:'cancel'},
      {text:uiText("Переключить"),onPress:()=>{if(tokenRef.current!==notificationSession)return;pendingNotification.current=item;void accountRegistry.activate(target.id,{close:()=>clearSession(true),open:async(account,saved)=>{const user=await api.me(account.baseUrl,saved);if(user.id!==account.userId)throw Error('Аккаунт не совпадает с сохранённым сеансом');await persistSession(account.baseUrl,saved,user,false);}}).catch(error=>{pendingNotification.current=null;setError(userError(error));});}},
    ]);
  }
  function changeComposer(text: string) {
    setComposer(text);
    notifyTyping(text.length > 0);
    if (activeRoomId)
      void local.repository.current
        ?.set(`draft:${activeRoomId}`, { text, reply: local.reply })
        .catch((e) => setError(userError(e)));
  }
  activeRoomIdRef.current = activeRoomId;
  callActiveRef.current =
    !!activeCall || !!incomingCall || callState !== "idle";

  useEffect(() => {
    // Ignore late failures from a previous session when another user has logged in.
    setUnauthorizedHandler((failedToken) => {
      if (failedToken !== tokenRef.current) return;
      void clearSession();
      setError(uiText("Сеанс завершён. Войдите снова."));
    });
    return () => setUnauthorizedHandler(null);
  }, []);

  useEffect(
    () =>
      restoreSessionEffect({
        restoreSessionToken:restoreRegisteredSessionToken,
        storage,
        normalizeBaseUrl,
        DEFAULT_BASE_URL,
        setBaseUrl,
        setMe,
        tokenRef,
        resetUnauthorizedGuard,
        setToken:activateNativeSession,
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
        onAuthenticated:()=>{if(callResumeRef.current&&callIdRef.current&&callRoomRef.current)wsRef.current?.send(JSON.stringify({type:'call.resume',call_id:callIdRef.current,room_id:callRoomRef.current,resume_token:callResumeRef.current}));},
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
      const engine=new E2EEncryption();engine.configure(nextBaseUrl,activeToken);await engine.initialize(userId);
      if(tokenRef.current!==activeToken)return;signalSession.current={token:activeToken,engine};
      if (unused >= 10) return;
      await api.uploadE2EKeys(
        nextBaseUrl,
        activeToken,
        deviceId,
        await engine.getPublicKeys(20),
      );
    } catch (e) {
      console.error("Failed to replenish pre-keys:", errorText(e));
    }
  }

  async function handleWsMessage(data: any) {
    const receivingToken = token;
    if(tokenRef.current!==receivingToken)return;
    if(data.type==='message.listened' && data.message_id){setMessages(prev=>prev.map(message=>message.id===data.message_id?{...message,listened_by:data.listened_by}:message));return;}
    if (data.type === "auth.required" || data.type === 'auth.error') {
      await clearSession();
      setError(uiText("Сеанс завершён. Войдите снова."));
      return;
    }
    if (data.type === "rooms.update" && Array.isArray(data.rooms)) {
      const prepared=await prepareRooms(data.rooms,baseUrl,token);if(tokenRef.current===receivingToken)setRooms(prepared);
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
      if(tokenRef.current!==receivingToken)return;
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
          [key]: { username: data.username || uiText("Кто-то"), roomId: data.room_id },
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
    if(String(data.type||'').startsWith('call.')){await handleCallSignal(data);return;}
  }

  async function bootstrap(nextBaseUrl: string, nextToken: string,registerLegacy=true) {
    const activation=sessionGenerationRef.current;
    const [user, roomList] = await Promise.all([
      api.me(nextBaseUrl, nextToken),
      api.rooms(nextBaseUrl, nextToken),
    ]);
    if(tokenRef.current!==nextToken||sessionGenerationRef.current!==activation)return;
    // Migrate the released clients' plaintext session token only after /me
    // confirms its owner. Existing encrypted history keeps its original name.
    const active=await accountRegistry.active();
    if(registerLegacy&&!active){const account={id:user.id,baseUrl:nextBaseUrl,userId:user.id,username:user.username};await accountRegistry.save(account,nextToken);if(tokenRef.current!==nextToken||sessionGenerationRef.current!==activation)return;await accountRegistry.activate(account.id,{close:async()=>{},open:async()=>{}});await removeSessionTokens();}
    if(tokenRef.current!==nextToken||sessionGenerationRef.current!==activation)return;
    await storage.set("mobile.user", JSON.stringify(user));
    setMe(user);
    setRooms(roomList);
    // Initial navigation must not wait for every preview's key derivation or prekeys.
    void prepareRooms(roomList, nextBaseUrl, nextToken).then(prepared => {
      if (tokenRef.current === nextToken&&sessionGenerationRef.current===activation) setRooms(prepared);
    }).catch(() => {});
    void replenishPrekeys(nextToken, user.id, nextBaseUrl);
  }

  async function clearSession(keepAccount=false,destroyLocalData=false) {
    sessionGenerationRef.current++;
    tokenRef.current = null;
    endCall();
    stopExpressiveLiveLocations();
    setComposer('');setWorkspace(null);setForwarding([]);setEditing(null);setAuthChallenge(null);setViewerMessage(null);
    const cancellingMedia = me ? cancelDecryptedMedia(me.id) : Promise.resolve();
    local.outbox.current?.stop();
    const oldRepo=local.repository.current;
    // Fence synchronously, and observe rejection immediately while other teardown runs.
    const eraseSecrets=(!keepAccount&&oldRepo?eraseSecretVault(oldRepo):Promise.resolve()).then(()=>null,error=>error);
    const disconnectPush=token?pushNotifications.disconnect(baseUrl,token,false,keepAccount).catch(()=>{}):Promise.resolve();
    const socket = wsRef.current;
    wsRef.current = null;
    socket?.close();
    if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
    reconnectTimerRef.current = null;
    Object.values(typingTimersRef.current).forEach(clearTimeout);
    typingTimersRef.current = {};
    setTypingUsers({});
    roomSecretCache.current.clear();
    signalSession.current=null;
    clearDerivedRoomKeys();
    setRooms([]);
    setMessages([]);
    setActiveRoomId(null);
    setActiveRoomMembers([]);
    try{
      if(!keepAccount&&me)removeIncomingAccountShares(me.id);
      if(!keepAccount){pendingNativeCall.current=null;callAccountPrompt.current=null;setAutoAnswerId(null);const active=await accountRegistry.active();if(active)await accountRegistry.remove(active);}
      await removeSessionTokens();
      await storage.del("mobile.user");
      await cancellingMedia;
      const eraseError=await eraseSecrets;
      if(destroyLocalData&&oldRepo){
        await removePrivateMedia(await oldRepo.privateFiles(),oldRepo.userId);
        // Full repository/key destruction also erases secrets if an ordinary KV write failed.
        await oldRepo.destroy();
      }else if(eraseError)throw eraseError;
    }finally{
      await eraseSecrets;
      // Changing me/token unmounts useAccountData, whose cleanup closes this repo.
      // Finish all repository reads/destruction before triggering that effect.
      try{await Promise.all([oldRepo?.close(),disconnectPush]);}
      finally{setToken(null);setMe(null);}
    }
  }

  async function persistSession(nextBaseUrl: string, nextToken: string, user?: User,register=true) {
    resetUnauthorizedGuard();
    activateNativeSession(nextToken);
    setBaseUrl(nextBaseUrl);
    const profile=user||await api.me(nextBaseUrl,nextToken);
    if(register){const account={id:profile.id,baseUrl:nextBaseUrl,userId:profile.id,username:profile.username};await accountRegistry.save(account,nextToken);await accountRegistry.activate(account.id,{close:async()=>{},open:async()=>{}});}
    await removeSessionTokens();
    if (user) {
      setMe(user);
      await storage.set("mobile.user", JSON.stringify(user)).catch(()=>{});
    }
    try {
      await bootstrap(nextBaseUrl, nextToken,false);
    } catch (e) {
      // The account and session already exist. Keep the signed-in state and allow refresh.
      setError(uiText("Вход выполнен. Не удалось обновить данные: {0}",userError(e)));
    }
  }

  async function submitAuth() {
    if (authBusy) return;
    setAuthBusy(true);
    setError(null);
    const nextBaseUrl = normalizeBaseUrl(DEFAULT_BASE_URL);
    try {
      let deviceKey=await SecureStore.getItemAsync('67chat.login-device');if(!deviceKey){deviceKey=newId()+newId();await SecureStore.setItemAsync('67chat.login-device',deviceKey);}
      const result =
        authMode === "register"
          ? await api.mobileRegister(nextBaseUrl, username.trim(), password)
          : await api.mobileLogin(nextBaseUrl, username.trim(), password,deviceKey);
      if('challenge' in result){setPassword('');setAuthChallenge(result.challenge);return;}
      if (!result.token) throw new Error('Сервер не подтвердил вход');
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
    const refreshingToken=token;
    if (!refreshingToken || tokenRef.current!==refreshingToken) return;
    setRefreshingRooms(true);
    setError(null);
    try {
      const [roomList, user] = await Promise.all([
        api.rooms(baseUrl, token),
        api.me(baseUrl, token),
      ]);
      if(tokenRef.current!==refreshingToken)return;
      const prepared=await prepareRooms(roomList, baseUrl, refreshingToken);
      if(tokenRef.current!==refreshingToken)return;
      setRooms(prepared);
      setMe(user);
    } catch (e) {
      if(tokenRef.current===refreshingToken)setError(errorText(e));
    } finally {
      if(tokenRef.current===refreshingToken)setRefreshingRooms(false);
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
    // Bootstrap supplies nextToken before React has rendered the new token.
    const activation=sessionGenerationRef.current;
    if(tokenRef.current!==nextToken||sessionGenerationRef.current!==activation)throw Error('Сеанс изменился');
    const cached = roomSecretCache.current.get(roomId);
    if (cached) return cached;
    const data = await api.roomKey(nextBaseUrl, nextToken, roomId);
    if(tokenRef.current!==nextToken||sessionGenerationRef.current!==activation)throw Error('Сеанс изменился');
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
    if(message.type!=='text' && message.meta?.media_encryption && !message._mediaDetails) {
      try {
        const info=message.meta.media_encryption as {key_message:Partial<Message>};
        const keyMessage=await prepareMessage({...info.key_message,id:message.id+'-file-key',type:'text',room_id:message.room_id,sender_id:message.sender_id} as Message,nextBaseUrl,nextToken);
        const details=mediaDetails(JSON.parse(keyMessage.text || '{}').details);
        message={...message,_mediaDetails:details,width:details.width || message.width,height:details.height || message.height,duration_sec:details.duration_ms?details.duration_ms/1000:message.duration_sec};
      }catch{/* Older messages or unavailable keys keep their original metadata. */}
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
            text: uiText("Не удалось расшифровать сообщение. Откройте веб-версию."),
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
          text: uiText("Не удалось расшифровать сообщение. Откройте веб-версию."),
        };
      }
    }
    if (message.encrypted_content && message.sender_id !== me?.id) {
      try {
        const engine=signalSession.current?.token===nextToken?signalSession.current.engine:null;if(!engine)throw Error('Ключи устройства ещё загружаются');
        const decrypted = await engine.decrypt(
          message.sender_id,
          JSON.parse(message.encrypted_content),
        );
        return { ...message, text: decrypted, _decrypted: true };
      } catch {
        return {
          ...message,
          text: uiText("Не удалось расшифровать сообщение. Откройте веб-версию."),
        };
      }
    }
    if (message.encrypted_content && !message._decrypted)
      return {
        ...message,
        text: uiText("Сообщение со сквозным шифрованием. Откройте веб-версию."),
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
    waveform?:number[],
  ) {
    if (!token || !activeRoomId) return;
    const roomId=activeRoomId;
    const sendingToken=token;
    try {
      setBusyUpload(true);
      const repository=local.repository.current;
      if(!repository || repository.userId!==me?.id)throw Error('Хранилище ещё загружается');
      const directory=`${FileSystem.documentDirectory}draft-media/${repository.userId}/`;
      await FileSystem.makeDirectoryAsync(directory,{intermediates:true});
      const durable=directory+newId()+'-'+file.name.replace(/[^a-zA-Z0-9._-]/g,'_');
      await FileSystem.copyAsync({from:file.uri,to:durable});
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
          __upload: {uri:durable,name:file.name,mime:file.type,details:mediaDetails({duration_ms:duration,waveform})},
          text: "",
          meta: {...(duration ? {duration_ms:duration} : {})},
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

  async function logout(destroyLocalData=false) {
    const previousToken = token;
    await clearSession(false,destroyLocalData);
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
  const callResumeRef=useRef<string|null>(null);
  const waitingCallRef=useRef<string|null>(null);
  const callAudioQueue=useRef<Promise<void>>(Promise.resolve());
  useEffect(()=>{let networkType:string|undefined;const sub=addNetworkStateListener(state=>{const type=String(state.type||'');if(state.isConnected&&networkType&&networkType!==type)void activeCallRef.current?.restartIce().catch(e=>setError(userError(e)));networkType=type;});return()=>sub.remove();},[]);
  useEffect(()=>{if(callState==='idle')return;const timer=setInterval(()=>setCallDuration(callStartedAt.current?Math.floor((Date.now()-callStartedAt.current)/1000):0),1000);return()=>clearInterval(timer);},[callState]);
  useEffect(()=>{if(!activeCall || callState==='idle' || (connection==='connected'&&!['failed','disconnected'].includes(callNetwork)))return;const timer=setTimeout(()=>{setError(uiText("Звонок завершён: соединение не восстановилось"));endCall();},30000);return()=>clearTimeout(timer);},[activeCall,callState,connection,callNetwork]);
  useEffect(() => {
    if (!token || !me || lockEnabled&&!unlocked) return;
    let live=true,busy=false;
    const current=()=>live&&tokenRef.current===token;
    const check=async()=>{if(busy)return;busy=true;try {
      const saved=await nativeCalls.pending();if(!saved||!current())return;
      const pending=pendingNativeCall.current?.call_id===saved.call_id?pendingNativeCall.current:saved;
      if(!/^[a-f0-9-]{36}$/i.test(pending.call_id)||!/^[a-f0-9-]{36}$/i.test(pending.recipient_id||''))throw Error('call_recipient_required');
      if(Number(pending.expires_at)<=Date.now()){pendingNativeCall.current=null;await nativeCalls.clear(pending.call_id);return;}
      if(pending.recipient_id!==me.id){
        if(callAccountPrompt.current===pending.call_id)return;
        const target=(await accountRegistry.list()).find(row=>row.userId===pending.recipient_id&&normalizeBaseUrl(row.baseUrl)===normalizeBaseUrl(baseUrl));
        if(!current())return;callAccountPrompt.current=pending.call_id;
        if(!target){Alert.alert(uiText("Другой аккаунт"),uiText("Войдите в аккаунт, которому адресовано уведомление."));return;}
        Alert.alert(uiText("Переключить аккаунт?"),uiText("Уведомление адресовано @{0}. {1}",target.username,uiText("Открыть его аккаунт?")),[
          {text:uiText("Отмена"),style:'cancel'},
          {text:uiText("Переключить"),onPress:()=>{if(!current())return;pendingNativeCall.current={...pending};void accountRegistry.activate(target.id,{close:()=>clearSession(true),open:async(account,savedToken)=>{const user=await api.me(account.baseUrl,savedToken);if(user.id!==pending.recipient_id||user.id!==account.userId)throw Error('call_recipient_mismatch');await persistSession(account.baseUrl,savedToken,user,false);}}).catch(error=>{pendingNativeCall.current=null;callAccountPrompt.current=null;setError(userError(error));});}},
        ]);return;
      }
      const data=await request<any>(baseUrl,`/calls/${pending.call_id}`,{token});if(!current())return;
      if(data.type!=='call.offer'||data.call_id!==pending.call_id)throw Error('call_snapshot_mismatch');
      pendingNativeCall.current=null;callAccountPrompt.current=null;
      await onWsMessageRef.current?.(data);if(current()&&pending.action==='answer')setAutoAnswerId(pending.call_id);
    }catch(error){if(current())setError(uiText("Звонок уже завершён или соединение недоступно"));}finally{busy=false;}};
    void check();const sub=AppState.addEventListener('change',state=>{if(state==='active')void check();});const action=nativeCalls.listen(()=>{callAccountPrompt.current=null;void check();});
    return ()=>{live=false;sub.remove();action?.remove();};
  },[token,me?.id,baseUrl,lockEnabled,unlocked]);
  useEffect(()=>{if((!lockEnabled||unlocked)&&autoAnswerId && incomingCall?.callId===autoAnswerId && connection==='connected'){setAutoAnswerId(null);void answerCall();}},[autoAnswerId,incomingCall,connection,lockEnabled,unlocked]);
  const pendingCallIce = useRef<{ roomId: string; candidate: any }[]>([]);
  const callTimeout = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => { if (callTimeout.current) clearTimeout(callTimeout.current); void activeCallRef.current?.end(); }, []);
  async function handleCallSignal(data:any){
    const matches=()=>data.room_id===callRoomRef.current&&(!data.call_id||!callIdRef.current||data.call_id===callIdRef.current);
    if(data.type==='call.created'){if(matches()&&activeCallRef.current){callIdRef.current=data.call_id;callResumeRef.current=data.resume_token||null;}return;}
    if(data.type==='call.waiting'){
      waitingCallRef.current=data.call_id;const callToken=tokenRef.current;
      const valid=()=>waitingCallRef.current===data.call_id&&tokenRef.current===callToken&&(!data.expires_at||data.expires_at>Date.now());
      Alert.alert(uiText("Второй входящий звонок"),uiText("{0} вызывает вас",data.from_username||uiText("Абонент")),[{text:uiText("Отклонить"),onPress:()=>{if(!valid())return;waitingCallRef.current=null;wsRef.current?.send(JSON.stringify({type:'call.waiting.reject',room_id:data.room_id,call_id:data.call_id}));}},{text:uiText("Завершить и ответить"),onPress:()=>{if(!valid())return;waitingCallRef.current=null;endCall(false);wsRef.current?.send(JSON.stringify({type:'call.waiting.accept',room_id:data.room_id,call_id:data.call_id}));setAutoAnswerId(data.call_id);}}],{cancelable:false});return;
    }
    if(data.type==='call.accepted'){
      if(waitingCallRef.current===data.call_id)waitingCallRef.current=null;
      if(data.call_id&&data.call_id===callIdRef.current&&data.room_id===callRoomRef.current)endCall(false);return;
    }
    if(data.type==='call.answered'){if(matches())callResumeRef.current=data.resume_token||null;return;}
    if(data.type==='call.resumed'||data.type==='call.peer_reconnected'){if(matches())void activeCallRef.current?.restartIce().catch(e=>setError(userError(e)));return;}
    if(data.type==='call.offer'){
      if(data.call_id&&callIdRef.current===data.call_id)return;
      if(activeCallRef.current||callIdRef.current){wsRef.current?.send(JSON.stringify({type:'call.hangup',room_id:data.room_id,call_id:data.call_id,reason:'busy'}));return;}
      void pauseAllMedia();
      ++callGeneration.current;callIdRef.current=data.call_id||null;callRoomRef.current=data.room_id;callResumeRef.current=data.resume_token||null;
      pendingCallIce.current=(data.candidates||[]).slice(0,128).map((candidate:any)=>({roomId:data.room_id,candidate}));
      armCallTimeout();setIncomingCall({callId:data.call_id,roomId:data.room_id,callerId:data.from_user_id,callerName:data.from_username||uiText("Абонент"),isVideo:!!data.is_video,offer:data.offer||data.sdp});return;
    }
    if((data.type==='call.hangup'||data.type==='call.end')&&waitingCallRef.current===data.call_id){waitingCallRef.current=null;return;}
    if(!matches())return;
    if(data.type==='call.hangup'||data.type==='call.end'){if(data.call_id&&!callIdRef.current)return;endCall(false);return;}
    const call=activeCallRef.current,generation=callGeneration.current;
    const current=()=>call!==null&&activeCallRef.current===call&&generation===callGeneration.current;
    try{
      if(data.type==='call.renegotiate'&&call){const answer=await call.handleOffer(data.offer||data.sdp);if(current())wsRef.current?.send(JSON.stringify({type:'call.renegotiate.answer',room_id:data.room_id,call_id:data.call_id,answer}));return;}
      if((data.type==='call.answer'||data.type==='call.renegotiate.answer')&&call){await call.handleAnswer(data.answer||data.sdp);return;}
      if(data.type==='call.ice'||data.type==='call.ice_candidate'){if(call)await call.addIceCandidate(data.candidate||data.ice);else{pendingCallIce.current.push({roomId:data.room_id,candidate:data.candidate||data.ice});if(pendingCallIce.current.length>128)pendingCallIce.current.shift();}}
    }catch(error){if(current())setError(userError(error));}
  }
  function armCallTimeout() {
    clearCallTimeout();const generation=callGeneration.current,id=callIdRef.current;
    callTimeout.current=setTimeout(()=>{if(generation!==callGeneration.current||id!==callIdRef.current)return;setError(uiText("Абонент не ответил"));endCall(true,'timeout');},45000);
  }
  function clearCallTimeout(){if(callTimeout.current)clearTimeout(callTimeout.current);callTimeout.current=null;}
  function enqueueCallAudio(task:()=>Promise<void>){const next=callAudioQueue.current.then(task);callAudioQueue.current=next.catch(()=>{});return next;}
  function makeCallEvents(roomId:string,attempt:number,callToken:string):CallEvents {
    const current=()=>attempt===callGeneration.current&&tokenRef.current===callToken;
    return {
      onRenegotiation:offer=>{if(current())wsRef.current?.send(JSON.stringify({type:'call.renegotiate',room_id:roomId,call_id:callIdRef.current,offer}));},
      onScreenShareChange:enabled=>{if(current())setSharingScreen(enabled);},
      onLocalStream:stream=>{if(current())setLocalStream(stream);},
      onRemoteStream:stream=>{if(current())setRemoteStream(stream);},
      onIceCandidate:candidate=>{if(current())wsRef.current?.send(JSON.stringify({type:'call.ice',room_id:roomId,call_id:callIdRef.current,candidate}));},
      onConnectionStateChange:state=>{if(!current())return;setCallNetwork(state);if(state==='connected'){callStartedAt.current ||= Date.now();clearCallTimeout();setCallState('connected');}if(state==='closed')endCall();},
      onError:error=>{if(!current())return;setError(error.message);endCall();},
    };
  }
  async function startCall(isVideo:boolean){
    try{await assertCallsAllowed();}catch(error){setError(error instanceof Error?error.message:uiText("Звонки отключены для защищённой сети"));return;}
    if(!activeRoomId||!token||activeCallRef.current||incomingCall)return;
    if(connection!=='connected'){setError(uiText("Дождитесь подключения для звонка"));return;}
    if(rooms.find(room=>room.id===activeRoomId)?.kind==='group'){setWorkspace('calls:group');return;}
    const roomId=activeRoomId,callToken=token,attempt=++callGeneration.current;
    const current=()=>attempt===callGeneration.current&&tokenRef.current===callToken;
    callRoomRef.current=roomId;callIdRef.current=null;callResumeRef.current=null;pendingCallIce.current=[];setIsAudioEnabled(true);setCallState('calling');
    let call:WebRTCCall|null=null;
    try{
      const ice=await api.iceServers(baseUrl,callToken).then(r=>r.iceServers).catch(()=>undefined);if(!current())return;
      call=new WebRTCCall({audio:true,video:isVideo},makeCallEvents(roomId,attempt,callToken),true,ice);
      activeCallRef.current=call;setActiveCall(call);
      await enqueueCallAudio(async()=>{if(current())await beginAudioCall();});if(!current()){await call.end();return;}
      await call.start();if(!current()){await call.end();return;}
      await enqueueCallAudio(async()=>{if(!current())return;await nativeCalls.start(isVideo);if(!current())return;await nativeCalls.speaker(isVideo);});if(!current()){await call.end();return;}
      setSpeakerEnabled(isVideo);
      for(const item of pendingCallIce.current.splice(0)){if(!current()){await call.end();return;}if(item.roomId===roomId)await call.addIceCandidate(item.candidate);}
      if(!current()){await call.end();return;}
      armCallTimeout();setIsVideoEnabled(isVideo);
      const offer=call.getLocalDescription();wsRef.current?.send(JSON.stringify({type:'call.offer',room_id:roomId,offer,sdp:offer,is_video:isVideo}));
    }catch(error){if(!current()){await call?.end();return;}console.error('Failed to start call:',error);setError(uiText("Не удалось начать звонок. Проверьте разрешения микрофона и камеры."));endCall();}
  }
  async function answerCall(){
    try{await assertCallsAllowed();}catch(error){setError(error instanceof Error?error.message:uiText("Звонки отключены для защищённой сети"));return;}
    if(!incomingCall||!token||activeCallRef.current)return;
    if(connection!=='connected'){setError(uiText("Подключаемся к звонку. Нажмите ответить после восстановления соединения."));return;}
    const incoming=incomingCall,roomId=incoming.roomId,callToken=token,attempt=++callGeneration.current;
    const current=()=>attempt===callGeneration.current&&tokenRef.current===callToken;
    callRoomRef.current=roomId;callIdRef.current=incoming.callId||null;setIsAudioEnabled(true);clearCallTimeout();
    let call:WebRTCCall|null=null;
    try{
      const ice=await api.iceServers(baseUrl,callToken).then(r=>r.iceServers).catch(()=>undefined);if(!current())return;
      call=new WebRTCCall({audio:true,video:incoming.isVideo},makeCallEvents(roomId,attempt,callToken),false,ice);activeCallRef.current=call;setActiveCall(call);
      await enqueueCallAudio(async()=>{if(current())await beginAudioCall();});if(!current()){await call.end();return;}
      await call.start();if(!current()){await call.end();return;}
      await enqueueCallAudio(async()=>{if(!current())return;await nativeCalls.start(incoming.isVideo);if(!current())return;await nativeCalls.speaker(incoming.isVideo);});if(!current()){await call.end();return;}
      setSpeakerEnabled(incoming.isVideo);
      if(incoming.callId)await nativeCalls.clear(incoming.callId);if(!current()){await call.end();return;}
      const answer=await call.handleOffer(incoming.offer);if(!current()){await call.end();return;}
      for(const item of pendingCallIce.current.splice(0)){if(!current()){await call.end();return;}if(item.roomId===roomId)await call.addIceCandidate(item.candidate);}
      if(!current()){await call.end();return;}
      setCallState(state=>state==='connected'?state:'calling');armCallTimeout();setIsVideoEnabled(incoming.isVideo);setIncomingCall(null);
      void mobileApi(baseUrl,callToken).room(roomId).then(room=>{if(current())return openRoom(room);}).catch(()=>{});
      wsRef.current?.send(JSON.stringify({type:'call.answer',room_id:roomId,call_id:incoming.callId,answer,sdp:answer}));
    }catch(error){if(!current()){await call?.end();return;}console.error('Failed to answer call:',error);setError(uiText("Не удалось ответить на звонок. Проверьте разрешения микрофона и камеры."));endCall(true,'rejected');}
  }
  function rejectCall(){if(incomingCall)endCall(true,'rejected');}
  function endCall(signal=true,reason='ended'){
    ++callGeneration.current;callStartedAt.current=null;setCallDuration(0);setCallNetwork('');setSharingScreen(false);
    const call=activeCallRef.current,roomId=callRoomRef.current,id=callIdRef.current;
    activeCallRef.current=null;callRoomRef.current=null;callIdRef.current=null;callResumeRef.current=null;pendingCallIce.current=[];clearCallTimeout();
    if(id)void nativeCalls.clear(id);if(call)void call.end();
    void enqueueCallAudio(async()=>{try{await nativeCalls.stop();}finally{await endAudioCall();}}).catch(()=>{});
    if(signal&&roomId&&wsRef.current?.readyState===WebSocket.OPEN)wsRef.current.send(JSON.stringify({type:'call.hangup',room_id:roomId,call_id:id,reason}));
    setActiveCall(null);setLocalStream(null);setRemoteStream(null);setCallState('idle');setIncomingCall(null);
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
        {qrLogin?<QrLogin baseUrl={DEFAULT_BASE_URL} onCancel={()=>setQrLogin(false)} onSuccess={async session=>{setQrLogin(false);await persistSession(DEFAULT_BASE_URL,session.token!,session.user);setUsername('');setPassword('');}}/>:authChallenge?<AuthChallenge baseUrl={DEFAULT_BASE_URL} challenge={authChallenge} onCancel={()=>setAuthChallenge(null)} onSuccess={async session=>{setAuthChallenge(null);await persistSession(DEFAULT_BASE_URL,session.token!,session.user);setUsername('');setPassword('');}}/>:<AuthScreen
          authMode={authMode}
          username={username}
          password={password}
          error={error}
          busy={authBusy}
          onChangeMode={setAuthMode}
          onChangeUsername={setUsername}
          onChangePassword={setPassword}
          onSubmit={submitAuth}
          onQR={()=>{setPassword('');setQrLogin(true);}}
        />}
      </>
    );
  }

  async function sendCatalogAsset(roomId:string,item:{media_id:string;kind:'sticker'|'gif';label?:string;mime?:string}){
    const sendingToken=token,repository=local.repository.current;
    if(!repository||!sendingToken||tokenRef.current!==sendingToken)throw Error('Сеанс изменился');
    if(!/^[a-f0-9-]{36}$/i.test(item.media_id)||!['sticker','gif'].includes(item.kind))throw Error('Некорректное вложение');
    const mime=item.mime||'image/webp',extension=mime==='image/gif'?'gif':mime.startsWith('video/')?'mp4':mime==='image/png'?'png':'webp';
    if(!/^(image\/(png|jpeg|webp|gif)|video\/(mp4|webm))$/.test(mime))throw Error('Формат вложения не поддерживается');
    const directory=`${FileSystem.documentDirectory}draft-media/${repository.userId}/`,uri=directory+newId()+`-catalog.${extension}`;
    let queued=false;
    try{
      await FileSystem.makeDirectoryAsync(directory,{intermediates:true});
      const result=await FileSystem.downloadAsync(mediaUrl(baseUrl,item.media_id)!,uri,{headers:{Authorization:`Bearer ${sendingToken}`}});
      if(result.status!==200)throw Error('Не удалось загрузить вложение набора');
      const info=await FileSystem.getInfoAsync(uri);if(!info.exists||info.isDirectory||info.size>20*1024*1024)throw Error('Вложение слишком большое или недоступно');
      if(tokenRef.current!==sendingToken||local.repository.current!==repository)throw Error('Сеанс изменился');
      await local.send('',{type:'media',__upload:{uri,name:`attachment.${extension}`,mime,details:{expressive:{kind:item.kind,label:item.label||''}}}},roomId,null);queued=true;
    }finally{if(!queued)await FileSystem.deleteAsync(uri,{idempotent:true}).catch(()=>{});}
  }
  async function exportMedia(message:Message){
    if(!me||!token)throw Error('Сначала войдите');
    const account=me.id,sendingToken=token;
    const estimate=Number((message.meta?.media_encryption as {size?:number}|undefined)?.size||message.media_size||0);
    if(estimate>20*1024*1024)throw Error('Вложение больше лимита экспорта 20 МБ');
    const source=await resolveMediaSource(message,baseUrl,account,{Authorization:`Bearer ${sendingToken}`},prepareMessage);
    let uri=source.uri,temporary='';
    try{
      if(tokenRef.current!==sendingToken)throw Error('Сеанс изменился');
      if(!uri.startsWith('file:')){temporary=`${FileSystem.cacheDirectory}export-${account}-${newId()}`;const result=await FileSystem.downloadAsync(uri,temporary,{headers:{Authorization:`Bearer ${sendingToken}`}});if(result.status!==200)throw Error('Не удалось загрузить вложение');uri=result.uri;}
      const info=await FileSystem.getInfoAsync(uri);if(!info.exists||info.isDirectory||info.size>20*1024*1024)throw Error('Вложение больше лимита экспорта 20 МБ');
      const base64=await FileSystem.readAsStringAsync(uri,{encoding:FileSystem.EncodingType.Base64});
      if(tokenRef.current!==sendingToken)throw Error('Сеанс изменился');return {name:source.name,mime:source.mime,base64};
    }finally{if(temporary)await FileSystem.deleteAsync(temporary,{idempotent:true}).catch(()=>{});}
  }
  const featureRepository=local.repository.current,featureScope=createSessionScope({account:me.id,generation:sessionGeneration,currentAccount:()=>tokenRef.current===token&&local.repository.current===featureRepository?me.id:null,currentGeneration:()=>sessionGenerationRef.current});
  const featureContext:FeatureContext={
    baseUrl,token,me,room:activeRoom,rooms,repository:local.repository.current,
    decode:featureScope.wrap(prepareMessage),buildText:featureScope.wrap(encodeText),targetMessage:featureTarget,exportMedia:featureScope.wrap(exportMedia),
    openContact:featureScope.wrap(async(username:string)=>{if(!/^[A-Za-z0-9_]{3,32}$/.test(username))throw Error('Некорректное имя пользователя');setWorkspace(null);setPendingLink({type:'profile',username});}),
    onOpen:featureScope.wrap(async(roomId,messageId)=>{const room=rooms.find(r=>r.id===roomId)||await mobileApi(baseUrl,token).room(roomId);featureScope.guard();await openRoom(room);featureScope.guard();if(messageId){await local.jump(messageId);featureScope.guard();setFocusId(messageId);}}),
    onSend:featureScope.wrap(async(roomId,text,body)=>{await local.send(text,body,roomId,null);}),sendCatalogAsset:featureScope.wrap(sendCatalogAsset),
    onRefresh:featureScope.wrap(refreshRooms),onClose:()=>{try{featureScope.guard();}catch{return;}setWorkspace(null);},
    onMode:featureScope.sync(mode=>setWorkspace(mode.includes(':')||mode.startsWith('account-')?mode:'conversation:'+mode)),
    setDraft:featureScope.wrap(async(roomId,text)=>{if(!featureRepository)throw Error('Хранилище ещё загружается');const prior=await featureRepository.get<{reply?:Message|null}>('draft:'+roomId,{});featureScope.guard();await featureRepository.set('draft:'+roomId,{...prior,text});featureScope.guard();if(activeRoomIdRef.current===roomId)setComposer(text);}),
  };
  const nativeWorkspace =
    !!workspace &&
    workspace !== "attachments" && workspace !== "circle" &&
    !workspace.startsWith("room:");
  const renderWorkspace = () => (
    <Workspace
      featureContext={featureContext}
      lockEnabled={lockEnabled}
      onChangeLock={async()=>{
        const lock=await describeLock();
        if(!lock.available)throw Error('Сначала настройте код блокировки или биометрию в Android');
        if(!await unlockApp(uiText("Подтвердите изменение блокировки 67chat")))throw Error('Изменение отменено');
        await setLockPreference(!lockEnabled);setLockEnabled(!lockEnabled);setUnlocked(true);
      }}
      onCheckUpdates={appUpdate.recheck}
      updateLabel={`${currentVersionName()} · ${{checking:uiText("Проверка…"),'up-to-date':uiText("Установлена последняя версия"),unavailable:uiText("Не удалось проверить. Повторите при подключении к сети"),available:uiText("Доступно обновление"),unsupported:uiText("Обновление недоступно для этой сборки")}[appUpdate.checkStatus] || uiText("Проверка при запуске")}`}
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
        await logout(true);
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
      forwardOperation={forwardOperation.current}
      enqueueForward={async (room, message,id) => {
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
          const key=await prepareMessage({...protectedFile.key_message,id:message.id+'-file-key',room_id:message.room_id,sender_id:message.sender_id,type:'text'} as Message);
          if(!key._decrypted)throw Error('Не удалось получить ключ вложения');
          body.meta={...(body.meta as any),caption_message:undefined,media_encryption:{...protectedFile,key_message:await encodeText(room.id,key.text || '')}};
        }
        if(tokenRef.current!==sendingToken)throw Error('Сеанс изменился');
        await local.send(message.text || "", {...body,__queue_id:id||newId()}, room.id, null);
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
    <FeatureProvider.Provider value={{context:featureContext,events:messages}}><MediaDecoder.Provider value={{baseUrl,userId:me.id,decode:prepareMessage,onListened:message=>{
      if(message.id.startsWith('local-') || message.listened_by?.includes(me.id))return;
      void local.markListened(message).catch(()=>{});
    }}}><MediaSession.Provider value={{ Authorization: `Bearer ${token}` }}>
      <SafeAreaView style={styles.screen}>
        <StatusBar style={theme.dark ? "light" : "dark"} />
        <ConnectionBanner
          status={connection==='connected' && local.syncStatus==='syncing'?'syncing':connection}
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
                drafts={draftPreviews}
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
                  onPins={()=>{setFeatureTarget(null);setWorkspace('conversation:pins');}}
                  onUnpin={activeRoom.pinned_message_id&&(activeRoom.kind==='dm' || ['owner','admin'].includes(activeRoom.my_role || '')) ? ()=>{void request(baseUrl,`/rooms/${activeRoom.id}/pins/${activeRoom.pinned_message_id}`,{token,method:'DELETE'}).then(()=>refreshRooms()).catch(e=>setError(userError(e)));}:undefined}
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
                  onVoiceRecorded={async (uri, duration, waveform) => {
                    await sendPickedUpload("voice", {
                      uri,
                      name: "voice.m4a",
                      type: "audio/mp4",
                    }, duration, waveform);
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
                        .catch((e) => setError(userError(e)));
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
                        setError(uiText("Пользователь не найден"));
                      })
                      .catch((e) => setError(userError(e)));
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
                      .catch((e) => setError(userError(e)));
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
                      .catch((e) => setError(userError(e)));
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
            await request(baseUrl,`/rooms/${message.room_id}/pins/${message.id}`,{token,method:'PUT',body:{}});
            if(tokenRef.current===token)await refreshRooms();
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
            forwardOperation.current=newId();
            setForwarding(items);
            setWorkspace("forward");
          }}
          onHistory={message=>{setFeatureTarget(message);setWorkspace('conversation:history');}}
          onComments={message=>{setFeatureTarget(message);setWorkspace('conversation:comments');}}
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
                <Text style={styles.callModalTitle}>{uiText("Входящий вызов")}</Text>
                <Text style={styles.callModalName}>
                  {incomingCall.callerName}
                </Text>
                <Text style={styles.callModalType}>
                  {incomingCall.isVideo ? uiText("Видеовызов") : uiText("Аудиовызов")}
                </Text>
                <View style={styles.callModalButtons}>
                  <Pressable
                    style={[styles.callButton, styles.callButtonAccept]}
                    onPress={answerCall}
                  >
                    <Text style={styles.callButtonText}>{uiText("✓ Ответить")}</Text>
                  </Pressable>
                  <Pressable
                    style={[styles.callButton, styles.callButtonReject]}
                    onPress={rejectCall}
                  >
                    <Text style={styles.callButtonText}>{uiText("✕ Отклонить")}</Text>
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
                    ? uiText("Звоним...")
                    : callState === "ringing"
                      ? uiText("Звонит...")
                      : uiText("Подключено")}
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
                  <Pressable style={styles.callControlButton} accessibilityLabel={speakerEnabled ? uiText("Выключить громкую связь") : uiText("Громкая связь")} onPress={()=>{const value=!speakerEnabled;setSpeakerEnabled(value);void nativeCalls.speaker(value)?.catch(()=>setError(uiText("Не удалось переключить звук")));}}>
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
        <ShareInConfirm key={me.id} context={featureContext} onShareFile={async(roomId,uri,mime,queueId)=>{
          const repo=local.repository.current;
          const check=()=>{if(!repo||local.repository.current!==repo||repo.userId!==me.id||tokenRef.current!==token)throw Error('Сеанс изменился');};
          check();if(!/^[A-Za-z0-9_-]{8,100}$/.test(queueId))throw Error('Некорректный идентификатор входящей отправки');
          const directory=`${FileSystem.documentDirectory}draft-media/${repo!.userId}/`,target=directory+queueId+'.shared',temporary=target+'.importing';
          await FileSystem.makeDirectoryAsync(directory,{intermediates:true});check();
          const source=await FileSystem.getInfoAsync(uri,{md5:true});check();
          if(!source.exists||source.isDirectory||!source.md5)throw Error('Входящий файл недоступен. Выберите его заново.');
          const existing=await FileSystem.getInfoAsync(target,{md5:true});check();
          if(existing.exists){if(existing.isDirectory||existing.md5!==source.md5)throw Error('Сохранённый файл не совпадает с входящим. Отмените отправку и выберите файл заново.');}
          else try{
            await FileSystem.deleteAsync(temporary,{idempotent:true});check();await FileSystem.copyAsync({from:uri,to:temporary});check();
            const copied=await FileSystem.getInfoAsync(temporary,{md5:true});check();
            if(!copied.exists||copied.isDirectory||copied.md5!==source.md5)throw Error('Не удалось полностью скопировать входящий файл');
            await FileSystem.moveAsync({from:temporary,to:target});check();
          }catch(error){await FileSystem.deleteAsync(temporary,{idempotent:true}).catch(()=>{});throw error;}
          check();await local.send('',{type:'media',__queue_id:queueId,__upload:{uri:target,name:uiText("Вложение"),mime}},roomId,null);
        }}/>
      </SafeAreaView>
    </MediaSession.Provider></MediaDecoder.Provider></FeatureProvider.Provider>
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
          <Text style={styles.viewerCloseText}>{uiText("Закрыть")}</Text>
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
                {uiText("Воспроизвести")}{message.duration_sec
                  ? uiText(" ({0}с)",Math.round(message.duration_sec))
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
                ).catch((e) => Alert.alert(uiText("Файл"), String(e.message || e)));
              }}
            >
              <Text style={styles.viewerFileTitle}>
                {message.text || uiText("Открыть файл")}
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
