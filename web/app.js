import {locale as uiLocale} from "./i18n.js";
import {html as escapeUi} from './ui.js';
import {t as uiText} from "./i18n.js";
import {t,locale,setLanguage,subscribeLanguage} from './i18n.js';
import {enhancePeerCall} from './call-features.js';
import {openConversationFeature,wrapEncryptedBody,unwrapEncryptedBody,formatConversationText} from './conversation-features.js';
import {DraftSync,draftValue} from './draft-sync.js';
import {parse67Link,messageLink} from './deep-links.js';
import {openExpressiveFeature,renderExpressiveMessage,stopExpressiveLiveLocations} from './expressive-features.js';
import {parseMessagePayload,isExpressiveControl,expressiveSummary} from './expressive-payloads.js';
import {fetchAllRoomMessages} from './expressive-history.js';
import {expressiveHistoryLoader as expressiveEventHistory,expressiveHistoryKey} from './expressive-event-history.js';
import {openAccountFeature,createWebDeviceProof,decryptBackup,encryptBackup} from './account-features.js';
import {completeWebAccountChallenge,loginWithWebQr,webLoginDevice} from './account-auth.js';
import {stageWebBackup,restoreWebBackup,recoverWebBackup,exportWebAccountKeys} from './account-restore.js';
import {openQualityFeatures} from './quality-features.js';
import {shouldAutoDownload} from './quality-policy.js';
import { createFolderManager } from './folders.js';
import { createChatActions } from './chat-actions.js';
import { createMediaController } from './media.js';
import { createChatRuntime } from './chat-runtime.js';
import { mergeMessages, createAccountStore, eraseAccountStore } from './local-data.js';
import { reconcileMessages } from './message-list.js';
import { createAuthFlow } from './auth-flow.js';
import { userError, toast } from './errors.js';
import { icon, avatar, groupWith, presenceLabel, reactionPeople, plural } from './ui.js';
import { renderMessengerShell, renderLogin, renderRoomRow, iconButton } from './views.js';
import { createWebNotifications } from './notifications.js';
import { diagnoseConnection } from './diagnostics.js';
import {mediaDetails} from './media-details.js';
import {renderVoicePlayer,bindVoicePlayers,stopVoicePlayers} from './voice-player.js';
import {groupMessageAlbums} from './message-albums.js';
import {createMediaObjectCache} from './media-object-cache.js';
import {selectedBrowserAccount,selectBrowserAccount,browserAccountHeaders,browserSessionGeneration,invalidateBrowserSession,accountMediaUrl,notificationTarget,initialBrowserAccount} from './browser-accounts.js';
import {createSessionScope} from './session-scope.js';
import {eraseSecretVault,pruneSecretVault} from './secret-vault.js';

window.checkConnection = async () => {
  const dialog=document.createElement('dialog');dialog.className='actions-dialog';
  const heading=document.createElement('h2');heading.textContent=uiText("Проверка соединения");const body=document.createElement('div');body.textContent=uiText("Проверяем API, WebSocket и маршрут звонка…");
  const close=document.createElement('button');close.textContent=uiText("Закрыть");close.onclick=()=>{dialog.close();dialog.remove();};dialog.append(heading,body,close);document.body.append(dialog);dialog.showModal();
  const account=state.me?.id;
  const results=await diagnoseConnection({request:api,wsUrl:location.origin.replace(/^http/,'ws')+'/ws'});
  if(!dialog.open || state.me?.id!==account)return;body.replaceChildren();
  for(const result of results){const line=document.createElement('p');line.textContent=`${result.title}: ${result.ok?uiText("Работает"):uiText("Нужна проверка")}. ${result.action}`;body.append(line);}
};
const $ = (q, el=document) => el.querySelector(q);
const $$ = (q, el=document) => Array.from(el.querySelectorAll(q));
const API = window.__APP_CONFIG__?.apiBaseUrl || "/api";

let state = {
  me: null,
  rooms: [],
  activeRoomId: null,
  messages: [],
  ws: null,
  wsAuthed: false,
  wsReconnectAttempts: 0,
  wsReconnectTimer: null,
  wsAuthTimer: null,
  e2eManager: null,
  e2eReady: false,
  pc: null,
  localStream: null,
  recorder: null,
  roomMembers: [],
  searchQ: "",
  searchResults: [],
  searching: false,
  typingByRoom: {},
  viewProfile: null,
  editingRoom: false,
  recordingVideoNote: false,
  adminPanel: false,
  adminUsers: [],
  themeMode: localStorage.getItem("themeMode") || localStorage.getItem("theme") || "light",
  darkTheme: localStorage.getItem("theme") === "dark",
  fontSize: Number(localStorage.getItem("messageFontSize")) || 15,
  autoDownload: localStorage.getItem("autoDownload") === "yes",
  archiveOpen: false,
  selectedMessages: new Set(),
  composeOpen: false,
  settingsOpen: false,
  uiMenu: null,
  peopleResults: [],
  roomLoading: false,
  roomLoadError: false,
  backupPanel: false,
  recoveryPhrase: null,
  showRecoveryPhrase: false,
  autoBackupInterval: null,
  sessionsPanel: false,
  sessions: [],
  safetyNumberPanel: false,
  safetyNumberData: null,
  notificationsEnabled: false,
  notificationsPermission: 'default',
  serviceWorkerRegistration: null,
  searchMode: false,
  searchQuery: '',
  searchResults: [],
  currentSearchIndex: 0,
  replyTo: null,
  editMessage: null,
  composerText: "",
  createRoomMode: null, // 'group' | 'channel' | null
  createRoomQuery: "",
  createRoomResults: [],
  createRoomSelected: [],
  createRoomTitle: "",
  callGeneration:0,currentCallId:null,callResumeToken:null,callInitiator:false,callRecovery:null,callOfferTimer:null,sharingScreen:false,callWaitingId:null,autoAnswerCallId:null,
  callState: 'idle', // 'idle', 'calling', 'connected', 'incoming'
  callTimer: null,
  callStartTime: null,
  callDisconnectTimer: null,
  pendingIceCandidates: [],
  currentCallRoomId: null,
  currentCallPeerId: null,
  activeDmPeer: null,
  isVideoCall: false, // Track if current call is video
  isVideoEnabled: true, // Track if video is enabled
  isAudioEnabled: true, // Track if audio is enabled
  audioOutputMode: 'earpiece' // 'earpiece' or 'speaker'
};

// Touch handling for swipe-to-reply
let touchStartX = 0;
let touchStartY = 0;
let touchMsgId = null;
let messagePressTimer = null;
let didLongPress = false;
window.cancelMessagePress = () => { clearTimeout(messagePressTimer); messagePressTimer = null; };

window.handleTouchStart = (e, msgId) => {
  touchStartX = e.touches[0].clientX;
  touchStartY = e.touches[0].clientY;
  touchMsgId = msgId;
  didLongPress = false;
  window.cancelMessagePress();
  const target = e.currentTarget;
  messagePressTimer = setTimeout(() => {
    didLongPress = true;
    window.openMessageMenu(msgId, { target, currentTarget: target, preventDefault() {}, stopPropagation() {} });
  }, 450);
};

window.handleTouchEnd = (e) => {
  window.cancelMessagePress();
  if (didLongPress) { touchMsgId = null; return; }
  if (!touchMsgId) return;
  const touchEndX = e.changedTouches[0].clientX;
  const touchEndY = e.changedTouches[0].clientY;
  const deltaX = touchStartX - touchEndX;
  const deltaY = Math.abs(touchStartY - touchEndY);

  // Swipe left (deltaX > 50) and not vertical swipe
  if (deltaX > 50 && deltaY < 100) {
    replyToMessage(touchMsgId);
  }
  touchMsgId = null;
};

// Reply to message
window.replyToMessage = (msgId, event) => {
  if (event) event.stopPropagation();
  const msg = state.messages.find(m => m.id === msgId);
  if (!msg) return;
  state.editMessage = null;
  state.replyTo = msg;
  mountPreservingInput();
  // Focus input
  setTimeout(() => {
    const inp = $("#inp");
    if (inp) inp.focus();
  }, 100);
};

window.cancelReply = () => {
  state.replyTo = null;
  mountPreservingInput();
};

// Chat-list previews must match what Telegram shows: media kinds are named,
// a draft is announced, and long text is left to CSS ellipsis (not sliced
// here, which used to break emoji surrogate pairs).
const PREVIEW_KINDS = { voice: "Голосовое сообщение", video_note: "Видеокружок", media: "Вложение", call: "Звонок" };
function previewFor(room, me) {
  const mine = room.last_msg_sender_id === me?.id;
  const direct = room.kind === "dm" && !room.is_saved;
  let text = room._lastText || room.last_msg_text || "";
  const encrypted = room.last_msg_encrypted_content || room.last_msg_meta?.encrypted || room.last_msg_meta?.e2e;
  if (!text && encrypted) text = uiText("Зашифрованное сообщение");
  if (!text) text = uiText(PREVIEW_KINDS[room.last_msg_type]||"") || "";
  if (room.draft) return (""+uiText("Черновик:")+" ") + room.draft;
  if (mine && (room._lastText || room.last_msg_text)) text = (""+uiText("Вы:")+" ") + text;
  else if (!direct && room.last_msg_sender_username && room._lastText) text = (room.last_msg_sender_name || room.last_msg_sender_username) + ": " + text;
  return text;
}

window.pinMessage = async (id) => {
  const room=state.activeRoomId;if (!room) return;
  await api(`/rooms/${room}/pins/${id}`, { method: 'PUT', body: '{}' });
  toast(uiText("Сообщение закреплено"));
  await loadRooms();
  await mount();
};

window.unpinMessage = async () => {
  const room=state.rooms.find(room=>room.id===state.activeRoomId),id=room?.pinned_message_id;if (!id) return;
  await api(`/rooms/${room.id}/pins/${id}`, { method: 'DELETE' });
  await loadRooms();
  await mount();
};

window.scrollToMessage = (id, event) => { event?.stopPropagation?.(); void chatRuntime.jump(id).catch(e => toast(userError(e))); };

// Create room (group/channel) with user search
window.openCreateRoom = (mode) => {
  state.createRoomMode = mode;
  state.createRoomQuery = "";
  state.createRoomResults = [];
  state.createRoomSelected = [];
  state.createRoomTitle = "";
  mount();
};

window.closeCreateRoom = () => {
  state.createRoomMode = null;
  mount();
};

let createRoomSearchTimeout = null;
window.onCreateRoomQueryChange = (query) => {
  state.createRoomQuery = query;
  // Don't re-render, just update results
  if (query.length >= 2) {
    clearTimeout(createRoomSearchTimeout);
    createRoomSearchTimeout = setTimeout(async () => {
      const res = await api(`/users/search?q=${encodeURIComponent(query)}`);
      state.createRoomResults = res || [];
      // Filter out already selected users
      const selectedIds = state.createRoomSelected.map(u => u.id);
      state.createRoomResults = state.createRoomResults.filter(u => !selectedIds.includes(u.id));
      // Only re-render the modal content, not full page
      updateCreateRoomResults();
    }, 300);
  } else {
    state.createRoomResults = [];
    updateCreateRoomResults();
  }
};

function updateCreateRoomResults() {
  const resultsEl = document.getElementById('createRoomResults');
  if (!resultsEl) return;

  if (state.createRoomResults.length) {
    resultsEl.innerHTML = state.createRoomResults.map(u => `
      <div class="search-item" onclick='toggleSelectUser(${safeUserJson(u)})'>
        ${u.avatar_id ? `<img class="avatar-sm" src="${avatarUrl(u.avatar_id)}">` : `<div class="avatar-placeholder-sm"></div>`}
        <div class="search-item-text">
          <div>${escapeHtml(displayName(u))}</div>
          <div class="meta">@${escapeHtml(u.username)}</div>
        </div>
        <span class="check">+</span>
      </div>
    `).join('');
    resultsEl.style.display = 'block';
  } else {
    resultsEl.innerHTML = '';
    resultsEl.style.display = 'none';
  }

  // Update selected chips
  const selectedEl = document.getElementById('createRoomSelected');
  if (selectedEl) {
    if (state.createRoomSelected.length) {
      selectedEl.innerHTML = state.createRoomSelected.map(u => `
        <div class="selected-chip" onclick='toggleSelectUser(${safeUserJson(u)})'>
          ${escapeHtml(displayName(u))}
          <span class="remove">✕</span>
        </div>
      `).join('');
      selectedEl.parentElement.style.display = 'block';
    } else {
      selectedEl.parentElement.style.display = 'none';
    }
  }
}

window.toggleSelectUser = (user) => {
  const idx = state.createRoomSelected.findIndex(u => u.id === user.id);
  if (idx >= 0) {
    state.createRoomSelected.splice(idx, 1);
  } else {
    state.createRoomSelected.push(user);
  }
  state.createRoomQuery = "";
  state.createRoomResults = [];
  mount();
};

window.onCreateRoomTitleChange = (title) => {
  state.createRoomTitle = title;
};

window.submitCreateRoom = async () => {
  if (!state.createRoomTitle.trim()) {
    alert(uiText("Введите название"));
    return;
  }
  const usernames = state.createRoomSelected.map(u => u.username);
  const res = await api("/rooms", {
    method: "POST",
    body: JSON.stringify({
      kind: state.createRoomMode,
      title: state.createRoomTitle.trim(),
      members: usernames
    })
  });
  if (res?.id) {
    state.createRoomMode = null;
    await loadRooms();
    state.activeRoomId = res.id;
    mount();
  } else {
    alert(uiText("Ошибка создания"));
  }
};

// Reactions
const COMMON_EMOJIS = ['👍', '❤️', '😂', '😮', '😢', '👏', '🔥', '🎉'];

window.showReactionPicker = (msgId, event) => {
  if (event) event.stopPropagation();

  // Remove existing picker
  const existing = $("#reactionPicker");
  if (existing) existing.remove();

  const picker = document.createElement("div");
  picker.id = "reactionPicker";
  picker.innerHTML = COMMON_EMOJIS.map(emoji =>
    `<button class="reaction-btn" onclick="addReaction('${msgId}', '${emoji}')">${emoji}</button>`
  ).join('');

  // Position near click but keep in viewport
  const rect = event?.target?.getBoundingClientRect();
  if (rect) {
    picker.style.position = 'fixed';
    // Center horizontally on the button, but keep in viewport
    let left = rect.left + rect.width / 2 - 140; // 140 = half of picker width (~280px)
    left = Math.max(10, Math.min(left, window.innerWidth - 290));
    picker.style.left = `${left}px`;
    picker.style.top = `${Math.max(8, Math.min(innerHeight - 65, rect.top - 55))}px`;
    picker.style.zIndex = '1000';
  }

  document.body.appendChild(picker);

  // Close on click outside
  setTimeout(() => {
    document.addEventListener('click', function closePicker(e) {
      if (!picker.contains(e.target)) {
        picker.remove();
        document.removeEventListener('click', closePicker);
      }
    }, { once: true });
  }, 100);
};

window.addReaction = (msgId, emoji) => {
  const picker = $("#reactionPicker");
  if (picker) picker.remove();

  state.ws?.send(JSON.stringify({ type: "reaction.add", message_id: msgId, emoji }));
};

window.removeReaction = (msgId, emoji, event) => {
  if (event) event.stopPropagation();
  state.ws?.send(JSON.stringify({ type: "reaction.remove", message_id: msgId, emoji }));
};

// Theme toggle
window.toggleTheme = () => {
  state.darkTheme = !state.darkTheme;
  state.themeMode = state.darkTheme ? "dark" : "light"; localStorage.setItem("themeMode", state.themeMode);
  localStorage.setItem("theme", state.darkTheme ? "dark" : "light");
  applyTheme();
  mountPreservingInput();
};

function applyTheme() {
  if (state.themeMode === "system") state.darkTheme = matchMedia("(prefers-color-scheme: dark)").matches;
  document.documentElement.style.colorScheme = state.darkTheme ? "dark" : "light";
  document.documentElement.style.setProperty("--message-font", state.fontSize + "px");
  document.body.classList.toggle("message-selection", !!state.selectedMessages?.size);
  document.body.classList.toggle("light-theme", !state.darkTheme);
  document.querySelector('meta[name="theme-color"]')?.setAttribute('content', state.darkTheme ? '#181819' : '#14B88F');
}
let qualityPreference=null,qualityAccount=null;
function applyQualityPreference(p){setLanguage(p.language);localStorage.setItem('ui.language',p.language);qualityPreference=p;qualityAccount=state.me?.id||null;const root=document.body,accent={emerald:'#0FA37F',blue:'#3B82F6',violet:'#8B5CF6'};root.style.setProperty('--accent',accent[p.accent]||accent.emerald);root.style.setProperty('--quality-wallpaper-image',{plain:'none',dots:'radial-gradient(#6b728055 1px,transparent 1px)',mesh:'linear-gradient(45deg,#6b728022 25%,transparent 25%,transparent 75%,#6b728022 75%)'}[p.wallpaper]||'none');root.style.setProperty('--quality-wallpaper-size',p.wallpaper==='dots'?'16px 16px':'32px 32px');void hydrateVisibleMedia();}
async function refreshQualityPreference(){const account=state.me?.id;if(!account)return;const value=await api('/me/quality');if(state.me?.id===account)applyQualityPreference(value);}

// --- Utils ---
async function api(path, opts={}) {
  const accountGeneration=browserSessionGeneration(),requestAccount=opts.headers?.['X-Account-Id']||opts.headers?.['x-account-id']||selectedBrowserAccount();
  const controller = new AbortController(), timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const r = await fetch(API + path, { ...opts, signal: opts.signal || controller.signal,
      headers: { ...browserAccountHeaders(),...(opts.body === undefined || opts.body instanceof FormData ? {} : { "Content-Type":"application/json" }), ...(opts.headers||{}) }, credentials: "include" });
    const body = await r.json().catch(() => null);
    if (r.status === 401 && accountGeneration===browserSessionGeneration() && requestAccount===selectedBrowserAccount() && !isAuthEntryPath(path)) handleUnauthorized();
    if (!r.ok) throw Object.assign(new Error(body?.error || `HTTP ${r.status}`), { code: body?.error, status: r.status });
    return body;
  } finally { clearTimeout(timeout); }
}

// Login/register/logout legitimately answer 401 (bad credentials etc.) and must
// not trigger the global "session expired" handling.
function isAuthEntryPath(path) {
  return /^\/auth\/(login|register|logout|accounts(?:\/[^?]*)?|account-challenge\/complete|account-handoff(?:\/claim)?)(\?|$)/.test(String(path || ""));
}

// Clear all client-side session state and stop everything tied to the session.
// Used by logout, terminate-all-sessions and the forced logout on 401 / WS auth failure.
function resetClientSession() {
  invalidateBrowserSession();if(state.me?.id)localStorage.removeItem('67chat-profile:'+state.me.id);
  void webNotifications.pause().catch(()=>{});
  stopVoicePlayers();
  window.hangupCall?.(true,'cancelled');
  for(const dialog of document.querySelectorAll?.('dialog')||[]){dialog.close?.();dialog.remove();}
  if (state.autoBackupInterval) {
    clearInterval(state.autoBackupInterval);
    state.autoBackupInterval = null;
  }
  webDraftCoordinator?.stop();webDraftCoordinator=null;webDraftOwner=null;for(const timer of webDraftTimers.values())clearTimeout(timer);webDraftTimers.clear();webDraftReplies.clear();webMessageTTLs.clear();draftConflicts.clear();cancelExpressiveLoads();expressiveEventHistory.clear();stopExpressiveLiveLocations(state.me?.id);
  webMedia.stop();
  webMediaUrls.clear();
  chatRuntime.stop();
  localStorage.removeItem("67chat-profile");
  stopPreKeyMonitor();
  e2eSessionInflight.clear();
  e2ePeerDeviceIds.clear();
  roomSecretCache.clear();groupE2eRooms.clear();groupPlainCache.clear();groupInboxInflight.clear();
  e2eInitTask=null;preKeyReplenishInflight=null;
  state.e2eManager = null;
  state.e2eReady = false;

  closeWS();
  clearAllTyping();

  state.me = null;
  qualityPreference=null;qualityAccount=null;document.body.style.removeProperty('--accent');document.body.style.removeProperty('--quality-wallpaper-image');
  webDrafts.clear();
  state.settingsOpen = false;
  state.composeOpen = false;
  state.uiMenu = null;
  state.peopleResults = [];
  state.rooms = [];
  state.activeRoomId = null;
  state.messages = [];
  state.roomMembers = [];
  state.activeDmPeer = null;
  state.sessions = [];
  state.sessionsPanel = false;
  state.backupPanel = false;
  state.adminPanel = false;
  state.viewProfile = null;
  state.editingRoom = false;
  state.safetyNumberPanel = false;
}

// --- Typing indicators (auto-expire if the "stop" event never arrives) ---
const TYPING_TTL_MS = 8000;
const typingExpiry = new Map(); // "roomId:userId" -> timeout id

function setTypingUser(roomId, userId, username) {
  const key = `${roomId}:${userId}`;
  clearTimeout(typingExpiry.get(key));
  typingExpiry.set(key, setTimeout(() => clearTypingUser(roomId, userId, true), TYPING_TTL_MS));
  state.typingByRoom = { ...state.typingByRoom, [roomId]: { ...(state.typingByRoom[roomId] || {}), [userId]: username } };
}

function clearTypingUser(roomId, userId, rerender = false) {
  const key = `${roomId}:${userId}`;
  clearTimeout(typingExpiry.get(key));
  typingExpiry.delete(key);
  const current = state.typingByRoom[roomId];
  if (!current || !(userId in current)) return;
  const next = { ...current };
  delete next[userId];
  state.typingByRoom = { ...state.typingByRoom, [roomId]: next };
  if (rerender && state.me && String(roomId) === String(state.activeRoomId)) render();
}

function clearAllTyping(rerender = false) {
  for (const t of typingExpiry.values()) clearTimeout(t);
  typingExpiry.clear();
  const had = Object.values(state.typingByRoom).some(m => m && Object.keys(m).length);
  state.typingByRoom = {};
  if (rerender && had && state.me) render();
}

// --- WebSocket lifecycle helpers ---
const WS_AUTH_TIMEOUT_MS = 10000;
// Close codes that mean "not allowed" rather than a network drop.
const WS_AUTH_CLOSE_CODES = new Set([1008, 4001, 4003, 4401, 4403]);

// Intentional close: no auto-reconnect.
function closeWS() {
  if (state.wsReconnectTimer) { clearTimeout(state.wsReconnectTimer); state.wsReconnectTimer = null; }
  if (state.wsAuthTimer) { clearTimeout(state.wsAuthTimer); state.wsAuthTimer = null; }
  state.wsReconnectAttempts = 0;
  state.wsAuthed = false;
  const ws = state.ws;
  state.ws = null; // handlers ignore events from a socket that is no longer current
  if (ws) { try { ws.close(); } catch {} }
}

function scheduleWSReconnect() {
  if (!state.me || state.wsReconnectTimer) return;
  // Exponential backoff: 1s, 2s, 4s, 8s ... max 30s
  const delay = Math.min(1000 * Math.pow(2, state.wsReconnectAttempts), 30000);
  state.wsReconnectAttempts++;
  state.wsReconnectTimer = setTimeout(() => {
    state.wsReconnectTimer = null;
    if (state.me) connectWS();
  }, delay);
}

// The server rejected / never confirmed our WS auth. Distinguish an expired
// session (HTTP 401 -> api() performs the forced logout) from a transient
// problem (retry with backoff).
async function recoverFromWSAuthFailure() {
  if (!state.me) return;
  const me = await api("/me");
  if (!state.me) return; // handleUnauthorized() already logged us out
  if (!me?.id) {
    // Not a 401 (api() would have handled it): treat as a transient failure.
    try { state.ws?.close(); } catch {}
    return;
  }
  // Session is valid but WS auth failed: drop the socket and retry with backoff.
  try { state.ws?.close(); } catch {}
}

// Fires at most once per session: after the first 401 state.me is null, so
// concurrent/late 401s (and the login screen's own requests) are no-ops.
// Raw fetch() uploads bypass api(); route their responses through the same 401 handling.
function checkUnauthorized(res) {
  if (res && res.status === 401) handleUnauthorized();
  return res;
}

function handleUnauthorized() {
  if (!state.me) return;
  resetClientSession();
  mount();
}

function avatarUrl(id) {
  return id ? accountMediaUrl(`/api/media/${id}`) : null;
}

function displayName(user) {
  return user?.display_name || user?.username || "User";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function escapeJsSingleQuoted(value) {
  return String(value ?? "")
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "\\'")
    .replace(/\r/g, "\\r")
    .replace(/\n/g, "\\n")
    .replace(/</g, "\\x3C");
}

function safeUserJson(user) {
  return JSON.stringify(user).replaceAll("'", "&#39;").replaceAll("<", "\\u003c");
}

function profilePath(username) {
  return `/profile/${encodeURIComponent(username)}`;
}

function currentProfileUsernameFromRoute() {
  const match = location.pathname.match(/^\/(?:user|profile)\/([A-Za-z0-9_]+)\/?$/);
  return match ? decodeURIComponent(match[1]) : null;
}

function updateProfileRoute(username, replace = false) {
  const target = username ? profilePath(username) : "/";
  if (location.pathname === target) return;
  const method = replace ? "replaceState" : "pushState";
  history[method]({}, "", target);
}

async function syncProfileRoute() {
  const username = currentProfileUsernameFromRoute();
  if (!state.me) return;
  if (!username) {
    if (state.viewProfile && !state.viewProfile.isMe) {
      state.viewProfile = null;
      mount();
    }
    return;
  }
  if (state.viewProfile?.username?.toLowerCase() === username.toLowerCase()) return;
  const user = await api(`/users/by-username/${encodeURIComponent(username)}`);
  if (!user) {
    updateProfileRoute(null, true);
    return;
  }
  state.viewProfile = user;
  mount();
}

function formatMessageTime(value) {
  return new Date(value).toLocaleTimeString(uiLocale(), { hour: "2-digit", minute: "2-digit" });
}

function formatDayLabel(value) {
  const date = new Date(value);
  const now = new Date();
  const sameDay = date.toDateString() === now.toDateString();
  if (sameDay) return uiText("Сегодня");
  const yesterday = new Date();
  yesterday.setDate(now.getDate() - 1);
  if (date.toDateString() === yesterday.toDateString()) return uiText("Вчера");
  return date.toLocaleDateString(uiLocale(), { day: "numeric", month: "long" });
}

let scrollDateHideTimer = null;

function visibleDayLabel(box) {
  const separators = Array.from(box.querySelectorAll(".day-separator[data-day-label]"));
  if (!separators.length) return "";
  const threshold = box.scrollTop + 24;
  let current = separators[0];
  for (const separator of separators) {
    if (separator.offsetTop <= threshold) current = separator;
    else break;
  }
  return current?.dataset?.dayLabel || "";
}

function syncScrollDateIndicator(box, show = false) {
  const indicator = $("#scrollDateIndicator");
  if (!box || !indicator) return;
  const label = visibleDayLabel(box);
  if (!label) {
    indicator.classList.remove("visible");
    return;
  }
  indicator.textContent = label;
  if (!show) return;
  indicator.classList.add("visible");
  if (scrollDateHideTimer) clearTimeout(scrollDateHideTimer);
  scrollDateHideTimer = setTimeout(() => {
    indicator.classList.remove("visible");
  }, 900);
}

function bindMessagesScrollIndicator() {
  const box = $("#msgs");
  if (!box || box.dataset.dateIndicatorBound === "1") return;
  box.dataset.dateIndicatorBound = "1";
  box.addEventListener("scroll", () => syncScrollDateIndicator(box, true), { passive: true });
}

function dayKey(value) {
  const date = new Date(value);
  return `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}`;
}

function renderMentionText(value) {
  const text = String(value ?? "");
  const regex = /(^|[\s(>])@([A-Za-z0-9_]{3,30})/g;
  let html = "";
  let lastIndex = 0;
  let match;
  while ((match = regex.exec(text)) !== null) {
    const full = match[0];
    const prefix = match[1];
    const username = match[2];
    const start = match.index;
    const mentionStart = start + prefix.length;
    html += escapeHtml(text.slice(lastIndex, start)).replace(/\n/g, "<br>");
    html += escapeHtml(prefix).replace(/\n/g, "<br>");
    html += `<a class="mention-link" href="${profilePath(username)}" onclick="openUserProfile('${escapeJsSingleQuoted(username)}', event)">@${escapeHtml(username)}</a>`;
    lastIndex = mentionStart + username.length + 1;
    if (full.endsWith("\n")) lastIndex -= 1;
  }
  html += escapeHtml(text.slice(lastIndex)).replace(/\n/g, "<br>");
  return html;
}

function getLocalTurnConfig() {
  if (location.hostname !== "localhost" && location.hostname !== "127.0.0.1") return null;
  return {
    urls: [
      `turn:${location.hostname}:3478?transport=udp`,
      `turn:${location.hostname}:3478?transport=tcp`
    ],
    username: "messenger",
    credential: "change-me"
  };
}

// Server-issued, time-limited TURN credentials (GET /rtc/ice-servers), cached until near expiry.
let rtcIceCache = null;
async function loadRtcIceServers() {
  if (rtcIceCache && rtcIceCache.expires > Date.now()) return;
  const data = await api("/rtc/ice-servers");
  if (!data || !Array.isArray(data.iceServers) || !data.iceServers.length) return;
  const ttlMs = Math.max(60, Number(data.ttl) || 3600) * 1000;
  rtcIceCache = { servers: data.iceServers, expires: Date.now() + ttlMs * 0.8 };
}

function getRtcIceServers(){return rtcIceCache?.servers||[];}

function getAudioConstraints() {
  return {
    audio: {
      echoCancellation: true,
      noiseSuppression: true,
      autoGainControl: true,
      channelCount: 1
    },
    video: false
  };
}

async function flushPendingIceCandidates(pc=state.pc,generation=state.callGeneration) {
  if (!pc?.remoteDescription || pc!==state.pc || generation!==state.callGeneration) return;
  const pending = state.pendingIceCandidates.splice(0);
  for (const candidate of pending) {
    try {
      if(pc!==state.pc||generation!==state.callGeneration)return;
      await pc.addIceCandidate(new RTCIceCandidate(candidate));
    } catch (e) {
      console.error("Failed to add queued ICE candidate:", e);
    }
  }
}

function updateCallStatus(text, showTimer = false) {
  const statusEl = $("#callStatus");
  const timerEl = $("#callTimer");
  if (statusEl) statusEl.textContent = text;
  if (timerEl) timerEl.style.display = showTimer ? "block" : "none";
}

function startCallTimer() {
  if (state.callTimer) return;
  state.callStartTime = Date.now();
  state.callTimer = setInterval(() => {
    const timerEl = $("#callTimer");
    if (timerEl && state.callStartTime) {
      const seconds = Math.floor((Date.now() - state.callStartTime) / 1000);
      const minutes = Math.floor(seconds / 60);
      const secs = seconds % 60;
      timerEl.textContent = minutes.toString().padStart(2, '0') + ':' + secs.toString().padStart(2, '0');
    }
  }, 1000);
}

function clearCallDisconnectTimer() {
  if (!state.callDisconnectTimer) return;
  clearTimeout(state.callDisconnectTimer);
  state.callDisconnectTimer = null;
}

function wirePeerConnectionHandlers({ roomId, peerUserId }) {
  if (!state.pc) return;
  const pc=state.pc,generation=state.callGeneration,current=()=>state.pc===pc&&state.callGeneration===generation;
  state.callRecovery=enhancePeerCall({peerConnection:pc,localStream:state.localStream,hangup:()=>{if(current())hangupCall();}},{isInitiator:state.callInitiator,sendOffer:offer=>{if(current()&&state.wsAuthed)state.ws?.send(JSON.stringify({type:'call.renegotiate',room_id:roomId,call_id:state.currentCallId,offer}));},onError:error=>{if(current())toast(userError(error));}});
  state.currentCallRoomId = roomId || null;
  state.currentCallPeerId = peerUserId || null;

  pc.onicecandidate = ev => {
    if (!current() || !ev.candidate) return;
    const payload = { type: "call.ice", call_id:state.currentCallId, candidate: ev.candidate };
    if (roomId) payload.room_id = roomId;
    if (peerUserId) payload.to_user_id = peerUserId;
    state.ws?.send(JSON.stringify(payload));
  };

  pc.ontrack = e => {
    if(!current())return;
    if (state.isVideoCall) {
      // Handle video call
      const remoteVideo = document.getElementById('remoteVideo');
      if (remoteVideo && e.streams[0]) {
        remoteVideo.srcObject = e.streams[0];
        remoteVideo.muted = false;
        remoteVideo.playsInline = true;
        remoteVideo.setAttribute("playsinline", "true");
        remoteVideo.setAttribute("webkit-playsinline", "true");
        remoteVideo.autoplay = true;

        const playVideo = async () => {
          try {
            await remoteVideo.play();
          } catch (e) {
            // intentionally ignored
          }
        };

        playVideo();
        document.addEventListener("click", playVideo, { once: true });
        document.addEventListener("touchstart", playVideo, { once: true });
      }
    } else {
      // Handle audio call
      const audio = $("#remoteAudio");
      if (!audio) return;
      audio.srcObject = e.streams[0];
      audio.muted = false;
      audio.volume = 1;
      audio.playsInline = true;
      audio.setAttribute("playsinline", "true");
      audio.setAttribute("webkit-playsinline", "true");
      audio.autoplay = true;
      audio.disableRemotePlayback = true;
      audio.setAttribute("disableremoteplayback", "true");

      const playAudio = async () => {
        try {
          await audio.play();
        } catch (e) {
          // intentionally ignored
        }
      };

      playAudio();
      document.addEventListener("click", playAudio, { once: true });
      document.addEventListener("touchstart", playAudio, { once: true });
    }
  };

  const handleConnectionState = () => {
    if(!current())return;
    const stateName = pc.connectionState || pc.iceConnectionState || "";
    if (stateName === "connected") {
      clearCallDisconnectTimer();if(state.callOfferTimer)clearTimeout(state.callOfferTimer);state.callOfferTimer=null;
      state.callState = "connected";
      updateCallStatus(uiText("На связи"), true);
      startCallTimer();
      flushPendingIceCandidates();
      return;
    }
    if (stateName === "connecting" || stateName === "checking") {
      clearCallDisconnectTimer();
      updateCallStatus(uiText("Соединение..."), false);
      return;
    }
    if (stateName === "disconnected" || stateName === "failed") {
      updateCallStatus(uiText("Переподключение..."), false);
      if (!state.callDisconnectTimer) {
        state.callDisconnectTimer = setTimeout(() => {
          state.callDisconnectTimer = null;
          if (current() && state.callState !== "idle") hangupCall();
        }, 30000);
      }
      return;
    }
    if (stateName === "closed") {
      clearCallDisconnectTimer();
      hangupCall(false);
    }
  };

  pc.onconnectionstatechange = handleConnectionState;
  pc.oniceconnectionstatechange = handleConnectionState;
}

function captureInputState() {
  const input = document.activeElement;
  if (!(input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement)) return null;
  if (!input.id) return null;
  return {
    id: input.id,
    isFocused: true,
    value: input.value,
    start: input.selectionStart ?? 0,
    end: input.selectionEnd ?? 0
  };
}

function restoreInputState(snapshot) {
  const input = document.getElementById(snapshot?.id || "");
  if (!input || !snapshot) return;
  if (input.value !== snapshot.value) input.value = snapshot.value;
  if (!snapshot.isFocused) return;
  requestAnimationFrame(() => {
    input.focus({ preventScroll: true });
    try {
      input.setSelectionRange(snapshot.start, snapshot.end);
    } catch {}
  });
}

async function mountPreservingInput() {
  const snapshot = captureInputState();
  if (snapshot?.id === "inp") state.composerText = snapshot.value;
  await mount();
  restoreInputState(snapshot);
}

const keyStorageId = (id) => `k_${id}`;
const wrappedKeyStorageId = (id) => `kw2_${id}`;
const deviceMasterStorageId = "e2ee_device_master_v2";
const roomSecretCache = new Map();
const e2ePeerDeviceIds = new Map();
let searchTimer = null;
let typingTimer = null;
let typingActive = false;

// --- E2EE Logic ---
function bytesToBase64(bytes) {
  return btoa(String.fromCharCode(...bytes));
}

function base64ToBytes(value) {
  return Uint8Array.from(atob(value), c => c.charCodeAt(0));
}

async function sha256Base64(value) {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return bytesToBase64(new Uint8Array(digest));
}

function tryParseJson(value) {
  try { return JSON.parse(value); } catch { return null; }
}

async function getDeviceMasterSecret() {
  let secret = localStorage.getItem(deviceMasterStorageId);
  if (secret) return secret;
  secret = bytesToBase64(crypto.getRandomValues(new Uint8Array(32)));
  localStorage.setItem(deviceMasterStorageId, secret);
  return secret;
}

async function getDeviceWrapKey() {
  const enc = new TextEncoder();
  const master = await getDeviceMasterSecret();
  const baseKey = await crypto.subtle.importKey("raw", enc.encode(master), "PBKDF2", false, ["deriveKey"]);
  return crypto.subtle.deriveKey(
    { name: "PBKDF2", salt: enc.encode(`wrap:${location.host}`), iterations: 150000, hash: "SHA-256" },
    baseKey,
    { name: "AES-GCM", length: 256 },
    false,
    ["encrypt", "decrypt"]
  );
}

async function storeWrappedRoomPassphrase(roomId, passphrase) {
  const wrapKey = await getDeviceWrapKey();
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const data = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, wrapKey, new TextEncoder().encode(passphrase));
  localStorage.setItem(wrappedKeyStorageId(roomId), JSON.stringify({ v: 2, iv: bytesToBase64(iv), data: bytesToBase64(new Uint8Array(data)) }));
  localStorage.removeItem(keyStorageId(roomId));
}

async function getRoomPassphrase(roomId) {
  const wrapped = localStorage.getItem(wrappedKeyStorageId(roomId));
  if (wrapped) {
    const payload = tryParseJson(wrapped);
    if (payload?.iv && payload?.data) {
      try {
        const wrapKey = await getDeviceWrapKey();
        const plain = await crypto.subtle.decrypt({ name: "AES-GCM", iv: base64ToBytes(payload.iv) }, wrapKey, base64ToBytes(payload.data));
        return new TextDecoder().decode(plain);
      } catch {}
    }
  }
  const legacy = localStorage.getItem(keyStorageId(roomId));
  if (legacy) {
    await storeWrappedRoomPassphrase(roomId, legacy);
    return legacy;
  }
  const roomSecret = await getRoomServerSecret(roomId);
  if (!roomSecret) return null;
  const generated = await sha256Base64(`room-passphrase-v2:${roomId}:${roomSecret}`);
  await storeWrappedRoomPassphrase(roomId, generated);
  return generated;
}

async function getRoomServerSecret(roomId) {
  const session=captureWebSession();session.guard();
  if (roomSecretCache.has(roomId)) return roomSecretCache.get(roomId);
  const data = await api(`/rooms/${roomId}/key`);
  session.guard();
  const secret = data?.key || null;
  if (secret) roomSecretCache.set(roomId, secret);
  return secret;
}

async function getStableRoomPassphrase(roomId) {
  const roomSecret = await getRoomServerSecret(roomId);
  if (!roomSecret) return null;
  return sha256Base64(`room-passphrase-v2:${roomId}:${roomSecret}`);
}

async function deriveRoomAesKey(passphrase, saltValue, iterations) {
  const enc = new TextEncoder();
  const baseKey = await crypto.subtle.importKey("raw", enc.encode(passphrase), "PBKDF2", false, ["deriveKey"]);
  return crypto.subtle.deriveKey(
    { name: "PBKDF2", salt: enc.encode(saltValue), iterations, hash: "SHA-256" },
    baseKey,
    { name: "AES-GCM", length: 256 },
    false,
    ["encrypt", "decrypt"]
  );
}

async function getCryptoKey(roomId, msgMeta = null) {
  const session=captureWebSession();session.guard();
  if (msgMeta?.e2ee_v === 2) {
    const passphrase = await getStableRoomPassphrase(roomId);
    session.guard();
    if (!passphrase) return null;
    const roomSecret = await getRoomServerSecret(roomId);
    session.guard();
    if (!roomSecret) return null;
    return deriveRoomAesKey(passphrase, `room-v2:${roomId}:${roomSecret}`, 250000);
  }
  const passphrase = await getRoomPassphrase(roomId);
  session.guard();
  if (!passphrase) return null;
  return deriveRoomAesKey(passphrase, roomId, 100000);
}

async function decryptMsg(roomId, msg) {
  if (!msg) return "";
  if (msg.encrypted_content && msg.meta?.e2e_group) return decryptGroupMsg(roomId, msg);
  if (msg.encrypted_content && msg.meta?.e2e) {
    if (state.e2eReady) {
      try {
        const ciphertext = JSON.parse(msg.encrypted_content);
        const plaintext = await decryptMessageE2E(msg.sender_id, ciphertext);
        if (plaintext) return plaintext;
      } catch (e) {
        console.error('E2E decryption error:', e);
      }
    }
    return "[Encrypted for another device]";
  }
  if (msg.text == null) return "";

  // Try E2E decryption first if encrypted_content exists
  if (msg.encrypted_content && msg.meta?.e2e && state.e2eReady) {
    try {
      const ciphertext = JSON.parse(msg.encrypted_content);
      const plaintext = await decryptMessageE2E(msg.sender_id, ciphertext);
      if (plaintext) {
        return plaintext;
      }
      // If decryption failed, show error
      return uiText("🔐 [Не удалось расшифровать E2E сообщение]");
    } catch (e) {
      console.error('E2E decryption error:', e);
      return uiText("❌ [Ошибка E2E расшифровки]");
    }
  }

  // Fallback to room-based encryption for groups/channels
  if (!msg.meta?.encrypted) return String(msg.text);
  try {
    const key = await getCryptoKey(roomId, msg.meta || null);
    if (!key) return uiText("🔐 [Зашифровано]");
    const iv = base64ToBytes(msg.meta.iv);
    const data = base64ToBytes(msg.text);
    const dec = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, data);
    return new TextDecoder().decode(dec);
  } catch (e) { return uiText("❌ Ошибка ключа"); }
}

// --- E2E Encryption (Signal Protocol) ---
// Работает прозрачно в фоне, пользователь не видит
let e2eInitTask = null;
function captureWebSession(){return createSessionScope({account:state.me?.id,generation:browserSessionGeneration(),currentAccount:()=>state.me?.id,currentGeneration:browserSessionGeneration});}
async function publishWebDeviceBundle(manager,deviceId,bundle,session=captureWebSession()){
 const request=session.wrap(featureRequest);session.guard();
 let capabilities;try{capabilities=await request('/me/account/capabilities');}catch(error){if(error.status!==404)throw error;}session.guard();
 const proof=capabilities?.device_proof?await createWebDeviceProof(request,manager,deviceId,bundle.identityKey):null;session.guard();
 return request('/e2e/keys',{method:'POST',body:{device_id:String(deviceId),identity_key:bundle.identityKey,registration_id:bundle.registrationId,signed_pre_key:{key_id:bundle.signedPreKey.keyId,public_key:bundle.signedPreKey.publicKey,signature:bundle.signedPreKey.signature},pre_keys:bundle.preKeys.map(pk=>({key_id:pk.keyId,public_key:pk.publicKey})),...(proof?{device_proof:proof}:{})}});
}
function initE2E() {
  if (state.e2eReady && state.e2eManager?.ready) return Promise.resolve();
  if (e2eInitTask) return e2eInitTask;
  const userId=state.me?.id;
  if (!userId) return Promise.resolve();
  const session=captureWebSession();let task;task=(async()=>{
    if (!window.libsignal) await new Promise(resolve=>{ const done=()=>{window.removeEventListener('libsignal-ready',done);resolve();};window.addEventListener('libsignal-ready',done);setTimeout(done,5000); });
    session.guard();if (!window.libsignal || !window.SigStore) return;
    const manager=new window.E2EManager(), deviceId=getE2EDeviceId(), backend=window.SigStore.forAccount(userId);
    manager.useStore(userId,deviceId,backend);
    const loaded=await manager.loadFromIndexedDB(userId);
    session.guard();
    if (!loaded) {
      await manager.init(userId,deviceId,backend);session.guard();await manager.saveToIndexedDB(userId);session.guard();
    }
    // A new HTTP session must prove possession even when the pre-key pool is full.
    const bundle=await manager.getPublicBundle();session.guard();await publishWebDeviceBundle(manager,deviceId,bundle,session);session.guard();
    state.e2eManager=manager; state.e2eReady=true;
    await maybeReplenishPreKeys(deviceId,{force:true});session.guard();startPreKeyMonitor();
  })().catch(error=>{console.warn(uiText("Ключи шифрования пока недоступны:"),error.message);}).finally(()=>{if(e2eInitTask===task)e2eInitTask=null;});
  e2eInitTask=task;return task;
}

// If the server reports fewer than 10 unused one-time pre-keys, generate a fresh
// batch and publish it. A depleted bundle weakens forward secrecy on new sessions.
const PREKEY_LOW_WATERMARK = 10;
const PREKEY_CHECK_INTERVAL_MS = 10 * 60 * 1000;
const PREKEY_MIN_RECHECK_MS = 30 * 1000;
let preKeyReplenishInflight = null;
let preKeyLastCheck = 0;
let preKeyTimer = null;

function maybeReplenishPreKeys(deviceId, { force = false } = {}) {
  // Guard against concurrent replenish (periodic + post-consume + login).
  if (preKeyReplenishInflight) return preKeyReplenishInflight;
  if (!force && Date.now() - preKeyLastCheck < PREKEY_MIN_RECHECK_MS) return Promise.resolve();
  preKeyLastCheck = Date.now();
  preKeyReplenishInflight = doReplenishPreKeys(deviceId || getE2EDeviceId())
    .catch((e) => { console.error("Pre-key replenish failed:", e?.message || e); })
    .finally(() => { preKeyReplenishInflight = null; });
  return preKeyReplenishInflight;
}

async function doReplenishPreKeys(deviceId) {
  if (!state.e2eReady || !state.e2eManager || !state.me) return;
  const session=captureWebSession(),manager=state.e2eManager,userId=state.me.id;
  const info = await api(`/e2e/prekeys/count?device_id=${encodeURIComponent(deviceId)}`);
  session.guard();
  if (!info || typeof info.unused !== 'number' || info.unused >= PREKEY_LOW_WATERMARK) return;
  const bundle = await manager.replenishPreKeys(userId, deviceId);session.guard();
  if (!bundle || !bundle.signedPreKey || !bundle.signedPreKey.signature) return;
  const res = await publishWebDeviceBundle(manager,deviceId,bundle,session);session.guard();
  if (res?.ok) {
    // Persist the new private pre-keys and the advanced id counter.
    await manager.saveToIndexedDB(userId);
  }
}

function startPreKeyMonitor() {
  if (preKeyTimer) return;
  preKeyTimer = setInterval(() => { maybeReplenishPreKeys(); }, PREKEY_CHECK_INTERVAL_MS);
}

function stopPreKeyMonitor() {
  if (preKeyTimer) { clearInterval(preKeyTimer); preKeyTimer = null; }
  preKeyLastCheck = 0;
}

function getE2EDeviceId() {
  const userId = state.me?.id || "anonymous";
  const storageId = `e2e_device_id_${userId}`;
  let deviceId = localStorage.getItem(storageId);
  if (!deviceId) {
    const raw = crypto.getRandomValues(new Uint32Array(1))[0] || Date.now();
    deviceId = String(raw);
    localStorage.setItem(storageId, deviceId);
  }
  return deviceId;
}

const e2eSessionInflight = new Map();
async function encryptMessageE2E(recipientId, plaintext, targetDevice) {
  const owner=state.me?.id,ownDevice=getE2EDeviceId();
  await initE2E(); if(!state.e2eReady || !state.e2eManager) throw Error("Ключи ещё загружаются");
  if(state.me?.id!==owner)throw Error("Сеанс изменился");
  const manager=state.e2eManager;
  let device=targetDevice;
  if(!device) device=(await api(`/e2e/devices/${recipientId}`))[0]?.device_id;
  if(!device) throw Error("Участнику нужно обновить приложение для шифрованной группы");
  const key=`${recipientId}:${device}.${device}`;
  if (!await manager.store.loadSession(key)) {
    let task=e2eSessionInflight.get(key);
    if(!task) {task=(async()=>{ const bundle=await api(`/e2e/keys/${recipientId}?device_id=${encodeURIComponent(device)}`);
      await manager.buildSession(recipientId,device,{registrationId:bundle.registration_id,identityKey:bundle.identity_key,signedPreKey:bundle.signed_pre_key,preKey:bundle.pre_key});
    })();e2eSessionInflight.set(key,task);task.finally(()=>e2eSessionInflight.delete(key)).catch(()=>{});}
    await task;
  }
  if(state.me?.id!==owner)throw Error("Сеанс изменился");
  return {...await manager.encryptMessage(recipientId,device,plaintext),sender_device_id:ownDevice,recipient_device_id:device};
}
async function decryptMessageE2E(senderId,ciphertext) {
  const owner=state.me?.id;
  await initE2E(); if(!state.e2eReady || !state.e2eManager)throw Error("Ключи ещё загружаются");
  if(state.me?.id!==owner)throw Error("Сеанс изменился");const manager=state.e2eManager;
  const device=ciphertext.sender_device_id || (await api(`/e2e/devices/${senderId}`))[0]?.device_id || '1';
  if(state.me?.id!==owner)throw Error("Сеанс изменился");
  const plain=await manager.decryptMessage(senderId,device,ciphertext);
  if(ciphertext.type===3)void maybeReplenishPreKeys();
  return plain;
}

// --- Group E2E (Sender Keys) ---
// Opt-in per group (rooms.e2e_enabled). Each member's sender key reaches the others
// through the pairwise Signal sessions; the server only stores opaque payloads.
const GROUP_E2E_MAX_MEMBERS = 500;
const groupE2eRooms = new Map(); // roomId -> boolean, from GET /rooms/:id/e2e
const groupPlainCache = new Map(); // envelope JSON -> plaintext (own messages this session)
const groupInboxInflight = new Map();

function groupKey(roomId, suffix, owner=state.me?.id) {
  return `gk:${owner}:${roomId}:${suffix}`;
}

async function roomE2eEnabled(roomId) {
  if (!state.me || !roomId) return false;
  const session=captureWebSession();
  if (groupE2eRooms.has(roomId)) return groupE2eRooms.get(roomId);
  const data = await api(`/rooms/${roomId}/e2e`);
  session.guard();
  const on = !!data?.enabled;
  groupE2eRooms.set(roomId, on);
  return on;
}

async function fetchAllMemberIds(roomId) {
  const ids = [];
  for (let offset = 0; ; offset += 200) {
    const page = await api(`/rooms/${roomId}/members?limit=200&offset=${offset}`) || [];
    for (const m of page) ids.push(String(m.id));
    if (page.length < 200) break;
  }
  return ids;
}

// Keeps: own current key under "own", every own key under "own:<keyId>" (old
// messages stay readable after rotation), and inbound keys under "in:<senderId>".
async function saveOwnSenderKey(roomId, rec,owner=state.me?.id) {
  await window.SigStore.put(groupKey(roomId, 'own',owner), rec);
  await window.SigStore.put(groupKey(roomId, `own:${rec.keyId}`,owner), rec);
}

async function distributeOwnSenderKey(roomId,members,{rotate=false}={}) {
  const session=captureWebSession();session.guard();const owner=state.me?.id,ownDevice=getE2EDeviceId();
  const targets=[];
  for (const user of members) {
    const devices=await api(`/e2e/devices/${user}`);session.guard();
    if(state.me?.id!==owner)throw Error("Сеанс изменился");
    if(!devices.length)throw Error("Участнику нужно обновить приложение для шифрованной группы");
    for(const device of devices)if(String(user)!==String(owner) || device.device_id!==ownDevice) targets.push({user:String(user),device:device.device_id});
  }
  const tokens=targets.map(t=>t.user+'|'+t.device);
  let rec=await window.SigStore.get(groupKey(roomId,'own',owner),null);session.guard();
  if(rotate || !rec || rec.n>=1000 || rec.distributedTo.some(token=>!tokens.includes(token)))rec=await window.GroupCrypto.createSenderKey();
  session.guard();const recipients=[];
  for(const target of targets.filter(t=>!rec.distributedTo.includes(t.user+'|'+t.device))) recipients.push({user_id:target.user,device_id:target.device,payload:JSON.stringify(await encryptMessageE2E(target.user,JSON.stringify({...window.GroupCrypto.distributionFor(rec),room_id:roomId}),target.device))});
  if(state.me?.id!==owner)throw Error("Сеанс изменился");
  session.guard();if(recipients.length)await api(`/rooms/${roomId}/sender-keys`,{method:'POST',body:JSON.stringify({key_id:rec.keyId,recipients})});
  session.guard();rec.distributedTo=tokens;await saveOwnSenderKey(roomId,rec,owner);session.guard();return rec;
}
let groupSequence=Promise.resolve();
function groupSerial(fn) {const owner=state.me?.id,session=captureWebSession();const task=groupSequence.then(session.wrap(()=>fn(owner)));groupSequence=task.catch(()=>{});return task;}
function encryptGroupText(roomId,text) { return groupSerial(owner=>encryptGroupTextUnlocked(roomId,text,owner)); }
async function encryptGroupTextUnlocked(roomId, text,owner) {
  const session=captureWebSession();session.guard();
  await initE2E();
  session.guard();
  if (!state.e2eReady || !state.e2eManager) throw new Error("E2E не готово на этом устройстве");
  const members = await fetchAllMemberIds(roomId);
  session.guard();
  if(state.me?.id!==owner)throw Error("Сеанс изменился");
  if (members.length > GROUP_E2E_MAX_MEMBERS) throw new Error(`В группе с E2E не более ${GROUP_E2E_MAX_MEMBERS} ${escapeUi("участников")}`);
  const rec = await distributeOwnSenderKey(roomId, members);
  session.guard();
  if(state.me?.id!==owner)throw Error("Сеанс изменился");
  const { envelope, state: next } = await window.GroupCrypto.encryptWithState(rec, roomId, String(owner), text);
  session.guard();
  await saveOwnSenderKey(roomId, next,owner);
  const encrypted_content = JSON.stringify(envelope);
  groupPlainCache.set(encrypted_content, text);
  await createAccountStore(owner).set('group-plain:'+encrypted_content,text);
  return { type: "text", text: null, meta: { e2e_group: true, v: 1 }, encrypted_content };
}

// A member left or was removed: online members re-key immediately. Offline senders
// still re-key on their next message (see docs/GROUP_E2E.md).
async function rekeyOnMembershipChange(roomId) {
  try {
    if (!(await roomE2eEnabled(roomId))) return;
    const members = await fetchAllMemberIds(roomId);
    if (!members.includes(String(state.me.id))) return;
    if (!(await window.SigStore.get(groupKey(roomId, 'own'), null))) return; // nothing distributed yet
    await groupSerial(()=>distributeOwnSenderKey(roomId, members, { rotate: true }));
  } catch (e) {
    console.error('Group re-key failed:', e.message);
  }
}

// Fetch pending sender-key distributions addressed to me; import and ack each one.
// Rows that fail to decrypt stay pending so a later attempt can retry them.
function syncGroupInbox(roomId) {
  const owner=state.me?.id,device=getE2EDeviceId(),inflightKey=owner+':'+roomId;
  if (groupInboxInflight.has(inflightKey)) return groupInboxInflight.get(inflightKey);
  const p = (async () => {
    const rows = await api(`/rooms/${roomId}/sender-keys?device_id=${device}`) || [];
    for (const row of rows) {
      try {
        if(state.me?.id!==owner)return;
        const pending=groupKey(roomId,'distribution:'+row.id,owner);
        let d=await window.SigStore.get(pending,null);
        if(!d) {const plain=await decryptMessageE2E(row.from_user_id,JSON.parse(row.payload)); if(!plain)continue;d=JSON.parse(plain);await window.SigStore.put(pending,d);}
        if (d.room_id !== roomId) continue;
        const key = groupKey(roomId, `in:${row.from_user_id}:${d.kid}`,owner);
        const cur = await window.SigStore.get(key, null);
        if (!cur || cur.keyId !== d.kid) {
          const initial=window.GroupCrypto.receiverStateFromDistribution(d);
          await window.SigStore.put(key,{...initial,initial});
        }
        if(state.me?.id!==owner)return;
        await api(`/rooms/${roomId}/sender-keys/${row.id}?device_id=${device}`, { method: 'DELETE' });
      } catch (e) {
        console.error('Sender key import failed:', e.message);
      }
    }
  })().finally(() => groupInboxInflight.delete(inflightKey));
  groupInboxInflight.set(inflightKey, p);
  return p;
}

const GROUP_KEY_UNAVAILABLE = "🔐 [Ключ отправителя недоступен]";

function decryptGroupMsg(roomId,msg) {return groupSerial(owner=>decryptGroupMsgUnlocked(roomId,msg,owner));}
async function decryptGroupMsgUnlocked(roomId,msg,owner) {
  const content=msg.encrypted_content,local=createAccountStore(owner);
  const cached=await local.get('group-plain:'+content,null);if(cached!==null)return cached;
  if(state.me?.id!==owner)throw Error("Сеанс изменился");
  const env=JSON.parse(content),senderId=String(msg.sender_id),inKey=groupKey(roomId,`in:${senderId}:${env.kid}`,owner);
  const own=senderId===String(owner) ? await window.SigStore.get(groupKey(roomId,`own:${env.kid}`,owner),null) : null;
  let incoming=await window.SigStore.get(inKey,null);
  if(!incoming && !own) {await syncGroupInbox(roomId);incoming=await window.SigStore.get(inKey,null);}
  if(!incoming && !own)return GROUP_KEY_UNAVAILABLE;
  try {
    let result;
    if(own)result=await window.GroupCrypto.decryptWithState(window.GroupCrypto.ownReceiverState(own),roomId,senderId,env);
    else {try {result=await window.GroupCrypto.decryptWithState(incoming,roomId,senderId,env);}catch(error) {if(!incoming.initial)throw error;result=await window.GroupCrypto.decryptWithState(incoming.initial,roomId,senderId,env);}
      if(result.state.n>=incoming.n)await window.SigStore.put(inKey,{...result.state,initial:incoming.initial});}
    await local.set('group-plain:'+content,result.plaintext);return result.plaintext;
  }catch(error){console.warn(uiText("Расшифровка группы:"),error.message);return uiText("🔐 Не удалось получить ключ сообщения");}
}

window.toggleGroupE2e = async () => {
  const roomId = state.activeRoomId;
  if (!roomId) return;
  const on = await roomE2eEnabled(roomId);
  const warning = on
    ? uiText("Отключить сквозное шифрование? Новые сообщения пойдут без E2E.")
    : uiText("Включить сквозное шифрование группы?\n\nУчастникам нужны актуальная версия сайта или приложения. Новые сообщения и ключи вложений будут защищены сквозным шифрованием. Сервер видит участников и время отправки.");
  if (!confirm(warning)) return;
  const res = await api(`/rooms/${roomId}/e2e`, { method: 'PUT', body: JSON.stringify({ enabled: !on }) });
  if (!res) { alert(uiText("Не удалось изменить настройку (нужны права владельца или администратора).")); return; }
  groupE2eRooms.set(roomId, !!res.enabled);
  render();
};

// --- WebSocket & WebRTC ---
function connectWS() {
  // Clear any existing reconnect timer
  if (state.wsReconnectTimer) {
    clearTimeout(state.wsReconnectTimer);
    state.wsReconnectTimer = null;
  }

  // Never keep two sockets alive at once.
  if (state.ws && state.ws.readyState <= 1) {
    const old = state.ws;
    state.ws = null;
    try { old.close(); } catch {}
  }
  if (state.wsAuthTimer) { clearTimeout(state.wsAuthTimer); state.wsAuthTimer = null; }

  const prot = location.protocol === "https:" ? "wss:" : "ws:";
  const selected=selectedBrowserAccount();
  const ws = new WebSocket(`${prot}//${location.host}/ws${selected&&selected!=='none'?'?account_id='+encodeURIComponent(selected):''}`);
  state.ws = ws;
  const isCurrent = () => state.ws === ws;

  // If the server never confirms auth (no auth.ok) within the timeout, treat it
  // as an auth failure.
  const armAuthTimer = (ms) => {
    if (state.wsAuthTimer) clearTimeout(state.wsAuthTimer);
    state.wsAuthTimer = setTimeout(() => {
      state.wsAuthTimer = null;
      if (isCurrent() && !state.wsAuthed) recoverFromWSAuthFailure();
    }, ms);
  };

  ws.onopen = () => {
    if (!isCurrent()) return;
    // Backoff is reset only after auth.ok, so a server that accepts the socket
    // but rejects auth cannot make us reconnect in a tight loop.
    ws.send(JSON.stringify({ type: "auth" }));
    armAuthTimer(WS_AUTH_TIMEOUT_MS);
  };

  ws.onerror = (err) => {
    console.error("WebSocket error:", err);
  };

  ws.onmessage = async (e) => {
    if (!isCurrent()) return;
    let data;
    try { data = JSON.parse(e.data); } catch { return; }

    if (data.type === "auth.ok") {
      if (state.wsAuthTimer) { clearTimeout(state.wsAuthTimer); state.wsAuthTimer = null; }
      state.wsReconnectAttempts = 0;
      state.wsAuthed = true;
      if(state.currentCallId&&state.callResumeToken&&state.currentCallRoomId)ws.send(JSON.stringify({type:'call.resume',call_id:state.currentCallId,room_id:state.currentCallRoomId,resume_token:state.callResumeToken}));
      if (state.activeRoomId) joinActiveRoom();
      updateChatPresence();
      void chatRuntime.syncHidden();
      void chatRuntime.syncEvents();
      void chatRuntime.flush();
      if (state.messages.some(m => !m.delivery)) void chatRuntime.newer();

      // Auto-save E2E keys periodically (every 5 minutes)
      if (!state.autoBackupInterval) {
        state.autoBackupInterval = setInterval(() => {
          autoSaveBackup();
        }, 5 * 60 * 1000);
      }

      return;
    }
    if (data.type === "auth.required" || data.type === "auth.error") {
      // The server also sends auth.required right at connect, before it sees our
      // own auth message, so give the real auth reply a moment before reacting.
      state.wsAuthed = false;
      armAuthTimer(data.type === "auth.error" ? 0 : 1500);
      return;
    }
    if (data.type === "error") {
      if (data.error === "blocked") {
        alert(uiText("Писать этому пользователю сейчас нельзя"));
      }
      return;
    }

    if (data.type === "message.new") {
      const roomId = data.message.room_id, account = state.me?.id;
      const prepared = await chatRuntime.received(data.message);
      if (state.me?.id !== account) return;
      clearTypingUser(roomId, data.message.sender_id, roomId === state.activeRoomId);
      void loadRooms().catch(() => {});
      if (prepared && (roomId !== state.activeRoomId || !document.hasFocus())) {
        const room = state.rooms.find(r => r.id === roomId);
        if (room && prepared.sender_id !== account) notifyNewMessage(prepared, room);
      }
    }

    // Handle messages read status
    if (data.type === "messages.read" && data.user_id !== state.me?.id) {
      const ids = new Set(data.message_ids || []);
      if (data.room_id === state.activeRoomId) {
        state.messages = state.messages.map(m => ids.has(m.id) ? { ...m, read_by: [...new Set([...(m.read_by || []), data.user_id])] } : m);
        render();
      }
      const local = chatRuntime.local();
      if (local) void local.get('messages:' + data.room_id, []).then(rows => local.saveMessages(data.room_id, rows.filter(m => ids.has(m.id)).map(m => ({ ...m, read_by: [...new Set([...(m.read_by || []), data.user_id])] })))).catch(() => {});
    }

    // Handle message deletion
    if(data.type==='message.delete'){document.querySelectorAll('[data-search-message-id="'+data.message_id+'"]').forEach(node=>node.remove());
      void chatRuntime.hide(data.room_id || state.activeRoomId,data.message_id).catch(()=>{});
      state.messages = state.messages.filter(m => m.id !== data.message_id);
      if (state.editMessage?.id === data.message_id) state.editMessage = null;
      render();
    }

    if (data.type === "message.update" && data.message) {
      await chatRuntime.received(data.message);
      if (state.editMessage?.id === data.message.id) state.editMessage = null;
    }
    if(data.type==='message.listened'){
      state.messages=state.messages.map(message=>message.id===data.message_id?{...message,listened_by:data.listened_by}:message);render();
    }

    // Handle reactions
    if (data.type === "reaction.add") {
      const msg = state.messages.find(m => m.id === data.message_id);
      if (msg) {
        if (!msg.reactions) msg.reactions = {};
        // Remove user's previous reaction if any
        for (const [emoji, users] of Object.entries(msg.reactions)) {
          const idx = users.indexOf(data.user_id);
          if (idx !== -1) {
            users.splice(idx, 1);
            if (users.length === 0) delete msg.reactions[emoji];
          }
        }
        // Add new reaction
        if (!msg.reactions[data.emoji]) msg.reactions[data.emoji] = [];
        if (!msg.reactions[data.emoji].includes(data.user_id)) {
          msg.reactions[data.emoji].push(data.user_id);
        }
        render();
      }
    }

    if (data.type === "reaction.remove") {
      const msg = state.messages.find(m => m.id === data.message_id);
      if (msg && msg.reactions && msg.reactions[data.emoji]) {
        msg.reactions[data.emoji] = msg.reactions[data.emoji].filter(id => id !== data.user_id);
        if (msg.reactions[data.emoji].length === 0) delete msg.reactions[data.emoji];
        render();
      }
    }

    if(String(data.type||'').startsWith('call.')){await handleWebCallSignal(data);return;}
    if (data.type === "sender_key.available" && data.room_id) {
      syncGroupInbox(data.room_id).then(async () => {
        if (state.activeRoomId !== data.room_id) return;
        for (const m of state.messages) {
          if (m.meta?.e2e_group) m._text = await decryptMsg(data.room_id, m);
        }
        renderMessages();
      });
    }
    if (data.type === "room.members_changed" && data.room_id) {
      rekeyOnMembershipChange(data.room_id);
    }
    if (data.type === "room.e2e" && data.room_id) {
      groupE2eRooms.set(data.room_id, !!data.enabled);
      if (state.activeRoomId === data.room_id) render();
    }
    if(data.type==='message.hide'&&data.message_id){document.querySelectorAll('[data-search-message-id="'+data.message_id+'"]').forEach(node=>node.remove());
      const room=data.room_id; await chatRuntime.hide(room,data.message_id);
      if(state.activeRoomId===room){state.messages=state.messages.filter(m=>m.id!==data.message_id);renderMessages();}
      return;
    }
    if(['room.pins','room.settings'].includes(data.type)&&data.room_id){await loadRooms();if(state.activeRoomId===data.room_id)await mountPreservingInput();}
    if (data.type === "room.pin" && data.room_id) {
      const room = state.rooms.find(item => item.id === data.room_id);
      if (room) room.pinned_message_id = data.message_id || null;
      if (state.activeRoomId === data.room_id) mount();
    }
    if (data.type === "room.deleted" && data.room_id) {
      if (state.activeRoomId === data.room_id) {
        state.activeRoomId = null;
        state.messages = [];
        state.roomMembers = [];
        state.activeDmPeer = null;
      }
      await loadRooms();
      mount();
      return;
    }

    if (data.type === "typing") {
      const roomId = String(data.room_id || "");
      if (!roomId) return;
      const uId = String(data.user_id || "");
      if (data.is_typing) setTypingUser(roomId, uId, data.username || "user");
      else clearTypingUser(roomId, uId);
      if (roomId === state.activeRoomId) render(); // Use render() instead of mount() to preserve focus
    }

    // Update room list in real-time
    if(data.type==='rooms.update'&&Array.isArray(data.rooms)){if(state.activeRoomId)void syncWebDraft(state.activeRoomId).catch(error=>toast(userError(error)));
      state.rooms = await prepareRoomPreviews(data.rooms);
      updateRoomsList();
    }
  };

  ws.onclose = (ev) => {
    if (!isCurrent()) return; // intentional close / superseded socket
    state.wsAuthed = false;
    updateChatPresence();
    if (state.wsAuthTimer) { clearTimeout(state.wsAuthTimer); state.wsAuthTimer = null; }
    clearAllTyping(true); // typing "stop" events can't arrive on a dead socket

    if (WS_AUTH_CLOSE_CODES.has(ev?.code)) {
      // Closed for authorization reasons: verify the session. A 401 from /me logs
      // us out through the central handler; otherwise fall back to backoff.
      recoverFromWSAuthFailure().finally(scheduleWSReconnect);
      return;
    }
    scheduleWSReconnect();
  };
}

async function handleWebCallSignal(data){
 const matches=()=>data.room_id===state.currentCallRoomId&&(!data.call_id||!state.currentCallId||data.call_id===state.currentCallId);
 if(data.type==='call.created'){if(matches()&&state.callState!=='idle'){state.currentCallId=data.call_id;state.callResumeToken=data.resume_token||null;}return;}
 if(data.type==='call.waiting'){
  document.getElementById('waitingCallDialog')?.remove();state.callWaitingId=data.call_id;const owner=state.me?.id;
  const dialog=document.createElement('dialog');dialog.id='waitingCallDialog';const text=document.createElement('p');text.textContent=uiText("Второй входящий: {0}",data.from_username||uiText("Абонент"));dialog.append(text);
  const valid=()=>state.callWaitingId===data.call_id&&state.me?.id===owner&&(!data.expires_at||data.expires_at>Date.now());
  for(const [label,accept]of[[uiText("Отклонить"),false],[uiText("Завершить текущий и ответить"),true]]){const button=document.createElement('button');button.textContent=label;button.onclick=()=>{if(valid()){state.callWaitingId=null;if(accept){hangupCall(false);state.autoAnswerCallId=data.call_id;}state.ws?.send(JSON.stringify({type:accept?'call.waiting.accept':'call.waiting.reject',room_id:data.room_id,call_id:data.call_id}));}dialog.close();};dialog.append(button);}
  dialog.addEventListener('close',()=>dialog.remove(),{once:true});document.body.append(dialog);dialog.showModal();setTimeout(()=>{if(dialog.isConnected){if(state.callWaitingId===data.call_id)state.callWaitingId=null;dialog.close();}},Math.max(0,(data.expires_at||Date.now()+45000)-Date.now()));return;
 }
 if(data.type==='call.accepted'){if(state.callWaitingId===data.call_id){state.callWaitingId=null;document.getElementById('waitingCallDialog')?.remove();}if(data.call_id&&data.call_id===state.currentCallId&&data.room_id===state.currentCallRoomId)hangupCall(false);return;}
 if(data.type==='call.answered'){if(matches())state.callResumeToken=data.resume_token||null;return;}
 if(data.type==='call.resumed'||data.type==='call.peer_reconnected'){if(matches())void state.callRecovery?.restart();return;}
 if(data.type==='call.offer'){if(String(data.from_user_id||'')!==String(state.me?.id||''))void handleCall(data);return;}
 if(data.type==='call.hangup'&&state.callWaitingId===data.call_id){state.callWaitingId=null;document.getElementById('waitingCallDialog')?.remove();return;}
 if(!matches())return;
 if(data.type==='call.hangup'){if(data.call_id&&!state.currentCallId)return;hangupCall(false);return;}
 const pc=state.pc,generation=state.callGeneration,current=()=>state.pc===pc&&generation===state.callGeneration;
 try{
  if(data.type==='call.renegotiate'&&pc){await pc.setRemoteDescription(new RTCSessionDescription(data.offer||data.sdp));if(!current())return;const answer=await pc.createAnswer();if(!current())return;await pc.setLocalDescription(answer);if(current())state.ws?.send(JSON.stringify({type:'call.renegotiate.answer',room_id:data.room_id,call_id:data.call_id,answer}));return;}
  if((data.type==='call.answer'||data.type==='call.renegotiate.answer')&&pc){await pc.setRemoteDescription(new RTCSessionDescription(data.answer||data.sdp));if(current())await flushPendingIceCandidates(pc,generation);return;}
  if(data.type==='call.ice'&&data.candidate){if(pc?.remoteDescription)await pc.addIceCandidate(new RTCIceCandidate(data.candidate));else if(state.pendingIceCandidates.length<128)state.pendingIceCandidates.push(data.candidate);}
 }catch(error){if(current())console.error('Call signaling failed:',error);}
}

async function handleCall(data) {
  const fromName = data.from_username || data.from_user_id || "unknown";
  data.sdp ||= data.offer;
  if (!data.sdp) { toast(uiText("Не удалось получить данные звонка")); return; }
  if(data.call_id&&state.currentCallId===data.call_id)return;
  if (state.callState !== 'idle') {
    try {
      state.ws?.send(JSON.stringify({ type: "call.hangup", call_id:data.call_id, reason:'busy',room_id: data.room_id, to_user_id: data.from_user_id }));
    } catch {}
    return;
  }
  pauseMessageMedia();

  // Detect if it's a video call by checking SDP
  const isVideoCall = data.is_video!==undefined?!!data.is_video:!!data.sdp.sdp?.includes('m=video');
  const generation=++state.callGeneration,current=()=>generation===state.callGeneration&&state.currentCallId===data.call_id;
  state.callInitiator=false;state.callResumeToken=data.resume_token||null;

  state.currentCallId=data.call_id;
  state.callState = 'incoming';
  state.currentCallRoomId = data.room_id || state.activeRoomId || null;
  state.currentCallPeerId = data.from_user_id || null;
  state.pendingIceCandidates = data.candidates || [];
  state.isVideoCall = isVideoCall;
  state.isVideoEnabled = isVideoCall;
  state.isAudioEnabled = true;

  // Show incoming call UI with avatar
  const caller = state.roomMembers?.find(m => m.id === data.from_user_id);
  const avatarHtml = caller?.avatar_id ? '<img src="' + avatarUrl(caller.avatar_id) + '" style="width:120px;height:120px;border-radius:50%;object-fit:cover;">' : '<div style="width:120px;height:120px;border-radius:50%;background:var(--accent);display:flex;align-items:center;justify-content:center;font-size:60px;">👤</div>';

  const callDialog = document.createElement("div");
  callDialog.id = "incomingCallDialog";
  callDialog.innerHTML =
    '<div style="position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.8);z-index:10000;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:20px;">' +
      '<div style="text-align:center;color:white;">' +
        '<div style="margin-bottom:20px;">' + avatarHtml + '</div>' +
        '<div style="font-size:24px;font-weight:bold;">' + escapeHtml(displayName(caller) || fromName) + '</div>' +
        '<div style="font-size:16px;opacity:0.8;">' + (isVideoCall ? uiText("Входящий видеовызов...") : uiText("Входящий звонок...")) + '</div>' +
      '</div>' +
      '<div style="display:flex;gap:20px;">' +
        '<button id="acceptCallBtn" style="width:80px;height:80px;border-radius:50%;background:#42d392;border:none;font-size:40px;cursor:pointer;">' + (isVideoCall ? '📹' : '📞') + '</button>' +
        '<button id="rejectCallBtn" style="width:80px;height:80px;border-radius:50%;background:#ff4d6d;border:none;font-size:40px;cursor:pointer;">📛</button>' +
      '</div>' +
    '</div>';
  document.body.appendChild(callDialog);

  const autoAnswer=state.autoAnswerCallId===data.call_id;state.autoAnswerCallId=null;let incomingTimer;
  const accepted = autoAnswer || await new Promise(resolve => {
    state.incomingCallResolve=resolve;
    incomingTimer=setTimeout(()=>resolve(false),Math.max(0,(data.expires_at || Date.now()+45000)-Date.now()));
    $("#acceptCallBtn").onclick = () => resolve(true);
    $("#rejectCallBtn").onclick = () => resolve(false);
  });

  clearTimeout(incomingTimer);callDialog.remove();if(!current())return;state.incomingCallResolve=null;
  if(state.callState==='idle' || (data.call_id && data.call_id!==state.currentCallId))return;
  if(!accepted){hangupCall(true,'rejected');return;}

  let stream;
  try {
    // Get media based on call type
    stream = await navigator.mediaDevices.getUserMedia(
      isVideoCall ? {
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true
        },
        video: {
          width: { ideal: 1280 },
          height: { ideal: 720 },
          facingMode: 'user'
        }
      } : getAudioConstraints()
    );
  }
  catch (e) {
    if(!current())return;hangupCall(true,'rejected');
    alert(isVideoCall ? uiText("Нет доступа к камере/микрофону.") : uiText("Нет доступа к микрофону."));
    return;
  }

  if(!current()){stream.getTracks().forEach(track=>track.stop());return;}
  state.localStream = stream;
  try{await loadRtcIceServers();}catch(error){if(current()){toast(userError(error));hangupCall();}return;}
  if(!current()){stream.getTracks().forEach(track=>track.stop());return;}
  const pc = new RTCPeerConnection({
    iceServers: getRtcIceServers(),
    iceTransportPolicy: "all",
    iceCandidatePoolSize: 4
  });

  state.pc=pc;
  if (isVideoCall) {
    showVideoCallUI();
    // Show local video
    const localVideo = document.getElementById('localVideo');
    if (localVideo) {
      localVideo.srcObject = stream;
    }
  } else {
    showAudioCallUI();
    // Set audio output to earpiece for audio calls
    if (state.audioOutputMode === 'earpiece') {
      setTimeout(() => setAudioOutputToEarpiece(), 500);
    }
  }

  updateCallStatus(uiText("Соединение..."), false);
  wirePeerConnectionHandlers({ roomId: data.room_id, peerUserId: data.from_user_id });

  stream.getTracks().forEach(t => pc.addTrack(t, stream));
  try {
    await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));if(!current())return;
    await flushPendingIceCandidates(pc,generation);if(!current())return;
    const answer=await pc.createAnswer();if(!current())return;
    await pc.setLocalDescription(answer);if(!current())return;
    state.ws?.send(JSON.stringify({type:'call.answer',call_id:data.call_id,room_id:data.room_id,answer,sdp:answer}));
  }catch(e){if(current()){console.error('Failed to handle call:',e);toast(uiText("Ошибка при установке звонка"));hangupCall();}}
}

function showCallUI() {
  let hud = $("#callHud");
  if (!hud) {
    hud = document.createElement("div");
    hud.id = "callHud";
    hud.innerHTML = `
      <div class="callVideos">
        <video id="remoteVideo" autoplay playsinline></video>
        <video id="localVideo" autoplay playsinline muted></video>
      </div>
      <div class="callControls">
        <button class="btn danger" onclick="hangupCall()">${escapeUi(uiText("Завершить"))}</button>
      </div>
    `;
    document.body.appendChild(hud);
  }
}

function showAudioCallUI() {
  let hud = $("#callHud");
  if (!hud) {
    hud = document.createElement("div");
    hud.id = "callHud";
    hud.innerHTML =
      '<div class="callAudio">' +
        '<div class="callAvatar">📞</div>' +
        `<div class="callStatus" id="callStatus">${escapeUi(uiText("Соединение..."))}</div>` +
        '<div class="callTimer" id="callTimer" style="display:none;">00:00</div>' +
        '<audio id="remoteAudio" autoplay playsinline webkit-playsinline></audio>' +
      '</div>' +
      '<div class="callControls">' +
        `<button id="toggleAudioBtn" class="btn" onclick="toggleAudio()" title="${escapeUi(uiText("Выключить микрофон"))}">🎤</button>` +
        `<button id="toggleSpeakerBtn" class="btn" onclick="toggleAudioOutput()" title="${escapeUi(uiText("Переключить на громкую связь"))}">📱</button>` +
        `<button class="btn danger" onclick="hangupCall()">${escapeUi(uiText("Завершить"))}</button>` +
      '</div>';
    document.body.appendChild(hud);
  }

  updateCallStatus(state.callState === "calling" ? uiText("Вызов...") : uiText("Соединение..."), false);
}

function showVideoCallUI() {
  let hud = $("#callHud");
  if (!hud) {
    hud = document.createElement("div");
    hud.id = "callHud";
    hud.className = "videoCallHud";
    hud.innerHTML =
      '<div class="videoCallContainer">' +
        '<video id="remoteVideo" class="remoteVideo" autoplay playsinline webkit-playsinline></video>' +
        '<video id="localVideo" class="localVideo" autoplay playsinline webkit-playsinline muted></video>' +
        `<div class="callStatus" id="callStatus">${escapeUi(uiText("Соединение..."))}</div>` +
        '<div class="callTimer" id="callTimer" style="display:none;">00:00</div>' +
      '</div>' +
      '<div class="callControls videoCallControls">' +
        `<button id="toggleVideoBtn" class="btn" onclick="toggleVideo()" title="${escapeUi(uiText("Выключить камеру"))}">📹</button>` +
        `<button id="toggleAudioBtn" class="btn" onclick="toggleAudio()" title="${escapeUi(uiText("Выключить микрофон"))}">🎤</button>` +
        `<button id="switchCameraBtn" class="btn" onclick="switchCamera()" title="${escapeUi(uiText("Переключить камеру"))}">🔄</button>` +
        `<button id="shareScreenBtn" class="btn" onclick="toggleCallScreenShare()" title="${escapeUi(uiText("Демонстрация экрана"))}">${escapeUi(uiText("Экран"))}</button>` +
        `<button class="btn danger" onclick="hangupCall()">${escapeUi(uiText("Завершить"))}</button>` +
      '</div>';
    document.body.appendChild(hud);
  }

  updateCallStatus(state.callState === "calling" ? uiText("Видеовызов...") : uiText("Соединение..."), false);
}

// Check if mobile device
function isMobile() {
  return /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
}

window.hangupCall = (notifyPeer = true,reason='ended') => {
  ++state.callGeneration;const endedCallId=state.currentCallId;state.callResumeToken=null;state.sharingScreen=false;
  if(state.callOfferTimer)clearTimeout(state.callOfferTimer);state.callOfferTimer=null;
  const recovery=state.callRecovery;state.callRecovery=null;void recovery?.dispose().catch(()=>{});
  // Clear timer
  if (state.callTimer) {
    clearInterval(state.callTimer);
    state.callTimer = null;
  }
  clearCallDisconnectTimer();
  state.callStartTime = null;
  state.callState = 'idle';
  state.incomingCallResolve?.(false);state.incomingCallResolve=null;state.currentCallId=null;
  state.pendingIceCandidates = [];
  state.isVideoCall = false;
  state.isVideoEnabled = true;
  state.isAudioEnabled = true;

  for(const id of ['remoteAudio','remoteVideo','localVideo']){const media=document.getElementById(id);if(media?.srcObject){media.srcObject.getTracks?.().forEach(track=>{try{track.stop();}catch{}});media.srcObject=null;}}
  // Close peer connection
  try {if(state.pc){state.pc.onconnectionstatechange=null;state.pc.oniceconnectionstatechange=null;state.pc.onicecandidate=null;state.pc.ontrack=null;state.pc.close();}} catch {}
  state.pc = null;

  // Stop all tracks in local stream
  if (state.localStream) {
    state.localStream.getTracks().forEach(t => { try { t.stop(); } catch(e) {} });
    state.localStream = null;
  }

  // Remove call UI
  const hud = $("#callHud");
  if (hud) hud.remove();

  // Remove incoming call dialog
  const dialog = document.getElementById("incomingCallDialog");
  if (dialog) dialog.remove();

  // Notify server
  try {
    if (notifyPeer && state.ws?.readyState === 1) {
      const payload = { type: "call.hangup",call_id:endedCallId,reason };
      if (state.currentCallRoomId) payload.room_id = state.currentCallRoomId;
      if (state.currentCallPeerId) payload.to_user_id = state.currentCallPeerId;
      state.ws.send(JSON.stringify(payload));
    }
  } catch {}
  state.currentCallRoomId = null;
  state.currentCallPeerId = null;

  // Only re-render call UI, not full page
  const callContainer = $("#callContainer");
  if (callContainer) callContainer.innerHTML = "";
};

window.toggleCallScreenShare=async()=>{const recovery=state.callRecovery,generation=state.callGeneration;if(!recovery)return;try{if(state.sharingScreen)await recovery.stopScreenShare();else await recovery.startScreenShare();if(generation!==state.callGeneration)return;state.sharingScreen=!state.sharingScreen;const button=document.getElementById('shareScreenBtn');if(button)button.textContent=state.sharingScreen?uiText("Остановить экран"):uiText("Экран");}catch(error){if(generation===state.callGeneration)toast(userError(error));}};

// Toggle video during call
window.toggleVideo = () => {
  if (!state.localStream || !state.isVideoCall) return;

  const videoTrack = state.localStream.getVideoTracks()[0];
  if (videoTrack) {
    state.isVideoEnabled = !state.isVideoEnabled;
    videoTrack.enabled = state.isVideoEnabled;

    // Update UI
    const btn = document.getElementById('toggleVideoBtn');
    if (btn) {
      btn.textContent = state.isVideoEnabled ? '📹' : '📹❌';
      btn.title = state.isVideoEnabled ? uiText("Выключить камеру") : uiText("Включить камеру");
    }

    // Hide/show local video
    const localVideo = document.getElementById('localVideo');
    if (localVideo) {
      localVideo.style.opacity = state.isVideoEnabled ? '1' : '0.3';
    }
  }
};

// Toggle audio during call
window.toggleAudio = () => {
  if (!state.localStream) return;

  const audioTrack = state.localStream.getAudioTracks()[0];
  if (audioTrack) {
    state.isAudioEnabled = !state.isAudioEnabled;
    audioTrack.enabled = state.isAudioEnabled;

    // Update UI
    const btn = document.getElementById('toggleAudioBtn');
    if (btn) {
      btn.textContent = state.isAudioEnabled ? '🎤' : '🎤❌';
      btn.title = state.isAudioEnabled ? uiText("Выключить микрофон") : uiText("Включить микрофон");
    }
  }
};

// Switch camera (front/back)
window.switchCamera = async () => {
  if (!state.localStream || !state.isVideoCall) return;

  const videoTrack = state.localStream.getVideoTracks()[0];
  if (!videoTrack) return;

  try {
    // Get current facing mode
    const settings = videoTrack.getSettings();
    const currentFacingMode = settings.facingMode || 'user';
    const newFacingMode = currentFacingMode === 'user' ? 'environment' : 'user';

    // Keep the current track available if acquiring the other camera fails.

    // Get new stream with different camera
    const newStream = await navigator.mediaDevices.getUserMedia({
      video: {
        facingMode: newFacingMode,
        width: { ideal: 1280 },
        height: { ideal: 720 }
      }
    });

    const newVideoTrack = newStream.getVideoTracks()[0];

    // Replace track in peer connection
    const sender = state.pc.getSenders().find(s => s.track?.kind === 'video');
    if (sender) {
      await sender.replaceTrack(newVideoTrack);
    }

    // Replace track in local stream
    videoTrack.stop();
    state.localStream.removeTrack(videoTrack);
    state.localStream.addTrack(newVideoTrack);

    // Update local video element
    const localVideo = document.getElementById('localVideo');
    if (localVideo) {
      localVideo.srcObject = state.localStream;
    }
  } catch (error) {
    console.error('Failed to switch camera:', error);
    alert(uiText("Не удалось переключить камеру"));
  }
};

// Toggle audio output (earpiece/speaker)
window.toggleAudioOutput = async () => {
  try {
    const audioElement = document.querySelector('audio');
    if (!audioElement || typeof audioElement.setSinkId !== 'function') {
      alert(uiText("Переключение динамика не поддерживается в этом браузере"));
      return;
    }

    if (state.audioOutputMode === 'earpiece') {
      // Switch to speaker
      await audioElement.setSinkId('default');
      state.audioOutputMode = 'speaker';

      // Update UI
      const btn = document.getElementById('toggleSpeakerBtn');
      if (btn) {
        btn.textContent = '🔊';
        btn.title = uiText("Переключить на слуховой динамик");
      }
    } else {
      // Switch to earpiece
      await setAudioOutputToEarpiece();
    }
  } catch (error) {
    console.error('Failed to toggle audio output:', error);
  }
};

// Set audio output to earpiece
async function setAudioOutputToEarpiece() {
  try {
    const audioElement = document.querySelector('audio');
    if (!audioElement || typeof audioElement.setSinkId !== 'function') {
      return false;
    }

    // Get available audio output devices
    const devices = await navigator.mediaDevices.enumerateDevices();
    const audioOutputs = devices.filter(device => device.kind === 'audiooutput');

    // Try to find earpiece/phone speaker
    const earpiece = audioOutputs.find(device =>
      device.label.toLowerCase().includes('earpiece') ||
      device.label.toLowerCase().includes('phone') ||
      device.label.toLowerCase().includes('receiver')
    );

    if (earpiece) {
      await audioElement.setSinkId(earpiece.deviceId);
      state.audioOutputMode = 'earpiece';

      // Update UI
      const btn = document.getElementById('toggleSpeakerBtn');
      if (btn) {
        btn.textContent = '📱';
        btn.title = uiText("Переключить на громкую связь");
      }
      return true;
    } else {
      // Fallback: use first non-default device
      const nonDefault = audioOutputs.find(device => device.deviceId !== 'default');
      if (nonDefault) {
        await audioElement.setSinkId(nonDefault.deviceId);
        state.audioOutputMode = 'earpiece';

        const btn = document.getElementById('toggleSpeakerBtn');
        if (btn) {
          btn.textContent = '📱';
          btn.title = uiText("Переключить на громкую связь");
        }
        return true;
      }
    }

    return false;
  } catch (error) {
    console.error('Failed to set earpiece:', error);
    return false;
  }
}

function joinActiveRoom() {
  if (!state.ws || state.ws.readyState !== 1 || !state.activeRoomId) return;
  state.ws.send(JSON.stringify({ type: "rooms.join", room_id: state.activeRoomId }));
}

// --- Search ---
async function refreshSearch() {
  const q = (state.searchQ || '').trim();
  const account = state.me?.id;
  if (!q) { state.peopleResults = []; state.searching = false; mountPreservingInput(); return; }
  state.searching = true;
  mountPreservingInput();
  let results = [];
  try { results = await api(`/users/search?q=${encodeURIComponent(q)}`); }
  catch { /* Keep a readable empty state when offline. */ }
  if (state.searchQ.trim() !== q || state.me?.id !== account || !state.composeOpen) return;
  state.peopleResults = Array.isArray(results) ? results : [];
  state.searching = false;
  mountPreservingInput();
}

window.onSearchInput = v => {
  state.searchQ = String(v || "");
  if (searchTimer) clearTimeout(searchTimer);
  searchTimer = setTimeout(refreshSearch, 250);
};

window.onComposerInput = v => {
  state.composerText = String(v || "");
  if (state.activeRoomId&&!state.editMessage){chatRuntime.draft(state.activeRoomId,state.composerText);queueWebDraftSync(state.activeRoomId);}
  resizeComposer();
  if (!state.wsAuthed || !state.activeRoomId || !state.ws) return;
  const hasText = String(v || "").trim().length > 0;
  if (hasText && !typingActive) {
    typingActive = true;
    state.ws.send(JSON.stringify({ type: "typing", room_id: state.activeRoomId, is_typing: true }));
  }
  if (typingTimer) clearTimeout(typingTimer);
  typingTimer = setTimeout(() => {
    if (!typingActive) return;
    typingActive = false;
    try { state.ws?.send(JSON.stringify({ type: "typing", room_id: state.activeRoomId, is_typing: false })); } catch {}
  }, 2500);
};

// --- Room/Chat actions ---
window.startDM = async username => {
  const u = String(username || "").trim();
  if (!u) return;
  const res = await api("/rooms", { method: "POST", body: JSON.stringify({ kind: "dm", title: null, members: [u] }) });
  if (res?.id) {
    await loadRooms();
    await selectRoom(res.id);
    return;
  }
  alert(uiText("Не удалось открыть диалог"));
};

window.addGroup = async () => {
  const membersRaw = (prompt(uiText("Участники (usernames через запятую):"), "") || "").trim();
  const members = membersRaw ? membersRaw.split(",").map(s => s.trim()).filter(Boolean) : [];
  const title = (prompt(uiText("Название группы:"), uiText("Группа")) || uiText("Группа")).trim();
  if (!members.length) return;
  const res = await api("/rooms", { method: "POST", body: JSON.stringify({ kind: "group", title, members }) });
  if (res?.id) { await loadRooms(); await selectRoom(res.id); }
};

window.addChannel = async () => {
  const title = (prompt(uiText("Название канала:"), uiText("Мой канал")) || uiText("Мой канал")).trim();
  if (!title) return;
  const res = await api("/rooms", { method: "POST", body: JSON.stringify({ kind: "channel", title, members: [] }) });
  if (res?.id) { await loadRooms(); await selectRoom(res.id); }
};

window.backToList = (fromRoute = false) => { if (!fromRoute) { const url = new URL(location.href); url.searchParams.delete("room"); history.pushState({}, "", url); }  saveWebDraft(); ++roomRequestId; state.activeRoomId = null; state.messages = []; state.roomMembers = []; state.activeDmPeer = null; state.editingRoom = false; state.uiMenu = null; mount(); };

// --- Profile ---
window.showMyProfile = () => {
  state.viewProfile = { ...state.me, isMe: true };
  if (currentProfileUsernameFromRoute()) updateProfileRoute(null, true);
  mount();
};
window.showProfile = async (userId, options = {}) => {
  if (!userId || userId === "null" || userId === "undefined") { console.error("Invalid userId:", userId); return; }
  const user = await api(`/users/${userId}`);
  if (user) {
    state.viewProfile = user;
    if (!options.skipRoute && user.username) updateProfileRoute(user.username, !!options.replaceRoute);
    mount();
  }
};
window.openProfile = (userId, event) => {
  if (event) event.stopPropagation();
  showProfile(userId);
};
window.showProfileByUsername = async (username, options = {}) => {
  const u = String(username || "").trim();
  if (!u) return;
  const user = await api(`/users/by-username/${encodeURIComponent(u)}`);
  if (!user) return;
  state.viewProfile = user;
  if (!options.skipRoute) updateProfileRoute(user.username, !!options.replaceRoute);
  mount();
};
window.openUserProfile = (username, event) => {
  if (event) {
    event.preventDefault();
    event.stopPropagation();
  }
  showProfileByUsername(username);
};
window.closeProfile = () => {
  state.viewProfile = null;
  if (currentProfileUsernameFromRoute()) updateProfileRoute(null, true);
  mount();
};

window.toggleUserBlock = async userId => {
  const profile = state.viewProfile;
  if (!profile || !userId) return;
  const isBlocked = !!profile.blocked_by_me;
  const path = `/users/${userId}/block`;
  const res = await api(path, { method: isBlocked ? "DELETE" : "POST", body: JSON.stringify({}) });
  if (!res?.ok) {
    alert(isBlocked ? uiText("Не удалось разбанить пользователя") : uiText("Не удалось забанить пользователя"));
    return;
  }
  const refreshed = await api(`/users/${userId}`);
  if (refreshed) state.viewProfile = refreshed;
  if (state.activeDmPeer?.id === userId) state.activeDmPeer = refreshed;
  mount();
};

window.deleteConversationWithUser = async userId => {
  const dmRoom = state.rooms.find(r => r.kind === "dm" && String(r.other_user_id || "") === String(userId || ""));
  if (!dmRoom) {
    alert(uiText("Личная переписка не найдена"));
    return;
  }
  if (!confirm(uiText("Удалить всю переписку у обеих сторон? Это действие нельзя отменить."))) return;
  const res = await api(`/rooms/${dmRoom.id}`, { method: "DELETE" });
  if (!res?.ok) {
    alert(uiText("Не удалось удалить переписку"));
    return;
  }
  if (state.activeRoomId === dmRoom.id) {
    state.activeRoomId = null;
    state.messages = [];
    state.roomMembers = [];
    state.activeDmPeer = null;
  }
  state.viewProfile = null;
  await loadRooms();
  if (currentProfileUsernameFromRoute()) updateProfileRoute(null, true);
  mount();
};

window.changePassword = async () => {
  const newPass = prompt(uiText("Новый пароль (минимум 6 символов):"));
  if (!newPass || newPass.length < 6) {
    alert(uiText("Пароль слишком короткий"));
    return;
  }
  const res = await api("/me/password", {
    method: "PUT",
    body: JSON.stringify({ new_password: newPass })
  });
  if (res?.ok) {
    alert(uiText("Пароль изменен! Перезайдите в аккаунт."));
    logout();
  } else {
    alert((""+uiText("Ошибка:")+" ") + (res?.error || uiText("неизвестная")));
  }
};

// --- Admin Panel ---
window.openAdminPanel = () => { state.adminPanel = true; loadAdminUsers(); };
window.closeAdminPanel = () => { state.adminPanel = false; mount(); };

// --- Backup Panel ---
window.openBackupPanel = () => {
  state.backupPanel = true;
  state.recoveryPhrase = null;
  state.showRecoveryPhrase = false;
  mount();
};

window.closeBackupPanel = () => {
  state.backupPanel = false;
  state.recoveryPhrase = null;
  state.showRecoveryPhrase = false;
  mount();
};

window.generateBackup = async () => {
  try {
    if (!state.e2eManager || !state.e2eManager.ready) {
      await initE2E();
    }
    if (!state.e2eManager || !state.e2eManager.ready) {
      alert(uiText("Ключи пока не готовы, но сообщения продолжают работать через защищенный ключ чата."));
      return;
    }

    const recovery = await state.e2eManager.generateRecoveryPhrase();
    state.recoveryPhrase = recovery;
    state.showRecoveryPhrase = true;
    mount();
  } catch (err) {
    console.error('Failed to generate backup:', err);
    alert((""+uiText("Ошибка создания бэкапа:")+" ") + err.message);
  }
};

window.downloadBackup = () => {
  if (!state.recoveryPhrase) return;

  const backupData = {
    phrase: state.recoveryPhrase.phrase,
    checksum: state.recoveryPhrase.checksum,
    timestamp: state.recoveryPhrase.timestamp,
    username: state.me?.username,
    created: new Date(state.recoveryPhrase.timestamp).toISOString()
  };

  const blob = new Blob([JSON.stringify(backupData, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `messenger67-backup-${state.me?.username}-${Date.now()}.json`;
  a.click();
  URL.revokeObjectURL(url);
};

window.copyBackupToClipboard = async () => {
  if (!state.recoveryPhrase) return;

  const backupText = uiText("Messenger 67 Backup\nUsername: {0}\nDate: {1}\n\nRecovery Phrase:\n{2}\n\nChecksum: {3}\n\n⚠️ ВАЖНО: Храните этот бэкап в безопасном месте!\nЛюбой, у кого есть эта фраза, может прочитать ваши сообщения.",state.me?.username,new Date(state.recoveryPhrase.timestamp).toLocaleString(uiLocale()),state.recoveryPhrase.phrase,state.recoveryPhrase.checksum);

  try {
    await navigator.clipboard.writeText(backupText);
    alert(uiText("Бэкап скопирован в буфер обмена"));
  } catch (err) {
    console.error('Failed to copy:', err);
    alert(uiText("Ошибка копирования"));
  }
};

window.restoreFromBackup = () => { alert(uiText("Для безопасного восстановления используйте файл с паролем. Старый формат фразы не заменяет ключи действующего сеанса.")); window.importEncryptedBackup(); };

window.autoSaveBackup = async () => {
  try {
    if (!state.e2eManager || !state.e2eManager.ready) return;

    await state.e2eManager.saveToLocalStorage(state.me.id);
  } catch (err) {
    console.error('Auto-backup failed:', err);
  }
};

// Passphrase-encrypted backup file (PBKDF2-SHA256 + AES-GCM, see crypto.js).
window.exportEncryptedBackup = async () => {
  try {
    if (!state.e2eManager || !state.e2eManager.ready) await initE2E();
    if (!state.e2eManager || !state.e2eManager.ready) {
      alert(uiText("Ключи пока не готовы, но сообщения продолжают работать через защищенный ключ чата."));
      return;
    }
    const pass = prompt(uiText("Придумайте пароль для бэкапа (минимум 8 символов).\nБез него файл восстановить нельзя:"));
    if (pass == null) return;
    if (pass.length < 8) { alert(uiText("Пароль слишком короткий (минимум 8 символов)")); return; }
    const again = prompt(uiText("Повторите пароль:"));
    if (again == null) return;
    if (again !== pass) { alert(uiText("Пароли не совпадают")); return; }

    const text = await encryptBackup(await exportWebAccountKeys(state.e2eManager,state.me.id),pass,{userId:state.me.id,deviceId:getE2EDeviceId()});
    const blob = new Blob([text], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `messenger67-keys-${state.me?.username || 'user'}-${Date.now()}.e2ebackup.json`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  } catch (err) {
    console.error('Encrypted backup export failed:', err);
    alert((""+uiText("Ошибка создания бэкапа:")+" ") + (err?.message || err));
  }
};

let pendingWebRestore=null;
async function prepareWebBackupRestore(keys,userId){
 if(!window.libsignal)await new Promise(resolve=>{const done=()=>{window.removeEventListener('libsignal-ready',done);resolve();};window.addEventListener('libsignal-ready',done);setTimeout(done,5000);});
 if(!window.libsignal||!window.E2EManager)throw Error("Криптографический модуль ещё не готов");
 const stage=await stageWebBackup({keys,userId});
 if(state.me&&!confirm(uiText("Для восстановления потребуется новый вход с паролем аккаунта и двухэтапным подтверждением. Выйти сейчас? Рабочие ключи сохранятся до проверки копии сервером.")))return false;
 if(state.me)await window.logout();pendingWebRestore=stage;
 authReport(uiText("Копия проверена. Войдите в соответствующий аккаунт: ключи будут восстановлены до запуска синхронизации."));
 return true;
}
window.cancelWebBackupRestore=()=>{pendingWebRestore=null;authReport(uiText("Восстановление отменено. Прежние ключи сохранены."));};
window.importEncryptedBackup = () => {
 const input=document.createElement('input');input.type='file';input.accept='.json,application/json';
 input.onchange=async()=>{const file=input.files?.[0];if(!file)return;try{
  if(file.size>32*1024*1024)throw Error("Файл слишком большой");const pass=prompt(uiText("Пароль файла резервной копии (не пароль аккаунта):"));if(pass==null)return;
  const payload=await decryptBackup(await file.text(),pass,state.me?.id),userId=payload.user_id||payload.keys.account_id;
  if(!userId)throw Error("В файле отсутствует привязка к аккаунту");await prepareWebBackupRestore(payload.keys,userId);
 }catch(error){alert(userError(error));}};input.click();
};

// --- Sessions Panel ---
window.openSessionsPanel = async () => {
  state.sessionsPanel = true;
  await loadSessions();
  mount();
};

window.closeSessionsPanel = () => {
  state.sessionsPanel = false;
  mount();
};

async function loadSessions() {
  try {
    state.sessions = await api("/auth/sessions") || [];
    mount();
  } catch (err) {
    console.error('Failed to load sessions:', err);
    state.sessions = [];
  }
}

window.terminateSession = async (sessionId) => {
  if (!confirm(uiText("Завершить эту сессию?"))) return;

  try {
    await api(`/auth/sessions/${sessionId}`, { method: 'DELETE' });
    await loadSessions();
  } catch (err) {
    console.error('Failed to terminate session:', err);
    alert(uiText("Ошибка завершения сессии"));
  }
};

window.terminateOtherSessions = async () => {
  if (!confirm(uiText("Завершить все остальные сессии?\n\nВы останетесь авторизованы только на этом устройстве."))) return;

  try {
    const res = await api('/auth/sessions/terminate-others', { method: 'POST' });
    alert(uiText("Завершено сессий: {0}",res.terminated));
    await loadSessions();
  } catch (err) {
    console.error('Failed to terminate sessions:', err);
    alert(uiText("Ошибка завершения сессий"));
  }
};

window.terminateAllSessions = async () => {
  if (!confirm(uiText("Завершить ВСЕ сессии?\n\nВы будете разлогинены на всех устройствах, включая это."))) return;

  try {
    await api('/auth/sessions/terminate-all', { method: 'POST' });
    // Will be logged out
    resetClientSession();
    mount();
  } catch (err) {
    console.error('Failed to terminate all sessions:', err);
    alert(uiText("Ошибка завершения сессий"));
  }
};

// --- Safety Numbers ---
window.showSafetyNumber = async (userId) => {
  try {
    if (!state.e2eManager || !state.e2eManager.ready) {
      await initE2E();
    }
    if (!state.e2eManager || !state.e2eManager.ready) {
      alert(uiText("Ключи пока не готовы, но сообщения продолжают работать через защищенный ключ чата."));
      return;
    }

    // Get user info
    const user = state.roomMembers?.find(m => m.id === userId);
    if (!user) {
      alert(uiText("Пользователь не найден"));
      return;
    }

    // Get their identity key from server
    const bundle = await api(`/e2e/keys/${userId}`);
    if (!bundle || !bundle.identity_key) {
      alert(uiText("Ключи пользователя не найдены"));
      return;
    }

    // Generate safety number
    const safetyData = await state.e2eManager.generateSafetyNumber(
      state.me.id,
      userId,
      bundle.identity_key
    );

    state.safetyNumberPanel = true;
    state.safetyNumberData = {
      ...safetyData,
      user: user,
      userId: userId
    };
    mount();
  } catch (err) {
    console.error('Failed to generate safety number:', err);
    alert(uiText("Ошибка генерации кода безопасности"));
  }
};

window.closeSafetyNumber = () => {
  state.safetyNumberPanel = false;
  state.safetyNumberData = null;
  mount();
};

window.copySafetyNumber = async () => {
  if (!state.safetyNumberData) return;

  const text = uiText("Safety Number для {0}:\n\n{1}",state.safetyNumberData.user.username,state.safetyNumberData.number);

  try {
    await navigator.clipboard.writeText(text);
    alert(uiText("Код безопасности скопирован"));
  } catch (err) {
    console.error('Failed to copy:', err);
    alert(uiText("Ошибка копирования"));
  }
};

// --- Push Notifications ---
const webNotifications = createWebNotifications({
  api, account:()=>state.me?.id||null,activeRoom: () => state.activeRoomId,
  status({ enabled, permission, registration }) {
    state.notificationsEnabled = enabled;
    state.notificationsPermission = permission;
    state.serviceWorkerRegistration = registration;
    const node = $('#notificationSettingStatus');
    if (node) node.textContent = enabled ? uiText("Включены — нажмите, чтобы отключить") : uiText("Включить в этом браузере");
  },
});
async function initServiceWorker() {
  try { await webNotifications.start(); } catch { /* Settings offers an explicit retry. */ }
}
window.requestNotificationPermission = async () => {
  try { await webNotifications.toggle(); mountPreservingInput(); }
  catch (error) { alert(error.message || uiText("Не удалось включить уведомления")); }
};
window.testWebNotification = async () => {
  try { await webNotifications.test(); } catch (error) { alert(error.message); }
};
navigator.serviceWorker?.addEventListener('message', event => {
  if (event.data?.type === 'PUSH_SUBSCRIPTION_CHANGED' && state.me) void initServiceWorker();
  if(event.data?.type==='OPEN_ROOM')void openBrowserNotification(event.data).catch(error=>toast(userError(error)));
});
async function openBrowserNotification(data){
 const target=notificationTarget(data);if(!target)return;
 if(target.accountId!==undefined&&target.accountId!==state.me?.id){await switchBrowserAccount(target.accountId,target);return;}
 if(!state.me)return;
 const session=captureWebSession();await loadRooms();session.guard();await window.selectRoom(target.roomId,{around:target.messageId});
}

function showNotification(title, body, data = {}) {
  if (!state.notificationsEnabled || !state.serviceWorkerRegistration) return;

  // Don't show notification if tab is focused
  if (document.hasFocus()) return;

  state.serviceWorkerRegistration.showNotification(title, {
    body: body,
    icon: '/icons/app-192.png',
    badge: '/icons/notification-96.png',
    tag: data.tag || 'message',
    data: data,
    requireInteraction: false,
    silent: false
  });
}

function notifyNewMessage(message, room) {
  if (webNotifications.subscribed()) return;
  if (!state.notificationsEnabled) return;
  if (document.hasFocus() && state.activeRoomId === room.id) return;

  const sender = state.roomMembers?.find(m => m.id === message.sender_id);
  const senderName = sender ? displayName(sender) : uiText("Пользователь");

  let title = senderName;
  let body = message._text || message.text || uiText("Новое сообщение");

  // Truncate long messages
  if (body.length > 100) {
    body = body.substring(0, 97) + '...';
  }

  // Add room context for groups/channels
  if (room.kind !== 'dm') {
    title = uiText("{0} в {1}",senderName,room.title || uiText("группе"));
  }

  showNotification(title, body, {
    url: '/',
    roomId: room.id,
    messageId: message.id,
    recipientId:state.me?.id,tag: `message-${state.me?.id}-${room.id}`
  });
}

async function loadAdminUsers() {
  if (!state.adminPanel) return;
  state.adminUsers = await api("/admin/users") || [];
  mount();
}

window.adminResetPassword = async (userId, username) => {
  const newPass = prompt(uiText("Новый пароль для @{0}:",username));
  if (!newPass || newPass.length < 6) { alert(uiText("Пароль слишком короткий")); return; }
  const res = await api(`/admin/users/${userId}/password`, { method: "PUT", body: JSON.stringify({ new_password: newPass }) });
  if (res?.ok) alert(uiText("Пароль изменен!"));
  else alert((""+uiText("Ошибка:")+" ") + (res?.error || uiText("неизвестная")));
};

window.adminToggleBan = async (userId, username, currentBan) => {
  const action = currentBan ? uiText("разбанить") : uiText("забанить");
  if (!confirm(uiText("{0} пользователя @{1}?",action,username))) return;
  const res = await api(`/admin/users/${userId}/ban`, { method: "PUT", body: JSON.stringify({ banned: !currentBan }) });
  if (res?.ok) { alert(uiText("Готово!")); loadAdminUsers(); }
  else alert((""+uiText("Ошибка:")+" ") + (res?.error || uiText("неизвестная")));
};

window.adminDeleteUser = async (userId, username) => {
  if (!confirm(uiText("ПОЛНОСТЬЮ удалить пользователя @{0}?\n\nЭто действие нельзя отменить!\nПользователь исчезнет из всех групп, каналов и чатов.",username))) return;
  const res = await api(`/admin/users/${userId}`, { method: "DELETE" });
  if (res?.ok) { alert(uiText("Пользователь полностью удалён!")); loadAdminUsers(); }
  else alert((""+uiText("Ошибка:")+" ") + (res?.error || uiText("неизвестная")));
};

window.updateMyName = async () => {
  const name = prompt(uiText("Отображаемое имя:"), state.me?.display_name || "");
  if (name !== null) {
    await api("/me", { method: "PUT", body: JSON.stringify({ display_name: name || null }) });
    state.me = await api("/me");
    mount();
  }
};

window.uploadMyAvatar = () => $("#avatarInput")?.click();
window.onAvatarSelected = async () => {
  const input = $("#avatarInput");
  const f = input?.files?.[0];
  if (!f) return;
  const fd = new FormData();
  fd.append("file", f);
  const res = checkUnauthorized(await fetch("/api/me/avatar", { method: "POST", body: fd, credentials: "include" }));
  const data = await res.json().catch(() => null);
  if (data?.avatar_id) { state.me.avatar_id = data.avatar_id; mount(); }
  input.value = "";
};

// --- Room settings ---
window.openRoomSettings = () => { state.editingRoom = true; mount(); };
window.closeRoomSettings = () => { state.editingRoom = false; mount(); };

window.updateRoomTitle = async () => {
  const room = state.rooms.find(r => r.id === state.activeRoomId);
  const title = prompt(uiText("Название группы:"), room?.title || "");
  if (title !== null) {
    await api(`/rooms/${state.activeRoomId}`, { method: "PUT", body: JSON.stringify({ title: title || null }) });
    await loadRooms();
    mount();
  }
};

window.uploadRoomAvatar = () => $("#roomAvatarInput")?.click();
window.onRoomAvatarSelected = async () => {
  const input = $("#roomAvatarInput");
  const f = input?.files?.[0];
  if (!f) return;
  const fd = new FormData();
  fd.append("file", f);
  checkUnauthorized(await fetch(`/api/rooms/${state.activeRoomId}/avatar`, { method: "POST", body: fd, credentials: "include" }));
  await loadRooms();
  mount();
  input.value = "";
};

window.addMember = async () => {
  const username = prompt(uiText("Username нового участника:"));
  if (!username) return;
  const res = await api(`/rooms/${state.activeRoomId}/members`, { method: "POST", body: JSON.stringify({ username }) });
  if (res?.ok) { state.roomMembers = await api(`/rooms/${state.activeRoomId}/members`) || []; mount(); }
  else alert(uiText("Не удалось добавить участника"));
};

window.removeMember = async userId => {
  if (!confirm(uiText("Удалить участника?"))) return;
  await api(`/rooms/${state.activeRoomId}/members/${userId}`, { method: "DELETE" });
  state.roomMembers = await api(`/rooms/${state.activeRoomId}/members`) || [];
  mount();
};

window.toggleAdmin = async (userId, currentRole) => {
  const newRole = currentRole === "admin" ? "member" : "admin";
  await api(`/rooms/${state.activeRoomId}/members/${userId}/role`, { method: "PUT", body: JSON.stringify({ role: newRole }) });
  state.roomMembers = await api(`/rooms/${state.activeRoomId}/members`) || [];
  mount();
};

window.deleteRoom = async () => {
  if (!confirm(uiText("Удалить группу навсегда? Это действие нельзя отменить."))) return;
  await api(`/rooms/${state.activeRoomId}`, { method: "DELETE" });
  state.activeRoomId = null;
  state.messages = [];
  state.editingRoom = false;
  await loadRooms();
  mount();
};

window.leaveRoom = async () => {
  if (!confirm(uiText("Покинуть группу?"))) return;
  await api(`/rooms/${state.activeRoomId}/members/${state.me.id}`, { method: "DELETE" });
  state.activeRoomId = null;
  state.messages = [];
  state.editingRoom = false;
  await loadRooms();
  mount();
};

window.deleteDMChat = async () => {
  if (!confirm(uiText("Удалить личную переписку? Все сообщения будут безвозвратно удалены."))) return;
  await api(`/rooms/${state.activeRoomId}`, { method: "DELETE" });
  state.activeRoomId = null;
  state.messages = [];
  state.editingRoom = false;
  state.activeDmPeer = null;
  await loadRooms();
  mount();
};

window.showAddMemberModal = () => {
  const modal = document.createElement("div");
  modal.id = "addMemberModal";
  modal.style.cssText = `
    position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.5);z-index:10000;
    display:flex;align-items:center;justify-content:center;
  `;

  // Get users from existing DMs
  const existingUsers = state.rooms
    .filter(r => r.kind === 'dm')
    .map(r => {
      const member = state.roomMembers?.find(m => m.id !== state.me?.id);
      return member ? {
        id: member.id,
        username: member.username,
        display_name: member.display_name,
        avatar_id: member.avatar_id
      } : null;
    })
    .filter(Boolean);

  modal.innerHTML = `
    <div style="background:var(--bg);border-radius:12px;padding:20px;max-width:500px;max-height:70vh;overflow:auto;width:90%;">
      <h3>${escapeUi(uiText("Добавить участников"))}</h3>
      <div style="margin-bottom:15px;">
        <input type="text" id="memberSearch" placeholder="${escapeUi(uiText("Поиск пользователей..."))}"
               style="width:100%;padding:8px;border:1px solid var(--line);border-radius:6px;background:var(--bg);color:var(--text);">
      </div>
      <div id="memberList" style="max-height:300px;overflow-y:auto;margin-bottom:15px;">
        ${existingUsers.map(u => `
          <div style="display:flex;align-items:center;gap:10px;padding:8px;border:1px solid var(--line);border-radius:6px;margin-bottom:5px;">
            <input type="checkbox" id="check-${u.id}" value="${u.id}" style="cursor:pointer;">
            <label for="check-${u.id}" style="flex:1;display:flex;align-items:center;gap:10px;cursor:pointer;">
              ${u.avatar_id ? `<img src="${avatarUrl(u.avatar_id)}" style="width:32px;height:32px;border-radius:50%;object-fit:cover;">` : `<div style="width:32px;height:32px;border-radius:50%;background:var(--accent);display:flex;align-items:center;justify-content:center;">👤</div>`}
              <div>
                <div style="font-weight:bold;">${escapeHtml(displayName(u))}</div>
                <div style="font-size:12px;opacity:0.7;">@${escapeHtml(u.username)}</div>
              </div>
            </label>
          </div>
        `).join('')}
      </div>
      <div style="display:flex;gap:10px;justify-content:flex-end;">
        <button class="btn" onclick="closeAddMemberModal()">${escapeUi(uiText("Отмена"))}</button>
        <button class="btn primary" onclick="addSelectedMembers()">${escapeUi(uiText("Добавить выбранных"))}</button>
      </div>
    </div>
  `;

  document.body.appendChild(modal);

  // Add search functionality
  const searchInput = modal.querySelector('#memberSearch');
  const memberList = modal.querySelector('#memberList');

  searchInput?.addEventListener('input', async (e) => {
    const query = e.target.value.trim();

    if (query.length < 2) {
      // Show existing users if query is too short
      memberList.innerHTML = existingUsers.map(u => `
        <div style="display:flex;align-items:center;gap:10px;padding:8px;border:1px solid var(--line);border-radius:6px;margin-bottom:5px;">
          <input type="checkbox" id="check-${u.id}" value="${u.id}" style="cursor:pointer;">
          <label for="check-${u.id}" style="flex:1;display:flex;align-items:center;gap:10px;cursor:pointer;">
            ${u.avatar_id ? `<img src="${avatarUrl(u.avatar_id)}" style="width:32px;height:32px;border-radius:50%;object-fit:cover;">` : `<div style="width:32px;height:32px;border-radius:50%;background:var(--accent);display:flex;align-items:center;justify-content:center;">👤</div>`}
            <div>
              <div style="font-weight:bold;">${escapeHtml(displayName(u))}</div>
              <div style="font-size:12px;opacity:0.7;">@${escapeHtml(u.username)}</div>
            </div>
          </label>
        </div>
      `).join('');
      return;
    }

    try {
      // Search users via API
      const searchResults = await api(`/users/search?q=${encodeURIComponent(query)}`);

      memberList.innerHTML = searchResults.map(u => `
        <div style="display:flex;align-items:center;gap:10px;padding:8px;border:1px solid var(--line);border-radius:6px;margin-bottom:5px;">
          <input type="checkbox" id="check-${u.id}" value="${u.id}" style="cursor:pointer;">
          <label for="check-${u.id}" style="flex:1;display:flex;align-items:center;gap:10px;cursor:pointer;">
            ${u.avatar_id ? `<img src="${avatarUrl(u.avatar_id)}" style="width:32px;height:32px;border-radius:50%;object-fit:cover;">` : `<div style="width:32px;height:32px;border-radius:50%;background:var(--accent);display:flex;align-items:center;justify-content:center;">👤</div>`}
            <div>
              <div style="font-weight:bold;">${escapeHtml(displayName(u))}</div>
              <div style="font-size:12px;opacity:0.7;">@${escapeHtml(u.username)}</div>
            </div>
          </label>
        </div>
      `).join('');
    } catch (error) {
      console.error('Search error:', error);
      memberList.innerHTML = `<div style="padding:20px;text-align:center;color:var(--muted);">${escapeUi(uiText("Ошибка поиска"))}</div>`;
    }
  });

  // Show existing users initially
  if (searchInput) {
    searchInput.value = '';
    searchInput.dispatchEvent(new Event('input'));
  }
};

window.closeAddMemberModal = () => {
  const modal = document.getElementById('addMemberModal');
  if (modal) modal.remove();
};

window.addSelectedMembers = async () => {
  const modal = document.getElementById('addMemberModal');
  const searchInput = modal?.querySelector('#memberSearch');
  const query = searchInput?.value.trim();

  // Legacy mass-add shortcut disabled for safety.
  if (false) {
    try {
      const result = await api(`/rooms/${state.activeRoomId}/members`, {
        method: "POST",
        body: JSON.stringify({ username: query })
      });

      closeAddMemberModal();
      state.roomMembers = await api(`/rooms/${state.activeRoomId}/members`) || [];
      mount();

      if (result.command) {
        alert(uiText("Добавлено {0} пользователей",result.added || 0));
      } else {
        alert(uiText("Пользователь добавлен"));
      }
    } catch (error) {
      console.error('Add member error:', error);
      alert((""+uiText("Ошибка добавления:")+" ") + (error.message || uiText("неизвестная ошибка")));
    }
    return;
  }

  // Normal user selection flow
  const checkboxes = modal?.querySelectorAll('input[type="checkbox"]:checked');

  if (!checkboxes || checkboxes.length === 0) {
    alert(uiText("Выберите хотя бы одного пользователя"));
    return;
  }

  const userIds = Array.from(checkboxes).map(cb => cb.value);

  try {
    for (const userId of userIds) {
      await api(`/rooms/${state.activeRoomId}/members`, {
        method: "POST",
        body: JSON.stringify({ user_id: userId })
      });
    }

    closeAddMemberModal();
    state.roomMembers = await api(`/rooms/${state.activeRoomId}/members`) || [];
    mount();
    alert(uiText("Добавлено: {0} {1}",userIds.length,plural(userIds.length, uiText("участник"), uiText("участника"), uiText("участников"))));
  } catch (error) {
    console.error('Add members error:', error);
    alert((""+uiText("Ошибка добавления:")+" ") + (error.message || uiText("неизвестная ошибка")));
  }
};

window.handleMessageKeydown = (event) => {
  if(event.isComposing||event.keyCode===229)return;
  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault();
    doSend();
  }
};

window.startEditMessage = (msgId, event) => {
  if (event) event.stopPropagation();
  const msg = state.messages.find(m => m.id === msgId);
  if (!msg || String(msg.sender_id || "") !== String(state.me?.id || "") || msg.type !== "text") return;
  if(parseMessagePayload(msg._text??msg.text))return toast(uiText("Такое событие нельзя изменять"));
  state.editMessage = msg;
  state.replyTo = null;
  state.composerText = String(msg._text ?? msg.text ?? "");
  mountPreservingInput();
  setTimeout(() => {
    const inp = $("#inp");
    if (!inp) return;
    inp.focus();
    try { inp.setSelectionRange(inp.value.length, inp.value.length); } catch {}
  }, 50);
};

window.cancelEdit = () => {
  state.editMessage = null;
  state.composerText = "";
  mount();
  setTimeout(() => {
    const inp = $("#inp");
    if (inp) inp.focus();
  }, 50);
};

window.deleteMessage = async msgId => {
  if (!confirm(uiText("Удалить сообщение у всех участников?"))) return;
  const roomId = state.activeRoomId, account = state.me?.id;
  try {
    const result = await api(`/rooms/${roomId}/messages/${msgId}`, { method: 'DELETE' });
    if (!result?.ok) throw new Error("Не удалось удалить сообщение");
    await chatRuntime.local()?.removeMessage(roomId, msgId);
    if (account === state.me?.id && roomId === state.activeRoomId) { state.messages = state.messages.filter(m => m.id !== msgId); if (state.editMessage?.id === msgId) state.editMessage = null; render(); }
  } catch (error) { toast(userError(error)); }
};

// Global error handler
window.alert = message => toast(message);
window.addEventListener('error', (event) => {
  console.error('Global error:', event.error);
  alert((""+uiText("Произошла ошибка:")+" ") + (event.error?.message || uiText("неизвестная ошибка")));
});

// Global unhandled promise rejection handler
window.addEventListener('unhandledrejection', (event) => {
  console.error('Unhandled promise rejection:', event.reason);
  alert((""+uiText("Произошла ошибка:")+" ") + (event.reason?.message || uiText("неизвестная ошибка")));
});

const webDrafts = new Map();
const chatRuntime = createChatRuntime({ state, request: api, decode: decodeForWeb, buildText: buildTextMessage,
  mount, refresh: mountPreservingInput, render: renderMessages, report: toast, drafts: webDrafts, loadRooms,eventHistory:true });
async function decodeForWeb(roomId, message) {
  const decoded = { ...message };
  if(message.meta?.media_encryption && !message._mediaDetails)try {
    const info=message.meta.media_encryption;
    const key=await decryptMsg(roomId,{...info.key_message,room_id:roomId,sender_id:message.sender_id,id:message.id+'-file-key',type:'text'});
    decoded._mediaDetails=mediaDetails(JSON.parse(key).details);
    decoded.width=decoded._mediaDetails.width || message.width;decoded.height=decoded._mediaDetails.height || message.height;
    if(decoded._mediaDetails.duration_ms)decoded.duration_sec=decoded._mediaDetails.duration_ms/1000;
  }catch{}
  if(message.type!=='text' && message.meta?.caption_message)decoded.text=await decryptMsg(roomId,{...message.meta.caption_message,room_id:roomId,sender_id:message.sender_id,type:'text',id:message.id+'-caption'});
  if(message.type==='text'){decoded._text=await decryptMsg(roomId,message);decoded._decrypted=!new Set(['[Encrypted for another device]',uiText("🔐 [Не удалось расшифровать E2E сообщение]"),uiText("❌ [Ошибка E2E расшифровки]"),uiText("🔐 [Зашифровано]"),uiText("❌ Ошибка ключа"),GROUP_KEY_UNAVAILABLE,uiText("🔐 Не удалось получить ключ сообщения")]).has(decoded._text);}
  if (message.reply_to) {
    const preview = message.meta?.reply_preview;
    const original = state.messages.find(m => m.id === message.reply_to.id);
    let text = original?._text;
    if (preview) text = await decryptMsg(roomId, { ...preview, type: 'text', room_id: roomId, sender_id: message.sender_id, id: message.id + '-reply' });
    decoded.reply_to = { ...message.reply_to, sender_name: message.reply_to.sender_name || message.reply_to.sender_display_name || message.reply_to.sender_username || uiText("Пользователь"), text: text ?? (preview ? uiText("Ответ на сообщение") : message.reply_to.text || uiText("Вложение")) };
  }
  return decoded;
}
// Shared feature adapters capture account identity before every asynchronous action.
let webDraftCoordinator=null,webDraftOwner=null;const webDraftTimers=new Map();
const webDraftReplies=new Map(),draftConflicts=new Map(),webMessageTTLs=new Map(),expressiveLoads=new Map();
const featureRequest=(path,options={})=>api(path,{...options,body:options.body===undefined||options.body instanceof FormData||typeof options.body==='string'?options.body:JSON.stringify(options.body)});
function webActionDialog(title){const dialog=document.createElement('dialog');dialog.className='actions-dialog';const heading=document.createElement('h2');heading.textContent=title;const content=document.createElement('div'),status=document.createElement('p'),close=document.createElement('button');status.setAttribute('role','status');close.textContent=uiText("Закрыть");close.onclick=()=>dialog.close();dialog.append(heading,content,status,close);dialog.addEventListener('close',()=>dialog.remove(),{once:true});document.body.append(dialog);dialog.showModal();return {dialog,content,status};}
function dialogButton(parent,label,action,status){const button=document.createElement('button');button.textContent=label;button.type='button';button.onclick=async()=>{button.disabled=true;try{await action();}catch(error){if(status)status.textContent=userError(error);else toast(userError(error));}finally{button.disabled=false;}};parent.append(button);return button;}
function activeDraftValue(room){return {text:webDrafts.get(room)||'',reply:state.activeRoomId===room?state.replyTo||null:webDraftReplies.get(room)||null};}
async function applyWebDraft(room,value,expected){if(expected&&JSON.stringify(draftValue(activeDraftValue(room)))!==JSON.stringify(draftValue(expected)))return false;const next=draftValue(value);chatRuntime.draft(room,next.text);webDraftReplies.set(room,next.reply);if(state.activeRoomId===room&&!state.editMessage){state.composerText=next.text;state.replyTo=next.reply;const input=$('#inp');if(input){input.value=next.text;resizeComposer();}}await chatRuntime.local()?.set('draft-reply:'+room,next.reply);return true;}
function draftConflictDialog(room,value){draftConflicts.set(room,value);const existing=document.getElementById('draftConflict');if(existing)existing.remove();const {dialog,content,status}=webActionDialog(uiText("Черновик изменён на другом устройстве"));dialog.id='draftConflict';const local=document.createElement('p'),remote=document.createElement('p'),merged=document.createElement('textarea');local.textContent=(""+uiText("На этом устройстве:")+" ")+value.local.text;remote.textContent=(""+uiText("На другом устройстве:")+" ")+value.remote.text;merged.value=value.local.text+'\n'+value.remote.text;merged.setAttribute('aria-label',uiText("Объединённый черновик"));content.append(local,remote,merged);const choose=async selected=>{await chatRuntime.local().set('draft-sync:'+room,{revision:value.revision,value:value.remote});await applyWebDraft(room,selected);draftConflicts.delete(room);dialog.close();await ensureWebDraftSync()?.sync(room);};dialogButton(content,uiText("Оставить текущий"),()=>choose(activeDraftValue(room)),status);dialogButton(content,uiText("Принять с другого устройства"),()=>choose(value.remote),status);dialogButton(content,uiText("Сохранить объединённый"),()=>choose({...value.local,text:merged.value}),status);}
function ensureWebDraftSync(){const account=state.me?.id,local=chatRuntime.local();if(!account||!local)return null;if(webDraftOwner===account&&webDraftCoordinator)return webDraftCoordinator;webDraftCoordinator?.stop();webDraftOwner=account;webDraftCoordinator=new DraftSync({
 state:room=>local.get('draft-sync:'+room,null),local:room=>activeDraftValue(room),fetch:room=>featureRequest(`/rooms/${room}/draft`),
 encode:async(room,value)=>{const body=await buildTextMessage(room,state.rooms.find(r=>r.id===room),JSON.stringify({format:'67chat-draft-v1',draft:draftValue(value)}));if(state.me?.id!==account)throw Error("Сеанс изменился");return wrapEncryptedBody(body);},
 decode:async(room,remote)=>{const body=unwrapEncryptedBody(remote.encrypted_content),message=await decodeForWeb(room,{...body,id:'draft-'+remote.revision,room_id:room,sender_id:account,type:'text',created_at:new Date().toISOString()});if(message._decrypted===false)throw Error("Не удалось расшифровать черновик");const text=message._text??message.text??'';try{const parsed=JSON.parse(text);if(parsed.format==='67chat-draft-v1')return draftValue(parsed.draft);}catch{}return {text,reply:null};},
 apply:async(room,value,expected)=>state.me?.id===account?applyWebDraft(room,value,expected):false,
 saveState:(room,value)=>state.me?.id===account?local.set('draft-sync:'+room,value):undefined,
 put:async(room,value)=>{if(state.me?.id!==account)throw Error("Сеанс изменился");return featureRequest(`/rooms/${room}/draft`,{method:'PUT',body:value});},
 conflict:(room,value)=>{if(state.me?.id===account)draftConflictDialog(room,value);}
 });return webDraftCoordinator;}
function queueWebDraftSync(room){clearTimeout(webDraftTimers.get(room));if(!room||state.editMessage)return;webDraftReplies.set(room,state.replyTo||null);void chatRuntime.local()?.set('draft-reply:'+room,state.replyTo||null);webDraftTimers.set(room,setTimeout(()=>{webDraftTimers.delete(room);void ensureWebDraftSync()?.sync(room).catch(error=>toast(userError(error)));},750));}
async function syncWebDraft(room){if(!room||state.editMessage)return;const local=chatRuntime.local(),account=state.me?.id;if(!webDraftReplies.has(room))webDraftReplies.set(room,await local?.get('draft-reply:'+room,null));if(state.me?.id!==account)return;if(state.activeRoomId===room&&!state.replyTo&&webDraftReplies.get(room)){state.replyTo=webDraftReplies.get(room);await mountPreservingInput();}await ensureWebDraftSync()?.sync(room);}
async function eraseDeletedWebAccount(account){
 if(state.me?.id===account)resetClientSession();
 await eraseAccountStore(account);await window.SigStore?.eraseAccount(account);
 localStorage.removeItem(`e2e_device_id_${account}`);await mount();
}
async function exportWebMedia(message){
 const session=captureWebSession();session.guard();const account=state.me?.id;if(!account)throw Error("Сначала войдите");
 if(Number(message.meta?.media_encryption?.size||message.media_size||0)>20*1024*1024)throw Error("Вложение больше лимита экспорта 20 МБ");
 const source=await resolveWebMedia(message);session.guard();const response=await fetch(source.url,{credentials:'include',signal:AbortSignal.timeout(30000)});
 if(!response.ok)throw Error("Не удалось загрузить вложение");const data=new Uint8Array(await response.arrayBuffer());
 session.guard();if(data.length>20*1024*1024)throw Error("Вложение больше лимита экспорта 20 МБ");if(state.me?.id!==account)throw Error("Сеанс изменился");
 return {name:source.name||'attachment',mime:source.mime||'application/octet-stream',base64:bytesToBase64(data)};
}
function pauseMessageMedia(){stopVoicePlayers();for(const player of document.querySelectorAll('#msgs audio,#msgs video,.media-viewer audio,.media-viewer video'))player.pause();webMedia.stop();}
function webFeatureContext(targetMessage=null){
 const account=state.me?.id,room=state.rooms.find(r=>r.id===state.activeRoomId),local=chatRuntime.local(),scope=captureWebSession();
 const request=async(path,options={})=>{
  // A detached call must still release its own server lease with its original
  // account cookie. The server additionally enforces exact owning session ID.
  const releasing=options.method==='POST'&&/^\/rooms\/[a-f0-9-]{36}\/calls\/leave$/i.test(path)&&/^[a-f0-9-]{36}$/i.test(options.body?.lease_id||'');
  if(!releasing)scope.guard();
  const result=await featureRequest(path,{...options,headers:{...options.headers,'X-Account-Id':account}});
  if(path==='/me/account/delete'&&options.method==='POST'){await eraseDeletedWebAccount(account);return result;}
  if(!releasing)scope.guard();return result;
 };
 return {request,me:state.me,room,rooms:state.rooms,targetMessage,
 pauseMedia:scope.sync(pauseMessageMedia),sendCatalogAsset:scope.wrap(sendWebCatalogAsset),exportMedia:scope.wrap(exportWebMedia),
 openRoom:scope.wrap(async(id,message)=>{if(!/^[a-f0-9-]{36}$/i.test(id))return window.startDM(id);await window.selectRoom(id,{around:message});}),
 enqueue:scope.wrap((id,text)=>chatRuntime.enqueue({roomId:id,text})),
 refresh:scope.wrap(async()=>{await loadRooms();scope.guard();await mountPreservingInput();}),
 local:local?{
  secretStore:local,
  get:scope.wrap((...args)=>local.get(...args)),set:scope.wrap((...args)=>local.set(...args)),
  deviceId:scope.wrap(async()=>{await initE2E();scope.guard();if(!state.e2eReady)throw Error('Ключи ещё не готовы');return String(getE2EDeviceId());}),
  isCurrent:()=>{try{scope.guard();return true;}catch{return false;}},
  applyQuality:scope.wrap(applyQualityPreference),clearMediaCache:scope.sync(()=>webMediaUrls.clear()),
  listAccounts:scope.wrap(async()=>(await request('/auth/accounts')).accounts),
  switchAccount:(...args)=>{scope.guard();return switchBrowserAccount(...args);},addAccount:()=>{scope.guard();return addBrowserAccount();},
  insertEmoji:scope.sync(emoji=>{const input=$('#inp');if(!input)return;const start=input.selectionStart??input.value.length,end=input.selectionEnd??start;input.setRangeText(emoji,start,end,'end');window.onComposerInput(input.value);input.focus();}),
  exportKeys:scope.wrap(async()=>{await initE2E();scope.guard();if(!state.e2eManager)throw Error("Ключи ещё не готовы");return exportWebAccountKeys(state.e2eManager,account);}),
  restoreBackup:scope.wrap(keys=>prepareWebBackupRestore(keys,account)),onDeleted:()=>eraseDeletedWebAccount(account)
 }:null,
 decode:scope.wrap(async message=>{const decoded=await decodeForWeb(message.room_id,message);return {...decoded,text:decoded._text??decoded.text};}),
 buildText:scope.wrap((id,text)=>buildTextMessage(id,state.rooms.find(r=>r.id===id),text)),
 setDraft:scope.wrap(async(id,text,reply)=>{await applyWebDraft(id,{text,reply:reply===undefined?webDraftReplies.get(id)||null:reply});scope.guard();queueWebDraftSync(id);}),
 draftReply:scope.sync(id=>webDraftReplies.get(id)),report:error=>{try{scope.guard();}catch{return;}toast(userError(error));}};
}
window.openConversationTool=(mode='conversation',messageId)=>void openConversationFeature(mode,webFeatureContext(state.messages.find(m=>m.id===messageId))).catch(error=>toast(userError(error)));
window.openExpressiveTool=mode=>void openExpressiveFeature(mode,webFeatureContext()).catch(error=>toast(userError(error)));
window.openCallTools=async(mode='history')=>{try{const {openCallFeature}=await import('./call-features.js');await openCallFeature(mode,webFeatureContext());}catch(error){toast(userError(error));}};
window.openAccountTool=mode=>void openAccountFeature(mode,webFeatureContext()).catch(error=>toast(userError(error)));
window.openQualityTool=()=>void openQualityFeatures(webFeatureContext()).catch(error=>toast(userError(error)));
window.openDraftConflict=()=>{const entry=draftConflicts.get(state.activeRoomId);if(entry)draftConflictDialog(state.activeRoomId,entry);else window.openConversationTool('drafts');};
window.markRoomRead=async()=>{const room=state.activeRoomId;if(!room)return;try{await featureRequest(`/rooms/${room}/read`,{method:'POST',body:{}});await loadRooms();await mountPreservingInput();}catch(error){toast(userError(error));}};
window.markAllRoomsRead=async()=>{try{await featureRequest('/me/read-all',{method:'POST',body:{}});await loadRooms();await mountPreservingInput();}catch(error){toast(userError(error));}};
window.jumpToUnread=async()=>{try{const room=state.activeRoomId;if(!room)return;const result=await api(`/rooms/${room}/history?limit=1`);if(result.first_unread_id)await chatRuntime.jump(result.first_unread_id);else toast(uiText("Непрочитанных сообщений нет"));}catch(error){toast(userError(error));}};
window.insertMessageFormat=kind=>{const input=$('#inp');if(!input)return;const markers={bold:['**','**'],italic:['__','__'],strike:['~~','~~'],spoiler:['||','||'],code:['`','`'],block:['```\n','\n```'],quote:['> ','']},pair=markers[kind];if(!pair)return;const start=input.selectionStart??0,end=input.selectionEnd??start,value=input.value.slice(start,end)||uiText("текст");input.setRangeText(pair[0]+value+pair[1],start,end,'select');window.onComposerInput(input.value);input.focus();};
window.openFormatTools=()=>{const {dialog,content}=webActionDialog(uiText("Форматирование"));for(const [kind,title] of Object.entries({bold:uiText("Жирный"),italic:uiText("Курсив"),strike:uiText("Зачёркнутый"),spoiler:uiText("Скрытый текст"),code:uiText("Код"),block:uiText("Блок кода"),quote:uiText("Цитата")}))dialogButton(content,title,async()=>{dialog.close();window.insertMessageFormat(kind);});};
window.openHiddenChats=()=>{const {content,status}=webActionDialog(uiText("Скрытые чаты"));for(const room of state.rooms.filter(r=>r.preferences?.hidden)){const row=document.createElement('p');row.textContent=room.title||room.other_display_name||room.other_username||uiText("Чат");dialogButton(row,uiText("Показать"),async()=>{await chatActions.preference(room.id,{hidden:false});row.remove();},status);dialogButton(row,uiText("Открыть"),()=>window.selectRoom(room.id),status);content.append(row);}};
window.manageFolderOrder=()=>{const {dialog,content,status}=webActionDialog(uiText("Порядок папок")),items=[...(state.folders||[])];const draw=()=>{content.replaceChildren();items.forEach((folder,index)=>{const row=document.createElement('p');row.textContent=folder.title+' · '+(folder.unread_count||0);dialogButton(row,'↑',async()=>{if(index){[items[index-1],items[index]]=[items[index],items[index-1]];draw();}},status);dialogButton(row,'↓',async()=>{if(index<items.length-1){[items[index+1],items[index]]=[items[index],items[index+1]];draw();}},status);content.append(row);});dialogButton(content,uiText("Сохранить порядок"),async()=>{await featureRequest('/me/folders/order',{method:'PUT',body:{items:items.map(({id,revision})=>({id,revision}))}});await loadRooms();await mountPreservingInput();dialog.close();},status);};draw();};
window.openGlobalHistorySearch=()=>{const account=state.me?.id,{dialog,content,status}=webActionDialog(uiText("Поиск по перепискам")),input=document.createElement('input'),files=document.createElement('input'),fileLabel=document.createElement('label'),results=document.createElement('div');input.placeholder=uiText("Текст, имя файла или подпись");input.setAttribute('aria-label',uiText("Поиск по сообщениям"));files.type='checkbox';fileLabel.append(files,document.createTextNode(uiText("Только вложения")));content.append(input,fileLabel,results);let generation=0;dialog.addEventListener('close',()=>++generation);dialogButton(content,uiText("Найти"),async()=>{const mine=++generation,q=input.value.trim().toLocaleLowerCase();if(q.length<2)throw Error("Введите хотя бы два символа");results.replaceChildren();await chatRuntime.syncHidden();let checked=0,found=0;for(const room of state.rooms.filter(room=>!room.preferences?.hidden)){if(mine!==generation||state.me?.id!==account)return;const messages=await fetchAllRoomMessages(room.id,async(id,before,limit)=>api(`/rooms/${id}/history?${new URLSearchParams({limit,...(before?{before}:{})})}`),{cancelled:()=>mine!==generation||state.me?.id!==account});for(const raw of messages){if(mine!==generation||state.me?.id!==account)return;const message=await decodeForWeb(room.id,raw);if(await chatRuntime.local()?.get('hidden:'+message.id,null))continue;await chatRuntime.local()?.saveMessages(room.id,[message]);checked++;const text=message._text??message.text??'',payload=parseMessagePayload(text);if(isExpressiveControl(payload)||files.checked&&message.type==='text')continue;const searchable=[text,message._mediaDetails?.name,message.meta?.media_encryption?.name,message.meta?.filename,message.media_mime].filter(Boolean).join(' ').toLocaleLowerCase();if(searchable.includes(q)){const hit=document.createElement('button');hit.dataset.searchMessageId=message.id;hit.textContent=`${room.title||room.other_display_name||room.other_username||uiText("Чат")} · ${new Date(message.created_at).toLocaleDateString(uiLocale())} · ${payload?expressiveSummary(payload):text||message.meta?.media_encryption?.name||uiText("Вложение")}`;hit.onclick=()=>{dialog.close();void window.selectRoom(room.id,{around:message.id});};results.append(hit);found++;}}status.textContent=uiText("Проверено сообщений: {0}; найдено: {1}",checked,found);}status.textContent+= (" "+uiText("· Вся доступная история проверена")+"");},status);dialogButton(content,uiText("Остановить"),async()=>{++generation;status.textContent=uiText("Поиск остановлен; результаты сохранены");},status);};
async function syncCanonicalWebRoute(){if(!state.me)return;const link=parse67Link(location.href);if(link?.type==='action')return link.action==='saved'?window.openSavedMessages():window.openNewChat();if(link?.type==='chat')return window.selectRoom(link.roomId,{fromRoute:true,around:link.messageId});if(link?.type==='profile')return window.openUserProfile(link.username);if(link?.type==='invite'){const joined=await api(`/invites/${encodeURIComponent(link.token)}/join`,{method:'POST',body:'{}'});await loadRooms();return window.selectRoom(joined.id||joined.room_id,{fromRoute:true});}const legacy=new URL(location.href).searchParams,room=legacy.get('room');if(room)return window.selectRoom(room,{fromRoute:true,around:legacy.get('msg')||undefined});await syncProfileRoute();}
function cancelExpressiveLoads(exceptKey){for(const [key,jobs]of expressiveLoads)if(key!==exceptKey){for(const job of jobs)job.cancel();expressiveLoads.delete(key);}}
async function hydrateExpressiveMessages(retry=false){
 const room=state.activeRoomId,account=state.me?.id,generation=browserSessionGeneration(),targets=[...document.querySelectorAll('[data-expressive-message]')];
 const key=room&&account?expressiveHistoryKey(account,location.origin,String(generation),room):null;cancelExpressiveLoads(key);
 if(!room||!account||!targets.length){cancelExpressiveLoads();return;}
 const context=webFeatureContext(),local=chatRuntime.local(),current=()=>state.me?.id===account&&browserSessionGeneration()===generation&&state.activeRoomId===room;
 const job=expressiveEventHistory({key,version:'session',retry,roomId:room,
  fetchPage:(id,before,limit)=>context.request(`/rooms/${id}/history?${new URLSearchParams({limit,...(before?{before}:{})})}`),
  decode:async row=>{const decoded=await context.decode(row);return await local?.get('hidden:'+row.id,null)?{...decoded,hidden:true}:decoded;},retain:row=>!!parseMessagePayload(row.text)});
 if(!expressiveLoads.has(key))expressiveLoads.set(key,new Set());expressiveLoads.get(key).add(job);
 for(const target of targets)if(!target.dataset.historyReady){const status=document.createElement('span'),cancel=document.createElement('button');status.textContent=uiText('Проверка истории…');cancel.textContent=uiText('Отменить загрузку');cancel.onclick=()=>{for(const pending of expressiveLoads.get(key)||[])pending.cancel();};target.replaceChildren(status,cancel);}
 try{
  const history=await job.promise;if(!current())return;
  const events=mergeMessages(state.messages.map(m=>({...m,text:m._text??m.text})),history).filter(m=>!m.hidden&&!m.deleted&&!m.deleted_at);
  for(const target of targets){if(!current()||!target.isConnected)return;const message=events.find(m=>m.id===target.dataset.expressiveMessage);if(!message)continue;const node=await renderExpressiveMessage(message,events,context);if(node&&target.isConnected&&current()){target.dataset.historyReady='true';target.replaceChildren(node);}}
 }catch(error){
  if(!current())return;
  for(const target of targets)if(target.isConnected){delete target.dataset.historyReady;const status=document.createElement('span'),button=document.createElement('button');status.textContent=String(error.message).includes('cancelled')?uiText('Загрузка истории отменена'):userError(error);button.textContent=uiText('Загрузить результаты');button.onclick=()=>void hydrateExpressiveMessages(true);target.replaceChildren(status,button);}
 }finally{const jobs=expressiveLoads.get(key);jobs?.delete(job);if(!jobs?.size)expressiveLoads.delete(key);}
}

async function encryptWebFile(file, roomId, details) {
    const owner=state.me?.id;if(!owner)throw Error("Сначала войдите");
    const key = await crypto.subtle.generateKey({ name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']), iv = crypto.getRandomValues(new Uint8Array(12));
    const data = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, await file.arrayBuffer());
    if(state.me?.id!==owner)throw Error("Сеанс изменился");
    const wrapped = await buildTextMessage(roomId, state.rooms.find(r => r.id === roomId), JSON.stringify({ key: bytesToBase64(new Uint8Array(await crypto.subtle.exportKey('raw', key))), iv: bytesToBase64(iv),details:mediaDetails(details) }));
    if (!wrapped.meta?.encrypted && !wrapped.meta?.e2e_group) throw new Error("Не удалось защитить ключ вложения. Повторите попытку.");
    if(state.me?.id!==owner)throw Error("Сеанс изменился");
    return { file: new File([data], 'attachment.bin', { type: 'application/octet-stream' }), kind: 'file', meta: { media_encryption: { v: 2, key_message: wrapped, name: file.name, mime: file.type || 'application/octet-stream', size: file.size } } };
  }
async function sendWebCatalogAsset(roomId,item){
 const session=captureWebSession();session.guard();const owner=state.me?.id;if(!owner)throw Error("Сначала войдите");
 if(!/^[a-f0-9-]{36}$/i.test(item.media_id)||!['sticker','gif'].includes(item.kind))throw Error("Некорректное вложение");
 const response=await fetch(accountMediaUrl('/api/media/'+encodeURIComponent(item.media_id)),{credentials:'include',signal:AbortSignal.timeout(30000)});
 if(!response.ok)throw Error("Не удалось загрузить вложение набора");
 const blob=await response.blob();session.guard();if(blob.size>20*1024*1024)throw Error("Вложение больше 20 МБ");
 const mime=item.mime||blob.type;if(!/^(image\/(png|jpeg|webp|gif)|video\/(mp4|webm))$/.test(mime))throw Error("Формат вложения не поддерживается");
 if(state.me?.id!==owner)throw Error("Сеанс изменился");
 const prepared=await encryptWebFile(new File([blob],'attachment',{type:mime}),roomId,{expressive:{kind:item.kind,label:item.label||''}});
 session.guard();if(state.me?.id!==owner)throw Error("Сеанс изменился");
 await chatRuntime.enqueue({roomId,upload:{file:prepared.file,kind:prepared.kind},body:{type:'media',meta:prepared.meta}});
}
const webMediaUrls = createMediaObjectCache();
const webMedia = createMediaController({ context: () => ({ userId: state.me?.id, roomId: state.activeRoomId, title: state.rooms.find(r => r.id === state.activeRoomId)?.other_display_name || state.rooms.find(r => r.id === state.activeRoomId)?.title || uiText("Переписка"), store: chatRuntime.local() }),
  enqueue: options => chatRuntime.enqueue(options), report: toast,
  encryptFile: encryptWebFile
});
async function resolveWebMedia(message) {
  const info = message.meta?.media_encryption, uri = accountMediaUrl('/api/media/' + encodeURIComponent(message.media_id));
  if (!info) return { url: uri, mime: message.media_mime, name: message.meta?.filename || '67chat-media' };
  const account = state.me?.id, cacheId = account + ':' + message.media_id;
  if(!account)throw Error("Сначала войдите");
  return webMediaUrls.resolve(cacheId,async signal=>{
  const text = await decryptMsg(message.room_id, { ...info.key_message, room_id: message.room_id, sender_id: message.sender_id, id: message.id + '-file-key', type: 'text' });
  let secret; try { secret = JSON.parse(text); } catch { throw new Error("Ключ вложения недоступен на этом устройстве"); }
  if(account!==state.me?.id||signal.aborted)throw Error("Сессия завершена");
  const response = await fetch(uri, { credentials: 'include',signal }); if (!response.ok) throw new Error("Не удалось загрузить вложение");
  const key = await crypto.subtle.importKey('raw', base64ToBytes(secret.key), { name: 'AES-GCM' }, false, ['decrypt']);
  const data = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: base64ToBytes(secret.iv) }, key, await response.arrayBuffer());
  if (account !== state.me?.id) throw new Error("Сессия завершена");
  return {blob:new Blob([data],{type:info.mime}),mime:info.mime,name:info.name};
  });
}
window.openWebMedia = id => { const message = state.messages.find(m => m.id === id); if (message) void webMedia.openViewer(message, state.messages, resolveWebMedia).catch(e => toast(userError(e))); };
async function hydrateVisibleMedia() {
  if (!state.autoDownload && qualityAccount!==state.me?.id) return;
  const account = state.me?.id;
  for (const node of document.querySelectorAll('[data-encrypted-media]')) {
    if (node.dataset.loading || node.getBoundingClientRect().top > innerHeight + 500) continue;
    const message = state.messages.find(m => m.id === node.dataset.encryptedMedia); if (!message) continue;
    if(qualityAccount===account){const mime=message.meta?.media_encryption?.mime||message.media_mime||'';const kind=message.type==='voice'?'voice':mime.startsWith('image/')?'image':mime.startsWith('video/')||message.type==='video_note'?'video':'file';const network=navigator.connection?.type==='wifi'?'wifi':navigator.connection?.type==='cellular'?'cellular':'unknown';if(!shouldAutoDownload(qualityPreference.download,kind,network))continue;}
    node.dataset.loading = 'yes';
    try { const result = await resolveWebMedia(message); if (!node.isConnected || account !== state.me?.id) return;
      if (result.mime?.startsWith('image/')) { const image = document.createElement('img'); image.className = 'media-image'; image.src = result.url; image.alt = message._mediaDetails?.expressive?.label||message.text || uiText("Фотография");if(message._mediaDetails?.expressive)image.style.cssText='width:180px;height:180px;object-fit:contain;background:transparent';node.replaceChildren(image); }
      else if(result.mime?.startsWith('video/')&&message._mediaDetails?.expressive){const video=document.createElement('video');video.src=result.url;video.muted=true;video.loop=true;video.autoplay=true;video.playsInline=true;video.style.cssText='width:180px;height:180px;object-fit:contain';node.replaceChildren(video);}
    } catch { node.dataset.loading = ''; }
  }
}
const folderManager = createFolderManager({state,request:(path,options={})=>{if(options.method==='PUT'&&/^\/me\/folders\/[^/]+$/.test(path)){const folder=state.folders?.find(f=>path.endsWith('/'+f.id));if(folder&&options.body)options={...options,body:JSON.stringify({...JSON.parse(options.body),revision:folder.revision})};}return api(path,options);}, refresh: async () => { await loadRooms(); await mountPreservingInput(); }, openRoom: (...args) => window.selectRoom(...args), report: toast });
window.openSavedMessages = folderManager.saved;
window.editChatFolder = folderManager.edit;
window.selectChatFolder = id => { state.selectedFolder = id; state.archiveOpen = false; mountPreservingInput(); };
const chatActions = createChatActions({ state, request: api, runtime: chatRuntime, report: toast, mount: mountPreservingInput, buildText: buildTextMessage, decrypt: decryptMsg, openViewer: (message, list) => webMedia.openViewer(message, list, resolveWebMedia) });
window.openRoomActions = chatActions.roomMenu;
window.setChatPreference = (id, change) => chatActions.preference(id, change);
window.toggleMessageSelection = chatActions.select;
window.clearMessageSelection = () => { state.selectedMessages.clear(); mountPreservingInput(); };
window.selectAllMessages = () => {
  // Everything currently loaded, which is what the user sees on screen.
  for (const message of state.messages || []) {
    if (message?.id) state.selectedMessages.add(message.id);
  }
  mountPreservingInput();
};
window.selectMessageIfNeeded=(id,event)=>{if(event.shiftKey){event.preventDefault();event.stopPropagation();window.selectMessageRange(id);}else if(state.selectedMessages.size){event.preventDefault();event.stopPropagation();state.selectionAnchor=id;chatActions.select(id);}};
window.copySelectedMessages = chatActions.copySelected;
window.deleteSelectedMessages = chatActions.removeSelected;
window.forwardSelectedMessages=()=>window.forwardToMany([...state.selectedMessages]);
window.selectMessageRange=id=>{const messages=state.messages,selected=[...state.selectedMessages],anchor=state.selectionAnchor||selected[0]||id,a=messages.findIndex(m=>m.id===anchor),b=messages.findIndex(m=>m.id===id);if(a<0||b<0)return;for(let i=Math.min(a,b);i<=Math.max(a,b);i++)state.selectedMessages.add(messages[i].id);state.selectionAnchor=anchor;void mountPreservingInput();};
window.showReactionPeople=id=>{const message=state.messages.find(m=>m.id===id);if(!message)return;const {content,status}=webActionDialog(uiText("Реакции"));for(const [emoji,ids] of Object.entries(message.reactions||{})){const people=reactionPeople(ids,[state.me,...state.roomMembers,state.activeDmPeer]);for(const person of people)dialogButton(content,emoji+' '+(person.display_name||person.username||person.id),()=>window.showProfile(person.id),status);}};
window.openReadTimer=()=>{const room=state.activeRoomId,{dialog,content,status}=webActionDialog(uiText("Исчезновение после прочтения"));const explain=document.createElement('p');explain.textContent=uiText("Таймер запускается при первом прочтении другим участником. Применяется к новым текстовым сообщениям.");content.append(explain);for(const seconds of [0,10,60,300,3600,86400])dialogButton(content,seconds?seconds+(" "+uiText("секунд")+""):uiText("Выключить"),async()=>{webMessageTTLs.set(room,seconds);await mountPreservingInput();dialog.close();},status);};
window.openComposeFeatures=()=>{const {dialog,content,status}=webActionDialog(uiText("Добавить к сообщению"));for(const [mode,title] of Object.entries({emoji:uiText("Эмодзи"),stickers:uiText("Стикеры и GIF"),poll:uiText("Опрос или викторина"),dice:uiText("Кубик"),contact:uiText("Контакт"),location:uiText("Геолокация"),export:uiText("Экспорт чата")}))dialogButton(content,title,async()=>{dialog.close();window.openExpressiveTool(mode);},status);dialogButton(content,uiText("Форматирование"),async()=>{dialog.close();window.openFormatTools();},status);dialogButton(content,uiText("Исчезновение после прочтения")+(webMessageTTLs.get(state.activeRoomId)?' · '+webMessageTTLs.get(state.activeRoomId)+(" "+uiText("с")+""):''),async()=>{dialog.close();window.openReadTimer();},status);dialogButton(content,uiText("Отложенные сообщения и напоминания"),async()=>{dialog.close();window.openConversationTool('scheduled');},status);};
window.forwardToMany=ids=>{const account=state.me?.id,messages=state.messages.filter(m=>ids.includes(m.id)),rooms=state.rooms.filter(r=>!r.preferences?.hidden),operation=crypto.randomUUID(),completed=new Set(),selected=new Set(),{dialog,content,status}=webActionDialog(uiText("Переслать в несколько чатов"));for(const room of rooms){const label=document.createElement('label'),check=document.createElement('input');check.type='checkbox';check.onchange=()=>check.checked?selected.add(room.id):selected.delete(room.id);label.append(check,document.createTextNode(room.title||room.other_display_name||room.other_username||uiText("Чат")));content.append(label,document.createElement('br'));}const comment=document.createElement('textarea');comment.placeholder=uiText("Комментарий (необязательно)");content.append(comment);dialogButton(content,uiText("Переслать выбранным"),async()=>{if(!selected.size)throw Error("Выберите получателей");comment.readOnly=true;const results=[];let allComplete=true;for(const room of rooms.filter(r=>selected.has(r.id))){let error=null;try{for(const [index,message] of messages.entries()){if(state.me?.id!==account)throw Error("Сеанс изменился");const key=room.id+':'+message.id;if(completed.has(key))continue;const text=message._text??message.text??'';if(isExpressiveControl(parseMessagePayload(text)))throw Error("Служебные события не пересылаются");const clientId=operation+'_'+rooms.indexOf(room)+'_'+index;if(message.type==='text')await chatRuntime.enqueue({roomId:room.id,text,clientId});else{const meta={...message.meta,forwarded:true};delete meta.caption_message;delete meta.reply_preview;if(meta.media_encryption){const info=meta.media_encryption,secret=await decryptMsg(message.room_id,{...info.key_message,type:'text',sender_id:message.sender_id,room_id:message.room_id,id:message.id+'-key'}),keyMessage=await buildTextMessage(room.id,room,secret);if(state.me?.id!==account)throw Error("Сеанс изменился");meta.media_encryption={...info,key_message:keyMessage};}await chatRuntime.enqueue({roomId:room.id,text:message.text||'',body:{type:message.type,media_id:message.media_id,text:null,meta},clientId});}completed.add(key);}if(comment.value&&!completed.has(room.id+':comment')){await chatRuntime.enqueue({roomId:room.id,text:comment.value,clientId:operation+'_'+rooms.indexOf(room)+'_comment'});completed.add(room.id+':comment');}}catch(e){error=userError(e);allComplete=false;}results.push((room.title||room.other_display_name||room.other_username||uiText("Чат"))+': '+(error||uiText("добавлено в очередь")));}status.textContent=results.join('\n');if(allComplete){state.selectedMessages.clear();await mountPreservingInput();} },status);};
window.openSharedMedia = () => void chatActions.sharedMedia().catch(e => toast(userError(e)));
window.retryQueuedMessage = id => void chatRuntime.retry(id).catch(e => toast(userError(e)));
window.cancelQueuedMessage = id => void chatRuntime.cancel(id).catch(e => toast(userError(e)));
window.loadOlderMessages = () => void chatRuntime.older();
window.jumpToLatest = async () => { if (state.chatHistory?.has_newer) await chatRuntime.open(state.activeRoomId, { latest: true }); const box = $('#msgs'); if (box) box.scrollTop = box.scrollHeight; state.newMessages = 0; };
window.cancelHistorySearch = () => chatRuntime.cancelSearch();
window.stateRoomId = () => state.activeRoomId;
window.openMessageMenu = (id, event) => {
  event.preventDefault(); event.stopPropagation();
  $('#messageActions')?.remove();
  const message = state.messages.find(item => item.id === id);
  const bubble = document.getElementById('msg-' + id);
  if (!message || !bubble) return;
  const rect = bubble.getBoundingClientRect();
  const menu = document.createElement('div');
  menu.id = 'messageActions'; menu.className = 'message-actions'; menu.setAttribute('role', 'menu');
  const add = (label, action, danger = false) => {
    const button = document.createElement('button');
    button.textContent = label; button.type = 'button'; button.setAttribute('role', 'menuitem');
    if (danger) button.className = 'danger-text';
    button.onclick = async e => { e.stopPropagation(); menu.remove(); try { await action(); } catch { alert(uiText("Не удалось выполнить действие. Повторите попытку.")); } };
    menu.appendChild(button);
  };
  add(uiText("Выделить"), () => chatActions.select(id));
  add(uiText("Переслать"),()=>window.forwardToMany([id]));
  add(uiText("Ответить"), () => window.replyToMessage(id));
  add(uiText("Закрепить"), () => window.pinMessage(id));
  add(uiText("История правок"),()=>window.openConversationTool('history',id));
  if(message.is_post&&state.rooms.find(room=>room.id===message.room_id)?.kind==='channel')add(uiText('Комментарии'),()=>window.openConversationTool('comments',id));
  add(uiText("Кто поставил реакции"),()=>window.showReactionPeople(id));
  add(uiText("Выделить диапазон"),()=>window.selectMessageRange(id));
  add(uiText("Поставить реакцию"), () => window.showReactionPicker(id, { target: bubble, stopPropagation() {} }));
  // A link that opens this exact message: ?room=<id>&msg=<id>
  add(uiText("Копировать ссылку"), async () => {
    await navigator.clipboard.writeText(messageLink(state.activeRoomId,id));
    toast(uiText("Ссылка на сообщение скопирована"));
  });
  if (message._text || message.text) add(uiText("Копировать текст"), () => navigator.clipboard.writeText(message._text || message.text));
  // Deleting only for yourself hides it locally and on your other devices,
  // leaving the message intact for everyone else.
  add(uiText("Удалить у себя"), async () => {
    if (!confirm(uiText("Удалить сообщение только у себя? У других оно останется."))) return;
    const roomId=message.room_id||state.activeRoomId,store=chatRuntime.local();document.querySelectorAll('[data-search-message-id="'+id+'"]').forEach(node=>node.remove());
    await api(`/rooms/${roomId}/messages/${id}/hide`, { method: 'DELETE' });
    await chatRuntime.hide(roomId,id);
    if(state.activeRoomId===roomId){state.messages=state.messages.filter(item=>item.id!==id);await mountPreservingInput();}
  }, true);
  if (message.sender_id === state.me.id) {
    if(message.type==='text'&&!parseMessagePayload(message._text??message.text))add(uiText("Изменить"), () => window.startEditMessage(id));
    add(uiText("Удалить у всех"), () => window.deleteMessage(id), true);
  }
  menu.style.left = Math.max(8, Math.min(innerWidth - 238, event.clientX || rect.left)) + 'px';
  menu.style.top = Math.max(8, Math.min(innerHeight - menu.children.length * 44 - 20, event.clientY || rect.bottom)) + 'px';
  document.body.appendChild(menu);
  menu.querySelector('button')?.focus({ preventScroll: true });
};
function saveWebDraft() {
  if(state.activeRoomId&&$('#inp')&&!state.editMessage){chatRuntime.draft(state.activeRoomId,$('#inp').value);queueWebDraftSync(state.activeRoomId);}
}
function resizeComposer() {
  const input = $('#inp');
  if (!input) return;
  input.style.height = '40px';
  input.style.height = Math.min(120, Math.max(40, input.scrollHeight)) + 'px';
}
window.openNewChat = () => {
  saveWebDraft(); state.composeOpen = true; state.settingsOpen = false; state.uiMenu = null;
  if (innerWidth <= 900) { ++roomRequestId; state.activeRoomId = null; }
  mount(); requestAnimationFrame(() => $('#peopleSearch')?.focus());
};
window.openChatList = () => { state.composeOpen = false; state.settingsOpen = false; state.uiMenu = null; mountPreservingInput(); };
window.openWebSettings = () => { state.composeOpen = false; state.settingsOpen = true; state.uiMenu = null; mountPreservingInput(); };
window.setWebTheme = theme => { state.themeMode = theme; state.darkTheme = theme === 'dark' || (theme === 'system' && matchMedia('(prefers-color-scheme: dark)').matches); localStorage.setItem('themeMode', theme); localStorage.setItem('theme', state.darkTheme ? 'dark' : 'light'); mountPreservingInput(); };
matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => { if (state.themeMode === 'system') { applyTheme(); void mountPreservingInput(); } });
window.setMessageFontSize = value => { state.fontSize = Math.max(12, Math.min(24, Number(value))); localStorage.setItem('messageFontSize', state.fontSize); applyTheme(); };
window.setAutoDownload = value => { state.autoDownload = !!value; localStorage.setItem('autoDownload', value ? 'yes' : 'no'); void hydrateVisibleMedia();void hydrateExpressiveMessages().catch(error=>toast(userError(error))); };
window.openArchive = () => { saveWebDraft(); state.archiveOpen = !state.archiveOpen; state.settingsOpen = false; if (innerWidth <= 900) state.activeRoomId = null; mountPreservingInput(); };
window.toggleWebMenu = (menu, event) => {
  event.stopPropagation();
  state.uiMenu = state.uiMenu === menu ? null : menu;
  mountPreservingInput();
  if (state.uiMenu) requestAnimationFrame(() => $('.web-popover button')?.focus());
};
window.closeWebMenu = () => { state.uiMenu = null; mountPreservingInput(); };
document.addEventListener('click', event => {
  const actions = $('#messageActions');
  if (actions && !actions.contains(event.target) && !event.target.closest('.message-menu-trigger')) actions.remove();
  if (state.uiMenu && !event.target.closest('.web-popover') && !event.target.closest('#accountMenuToggle,#chatMenuToggle')) window.closeWebMenu();
});
document.addEventListener('keydown', event => {
  if (event.key === 'Escape') {
    if (state.uiMenu) { const target = state.uiMenu === 'chat' ? 'chatMenuToggle' : 'accountMenuToggle'; window.closeWebMenu(); requestAnimationFrame(() => document.getElementById(target)?.focus()); }
    $('#messageActions')?.remove(); $('#reactionPicker')?.remove();
  }
  const menu = event.target.closest('.web-popover,.message-actions');
  if (menu && ['ArrowDown','ArrowUp'].includes(event.key)) {
    event.preventDefault(); const buttons = [...menu.querySelectorAll('button')];
    buttons[(buttons.indexOf(document.activeElement) + (event.key === 'ArrowDown' ? 1 : buttons.length - 1)) % buttons.length]?.focus();
  }
});
function updateChatPresence() {
  const room = state.rooms.find(item => item.id === state.activeRoomId);
  const node = $('#chatPresence');
  const typing = Object.values(state.typingByRoom[state.activeRoomId] || {});
  if (node) {
    node.textContent = typing.length ? (typing.length === 1 ? uiText("{0} печатает…",typing[0]) : uiText("Несколько человек печатают…")) : room?.kind === 'dm' ? presenceLabel(state.activeDmPeer) : `${state.roomMembers.length} ${escapeUi(uiText("участников"))}`;
    node.classList.toggle('online', !!typing.length || !!state.activeDmPeer?.online);
  }
  const connection = $('#webConnection');
  if (connection) { connection.hidden = state.wsAuthed && !state.syncing; connection.textContent = !navigator.onLine?uiText("Нет сети"):!state.wsAuthed?uiText("Подключение…"):uiText("Синхронизация…"); }
}
let refreshingPresence = false;
async function refreshWebPresence() {
  if (refreshingPresence || document.hidden || !state.me) return;
  const room = state.rooms.find(item => item.id === state.activeRoomId), account = state.me.id;
  if (room?.kind !== 'dm' || !room.other_user_id) return;
  refreshingPresence = true;
  try {
    const peer = await api(`/users/${room.other_user_id}`);
    if (state.activeRoomId === room.id && state.me?.id === account) { state.activeDmPeer = peer; updateChatPresence(); }
  } catch { /* Next foreground/poll retries without interrupting text entry. */ }
  finally { refreshingPresence = false; }
}
setInterval(refreshWebPresence, 25000);
document.addEventListener('visibilitychange', () => { if (!document.hidden) void refreshWebPresence(); });
window.addEventListener('online', updateChatPresence);
window.addEventListener('offline', updateChatPresence);

// --- Main UI ---
let parkedMessages = null;
async function mount() {
  applyTheme();
  const root = $("#app");
  const previous = $('#msgs');
  if (previous?.dataset.roomId && state.me) { previous.remove(); parkedMessages={roomId:previous.dataset.roomId,account:state.me.id,node:previous}; }
  if (!state.me || parkedMessages?.account!==state.me.id || parkedMessages?.roomId!==state.activeRoomId) parkedMessages=null;

  // Admin panel
  if (state.adminPanel) {
    root.innerHTML = renderAdminPanel();
    return;
  }

  // Backup panel
  if (state.backupPanel) {
    root.innerHTML = renderBackupPanel();
    return;
  }

  // Sessions panel
  if (state.sessionsPanel) {
    root.innerHTML = renderSessionsPanel();
    return;
  }

  // Safety Number panel
  if (state.safetyNumberPanel) {
    root.innerHTML = renderSafetyNumberPanel();
    return;
  }

  // Profile modal
  if (state.viewProfile) {
    root.innerHTML = renderProfileModal();
    return;
  }

  // Room settings modal
  if (state.editingRoom && state.activeRoomId) {
    root.innerHTML = renderRoomSettings();
    return;
  }

  if (!state.me) {
    root.innerHTML = renderLogin();
    return;
  }
  root.innerHTML = renderMessengerShell(state, renderComposeArea) +
    (state.createRoomMode ? renderCreateRoomModal() : '');
  if(parkedMessages && $('#msgs')) $('#msgs').replaceWith(parkedMessages.node);
  if (state.activeRoomId) render();
  resizeComposer();
}

function renderCreateRoomModal() {
  const isGroup = state.createRoomMode === 'group';
  const title = isGroup ? uiText("Создать группу") : uiText("Создать канал");

  return `
    <div class="modal-overlay create-room-modal" onclick="closeCreateRoom()">
      <div class="modal" onclick="event.stopPropagation()">
        <div class="modal-header">
          <h3>${title}</h3>
          <button class="btn" onclick="closeCreateRoom()">✕</button>
        </div>
        <div class="modal-body">
            <input class="input" id="createRoomTitleInput" placeholder="${escapeUi(uiText("Название..."))}" value="${state.createRoomTitle.replaceAll('"','&quot;')}" oninput="onCreateRoomTitleChange(this.value)">

          <div class="create-room-members">
            <div class="mini" style="margin-bottom:8px;">${escapeUi(uiText("Добавить участников:"))}</div>
            <input class="input" id="createRoomSearchInput" placeholder="${escapeUi(uiText("Поиск пользователей..."))}" value="${state.createRoomQuery.replaceAll('"','&quot;')}" oninput="onCreateRoomQueryChange(this.value)">

            <div id="createRoomResults" class="search-results" style="display:none;"></div>
          </div>

          <div id="createRoomSelectedContainer" style="margin-top:16px;${state.createRoomSelected.length ? '' : 'display:none;'}">
            <div class="mini" style="margin-bottom:8px;">${escapeUi(uiText("Выбрано:"))}</div>
            <div id="createRoomSelected" class="selected-users">
              ${state.createRoomSelected.map(u => `
                <div class="selected-chip" onclick='toggleSelectUser(${safeUserJson(u)})'>
                  ${escapeHtml(displayName(u))}
                  <span class="remove">✕</span>
                </div>
              `).join('')}
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn" onclick="closeCreateRoom()">${escapeUi(uiText("Отмена"))}</button>
          <button class="btn primary" onclick="submitCreateRoom()">${escapeUi(uiText("Создать"))}</button>
        </div>
      </div>
    </div>
  `;
}

function renderRoomItem(r) {
  return renderRoomRow(r, { ...state.me, activeRoomId: state.activeRoomId });
}

function renderProfileModal() {
  const p = state.viewProfile;
  const isMe = !!p.isMe || String(p.id || "") === String(state.me?.id || "");
  return `
    <div class="modal-overlay" onclick="closeProfile()">
      <div class="modal-content profile-modal" onclick="event.stopPropagation()">
        <div class="profile-header">
          ${p.avatar_id ? `<img class="avatar-lg" src="${avatarUrl(p.avatar_id)}" alt="">` : `<div class="avatar-placeholder-lg"></div>`}
          ${isMe ? `<button class="btn-edit-avatar" onclick="uploadMyAvatar()">📷</button>` : ""}
          <input type="file" id="avatarInput" hidden accept="image/*" onchange="onAvatarSelected()">
        </div>
        <div class="profile-info">
          <h2>${escapeHtml(displayName(p))} ${isMe ? `<button class="btn-sm" onclick="updateMyName()">✏️</button>` : ""}</h2>
          <div class="username">@${escapeHtml(p.username)}</div>
          ${!isMe ? `<div class="profile-link"><a href="${profilePath(p.username)}" onclick="event.preventDefault()">${location.origin}${profilePath(p.username)}</a></div>` : ""}
          <div class="status ${p.online ? 'online' : ''}">${p.online ? uiText("онлайн") : (p.last_seen ? uiText("был(а) {0}",new Date(p.last_seen).toLocaleString(uiLocale())) : "")}</div>
        </div>
        <div class="profile-actions">
          ${!isMe ? `<button class="btn primary" onclick="startDM('${escapeJsSingleQuoted(p.username)}');closeProfile();">${escapeUi(uiText("Написать"))}</button>` : ""}
          ${!isMe && state.e2eReady ? `<button class="btn" onclick="showSafetyNumber('${escapeJsSingleQuoted(p.id)}')">${escapeUi(uiText("🔐 Код безопасности"))}</button>` : ""}
          ${!isMe ? `<button class="btn ${p.blocked_by_me ? '' : 'danger'}" onclick="toggleUserBlock('${escapeJsSingleQuoted(p.id)}')">${p.blocked_by_me ? uiText("Разбанить") : uiText("Забанить")}</button>` : ""}
          ${!isMe ? `<button class="btn danger" onclick="deleteConversationWithUser('${escapeJsSingleQuoted(p.id)}')">${escapeUi(uiText("Удалить переписку"))}</button>` : ""}
          ${isMe ? `<button class="btn primary" onclick="changePassword()">${escapeUi(uiText("🔑 Сменить пароль"))}</button>` : ""}
          ${isMe && state.me?.isAdmin ? `<button class="btn danger" onclick="openAdminPanel();closeProfile();">${escapeUi(uiText("⚙️ Админ панель"))}</button>` : ""}
          <button class="btn" onclick="closeProfile()">${escapeUi(uiText("Закрыть"))}</button>
        </div>
      </div>
    </div>
  `;
}

function renderBackupPanel() {
  return `
    <div class="modal-overlay" onclick="closeBackupPanel()">
      <div class="modal-content backup-panel" onclick="event.stopPropagation()">
        <div class="modalHead">
          <div>
            <h2>${escapeUi(uiText("🔑 Бэкап ключей шифрования"))}</h2>
            <div class="mini">${escapeUi(uiText("Экспорт и восстановление E2E ключей"))}</div>
          </div>
          <button class="btn" onclick="closeBackupPanel()">${escapeUi(uiText("Закрыть"))}</button>
        </div>
        <div class="modalBody">
          ${state.showRecoveryPhrase && state.recoveryPhrase ? `
            <div class="backup-success">
              <h3>${escapeUi(uiText("✅ Бэкап создан успешно"))}</h3>
              <p class="warning-text">${escapeUi(uiText("⚠️ ВАЖНО: Сохраните эту информацию в безопасном месте!"))}</p>

              <div class="backup-info">
                <div class="backup-field">
                  <label>${escapeUi(uiText("Пользователь:"))}</label>
                  <div class="backup-value">@${state.me?.username}</div>
                </div>
                <div class="backup-field">
                  <label>${escapeUi(uiText("Дата создания:"))}</label>
                  <div class="backup-value">${new Date(state.recoveryPhrase.timestamp).toLocaleString(uiLocale())}</div>
                </div>
                <div class="backup-field">
                  <label>${escapeUi(uiText("Контрольная сумма:"))}</label>
                  <div class="backup-value mono">${state.recoveryPhrase.checksum}</div>
                </div>
              </div>

              <div class="backup-phrase-container">
                <label>Recovery Phrase:</label>
                <textarea class="backup-phrase" readonly>${state.recoveryPhrase.phrase}</textarea>
              </div>

              <div class="backup-actions">
                <button class="btn primary" onclick="downloadBackup()">${escapeUi(uiText("📥 Скачать файл"))}</button>
                <button class="btn" onclick="copyBackupToClipboard()">${escapeUi(uiText("📋 Копировать"))}</button>
              </div>

              <div class="backup-warning">
                <p><strong>${escapeUi(uiText("⚠️ Предупреждение:"))}</strong></p>
                <ul>
                  <li>${escapeUi(uiText("Любой, у кого есть эта фраза, может прочитать ваши сообщения"))}</li>
                  <li>${escapeUi(uiText("Храните бэкап в безопасном месте (менеджер паролей, зашифрованный диск)"))}</li>
                  <li>${escapeUi(uiText("Не отправляйте бэкап по незашифрованным каналам"))}</li>
                  <li>${escapeUi(uiText("При потере устройства используйте эту фразу для восстановления"))}</li>
                </ul>
              </div>
            </div>
          ` : `
            <div class="backup-intro">
              <h3>${escapeUi(uiText("Зачем нужен бэкап?"))}</h3>
              <p>${escapeUi(uiText("Ваши ключи E2E шифрования хранятся только на этом устройстве. Если вы потеряете доступ к устройству, вы потеряете доступ ко всем зашифрованным сообщениям."))}</p>

              <div class="backup-features">
                <div class="backup-feature">
                  <div class="backup-feature-icon">🔐</div>
                  <div class="backup-feature-text">
                    <strong>${escapeUi(uiText("Безопасное восстановление"))}</strong>
                    <p>${escapeUi(uiText("Восстановите доступ к сообщениям на новом устройстве"))}</p>
                  </div>
                </div>
                <div class="backup-feature">
                  <div class="backup-feature-icon">💾</div>
                  <div class="backup-feature-text">
                    <strong>${escapeUi(uiText("Полный экспорт"))}</strong>
                    <p>${escapeUi(uiText("Все ключи и сессии сохраняются в одном файле"))}</p>
                  </div>
                </div>
                <div class="backup-feature">
                  <div class="backup-feature-icon">✅</div>
                  <div class="backup-feature-text">
                    <strong>${escapeUi(uiText("Проверка целостности"))}</strong>
                    <p>${escapeUi(uiText("Контрольная сумма защищает от повреждения данных"))}</p>
                  </div>
                </div>
              </div>

              <div class="backup-actions-main">
                <button class="btn primary large" onclick="exportEncryptedBackup()">${escapeUi(uiText("🔒 Экспорт в файл (с паролем)"))}</button>
                <button class="btn large" onclick="importEncryptedBackup()">${escapeUi(uiText("📂 Импорт из файла (с паролем)"))}</button>
              </div>
              <div class="backup-info-box">
                <h4>${escapeUi(uiText("🔒 Зашифрованный файл"))}</h4>
                <p>${escapeUi(uiText("Ключи шифруются вашим паролем (PBKDF2-SHA256 + AES-GCM). Пароль нигде не сохраняется: если вы его забудете, файл не восстановить."))}</p>
              </div>

              <div class="backup-actions-main">
                <button class="btn large" onclick="generateBackup()">${escapeUi(uiText("🔑 Создать бэкап"))}</button>
                <button class="btn large" onclick="restoreFromBackup()">${escapeUi(uiText("📥 Восстановить из бэкапа"))}</button>
              </div>

              <div class="backup-info-box">
                <h4>${escapeUi(uiText("ℹ️ Автоматический бэкап"))}</h4>
                <p>${escapeUi(uiText("Ключи сохраняются в защищённом локальном хранилище этого браузера. При очистке данных сайта они будут удалены. Создайте защищённую паролем резервную копию и храните её отдельно."))}</p>
              </div>
            </div>
          `}
        </div>
      </div>
    </div>
  `;
}

function renderSafetyNumberPanel() {
  const data = state.safetyNumberData;
  if (!data) return '';

  // Format safety number into groups of 5 digits
  const formattedNumber = data.number.match(/.{1,15}/g)?.join('\n') || data.number;

  return `
    <div class="modal-overlay" onclick="closeSafetyNumber()">
      <div class="modal-content safety-number-panel" onclick="event.stopPropagation()">
        <div class="modalHead">
          <div>
            <h2>${escapeUi(uiText("🔐 Код безопасности"))}</h2>
            <div class="mini">${escapeUi(uiText("Верификация E2E шифрования с"))} ${escapeHtml(data.user.username)}</div>
          </div>
          <button class="btn" onclick="closeSafetyNumber()">${escapeUi(uiText("Закрыть"))}</button>
        </div>
        <div class="modalBody">
          <div class="safety-number-intro">
            <h3>${escapeUi(uiText("Что это?"))}</h3>
            <p>${escapeUi(uiText("Код безопасности (Safety Number) позволяет убедиться, что ваше E2E шифрование не скомпрометировано атакой \"человек посередине\" (MITM)."))}</p>
          </div>

          <div class="safety-number-display">
            <div class="safety-number-code">
              ${formattedNumber.split('\n').map(line => `<div class="safety-number-line">${line}</div>`).join('')}
            </div>
          </div>

          <div class="safety-number-actions">
            <button class="btn primary" onclick="copySafetyNumber()">${escapeUi(uiText("📋 Копировать код"))}</button>
          </div>

          <div class="safety-number-instructions">
            <h3>${escapeUi(uiText("Как проверить?"))}</h3>
            <ol>
              <li>${escapeUi(uiText("Попросите собеседника открыть ваш профиль и нажать \"🔐 Код безопасности\""))}</li>
              <li>${escapeUi(uiText("Сравните коды - они должны полностью совпадать"))}</li>
              <li>${escapeUi(uiText("Сравнивайте коды через безопасный канал (лично, по телефону, видеозвонок)"))}</li>
              <li>${escapeUi(uiText("Если коды совпадают - ваше соединение защищено от MITM атак"))}</li>
            </ol>
          </div>

          <div class="safety-number-warning">
            <p><strong>${escapeUi(uiText("⚠️ Важно:"))}</strong></p>
            <ul>
              <li>${escapeUi(uiText("Код меняется при переустановке приложения или смене устройства"))}</li>
              <li>${escapeUi(uiText("Если код изменился без причины - возможна атака MITM"))}</li>
              <li>${escapeUi(uiText("Не сравнивайте коды через незашифрованные каналы (SMS, email)"))}</li>
              <li>${escapeUi(uiText("Этот код уникален для пары пользователей"))}</li>
            </ul>
          </div>

          <div class="safety-number-technical">
            <details>
              <summary>${escapeUi(uiText("Технические детали"))}</summary>
              <div class="safety-number-tech-content">
                <p><strong>${escapeUi(uiText("Алгоритм:"))}</strong> ${escapeUi(uiText("SHA-256 хеш от комбинации публичных ключей обоих пользователей"))}</p>
                <p><strong>${escapeUi(uiText("Длина:"))}</strong> ${escapeUi(uiText("60 цифр (12 групп по 5)"))}</p>
                <p><strong>${escapeUi(uiText("Пользователи:"))}</strong> @${escapeHtml(state.me.username)} ↔ @${escapeHtml(data.user.username)}</p>
              </div>
            </details>
          </div>
        </div>
      </div>
    </div>
  `;
}

function renderSessionsPanel() {
  const sessions = state.sessions || [];
  const currentSession = sessions.find(s => s.isCurrent);
  const otherSessions = sessions.filter(s => !s.isCurrent);

  const getDeviceIcon = (deviceType) => {
    switch(deviceType) {
      case 'mobile': return '📱';
      case 'tablet': return '📱';
      case 'desktop': return '💻';
      default: return '🖥️';
    }
  };

  const formatLastActivity = (timestamp) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffMins < 1) return uiText("только что");
    if (diffMins < 60) return uiText("{0} мин назад",diffMins);
    if (diffHours < 24) return uiText("{0} ч назад",diffHours);
    if (diffDays < 7) return uiText("{0} дн назад",diffDays);
    return date.toLocaleDateString(uiLocale());
  };

  return `
    <div class="modal-overlay" onclick="closeSessionsPanel()">
      <div class="modal-content sessions-panel" onclick="event.stopPropagation()">
        <div class="modalHead">
          <div>
            <h2>${escapeUi(uiText("📱 Активные сессии"))}</h2>
            <div class="mini">${escapeUi(uiText("Управление устройствами"))}</div>
          </div>
          <button class="btn" onclick="closeSessionsPanel()">${escapeUi(uiText("Закрыть"))}</button>
        </div>
        <div class="modalBody">
          ${sessions.length === 0 ? `
            <div class="sessions-empty">
              <p>${escapeUi(uiText("Нет активных сессий"))}</p>
            </div>
          ` : `
            ${currentSession ? `
              <div class="session-section">
                <h3>${escapeUi(uiText("Текущая сессия"))}</h3>
                <div class="session-item current">
                  <div class="session-icon">${getDeviceIcon(currentSession.deviceType)}</div>
                  <div class="session-info">
                    <div class="session-name">${escapeHtml(String(currentSession.deviceName || uiText("Устройство")).replace(/^Unknown on /,'').replace(/^Unknown(?: Device)?$/,uiText("Неизвестное устройство")))}</div>
                    <div class="session-details">
                      <span class="session-ip">${currentSession.ipAddress || uiText("IP не определён")}</span>
                      <span class="session-activity">${formatLastActivity(currentSession.lastActivity)}</span>
                    </div>
                    <div class="session-created">${escapeUi(uiText("Создана:"))} ${new Date(currentSession.createdAt).toLocaleString(uiLocale())}</div>
                  </div>
                  <div class="session-badge current-badge">${escapeUi(uiText("Это устройство"))}</div>
                </div>
              </div>
            ` : ''}

            ${otherSessions.length > 0 ? `
              <div class="session-section">
                <div class="session-section-header">
                  <h3>${escapeUi(uiText("Другие сессии ("))}${otherSessions.length})</h3>
                  ${otherSessions.length > 0 ? `
                    <button class="btn danger small" onclick="terminateOtherSessions()">${escapeUi(uiText("Завершить все"))}</button>
                  ` : ''}
                </div>
                ${otherSessions.map(s => `
                  <div class="session-item">
                    <div class="session-icon">${getDeviceIcon(s.deviceType)}</div>
                    <div class="session-info">
                      <div class="session-name">${escapeHtml(String(s.deviceName || uiText("Устройство")).replace(/^Unknown on /,'').replace(/^Unknown(?: Device)?$/,uiText("Неизвестное устройство")))}</div>
                      <div class="session-details">
                        <span class="session-ip">${s.ipAddress || uiText("IP не определён")}</span>
                        <span class="session-activity">${formatLastActivity(s.lastActivity)}</span>
                      </div>
                      <div class="session-created">${escapeUi(uiText("Создана:"))} ${new Date(s.createdAt).toLocaleString(uiLocale())}</div>
                    </div>
                    <button class="btn danger small" onclick="terminateSession('${s.id}')">${escapeUi(uiText("Завершить"))}</button>
                  </div>
                `).join('')}
              </div>
            ` : ''}

            <div class="sessions-actions">
              <button class="btn danger" onclick="terminateAllSessions()">${escapeUi(uiText("Завершить все сессии (включая эту)"))}</button>
            </div>

            <div class="sessions-info-box">
              <h4>${escapeUi(uiText("ℹ️ О сессиях"))}</h4>
              <ul>
                <li>${escapeUi(uiText("Каждая сессия представляет вход с отдельного устройства или браузера"))}</li>
                <li>${escapeUi(uiText("Если вы видите незнакомое устройство, завершите его сессию"))}</li>
                <li>${escapeUi(uiText("При завершении сессии устройство будет разлогинено"))}</li>
                <li>${escapeUi(uiText("Рекомендуем периодически проверять активные сессии"))}</li>
              </ul>
            </div>
          `}
        </div>
      </div>
    </div>
  `;
}

function renderAdminPanel() {
  const users = state.adminUsers || [];
  return `
    <div class="modal-overlay" onclick="closeAdminPanel()">
      <div class="modal-content admin-panel" onclick="event.stopPropagation()">
        <div class="modalHead">
          <div>
            <h2>${escapeUi(uiText("⚙️ Админ панель"))}</h2>
            <div class="mini">${escapeUi(uiText("Пользователи:"))} ${users.length}</div>
          </div>
          <button class="btn" onclick="closeAdminPanel()">${escapeUi(uiText("Закрыть"))}</button>
        </div>
        <div class="modalBody admin-panel-body">
          <div class="admin-users-list">
            ${users.map(u => `
              <div class="admin-user-row">
                ${u.avatar_id ? `<img class="avatar-sm" src="${avatarUrl(u.avatar_id)}">` : `<div class="avatar-placeholder"></div>`}
                <div class="admin-user-info">
                  <div class="admin-user-title"><b>${escapeHtml(displayName(u))}</b> @${escapeHtml(u.username)} ${u.banned ? `<span class="admin-user-bad">${escapeUi(uiText("[ЗАБАНЕН]"))}</span>` : ''} ${u.online ? '🟢' : ''}</div>
                  <div class="mini">${escapeUi(uiText("Регистрация:"))} ${new Date(u.created_at).toLocaleDateString(uiLocale())}</div>
                </div>
                <div class="admin-user-actions">
                  <button class="btn-sm" onclick="adminResetPassword('${u.id}', '${escapeJsSingleQuoted(u.username)}')">${escapeUi(uiText("🔑 Пароль"))}</button>
                  <button class="btn-sm ${u.banned ? '' : 'danger'}" onclick="adminToggleBan('${u.id}', '${escapeJsSingleQuoted(u.username)}', ${u.banned})">${u.banned ? uiText("✅ Разбан") : uiText("🚫 Бан")}</button>
                  ${u.banned ? `<button class="btn-sm danger" onclick="adminDeleteUser('${u.id}', '${escapeJsSingleQuoted(u.username)}')">${escapeUi(uiText("🗑️ Удалить"))}</button>` : ''}
                </div>
              </div>
            `).join('')}
          </div>
        </div>
      </div>
    </div>
  `;
}

function renderRoomSettings() {
  const room = state.rooms.find(r => r.id === state.activeRoomId);
  const members = state.roomMembers || [];
  const myRole = members.find(m => m.id === state.me.id)?.role;
  const canEdit = myRole === "owner" || myRole === "admin";
  const isDM = room?.kind === "dm";

  return `
    <div class="modal-overlay" onclick="closeRoomSettings()">
      <div class="modal-content room-settings" onclick="event.stopPropagation()">
        <div class="profile-header">
          ${room?.avatar_id ? `<img class="avatar-lg" src="${avatarUrl(room.avatar_id)}" alt="">` : `<div class="avatar-placeholder-lg"></div>`}
          ${!isDM && canEdit ? `<button class="btn-edit-avatar" onclick="uploadRoomAvatar()">📷</button>` : ""}
          <input type="file" id="roomAvatarInput" hidden accept="image/*" onchange="onRoomAvatarSelected()">
        </div>
        <div class="profile-info">
          <h2>${isDM ? (members.find(m => m.id !== state.me.id)?.username || uiText("Пользователь")) : (room?.title || uiText("Группа"))} ${!isDM && canEdit ? `<button class="btn-sm" onclick="updateRoomTitle()">✏️</button>` : ""}</h2>
          <div class="status">${isDM ? uiText("Личная переписка") : `${members.length} ${room?.kind === "channel" ? plural(members.length, uiText("подписчик"), uiText("подписчика"), uiText("подписчиков")) : plural(members.length, uiText("участник"), uiText("участника"), uiText("участников"))}`}</div>
        </div>

        ${!isDM ? `
        <div class="members-section">
          <div class="members-header">
            <h3>${room?.kind === "channel" ? uiText("Подписчики") : uiText("Участники")}</h3>
            ${canEdit ? `<button class="btn-sm" onclick="showAddMemberModal()">${escapeUi(uiText("+ Добавить"))}</button>` : ""}
          </div>
          <div class="members-list">
            ${members.map(m => {
              const memberId = String(m.id || "");
              return `
              <div class="member-item">
                <div class="member-info clickable" onclick="showProfile('${memberId}')">
                  ${m.avatar_id ? `<img class="avatar-sm" src="${avatarUrl(m.avatar_id)}">` : `<div class="avatar-placeholder"></div>`}
                  <div>
                    <div class="member-name">${escapeHtml(displayName(m))} ${m.role === "owner" ? "👑" : m.role === "admin" ? "⭐" : ""}</div>
                    <div class="member-username">@${escapeHtml(m.username)}</div>
                  </div>
                </div>
                ${myRole === "owner" && memberId && memberId !== String(state.me?.id || "") ? `
                  <div class="member-actions">
                    <button class="btn-sm" onclick="toggleAdmin('${memberId}', '${escapeJsSingleQuoted(m.role)}')">${m.role === "admin" ? uiText("Снять админа") : uiText("Назначить админом")}</button>
                    <button class="btn-sm danger" onclick="removeMember('${memberId}')">✕</button>
                  </div>
                ` : ""}
              </div>
            `}).join("")}
          </div>
        </div>
        ` : ""}

        <div class="profile-actions">
          ${isDM ? `<button class="btn danger" onclick="deleteDMChat()">${escapeUi(uiText("🗑️ Удалить чат"))}</button>` : ""}
          ${!isDM && myRole === "owner" ? `<button class="btn danger" onclick="deleteRoom()">${escapeUi(uiText("🗑️ Удалить группу"))}</button>` : ""}
          ${!isDM && myRole !== "owner" ? `<button class="btn danger" onclick="leaveRoom()">${escapeUi(uiText("🚪 Покинуть группу"))}</button>` : ""}
          ${!isDM && (myRole === "owner" || myRole === "admin") ? `<button class="btn" onclick="toggleGroupE2e()">${escapeUi(uiText("🔒 Сквозное шифрование:"))} ${groupE2eRooms.get(state.activeRoomId) ? uiText("вкл") : uiText("выкл")}</button>` : ""}
          <button class="btn" onclick="closeRoomSettings()">${escapeUi(uiText("Закрыть"))}</button>
        </div>
      </div>
    </div>
  `;
}

function renderComposeArea(room) {
  if (!room) return "";
  if (state.roomLoading || state.roomLoadError) return `<div class="compose-hint">${escapeUi(uiText("Переписка загружается"))}</div>`;

  // Reply preview
  const replyHtml = state.replyTo ? `
    <div class="reply-preview">
      <div class="reply-content">
        <div class="reply-name">${escapeHtml(state.replyTo.sender_display_name || state.replyTo.sender_username || uiText("Пользователь"))}</div>
        <div class="reply-text">${escapeHtml((state.replyTo.text || '').slice(0, 100))}${(state.replyTo.text || '').length > 100 ? '...' : ''}</div>
      </div>
      <button class="btn-cancel-reply" onclick="cancelReply()">✕</button>
    </div>
  ` : '';
  const editHtml = state.editMessage ? `
    <div class="reply-preview edit-preview">
      <div class="reply-content">
        <div class="reply-name">${escapeUi(uiText("Редактирование сообщения"))}</div>
        <div class="reply-text">${escapeHtml((state.editMessage._text || state.editMessage.text || '').slice(0, 100))}</div>
      </div>
      <button class="btn-cancel-reply" onclick="cancelEdit()">✕</button>
    </div>
  ` : '';
  const dmBlocked = room.kind === "dm" && (state.activeDmPeer?.blocked_by_me || state.activeDmPeer?.blocks_me);
  if (dmBlocked) {
    return `<div class="compose" style="justify-content:center;color:var(--muted);">${state.activeDmPeer?.blocks_me ? uiText("Пользователь забанил вас, писать нельзя") : uiText("Вы забанили пользователя, переписка только для чтения")}</div>`;
  }

  if (room.kind !== "channel") {
    // Regular chat - full compose
    return `
      <div class="compose-wrapper">
        ${editHtml}
        ${replyHtml}
        <div class="compose">
          ${iconButton('attach',uiText("Прикрепить файл"),'triggerFileSelect()')}${iconButton('more',uiText("Другие сообщения"),'openComposeFeatures()')}
          <input type="file" id="file" multiple hidden onchange="uploadSelectedFile()">
          <textarea class="textarea" id="inp" rows="1" aria-label="${escapeUi(uiText("Сообщение"))}" placeholder="Сообщение..." oninput="onComposerInput(this.value)" onkeydown="handleMessageKeydown(event)">${escapeHtml(state.composerText || "")}</textarea>
          <button class="icon-button" id="btn_voice" onclick="toggleVoice()" aria-label="${escapeUi(uiText("Голосовое сообщение"))}" title="${escapeUi(uiText("Голосовое сообщение"))}">${icon('mic')}</button>
          <button class="icon-button circle-note-button" id="btn_video_note" onclick="toggleVideoNote()" aria-label="${escapeUi(uiText("Записать видеокружок"))}" title="${escapeUi(uiText("Видеокружок"))}">${icon('video')}</button>
          <button class="icon-button send-button" onclick="doSend()" aria-label="${escapeUi(uiText("Отправить сообщение"))}" title="${escapeUi(uiText("Отправить"))}">${icon('send')}</button>
        </div>
      </div>
    `;
  }
  // Channel - check permissions
  const myRole = state.roomMembers?.find(m => String(m.id) === String(state.me?.id))?.role;
  const canWrite = myRole === "owner" || myRole === "admin";
  if (canWrite) {
    return `
      <div class="compose-wrapper">
        ${editHtml}
        ${replyHtml}
        <div class="compose">
          ${iconButton('attach',uiText("Прикрепить файл"),'triggerFileSelect()')}${iconButton('more',uiText("Другие сообщения"),'openComposeFeatures()')}
          <input type="file" id="file" multiple hidden onchange="uploadSelectedFile()">
          <textarea class="textarea" id="inp" rows="1" aria-label="${escapeUi(uiText("Сообщение"))}" placeholder="Сообщение канала..." oninput="onComposerInput(this.value)" onkeydown="handleMessageKeydown(event)">${escapeHtml(state.composerText || "")}</textarea>
          <button class="icon-button send-button" onclick="doSend()" aria-label="${escapeUi(uiText("Отправить сообщение"))}" title="${escapeUi(uiText("Отправить"))}">${icon('send')}</button>
        </div>
      </div>
    `;
  }
  return `<div class="compose" style="justify-content:center;color:var(--muted);">${escapeUi(uiText("📢 Канал только для чтения"))}</div>`;
}

function renderStatus(m) {
  if (m.delivery) {
    const labels = { sending: uiText("Отправляется…"), queued: uiText("Ожидает соединения"), failed: uiText("Не отправлено") };
    const id = escapeJsSingleQuoted(m.client_message_id);
    return `<span class="delivery-status" title="${escapeHtml(m.error || '')}">${labels[m.delivery] || ''}</span>${m.delivery === 'failed' ? `<button class="inline-action" onclick="retryQueuedMessage('${id}')">${escapeUi(uiText("Повторить"))}</button>` : ''}${m.delivery !== 'sending' ? `<button class="inline-action" onclick="cancelQueuedMessage('${id}')">${escapeUi(uiText("Отменить"))}</button>` : ''}`;
  }
  // Only show for my messages
  const senderId = String(m.sender_id || "");
  const myId = String(state.me?.id || "");
  if (senderId !== myId) return "";

  // Check read_by array for other users
  let readBy = m.read_by;
  // Parse if string (from JSON)
  if (typeof readBy === 'string') {
    try { readBy = JSON.parse(readBy); } catch { readBy = []; }
  }
  if (!Array.isArray(readBy)) readBy = [];

  // Normalize to array of strings
  readBy = readBy.map(x => typeof x === 'string' ? x : String(x));

  // Get other members in room (excluding myself)
  const otherMembers = (state.roomMembers || []).filter(x => String(x.id) !== myId);

  // Count how many other members have read this message
  const readCount = otherMembers.filter(x => readBy.includes(String(x.id))).length;
  const totalOthers = otherMembers.length;

  // Telegram logic:
  // - One checkmark (✓): Message sent to server
  // - Two checkmarks (✓✓): Message delivered to at least one recipient's device
  // - Blue double checkmarks: Message read by all recipients (in our case - read by everyone)

  if (totalOthers === 0) {
    // No other members (shouldn't happen in DM/group)
    return `<span class="msg-status" title="${escapeUi(uiText("Отправлено"))}">${icon('check')}</span>`;
  }

  if (readCount === 0) {
    // Not read by anyone yet - show single checkmark
    return `<span class="msg-status" title="${escapeUi(uiText("Отправлено"))}">${icon('check')}</span>`;
  }

  if (readCount >= totalOthers) {
    // Read by everyone - show blue double checkmarks
    return `<span class="msg-status read" title="${escapeUi(uiText("Прочитано всеми"))}">${icon('checks')}</span>`;
  }

  // Read by some but not all - show gray double checkmarks
  return `<span class="msg-status delivered" title="Прочитано (${readCount}/${totalOthers})">${icon('checks')}</span>`;
}

function shouldStickToBottom(box) {
  if (!box) return true;
  const threshold = 80;
  return box.scrollHeight - box.scrollTop - box.clientHeight < threshold;
}

function formatNoteDuration(seconds) {
  const total = Math.max(0, Math.round(Number(seconds) || 0));
  return `${Math.floor(total / 60)}:${String(total % 60).padStart(2, "0")}`;
}

// Video note (кружок): circular preview, tap to play/pause, progress ring and a
// mute toggle — the way Telegram renders them. The ring is an SVG circle whose
// stroke-dashoffset tracks playback, so no per-frame layout work happens.
function renderVideoNote(m) {
  const src = accountMediaUrl(`/api/media/${m.media_id}`);
  const poster = m.thumb_path ? ` poster="${accountMediaUrl(`/api/media/${m.media_id}/thumb`)}"` : "";
  const duration = Number(m.duration_sec) > 0 ? formatNoteDuration(m.duration_sec) : "";
  return `
    <div class="video-note" data-video-note role="button" tabindex="0"
         aria-label="Видеокружок${duration ? `, ${duration}` : ""}"
         onclick="toggleVideoNotePlayback(this)"
         onkeydown="if(event.key===' '||event.key==='Enter'){event.preventDefault();toggleVideoNotePlayback(this);}">
      <video src="${src}"${poster} playsinline loop preload="metadata"></video>
      <svg class="vn-ring" viewBox="0 0 100 100" aria-hidden="true">
        <circle class="vn-ring-track" cx="50" cy="50" r="48"></circle>
        <circle class="vn-ring-progress" cx="50" cy="50" r="48"></circle>
      </svg>
      ${duration ? `<span class="vn-badge">${duration}</span>` : ""}
      <button type="button" class="vn-mute" aria-label="${escapeUi(uiText("Звук"))}" aria-pressed="false"
              onclick="toggleVideoNoteMute(event, this)">${icon('speaker')}</button>
    </div>
  `;
}

function renderMessageBody(m) {
  if(m._album?.length>1)return `<div class="message-album">${m._album.map((item,index)=>`<div class="album-tile" ${index?`id="msg-${escapeHtml(item.id)}"`:''} data-message-id="${escapeHtml(item.id)}" oncontextmenu="openMessageMenu('${escapeJsSingleQuoted(item.id)}',event)">${renderMessageBody(item)}</div>`).join('')}</div>`;
  if(m.type==='voice' && m.media_id)return renderVoicePlayer(m,m.meta?.media_encryption?(webMediaUrls.get(state.me?.id+':'+m.media_id)?.url || ''):accountMediaUrl('/api/media/'+encodeURIComponent(m.media_id)));
  if (m.meta?.media_encryption) return `<button class="encrypted-media" data-encrypted-media="${escapeHtml(m.id)}" onclick="openWebMedia('${escapeJsSingleQuoted(m.id)}')">${icon('attach')}<span>${escapeHtml(m.meta.media_encryption.name || uiText("Вложение"))} ${escapeUi(uiText("· Открыть"))}</span></button>`;
  if (m.type === "voice" && m.media_id) {
    return `<audio class="media-audio" src="${accountMediaUrl(`/api/media/${m.media_id}`)}" controls preload="metadata" playsinline></audio>`;
  }
  if (m.type === "video_note" && m.media_id) {
    return renderVideoNote(m);
  }
  if (m.type === "media" && m.media_id) {
    if (m.media_mime?.startsWith("image/")) {
      return `<img class="media-image" src="${accountMediaUrl(`/api/media/${m.media_id}`)}" onclick="openWebMedia('${escapeJsSingleQuoted(m.id)}')" alt="${escapeUi(uiText("Фотография"))}" loading="lazy">`;
    }
    if (m.media_mime?.startsWith("video/")) {
      return `<video class="media-video" src="${accountMediaUrl(`/api/media/${m.media_id}`)}" controls playsinline ${m.thumb_path ? `poster="${accountMediaUrl(`/api/media/${m.media_id}/thumb`)}"` : ""}></video>`;
    }
    return `<a class="media-file" href="${accountMediaUrl(`/api/media/${m.media_id}`)}" target="_blank">${escapeUi(uiText("📎 Файл"))}</a>`;
  }
  const plain=(m._text??m.text??'').toString(),payload=parseMessagePayload(plain);if(payload)return `<div data-expressive-message="${escapeHtml(m.id)}">${escapeHtml(expressiveSummary(payload))}</div>`;
  return `<div class="bText">${formatConversationText(plain,renderMentionText)}</div>`;
}

const VN_RING_LENGTH = 2 * Math.PI * 48;

function updateVideoNoteRing(note) {
  const video = note.querySelector("video");
  const ring = note.querySelector(".vn-ring-progress");
  if (!video || !ring) return;
  const duration = Number.isFinite(video.duration) && video.duration > 0 ? video.duration : 0;
  const fraction = duration ? Math.min(1, video.currentTime / duration) : 0;
  ring.style.strokeDasharray = String(VN_RING_LENGTH);
  ring.style.strokeDashoffset = String(VN_RING_LENGTH * (1 - fraction));
  ring.style.opacity = fraction > 0 ? "1" : "0";
}

// Only one video note plays at a time, like a voice message.
function pauseOtherVideoNotes(current) {
  for (const other of $$(".video-note")) {
    if (other === current) continue;
    const video = other.querySelector("video");
    if (video && !video.paused) video.pause();
  }
}

function bindVideoNote(note) {
  if (note.dataset.vnBound === "1") return;
  note.dataset.vnBound = "1";
  const video = note.querySelector("video");
  if (!video) return;
  video.addEventListener("timeupdate", () => updateVideoNoteRing(note));
  video.addEventListener("play", () => {
    note.classList.add("playing");
    pauseOtherVideoNotes(note);
    updateVideoNoteRing(note);
  });
  video.addEventListener("pause", () => note.classList.remove("playing"));
  video.addEventListener("ended", () => {
    note.classList.remove("playing");
    updateVideoNoteRing(note);
  });
  video.addEventListener("loadedmetadata", () => updateVideoNoteRing(note));
  // Start muted so a tap never blasts audio unexpectedly; the mute button
  // is the explicit opt-in, matching Telegram's default for кружки.
  video.muted = true;
  updateVideoNoteRing(note);
}

function bindAllVideoNotes() {
  for (const note of $$(".video-note")) bindVideoNote(note);
}

window.toggleVideoNotePlayback = (container) => {
  const video = container?.querySelector?.("video");
  if (!video) return;
  if (video.paused) {
    video.play().catch(() => {});
  } else {
    video.pause();
  }
};

window.toggleVideoNoteMute = (event, button) => {
  event?.stopPropagation?.();
  const note = button?.closest?.(".video-note");
  const video = note?.querySelector?.("video");
  if (!video) return;
  video.muted = !video.muted;
  button.setAttribute("aria-pressed", String(video.muted));
  button.classList.toggle("muted", video.muted);
};

function buildRenderedMessagesHtml() {
  const currentRoom = state.rooms.find(r => r.id === state.activeRoomId);
  const isDM = currentRoom?.kind === "dm";
  let lastRenderedDay = null;

  const display=groupMessageAlbums(state.messages.filter(m=>!isExpressiveControl(parseMessagePayload(m._text??m.text))));
  return display.map((m, index) => {
    const senderId = String(m.sender_id || "");
    const msgId = String(m.id || "");
    const mine = senderId === String(state.me?.id || "");
    const senderName = m.sender_display_name || m.sender_username || "?";
    const senderAvatar = m.sender_avatar_id ? avatarUrl(m.sender_avatar_id) : null;
    const safeSenderName = escapeHtml(senderName);
    const safeMsgId = escapeJsSingleQuoted(msgId);
    const showSenderInfo = !isDM && !mine;
    const isReplyingTo = state.replyTo?.id === msgId;
    const needsDaySeparator = lastRenderedDay !== dayKey(m.created_at);
    lastRenderedDay = dayKey(m.created_at);
    const meta = m.meta && typeof m.meta === "object" ? m.meta : {};
    const editedBadge = meta.edited || meta.edited_at ? `<span class="msg-edited">${escapeUi(uiText("изменено"))}</span>` : "";

    const replyPreview = m.reply_to ? `
      <div class="reply-reference" onclick="scrollToMessage('${m.reply_to.id}', event)">
        <div class="reply-line"></div>
        <div class="reply-info">
          <div class="reply-name">${escapeHtml(m.reply_to.sender_name || uiText("Пользователь"))}</div>
          <div class="reply-text">${escapeHtml((m.reply_to.text || '').slice(0, 60))}${(m.reply_to.text || '').length > 60 ? '...' : ''}</div>
        </div>
      </div>
    ` : '';

    const myId = String(state.me?.id || "");
    const reactions = m.reactions || {};
    const reactionsHtml = Object.keys(reactions).length ? `<div class="reactions">${Object.entries(reactions).map(([emoji, users]) => {
      const people = reactionPeople(users, [state.me, ...state.roomMembers, state.activeDmPeer]);
      const mine = people.some(person => String(person.id) === myId);
      const names = people.map(person => person.display_name || person.username).join(', ');
      const faces = isDM ? `<span class="reaction-faces">${people.map(person => avatar(person.id, person.display_name || person.username, avatarUrl(person.avatar_id))).join('')}</span>` : people.length;
      return `<button class="reaction-tag ${mine ? 'my-reaction' : ''}" title="${escapeHtml(names)}" aria-label="${escapeHtml(emoji + ': ' + names)}" onclick="${mine ? `removeReaction('${safeMsgId}', '${escapeJsSingleQuoted(emoji)}', event)` : `addReaction('${safeMsgId}', '${escapeJsSingleQuoted(emoji)}')`}">${escapeHtml(emoji)}${faces}</button>`;
    }).join('')}</div>` : '';
    const groupedBefore = groupWith(display[index - 1], m);
    const groupedAfter = groupWith(m, display[index + 1]);

    return `
      ${needsDaySeparator ? `<div class="day-separator" data-day-label="${escapeHtml(formatDayLabel(m.created_at))}"><span>${formatDayLabel(m.created_at)}</span></div>` : ""}
      <div class="bubble ${mine ? "me" : ""} ${isDM ? 'dm' : ''} ${isReplyingTo ? 'replying-to' : ''} ${groupedBefore ? 'group-before' : ''} ${groupedAfter ? 'group-after' : ''} ${state.selectedMessages?.has(msgId) ? 'selected-message' : ''}" onclick="selectMessageIfNeeded('${safeMsgId}',event)" id="msg-${escapeHtml(msgId)}" data-message-id="${escapeHtml(msgId)}"
           oncontextmenu="openMessageMenu('${safeMsgId}', event)"
           ontouchmove="cancelMessagePress()"
           ondblclick="replyToMessage('${safeMsgId}', event)"
           ontouchstart="handleTouchStart(event, '${safeMsgId}')"
           ontouchend="handleTouchEnd(event)">
        ${showSenderInfo && senderId ? `<div class="bAvatar">${senderAvatar ? `<img src="${senderAvatar}">` : `<div class="avatar-placeholder-sm"></div>`}</div>` : ""}
        <button class="icon-button message-menu-trigger" aria-label="${escapeUi(uiText("Действия с сообщением"))}" onclick="openMessageMenu('${safeMsgId}', event)">${icon('more')}</button>
        <div class="bContent">
          ${showSenderInfo && senderId ? `<div class="bSender">${safeSenderName}</div>` : ""}
          ${replyPreview}
          ${renderMessageBody(m)}
          ${m.type !== "text" && (m._text || m.text) ? `<div class="bText media-caption">${renderMentionText(m._text || m.text)}</div>` : ""}
          <div class="bTime">${formatMessageTime(m.created_at)} ${editedBadge}${renderStatus(m)}</div>
          ${reactionsHtml}
        </div>
      </div>
    `;
  }).join("");
}

function renderMessages() {
  updateChatPresence(); const box = $('#msgs'); if (!box) return;
  box.dataset.roomId=state.activeRoomId || '';
  if (state.roomLoading || state.roomLoadError) { box.innerHTML = state.roomLoadError ? `<div class="list-hint">${escapeUi(uiText("Не удалось загрузить переписку."))}<button class="btn" onclick="selectRoom(stateRoomId())">${escapeUi(uiText("Повторить"))}</button></div>` : `<div class="list-hint" role="status">${escapeUi(uiText("Загрузка сообщений…"))}</div>`; return; }
  const bottom = shouldStickToBottom(box);
  const older = state.chatHistory?.has_older !== false ? `<button id="loadOlderMessages" class="history-load" onclick="loadOlderMessages()" ${state.loadingOlder ? 'disabled' : ''}>${state.loadingOlder ? uiText("Загрузка…") : uiText("Загрузить предыдущие сообщения")}</button>` : '';
  reconcileMessages(box, `<div id="scrollDateIndicator" class="scroll-date-indicator" aria-hidden="true"></div>${older}${buildRenderedMessagesHtml()}`, bottom);
  bindMessagesScrollIndicator(); syncScrollDateIndicator(box, false); chatRuntime.bind(); bindAllVideoNotes();bindVoicePlayers(box,{findMessage:id=>state.messages.find(message=>message.id===id),source:resolveWebMedia,listened:message=>chatRuntime.markListened(message),report:error=>toast(userError(error))}); void hydrateVisibleMedia();void hydrateExpressiveMessages().catch(error=>toast(userError(error)));
}
function render() { renderMessages(); }

// --- Auth ---
function authReport(message, signedIn = false) {
  const node = $('#authError');
  if (node && !signedIn) { node.textContent = message; node.hidden = !message; }
  else if (message) toast(message);
}
let accountAuthBusy=false;
function setAccountAuthBusy(value){accountAuthBusy=value;document.querySelectorAll('.auth-card button,.auth-card input').forEach(node=>node.disabled=value);}
async function initializeAuthenticatedWeb(){
 const user=state.me,generation=browserSessionGeneration(),current=()=>state.me?.id===user?.id&&generation===browserSessionGeneration();if(!user)return;
 localStorage.setItem('67chat-profile:'+user.id,JSON.stringify(user));await chatRuntime.start(user);if(!current())return;await pruneSecretVault(chatRuntime.local());if(!current())return;
 connectWS();await loadRooms();if(!current())return;await syncCanonicalWebRoute();if(!current())return;
 void refreshQualityPreference().catch(()=>{});void initE2E();void initServiceWorker();
}
async function prepareAuthenticatedWebUser(user){
 selectBrowserAccount(user.id);
 try{
  await recoverWebBackup({user,request:featureRequest});
  if(pendingWebRestore){const result=await restoreWebBackup({stage:pendingWebRestore,user,request:featureRequest});pendingWebRestore=null;if(result.warning)toast(result.warning);}
 }catch(error){await featureRequest('/auth/logout',{method:'POST',body:{}}).catch(()=>{});throw error;}
}
async function switchBrowserAccount(id,target){
 if(accountAuthBusy)return;setAccountAuthBusy(true);resetClientSession();selectBrowserAccount(id);const generation=browserSessionGeneration();
 try{
  await api('/auth/accounts/select',{method:'POST',body:JSON.stringify({user_id:id})});if(generation!==browserSessionGeneration())return;
  const user=await api('/me');if(generation!==browserSessionGeneration())return;if(user?.id!==id)throw Error('account_session_unavailable');
  await prepareAuthenticatedWebUser(user);if(selectedBrowserAccount()!==id)return;state.me=user;
  const route=new URL('/',location.origin);if(target?.roomId)route.searchParams.set('room',target.roomId);if(target?.messageId)route.searchParams.set('msg',target.messageId);
  history.replaceState(null,'',route.pathname+route.search);await mount();await initializeAuthenticatedWeb();
 }
 catch(error){if(selectedBrowserAccount()===id){resetClientSession();selectBrowserAccount('none');await mount();authReport(userError(error));}}
 finally{setAccountAuthBusy(false);}
}
async function addBrowserAccount(){if(accountAuthBusy)return;resetClientSession();selectBrowserAccount('none');history.replaceState(null,'','/');await mount();}
window.openBrowserAccounts=async()=>{
 const {dialog,content,status}=webActionDialog(uiText('Аккаунты'));
 try{const {accounts}=await api('/auth/accounts');for(const account of accounts){dialogButton(content,account.username,async()=>{dialog.close();await switchBrowserAccount(account.id);},status);}dialogButton(content,uiText('Добавить аккаунт'),async()=>{dialog.close();await addBrowserAccount();},status);if(!accounts.length)status.textContent=uiText('Войдите в аккаунт');}
 catch(error){status.textContent=userError(error);}
};
const authenticate = createAuthFlow({ request: api, setUser: user => { state.me = user; }, render: () => mount(),
  resolveChallenge:challenge=>completeWebAccountChallenge(featureRequest,challenge),prepareUser:prepareAuthenticatedWebUser,
  initialize:initializeAuthenticatedWeb, report:authReport,busy:setAccountAuthBusy
});
window.login = () => {if(!accountAuthBusy)return authenticate('login',{username:$('#u').value.trim(),password:$('#p').value,device_key:webLoginDevice()});};
window.startQrLogin=async()=>{
 if(accountAuthBusy)return;if(pendingWebRestore)return authReport(uiText("Для восстановления копии войдите с паролем аккаунта."));
 setAccountAuthBusy(true);try{const result=await loginWithWebQr(featureRequest);if(!result?.user?.id)throw Error("Вход не подтверждён");await prepareAuthenticatedWebUser(result.user);state.me=result.user;await mount();void initializeAuthenticatedWeb().catch(error=>authReport(userError(error),true));}catch(error){authReport(userError(error));}finally{setAccountAuthBusy(false);}
};
window.retryBootstrap = async () => { try { await loadRooms(); } catch (error) { toast(userError(error)); } };

// Captcha handling
let captchaToken = "";
window.onCaptchaSuccess = (token) => { captchaToken = token; };

window.showRegister = async event => {
  const btn = event.currentTarget;
  try {
    const config = await api('/auth/config');
    if (!config.captcha_required) return window.register();
    const container = $('#captcha-container'); if (container) container.style.display = 'block';
    if (!window.hcaptcha) { const script = document.createElement('script'); script.src = 'https://js.hcaptcha.com/1/api.js'; script.async = true; document.head.append(script); }
    btn.onclick = () => window.register();
    authReport(uiText("Пройдите проверку и нажмите «Создать аккаунт»."));
  } catch (error) { authReport(userError(error)); }
};
window.register = async () => {
  if(accountAuthBusy)return;if(pendingWebRestore)return authReport(uiText("Для восстановления войдите в существующий аккаунт."));
  const username = $('#u').value.trim(), password = $('#p').value;
  if (!/^[a-zA-Z0-9_]{3,30}$/.test(username)) return authReport(uiText("Имя: 3–30 латинских букв, цифр или знаков подчёркивания."));
  if (password.length < 6) return authReport(uiText("Пароль должен содержать не менее 6 символов."));
  await authenticate('register', { username, password, captcha: captchaToken });
};

window.logout = async () => {
  if(accountAuthBusy)return;setAccountAuthBusy(true);
  const secretStore=chatRuntime.local();invalidateBrowserSession();
  for(const dialog of document.querySelectorAll('dialog')){dialog.close();dialog.remove();}
  try{if(secretStore)await eraseSecretVault(secretStore);
    const disconnect=webNotifications.disable(),logout=api('/auth/logout',{method:'POST',body:'{}'}),completing=Promise.allSettled([disconnect,logout]);
    resetClientSession();selectBrowserAccount('none');await mount();setAccountAuthBusy(true);
    await completing;
  }finally{setAccountAuthBusy(false);}
};

async function loadRooms() {
  const account = state.me?.id,generation=browserSessionGeneration(),local=chatRuntime.local(); if (!account) return;
  const current=()=>state.me?.id===account&&generation===browserSessionGeneration();
  const raw=await api('/rooms');if(!current())return;
  const rooms=await prepareRoomPreviews(raw||[]);
  if (!current()) return;
  state.rooms = rooms;
  const folders = await api("/me/folders");
  if (!current()) return;
  state.folders = Array.isArray(folders) ? folders : [];
  await local?.set("folders", state.folders);if(!current())return;
  await local?.set('rooms',rooms);if(current())updateRoomsList();
}

async function prepareRoomPreviews(rooms) {
  return Promise.all((rooms || []).map(async room => {
    if (room.last_msg_type !== "text") return room;
    const hasText = room.last_msg_text != null && room.last_msg_text !== "";
    const hasEncrypted = !!room.last_msg_encrypted_content || !!room.last_msg_meta?.encrypted || !!room.last_msg_meta?.e2e;
    if (!hasText && !hasEncrypted) return room;

    try {
      const text = await decryptMsg(room.id, {
        id: room.last_msg_id,
        room_id: room.id,
        sender_id: room.last_msg_sender_id,
        type: "text",
        text: room.last_msg_text,
        meta: room.last_msg_meta || {},
        encrypted_content: room.last_msg_encrypted_content || null
      });
      if (text && !text.startsWith("[Encrypted")) {
        return {...room,_lastText:expressiveSummary(parseMessagePayload(text))||text};
      }
    } catch (e) {
      // intentionally ignored
    }
    return room;
  }));
}

function updateRoomsList() {
  if (!state.activeRoomId) {
    mount(); // Sidebar/search state is rendered by mount in list view
  } else {
    // Only update room list sidebar
    const roomList = document.getElementById('roomList');
    if (roomList) {
      roomList.innerHTML=state.rooms.filter(r=>!r.preferences?.hidden&&!!r.preferences?.archived===!!state.archiveOpen&&(!state.selectedFolder||state.folders?.find(f=>f.id===state.selectedFolder)?.room_ids?.includes(r.id))).sort((a,b)=>Number(!!b.preferences?.pinned)-Number(!!a.preferences?.pinned)).map(r=>renderRoomItem(r)).join('');
    }
  }

  if (state.me && !state.activeRoomId && !state.rooms.length) mountPreservingInput();
  // Update page title with total unread count
  updatePageTitle();
}

function updatePageTitle() {
  const totalUnread = state.rooms.reduce((sum, r) => sum + (r.unread_count || 0), 0);

  if (totalUnread > 0) {
    document.title = `(${totalUnread > 99 ? '99+' : totalUnread}) 67chat`;
  } else {
    document.title = '67chat';
  }
  if ('setAppBadge' in navigator) {
    const operation = totalUnread ? navigator.setAppBadge(totalUnread) : navigator.clearAppBadge();
    operation?.catch(() => {});
  }
}

// --- Message Search ---
window.toggleSearch = () => {
  state.searchMode = !state.searchMode;

  if (!state.searchMode) {
    // Clear search
    state.searchQuery = '';
    state.searchResults = [];
    state.currentSearchIndex = 0;
    clearSearchHighlights();
  }

  mount();

  if (state.searchMode) {
    // Focus search input
    setTimeout(() => {
      const searchInput = document.getElementById('searchInput');
      if (searchInput) searchInput.focus();
    }, 100);
  }
};

let historySearchTimer;
window.searchMessages = query => { state.searchQuery = query; clearTimeout(historySearchTimer); chatRuntime.cancelSearch(); historySearchTimer = setTimeout(() => void chatRuntime.search(query).catch(e => toast(userError(e))), 250); };

window.nextSearchResult = () => {
  if (state.searchResults.length === 0) return;

  state.currentSearchIndex = (state.currentSearchIndex + 1) % state.searchResults.length;
  scrollToSearchResult(state.currentSearchIndex);
  mountPreservingInput();
};

window.prevSearchResult = () => {
  if (state.searchResults.length === 0) return;

  state.currentSearchIndex = (state.currentSearchIndex - 1 + state.searchResults.length) % state.searchResults.length;
  scrollToSearchResult(state.currentSearchIndex);
  mountPreservingInput();
};

function scrollToSearchResult(index) { const item = state.searchResults[index]; if (item) void chatRuntime.jump((item.msg || item).id).catch(e => toast(userError(e))); }

function clearSearchHighlights() {
  document.querySelectorAll('.search-highlight, .search-highlight-current').forEach(el => {
    el.classList.remove('search-highlight', 'search-highlight-current');
  });
}

// Keyboard shortcuts
document.addEventListener('keydown', (e) => {
  // Ctrl+F or Cmd+F - open search
  if ((e.ctrlKey || e.metaKey) && e.key === 'f' && state.activeRoomId) {
    e.preventDefault();
    if (!state.searchMode) {
      toggleSearch();
    }
  }

  // Escape - close search
  if (e.key === 'Escape' && state.searchMode) {
    toggleSearch();
  }

  // Ctrl+G or F3 - next result
  if (((e.ctrlKey || e.metaKey) && e.key === 'g') || e.key === 'F3') {
    e.preventDefault();
    if (state.searchMode && state.searchResults.length > 0) {
      if (e.shiftKey) {
        prevSearchResult();
      } else {
        nextSearchResult();
      }
    }
  }
});

let roomRequestId = 0;
window.selectRoom = async (id, options = {}) => {
  if(state.activeRoomId!==id)stopVoicePlayers();
  saveWebDraft(); ++roomRequestId; state.editingRoom = false;
  if(!options.fromRoute){const target=options.around?messageLink(id,options.around):'/chat/'+encodeURIComponent(id);history[options.replace?'replaceState':'pushState']({roomId:id},'',target);}
  if (state.wsAuthed) state.ws?.send(JSON.stringify({ type: 'rooms.join', room_id: id }));
  try { await chatRuntime.open(id, options);void syncWebDraft(id).catch(error=>toast(userError(error))); } catch (error) { toast(userError(error)); }
};
async function markMessagesRead() { chatRuntime.bind(); }

async function buildTextMessage(roomId, currentRoom, text) {
  const session=captureWebSession();session.guard();
  if(currentRoom?.kind==='group'){const enabled=await roomE2eEnabled(roomId);session.guard();if(enabled)return session.wrap(encryptGroupText)(roomId,text);}
  const key = await getCryptoKey(roomId, { e2ee_v: 2 });
  session.guard();
  if (key) {
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const enc = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, new TextEncoder().encode(text));
    session.guard();
    return {
      type: "text",
      text: bytesToBase64(new Uint8Array(enc)),
      meta: { encrypted: true, e2ee_v: 2, iv: bytesToBase64(iv) }
    };
  }

  return { type: "text", text, meta: { encrypted: false } };
}

// --- Messaging ---
let sendingDraft = false;
window.doSend = async () => {
  if (sendingDraft) return;
  const input = $('#inp'), roomId = state.activeRoomId, account = state.me?.id;
  const text=input?.value.trim(),reply=state.replyTo,editing=state.editMessage,selfDestruct=webMessageTTLs.get(roomId)||0;
  if (!text || !roomId) return;
  if (text.length > 5000) return toast(uiText("Сообщение слишком длинное: максимум 5000 символов."));
  sendingDraft = true;
  try {
    if (editing) {
      const body = await buildTextMessage(roomId, state.rooms.find(r => r.id === roomId), text);
      if (state.me?.id !== account) return;
      const sent = await api(`/rooms/${roomId}/messages/${editing.id}`, { method: 'PUT', body: JSON.stringify(body) });
      await chatRuntime.received(sent);
    }else if(selfDestruct){const room=state.rooms.find(r=>r.id===roomId),body=await buildTextMessage(roomId,room,text);body.meta={...body.meta,self_destruct_seconds:selfDestruct};if(reply){body.reply_to={id:reply.id,sender_name:reply.sender_display_name||reply.sender_username,text:uiText("Ответ на сообщение")};body.meta.reply_preview=await buildTextMessage(roomId,room,reply._text??reply.text??uiText("Вложение"));}if(state.me?.id!==account)return;await chatRuntime.enqueue({roomId,text,body,reply});}else await chatRuntime.enqueue({roomId,text,reply});
    if (state.me?.id === account && state.activeRoomId === roomId && $('#inp')?.value.trim() === text) {
      state.replyTo=null;state.editMessage=null;if(editing){state.composerText=webDrafts.get(roomId)||'';$('#inp').value=state.composerText;}else{state.composerText='';$('#inp').value='';chatRuntime.draft(roomId,'');queueWebDraftSync(roomId);}resizeComposer();
      if (reply || editing) await mountPreservingInput();
    }
  } catch (error) { toast(userError(error)); } finally { sendingDraft = false; }
};

window.setE2EE = async () => {
  const k = prompt(uiText("Ключ шифрования:"));
  if (k) await storeWrappedRoomPassphrase(state.activeRoomId, k);
  selectRoom(state.activeRoomId);
};

// Recording and uploads preserve the original account/recipient and require confirmation.
window.toggleVoice = () => void webMedia.compose('voice').catch(e => toast(userError(e)));
window.toggleVideoNote = () => void webMedia.compose('circle').catch(e => toast(userError(e)));
let selectedFileContext;
window.triggerFileSelect = () => {
  const dialog=document.createElement('dialog');dialog.className='actions-dialog';const title=document.createElement('h2');title.textContent=uiText("Вложения");dialog.append(title);
  const owner={userId:state.me?.id,roomId:state.activeRoomId,title:state.rooms.find(r=>r.id===state.activeRoomId)?.title || state.rooms.find(r=>r.id===state.activeRoomId)?.other_display_name || uiText("Переписка"),store:chatRuntime.local()};
  const close=()=>{dialog.close();dialog.remove();};
  const add=(label,action)=>{const b=document.createElement('button');b.textContent=label;b.onclick=()=>{close();action();};dialog.append(b);};
  const choose=accept=>{selectedFileContext=owner;const input=$('#file');if(input){input.accept=accept;input.click();}};
  add(uiText("Фото и видео"),()=>choose('image/*,video/*'));add(uiText("Файл"),()=>choose(''));add(uiText("Черновики вложений"),()=>void webMedia.compose('files',[],owner).catch(e=>toast(userError(e))));add(uiText("Голосовое сообщение"),()=>void webMedia.compose('voice',[],owner).catch(e=>toast(userError(e))));add(uiText("Видеокружок"),()=>void webMedia.compose('circle',[],owner).catch(e=>toast(userError(e))));add(uiText("Отмена"),()=>{});document.body.append(dialog);dialog.showModal();
};
window.uploadSelectedFile = () => { const input = $('#file'), files = [...(input?.files || [])],owner=selectedFileContext; if (input) input.value = ''; if (files.length) void webMedia.compose('files', files,owner).catch(e => toast(userError(e))); };

// --- Calls ---
let currentCamera = 'user'; // 'user' for front, 'environment' for back

window.startRoomCall = async (isVideo=false) => {
 if(!state.wsAuthed){toast(uiText("Нет подключения"));return;}if(state.callState!=='idle')return;
 const roomId=state.activeRoomId;if(!roomId)return;
 if(state.rooms.find(room=>room.id===roomId)?.kind==='group'){await window.openCallTools('group');return;}
 pauseMessageMedia();
 const generation=++state.callGeneration,current=()=>generation===state.callGeneration&&state.currentCallRoomId===roomId;
 state.callInitiator=true;state.currentCallId=null;state.callResumeToken=null;state.isVideoCall=!!isVideo;state.isVideoEnabled=!!isVideo;state.isAudioEnabled=true;state.callState='calling';state.currentCallRoomId=roomId;state.currentCallPeerId=null;state.pendingIceCandidates=[];
 if(isVideo)showVideoCallUI();else showAudioCallUI();let stream,pc;
 try{
  stream=await navigator.mediaDevices.getUserMedia({...getAudioConstraints(),video:isVideo?{facingMode:'user',width:{ideal:1280},height:{ideal:720}}:false});
  if(!current()){stream.getTracks().forEach(track=>track.stop());return;}state.localStream=stream;
  await loadRtcIceServers();if(!current()){stream.getTracks().forEach(track=>track.stop());return;}
  pc=new RTCPeerConnection({iceServers:getRtcIceServers(),iceTransportPolicy:'all',iceCandidatePoolSize:4});state.pc=pc;
  if(isVideo)showVideoCallUI();else showAudioCallUI();updateCallStatus(uiText("Вызов..."),false);wirePeerConnectionHandlers({roomId,peerUserId:null});stream.getTracks().forEach(track=>pc.addTrack(track,stream));
  state.callOfferTimer=setTimeout(()=>{if(current()&&state.callState==='calling'){toast(uiText("Абонент не ответил"));hangupCall(true,'timeout');}},45000);
  const offer=await pc.createOffer();if(!current())return;await pc.setLocalDescription(offer);if(!current())return;
  state.ws?.send(JSON.stringify({type:'call.offer',room_id:roomId,is_video:!!isVideo,offer,sdp:offer}));
 }catch(error){if(!current()){stream?.getTracks().forEach(track=>track.stop());pc?.close();return;}console.error('Failed to start call:',error);toast(uiText("Не удалось начать звонок. Проверьте разрешения микрофона и камеры."));hangupCall();}
};

window.jumpToDate=()=>{const room=state.activeRoomId,{dialog,content,status}=webActionDialog(uiText("Перейти к дате")),input=document.createElement('input');input.type='date';input.setAttribute('aria-label',uiText("Дата сообщения"));content.append(input);dialogButton(content,uiText("Перейти"),async()=>{if(!input.value)return;const result=await api(`/rooms/${room}/anchor?date=${encodeURIComponent(new Date(input.value+'T00:00:00').toISOString())}`);if(!result.message_id){status.textContent=uiText("После этой даты сообщений нет");return;}dialog.close();if(state.activeRoomId===room)await chatRuntime.jump(result.message_id);else await window.selectRoom(room,{around:result.message_id});},status);};

window.addEventListener('popstate',()=>void syncCanonicalWebRoute().catch(error=>toast(userError(error))));
window.addEventListener('online',()=>{if(state.activeRoomId)void syncWebDraft(state.activeRoomId).catch(error=>toast(userError(error)));});
document.addEventListener('visibilitychange',()=>{if(!document.hidden&&state.activeRoomId)void syncWebDraft(state.activeRoomId).catch(error=>toast(userError(error)));});


// --- Init ---
(async () => {
  try {
    const requestedAccount=initialBrowserAccount(location.href);if(requestedAccount)selectBrowserAccount(requestedAccount);
    // Upgrade legacy HttpOnly cookies before selecting a tab-scoped account.
    let bootGeneration=browserSessionGeneration();await mount();
    await api('/auth/accounts').catch(error=>{if(error.status!==404&&error.status!==401)console.warn('Account list unavailable');});
    if(bootGeneration!==browserSessionGeneration())return;
    const selected=selectedBrowserAccount();let rejected=false;
    const verifiedUser=selected==='none'?null:await api('/me').catch(error=>{rejected=[401,403].includes(error.status);if(!rejected)toast(userError(error));return null;});
    if(bootGeneration!==browserSessionGeneration())return;
    if(verifiedUser?.id&&selected&&selected!==verifiedUser.id)throw Error('Сервер не поддерживает изоляцию выбранного аккаунта');
    if(verifiedUser?.id){selectBrowserAccount(verifiedUser.id);bootGeneration=browserSessionGeneration();}
    try{await recoverWebBackup({user:verifiedUser,request:featureRequest});}catch(error){resetClientSession();await mount();authReport(userError(error));return;}
    if(bootGeneration!==browserSessionGeneration())return;
    const cached=selected&&selected!=='none'&&!rejected?localStorage.getItem('67chat-profile:'+selected):null;
    try {const user=JSON.parse(cached||'null');state.me=user?.id===selected?user:null;}catch{state.me=null;}
    if(state.me?.id) {
      await chatRuntime.start(state.me);if(bootGeneration!==browserSessionGeneration())return;await mount();
      const roomId=new URL(location.href).searchParams.get('room');
      if(roomId && state.rooms.some(room=>room.id===roomId))void selectRoom(roomId);
    } else await mount();
    if(bootGeneration!==browserSessionGeneration())return;
    const user=verifiedUser;
    if(!user?.id){await mount();return;}
    state.me=user;localStorage.setItem('67chat-profile:'+user.id,JSON.stringify(user));await chatRuntime.start(user);if(bootGeneration!==browserSessionGeneration())return;await pruneSecretVault(chatRuntime.local());if(bootGeneration!==browserSessionGeneration())return;void refreshQualityPreference().catch(()=>{});
    connectWS();void initE2E();void initServiceWorker();
    await loadRooms().catch(error=>toast(userError(error)));if(bootGeneration!==browserSessionGeneration())return;await mount();
    const roomId=new URL(location.href).searchParams.get('room');if(roomId && !state.activeRoomId && state.rooms.some(room=>room.id===roomId))await selectRoom(roomId);
    // A shared message link (?room=…&msg=…) scrolls to that message once the
    // history has rendered.
    const linkedMessage = new URL(location.href).searchParams.get('msg');
    if (linkedMessage && state.activeRoomId) {
      requestAnimationFrame(() => window.scrollToMessage?.(linkedMessage, { highlight: true }));
    }
    await syncCanonicalWebRoute();
  }catch(error){console.error('Init error:',error);toast(userError(error));await mount();}
})();

setLanguage(localStorage.getItem('ui.language')==='en'?'en':'ru');
document.documentElement.lang=locale().slice(0,2);
subscribeLanguage(()=>{document.documentElement.lang=locale().slice(0,2);void mountPreservingInput().catch(error=>toast(userError(error)));});
