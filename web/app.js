import { createFolderManager } from './folders.js';
import { createChatActions } from './chat-actions.js';
import { createMediaController } from './media.js';
import { createChatRuntime } from './chat-runtime.js';
import { mergeMessages, createAccountStore } from './local-data.js';
import { reconcileMessages } from './message-list.js';
import { createAuthFlow } from './auth-flow.js';
import { userError, toast } from './errors.js';
import { icon, avatar, groupWith, presenceLabel, reactionPeople, plural } from './ui.js';
import { renderMessengerShell, renderLogin, renderRoomRow, iconButton } from './views.js';
import { createWebNotifications } from './notifications.js';
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
  if (!text && encrypted) text = "Зашифрованное сообщение";
  if (!text) text = PREVIEW_KINDS[room.last_msg_type] || "";
  if (room.draft) return "Черновик: " + room.draft;
  if (mine && (room._lastText || room.last_msg_text)) text = "Вы: " + text;
  else if (!direct && room.last_msg_sender_username && room._lastText) text = (room.last_msg_sender_name || room.last_msg_sender_username) + ": " + text;
  return text;
}

window.pinMessage = async (id) => {
  if (!state.activeRoomId) return;
  await api(`/rooms/${state.activeRoomId}/pin`, { method: 'PUT', body: JSON.stringify({ message_id: id }) });
  toast('Сообщение закреплено');
  await loadRooms();
  await mount();
};

window.unpinMessage = async () => {
  if (!state.activeRoomId) return;
  await api(`/rooms/${state.activeRoomId}/pin`, { method: 'PUT', body: JSON.stringify({ message_id: null }) });
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
    alert("Введите название");
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
    alert("Ошибка создания");
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

// --- Utils ---
async function api(path, opts={}) {
  const controller = new AbortController(), timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const r = await fetch(API + path, { ...opts, signal: opts.signal || controller.signal,
      headers: { ...(opts.body === undefined || opts.body instanceof FormData ? {} : { "Content-Type":"application/json" }), ...(opts.headers||{}) }, credentials: "include" });
    const body = await r.json().catch(() => null);
    if (r.status === 401 && !isAuthEntryPath(path)) handleUnauthorized();
    if (!r.ok) throw Object.assign(new Error(body?.error || `HTTP ${r.status}`), { code: body?.error, status: r.status });
    return body;
  } finally { clearTimeout(timeout); }
}

// Login/register/logout legitimately answer 401 (bad credentials etc.) and must
// not trigger the global "session expired" handling.
function isAuthEntryPath(path) {
  return /^\/auth\/(login|register|logout)(\?|$)/.test(String(path || ""));
}

// Clear all client-side session state and stop everything tied to the session.
// Used by logout, terminate-all-sessions and the forced logout on 401 / WS auth failure.
function resetClientSession() {
  if (state.autoBackupInterval) {
    clearInterval(state.autoBackupInterval);
    state.autoBackupInterval = null;
  }
  webMedia.stop();
  for (const value of webMediaUrls.values()) URL.revokeObjectURL(value.url); webMediaUrls.clear();
  chatRuntime.stop();
  localStorage.removeItem("67chat-profile");
  stopPreKeyMonitor();
  e2eSessionInflight.clear();
  e2ePeerDeviceIds.clear();
  state.e2eManager = null;
  state.e2eReady = false;

  closeWS();
  clearAllTyping();

  state.me = null;
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
  return id ? `/api/media/${id}` : null;
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
  return `/user/${encodeURIComponent(username)}`;
}

function currentProfileUsernameFromRoute() {
  const match = location.pathname.match(/^\/user\/([A-Za-z0-9_]+)\/?$/);
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
  return new Date(value).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function formatDayLabel(value) {
  const date = new Date(value);
  const now = new Date();
  const sameDay = date.toDateString() === now.toDateString();
  if (sameDay) return "Сегодня";
  const yesterday = new Date();
  yesterday.setDate(now.getDate() - 1);
  if (date.toDateString() === yesterday.toDateString()) return "Вчера";
  return date.toLocaleDateString("ru-RU", { day: "numeric", month: "long" });
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

function getRtcIceServers() {
  if (rtcIceCache) return rtcIceCache.servers;
  const servers = [{ urls: "stun:stun.l.google.com:19302" }];
  const turn = window.__APP_CONFIG__?.turn || getLocalTurnConfig();
  if (turn?.urls && turn?.username && turn?.credential) {
    servers.push({ urls: turn.urls, username: turn.username, credential: turn.credential });
  }
  return servers;
}

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

async function flushPendingIceCandidates() {
  if (!state.pc?.remoteDescription) return;
  const pending = state.pendingIceCandidates.splice(0);
  for (const candidate of pending) {
    try {
      await state.pc.addIceCandidate(new RTCIceCandidate(candidate));
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
  state.currentCallRoomId = roomId || null;
  state.currentCallPeerId = peerUserId || null;

  state.pc.onicecandidate = ev => {
    if (!ev.candidate) return;
    const payload = { type: "call.ice", candidate: ev.candidate };
    if (roomId) payload.room_id = roomId;
    if (peerUserId) payload.to_user_id = peerUserId;
    state.ws?.send(JSON.stringify(payload));
  };

  state.pc.ontrack = e => {
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
    const stateName = state.pc?.connectionState || state.pc?.iceConnectionState || "";
    if (stateName === "connected") {
      clearCallDisconnectTimer();
      state.callState = "connected";
      updateCallStatus("На связи", true);
      startCallTimer();
      flushPendingIceCandidates();
      return;
    }
    if (stateName === "connecting" || stateName === "checking") {
      clearCallDisconnectTimer();
      updateCallStatus("Соединение...", false);
      return;
    }
    if (stateName === "disconnected") {
      updateCallStatus("Переподключение...", false);
      if (!state.callDisconnectTimer) {
        state.callDisconnectTimer = setTimeout(() => {
          state.callDisconnectTimer = null;
          if (state.callState !== "idle") hangupCall(false);
        }, 5000);
      }
      return;
    }
    if (["failed", "closed"].includes(stateName)) {
      clearCallDisconnectTimer();
      hangupCall(false);
    }
  };

  state.pc.onconnectionstatechange = handleConnectionState;
  state.pc.oniceconnectionstatechange = handleConnectionState;
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
  if (roomSecretCache.has(roomId)) return roomSecretCache.get(roomId);
  const data = await api(`/rooms/${roomId}/key`);
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
  if (msgMeta?.e2ee_v === 2) {
    const passphrase = await getStableRoomPassphrase(roomId);
    if (!passphrase) return null;
    const roomSecret = await getRoomServerSecret(roomId);
    if (!roomSecret) return null;
    return deriveRoomAesKey(passphrase, `room-v2:${roomId}:${roomSecret}`, 250000);
  }
  const passphrase = await getRoomPassphrase(roomId);
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
      return "🔐 [Не удалось расшифровать E2E сообщение]";
    } catch (e) {
      console.error('E2E decryption error:', e);
      return "❌ [Ошибка E2E расшифровки]";
    }
  }

  // Fallback to room-based encryption for groups/channels
  if (!msg.meta?.encrypted) return String(msg.text);
  try {
    const key = await getCryptoKey(roomId, msg.meta || null);
    if (!key) return "🔐 [Зашифровано]";
    const iv = base64ToBytes(msg.meta.iv);
    const data = base64ToBytes(msg.text);
    const dec = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, data);
    return new TextDecoder().decode(dec);
  } catch (e) { return "❌ Ошибка ключа"; }
}

// --- E2E Encryption (Signal Protocol) ---
// Работает прозрачно в фоне, пользователь не видит
let e2eInitTask = null;
function initE2E() {
  if (state.e2eReady && state.e2eManager?.ready) return Promise.resolve();
  if (e2eInitTask) return e2eInitTask;
  const userId=state.me?.id;
  if (!userId) return Promise.resolve();
  e2eInitTask=(async()=>{
    if (!window.libsignal) await new Promise(resolve=>{ const done=()=>{window.removeEventListener('libsignal-ready',done);resolve();};window.addEventListener('libsignal-ready',done);setTimeout(done,5000); });
    if (!window.libsignal || !window.SigStore || state.me?.id!==userId) return;
    const manager=new window.E2EManager(), deviceId=getE2EDeviceId(), backend=window.SigStore.forAccount(userId);
    manager.useStore(userId,deviceId,backend);
    const loaded=await manager.loadFromIndexedDB(userId);
    if (!loaded) {
      const bundle=await manager.init(userId,deviceId,backend);await manager.saveToIndexedDB(userId);
      await api('/e2e/keys',{method:'POST',body:JSON.stringify({device_id:deviceId,identity_key:bundle.identityKey,registration_id:bundle.registrationId,signed_pre_key:{key_id:bundle.signedPreKey.keyId,public_key:bundle.signedPreKey.publicKey,signature:bundle.signedPreKey.signature},pre_keys:bundle.preKeys.map(pk=>({key_id:pk.keyId,public_key:pk.publicKey}))})});
    }
    if(state.me?.id!==userId)return;
    state.e2eManager=manager; state.e2eReady=true;
    await maybeReplenishPreKeys(deviceId,{force:true});startPreKeyMonitor();
  })().catch(error=>{console.warn('Ключи шифрования пока недоступны:',error.message);}).finally(()=>{e2eInitTask=null;});
  return e2eInitTask;
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
  const info = await api(`/e2e/prekeys/count?device_id=${encodeURIComponent(deviceId)}`);
  if (!info || typeof info.unused !== 'number' || info.unused >= PREKEY_LOW_WATERMARK) return;
  const bundle = await state.e2eManager.replenishPreKeys(state.me.id, deviceId);
  if (!bundle || !bundle.signedPreKey || !bundle.signedPreKey.signature) return;
  const res = await api("/e2e/keys", {
    method: "POST",
    body: JSON.stringify({
      device_id: deviceId,
      identity_key: bundle.identityKey,
      registration_id: bundle.registrationId,
      signed_pre_key: {
        key_id: bundle.signedPreKey.keyId,
        public_key: bundle.signedPreKey.publicKey,
        signature: bundle.signedPreKey.signature
      },
      pre_keys: bundle.preKeys.map(pk => ({ key_id: pk.keyId, public_key: pk.publicKey }))
    })
  });
  if (res?.ok) {
    // Persist the new private pre-keys and the advanced id counter.
    await state.e2eManager.saveToIndexedDB(state.me.id);
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
  await initE2E(); if(!state.e2eReady || !state.e2eManager) throw Error('Ключи ещё загружаются');
  if(state.me?.id!==owner)throw Error('Сеанс изменился');
  const manager=state.e2eManager;
  let device=targetDevice;
  if(!device) device=(await api(`/e2e/devices/${recipientId}`))[0]?.device_id;
  if(!device) throw Error('Участнику нужно обновить приложение для шифрованной группы');
  const key=`${recipientId}:${device}.${device}`;
  if (!await manager.store.loadSession(key)) {
    let task=e2eSessionInflight.get(key);
    if(!task) {task=(async()=>{ const bundle=await api(`/e2e/keys/${recipientId}?device_id=${encodeURIComponent(device)}`);
      await manager.buildSession(recipientId,device,{registrationId:bundle.registration_id,identityKey:bundle.identity_key,signedPreKey:bundle.signed_pre_key,preKey:bundle.pre_key});
    })();e2eSessionInflight.set(key,task);task.finally(()=>e2eSessionInflight.delete(key)).catch(()=>{});}
    await task;
  }
  if(state.me?.id!==owner)throw Error('Сеанс изменился');
  return {...await manager.encryptMessage(recipientId,device,plaintext),sender_device_id:ownDevice,recipient_device_id:device};
}
async function decryptMessageE2E(senderId,ciphertext) {
  const owner=state.me?.id;
  await initE2E(); if(!state.e2eReady || !state.e2eManager)throw Error('Ключи ещё загружаются');
  if(state.me?.id!==owner)throw Error('Сеанс изменился');const manager=state.e2eManager;
  const device=ciphertext.sender_device_id || (await api(`/e2e/devices/${senderId}`))[0]?.device_id || '1';
  if(state.me?.id!==owner)throw Error('Сеанс изменился');
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
  if (groupE2eRooms.has(roomId)) return groupE2eRooms.get(roomId);
  const data = await api(`/rooms/${roomId}/e2e`);
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
  const owner=state.me?.id,ownDevice=getE2EDeviceId();
  const targets=[];
  for (const user of members) {
    const devices=await api(`/e2e/devices/${user}`);
    if(state.me?.id!==owner)throw Error('Сеанс изменился');
    if(!devices.length)throw Error('Участнику нужно обновить приложение для шифрованной группы');
    for(const device of devices)if(String(user)!==String(owner) || device.device_id!==ownDevice) targets.push({user:String(user),device:device.device_id});
  }
  const tokens=targets.map(t=>t.user+'|'+t.device);
  let rec=await window.SigStore.get(groupKey(roomId,'own',owner),null);
  if(rotate || !rec || rec.n>=1000 || rec.distributedTo.some(token=>!tokens.includes(token)))rec=await window.GroupCrypto.createSenderKey();
  const recipients=[];
  for(const target of targets.filter(t=>!rec.distributedTo.includes(t.user+'|'+t.device))) recipients.push({user_id:target.user,device_id:target.device,payload:JSON.stringify(await encryptMessageE2E(target.user,JSON.stringify({...window.GroupCrypto.distributionFor(rec),room_id:roomId}),target.device))});
  if(state.me?.id!==owner)throw Error('Сеанс изменился');
  if(recipients.length)await api(`/rooms/${roomId}/sender-keys`,{method:'POST',body:JSON.stringify({key_id:rec.keyId,recipients})});
  rec.distributedTo=tokens;await saveOwnSenderKey(roomId,rec,owner);return rec;
}
let groupSequence=Promise.resolve();
function groupSerial(fn) {const owner=state.me?.id;const task=groupSequence.then(()=>{if(state.me?.id!==owner)throw Error('Сеанс изменился');return fn(owner);});groupSequence=task.catch(()=>{});return task;}
function encryptGroupText(roomId,text) { return groupSerial(owner=>encryptGroupTextUnlocked(roomId,text,owner)); }
async function encryptGroupTextUnlocked(roomId, text,owner) {
  await initE2E();
  if (!state.e2eReady || !state.e2eManager) throw new Error('E2E не готово на этом устройстве');
  const members = await fetchAllMemberIds(roomId);
  if(state.me?.id!==owner)throw Error('Сеанс изменился');
  if (members.length > GROUP_E2E_MAX_MEMBERS) throw new Error(`В группе с E2E не более ${GROUP_E2E_MAX_MEMBERS} участников`);
  const rec = await distributeOwnSenderKey(roomId, members);
  if(state.me?.id!==owner)throw Error('Сеанс изменился');
  const { envelope, state: next } = await window.GroupCrypto.encryptWithState(rec, roomId, String(owner), text);
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
  if(state.me?.id!==owner)throw Error('Сеанс изменился');
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
  }catch(error){console.warn('Расшифровка группы:',error.message);return '🔐 Не удалось получить ключ сообщения';}
}

window.toggleGroupE2e = async () => {
  const roomId = state.activeRoomId;
  if (!roomId) return;
  const on = await roomE2eEnabled(roomId);
  const warning = on
    ? "Отключить сквозное шифрование? Новые сообщения пойдут без E2E."
    : "Включить сквозное шифрование группы?\n\nУчастникам нужны актуальная версия сайта или приложения. Новые сообщения и ключи вложений будут защищены сквозным шифрованием. Сервер видит участников и время отправки.";
  if (!confirm(warning)) return;
  const res = await api(`/rooms/${roomId}/e2e`, { method: 'PUT', body: JSON.stringify({ enabled: !on }) });
  if (!res) { alert("Не удалось изменить настройку (нужны права владельца или администратора)."); return; }
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
  const ws = new WebSocket(`${prot}//${location.host}/ws`);
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
      if (state.activeRoomId) joinActiveRoom();
      updateChatPresence();
      void chatRuntime.syncHidden();
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
        alert("Писать этому пользователю сейчас нельзя");
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
    if (data.type === "message.delete") {
      void chatRuntime.hide(data.room_id || state.activeRoomId,data.message_id).catch(()=>{});
      state.messages = state.messages.filter(m => m.id !== data.message_id);
      if (state.editMessage?.id === data.message_id) state.editMessage = null;
      render();
    }

    if (data.type === "message.update" && data.message) {
      await chatRuntime.received(data.message);
      if (state.editMessage?.id === data.message.id) state.editMessage = null;
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

    if(data.type=== "call.created")state.currentCallId=data.call_id;
    if (data.type === "call.offer") {
      if (String(data.from_user_id || "") !== String(state.me?.id || "")) handleCall(data);
    }
    if (data.type === "call.answer" && data.room_id===state.currentCallRoomId && state.pc && (data.sdp || data.answer)) {
      try {
        await state.pc.setRemoteDescription(new RTCSessionDescription(data.sdp || data.answer));
        await flushPendingIceCandidates();
      } catch (e) { console.error("setRemoteDescription failed:", e); }
    }
    if (data.type === "call.ice" && data.room_id===state.currentCallRoomId && data.candidate) {
      try {
        if (state.pc?.remoteDescription) await state.pc.addIceCandidate(new RTCIceCandidate(data.candidate));
        else state.pendingIceCandidates.push(data.candidate);
      } catch (e) { console.error("addIceCandidate failed:", e); }
    }
    if (data.type === "call.hangup" && (!state.currentCallId || data.call_id===state.currentCallId) && data.room_id===state.currentCallRoomId) { if (typeof window.hangupCall === "function") window.hangupCall(false); }
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
    if (data.type === 'message.hide' && data.message_id) {
      const room=data.room_id; await chatRuntime.hide(room,data.message_id);
      if(state.activeRoomId===room){state.messages=state.messages.filter(m=>m.id!==data.message_id);renderMessages();}
      return;
    }
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
    if (data.type === "rooms.update" && Array.isArray(data.rooms)) {
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

async function handleCall(data) {
  const fromName = data.from_username || data.from_user_id || "unknown";
  data.sdp ||= data.offer;
  if (!data.sdp) { toast("Не удалось получить данные звонка"); return; }
  if (state.callState !== 'idle') {
    try {
      state.ws?.send(JSON.stringify({ type: "call.hangup", room_id: data.room_id, to_user_id: data.from_user_id }));
    } catch {}
    return;
  }

  // Detect if it's a video call by checking SDP
  const isVideoCall = data.sdp.sdp && data.sdp.sdp.includes('m=video');

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
        '<div style="font-size:16px;opacity:0.8;">' + (isVideoCall ? 'Входящий видеовызов...' : 'Входящий звонок...') + '</div>' +
      '</div>' +
      '<div style="display:flex;gap:20px;">' +
        '<button id="acceptCallBtn" style="width:80px;height:80px;border-radius:50%;background:#42d392;border:none;font-size:40px;cursor:pointer;">' + (isVideoCall ? '📹' : '📞') + '</button>' +
        '<button id="rejectCallBtn" style="width:80px;height:80px;border-radius:50%;background:#ff4d6d;border:none;font-size:40px;cursor:pointer;">📛</button>' +
      '</div>' +
    '</div>';
  document.body.appendChild(callDialog);

  const accepted = await new Promise(resolve => {
    state.incomingCallResolve=resolve;
    setTimeout(()=>resolve(false),Math.max(0,(data.expires_at || Date.now()+45000)-Date.now()));
    $("#acceptCallBtn").onclick = () => resolve(true);
    $("#rejectCallBtn").onclick = () => resolve(false);
  });

  state.incomingCallResolve=null;callDialog.remove();
  if(state.callState==='idle' || (data.call_id && data.call_id!==state.currentCallId))return;
  if (!accepted) {
    state.callState = 'idle';
    try {
      state.ws?.send(JSON.stringify({ type: "call.hangup", room_id: data.room_id, to_user_id: data.from_user_id }));
    } catch {}
    return;
  }

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
    state.callState = 'idle';
    state.currentCallRoomId = null;
    state.currentCallPeerId = null;
    alert(isVideoCall ? "Нет доступа к камере/микрофону." : "Нет доступа к микрофону.");
    return;
  }

  state.localStream = stream;
  await loadRtcIceServers();
  state.pc = new RTCPeerConnection({
    iceServers: getRtcIceServers(),
    iceTransportPolicy: "all",
    iceCandidatePoolSize: 4
  });

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

  updateCallStatus("Соединение...", false);
  wirePeerConnectionHandlers({ roomId: data.room_id, peerUserId: data.from_user_id });

  stream.getTracks().forEach(t => state.pc.addTrack(t, stream));

  try {
    await state.pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
    await flushPendingIceCandidates();
    const answer = await state.pc.createAnswer();
    await state.pc.setLocalDescription(answer);
    state.ws?.send(JSON.stringify({ type: "call.answer", to_user_id: data.from_user_id, room_id: data.room_id, sdp: { type: answer.type, sdp: answer.sdp } }));
  } catch (e) { console.error("Failed to handle call:", e); alert("Ошибка при установке звонка"); hangupCall(); }
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
        <button class="btn danger" onclick="hangupCall()">Завершить</button>
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
        '<div class="callStatus" id="callStatus">Соединение...</div>' +
        '<div class="callTimer" id="callTimer" style="display:none;">00:00</div>' +
        '<audio id="remoteAudio" autoplay playsinline webkit-playsinline></audio>' +
      '</div>' +
      '<div class="callControls">' +
        '<button id="toggleAudioBtn" class="btn" onclick="toggleAudio()" title="Выключить микрофон">🎤</button>' +
        '<button id="toggleSpeakerBtn" class="btn" onclick="toggleAudioOutput()" title="Переключить на громкую связь">📱</button>' +
        '<button class="btn danger" onclick="hangupCall()">Завершить</button>' +
      '</div>';
    document.body.appendChild(hud);
  }

  updateCallStatus(state.callState === "calling" ? "Вызов..." : "Соединение...", false);
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
        '<div class="callStatus" id="callStatus">Соединение...</div>' +
        '<div class="callTimer" id="callTimer" style="display:none;">00:00</div>' +
      '</div>' +
      '<div class="callControls videoCallControls">' +
        '<button id="toggleVideoBtn" class="btn" onclick="toggleVideo()" title="Выключить камеру">📹</button>' +
        '<button id="toggleAudioBtn" class="btn" onclick="toggleAudio()" title="Выключить микрофон">🎤</button>' +
        '<button id="switchCameraBtn" class="btn" onclick="switchCamera()" title="Переключить камеру">🔄</button>' +
        '<button class="btn danger" onclick="hangupCall()">Завершить</button>' +
      '</div>';
    document.body.appendChild(hud);
  }

  updateCallStatus(state.callState === "calling" ? "Видеовызов..." : "Соединение...", false);
}

// Check if mobile device
function isMobile() {
  return /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
}

window.hangupCall = (notifyPeer = true) => {
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

  // Close peer connection
  try { state.pc?.close(); } catch {}
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
      const payload = { type: "call.hangup" };
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
      btn.title = state.isVideoEnabled ? 'Выключить камеру' : 'Включить камеру';
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
      btn.title = state.isAudioEnabled ? 'Выключить микрофон' : 'Включить микрофон';
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
    alert('Не удалось переключить камеру');
  }
};

// Toggle audio output (earpiece/speaker)
window.toggleAudioOutput = async () => {
  try {
    const audioElement = document.querySelector('audio');
    if (!audioElement || typeof audioElement.setSinkId !== 'function') {
      alert('Переключение динамика не поддерживается в этом браузере');
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
        btn.title = 'Переключить на слуховой динамик';
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
        btn.title = 'Переключить на громкую связь';
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
          btn.title = 'Переключить на громкую связь';
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
  if (state.activeRoomId) chatRuntime.draft(state.activeRoomId, state.composerText);
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
  alert("Не удалось открыть диалог");
};

window.addGroup = async () => {
  const membersRaw = (prompt("Участники (usernames через запятую):", "") || "").trim();
  const members = membersRaw ? membersRaw.split(",").map(s => s.trim()).filter(Boolean) : [];
  const title = (prompt("Название группы:", "Группа") || "Группа").trim();
  if (!members.length) return;
  const res = await api("/rooms", { method: "POST", body: JSON.stringify({ kind: "group", title, members }) });
  if (res?.id) { await loadRooms(); await selectRoom(res.id); }
};

window.addChannel = async () => {
  const title = (prompt("Название канала:", "Мой канал") || "Мой канал").trim();
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
    alert(isBlocked ? "Не удалось разбанить пользователя" : "Не удалось забанить пользователя");
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
    alert("Личная переписка не найдена");
    return;
  }
  if (!confirm("Удалить всю переписку у обеих сторон? Это действие нельзя отменить.")) return;
  const res = await api(`/rooms/${dmRoom.id}`, { method: "DELETE" });
  if (!res?.ok) {
    alert("Не удалось удалить переписку");
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
  const newPass = prompt("Новый пароль (минимум 6 символов):");
  if (!newPass || newPass.length < 6) {
    alert("Пароль слишком короткий");
    return;
  }
  const res = await api("/me/password", {
    method: "PUT",
    body: JSON.stringify({ new_password: newPass })
  });
  if (res?.ok) {
    alert("Пароль изменен! Перезайдите в аккаунт.");
    logout();
  } else {
    alert("Ошибка: " + (res?.error || "неизвестная"));
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
      alert('Ключи пока не готовы, но сообщения продолжают работать через защищенный ключ чата.');
      return;
    }

    const recovery = await state.e2eManager.generateRecoveryPhrase();
    state.recoveryPhrase = recovery;
    state.showRecoveryPhrase = true;
    mount();
  } catch (err) {
    console.error('Failed to generate backup:', err);
    alert('Ошибка создания бэкапа: ' + err.message);
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

  const backupText = `Messenger 67 Backup
Username: ${state.me?.username}
Date: ${new Date(state.recoveryPhrase.timestamp).toLocaleString()}

Recovery Phrase:
${state.recoveryPhrase.phrase}

Checksum: ${state.recoveryPhrase.checksum}

⚠️ ВАЖНО: Храните этот бэкап в безопасном месте!
Любой, у кого есть эта фраза, может прочитать ваши сообщения.`;

  try {
    await navigator.clipboard.writeText(backupText);
    alert('Бэкап скопирован в буфер обмена');
  } catch (err) {
    console.error('Failed to copy:', err);
    alert('Ошибка копирования');
  }
};

window.restoreFromBackup = async () => {
  const input = document.createElement('input');
  input.type = 'file';
  input.accept = '.json';

  input.onchange = async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    try {
      const text = await file.text();
      const backupData = JSON.parse(text);

      if (!backupData.phrase || !backupData.checksum) {
        alert('Неверный формат файла бэкапа');
        return;
      }

      if (confirm(`Восстановить бэкап для пользователя ${backupData.username}?\n\nВнимание: Текущие ключи будут заменены!`)) {
        await state.e2eManager.restoreFromRecoveryPhrase(backupData.phrase, backupData.checksum);

        // Save to localStorage
        await state.e2eManager.saveToLocalStorage(state.me.id);

        alert('Бэкап успешно восстановлен.');
        await initE2E();
        mount();
      }
    } catch (err) {
      console.error('Failed to restore backup:', err);
      alert('Ошибка восстановления бэкапа: ' + err.message);
    }
  };

  input.click();
};

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
      alert('Ключи пока не готовы, но сообщения продолжают работать через защищенный ключ чата.');
      return;
    }
    const pass = prompt('Придумайте пароль для бэкапа (минимум 8 символов).\nБез него файл восстановить нельзя:');
    if (pass == null) return;
    if (pass.length < 8) { alert('Пароль слишком короткий (минимум 8 символов)'); return; }
    const again = prompt('Повторите пароль:');
    if (again == null) return;
    if (again !== pass) { alert('Пароли не совпадают'); return; }

    const text = await state.e2eManager.exportKeysBackup(pass, { userId: state.me?.id, deviceId: getE2EDeviceId() });
    const blob = new Blob([text], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `messenger67-keys-${state.me?.username || 'user'}-${Date.now()}.e2ebackup.json`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  } catch (err) {
    console.error('Encrypted backup export failed:', err);
    alert('Ошибка создания бэкапа: ' + (err?.message || err));
  }
};

window.importEncryptedBackup = () => {
  const input = document.createElement('input');
  input.type = 'file';
  input.accept = '.json,application/json';
  input.onchange = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    try {
      if (!state.e2eManager) await initE2E();
      if (!state.e2eManager) { alert('E2E недоступно в этом браузере'); return; }
      if (!state.e2eManager.store) state.e2eManager.store = new SignalProtocolStore(window.SigStore || null);

      const text = await file.text();
      const pass = prompt('Введите пароль от бэкапа:');
      if (pass == null) return;
      if (!confirm('Восстановить ключи из файла?\n\nВнимание: текущие ключи на этом устройстве будут заменены!')) return;

      const info = await state.e2eManager.importKeysBackup(text, pass, { userId: state.me?.id });
      if (info.deviceId) localStorage.setItem(`e2e_device_id_${state.me.id}`, String(info.deviceId));
      await state.e2eManager.saveToIndexedDB(state.me.id);

      // Publish the restored public bundle so peers use the restored identity again.
      const deviceId = getE2EDeviceId();
      const b = await state.e2eManager.getPublicBundle();
      const res = b.signedPreKey && b.signedPreKey.signature ? await api("/e2e/keys", {
        method: "POST",
        body: JSON.stringify({
          device_id: deviceId,
          identity_key: b.identityKey,
          registration_id: b.registrationId,
          signed_pre_key: { key_id: b.signedPreKey.keyId, public_key: b.signedPreKey.publicKey, signature: b.signedPreKey.signature },
          pre_keys: b.preKeys.map(pk => ({ key_id: pk.keyId, public_key: pk.publicKey }))
        })
      }) : null;

      state.e2eReady = !!res?.ok;
      e2eSessionInflight.clear();
      e2ePeerDeviceIds.clear();
      if (state.e2eReady) { maybeReplenishPreKeys(deviceId, { force: true }); startPreKeyMonitor(); }
      alert(state.e2eReady
        ? 'Ключи восстановлены.'
        : 'Ключи восстановлены локально, но опубликовать их на сервере не удалось. Повторите попытку позже.');
      mount();
    } catch (err) {
      const code = err?.message;
      if (code === 'BAD_PASSPHRASE') alert('Неверный пароль или файл поврежден');
      else if (code === 'BAD_FORMAT') alert('Неверный формат файла бэкапа');
      else if (code === 'USER_MISMATCH') alert('Этот бэкап создан для другого пользователя');
      else { console.error('Encrypted backup import failed:', err); alert('Ошибка восстановления: ' + (code || err)); }
    }
  };
  input.click();
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
  if (!confirm('Завершить эту сессию?')) return;

  try {
    await api(`/auth/sessions/${sessionId}`, { method: 'DELETE' });
    await loadSessions();
  } catch (err) {
    console.error('Failed to terminate session:', err);
    alert('Ошибка завершения сессии');
  }
};

window.terminateOtherSessions = async () => {
  if (!confirm('Завершить все остальные сессии?\n\nВы останетесь авторизованы только на этом устройстве.')) return;

  try {
    const res = await api('/auth/sessions/terminate-others', { method: 'POST' });
    alert(`Завершено сессий: ${res.terminated}`);
    await loadSessions();
  } catch (err) {
    console.error('Failed to terminate sessions:', err);
    alert('Ошибка завершения сессий');
  }
};

window.terminateAllSessions = async () => {
  if (!confirm('Завершить ВСЕ сессии?\n\nВы будете разлогинены на всех устройствах, включая это.')) return;

  try {
    await api('/auth/sessions/terminate-all', { method: 'POST' });
    // Will be logged out
    resetClientSession();
    mount();
  } catch (err) {
    console.error('Failed to terminate all sessions:', err);
    alert('Ошибка завершения сессий');
  }
};

// --- Safety Numbers ---
window.showSafetyNumber = async (userId) => {
  try {
    if (!state.e2eManager || !state.e2eManager.ready) {
      await initE2E();
    }
    if (!state.e2eManager || !state.e2eManager.ready) {
      alert('Ключи пока не готовы, но сообщения продолжают работать через защищенный ключ чата.');
      return;
    }

    // Get user info
    const user = state.roomMembers?.find(m => m.id === userId);
    if (!user) {
      alert('Пользователь не найден');
      return;
    }

    // Get their identity key from server
    const bundle = await api(`/e2e/keys/${userId}`);
    if (!bundle || !bundle.identity_key) {
      alert('Ключи пользователя не найдены');
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
    alert('Ошибка генерации кода безопасности');
  }
};

window.closeSafetyNumber = () => {
  state.safetyNumberPanel = false;
  state.safetyNumberData = null;
  mount();
};

window.copySafetyNumber = async () => {
  if (!state.safetyNumberData) return;

  const text = `Safety Number для ${state.safetyNumberData.user.username}:\n\n${state.safetyNumberData.number}`;

  try {
    await navigator.clipboard.writeText(text);
    alert('Код безопасности скопирован');
  } catch (err) {
    console.error('Failed to copy:', err);
    alert('Ошибка копирования');
  }
};

// --- Push Notifications ---
const webNotifications = createWebNotifications({
  api, activeRoom: () => state.activeRoomId,
  status({ enabled, permission, registration }) {
    state.notificationsEnabled = enabled;
    state.notificationsPermission = permission;
    state.serviceWorkerRegistration = registration;
    const node = $('#notificationSettingStatus');
    if (node) node.textContent = enabled ? 'Включены — нажмите, чтобы отключить' : 'Включить в этом браузере';
  },
});
async function initServiceWorker() {
  try { await webNotifications.start(); } catch { /* Settings offers an explicit retry. */ }
}
window.requestNotificationPermission = async () => {
  try { await webNotifications.toggle(); mountPreservingInput(); }
  catch (error) { alert(error.message || 'Не удалось включить уведомления'); }
};
window.testWebNotification = async () => {
  try { await webNotifications.test(); } catch (error) { alert(error.message); }
};
navigator.serviceWorker?.addEventListener('message', event => {
  if (event.data?.type === 'PUSH_SUBSCRIPTION_CHANGED' && state.me) void initServiceWorker();
  if (event.data?.type === 'OPEN_ROOM' && event.data.roomId && state.me) {
    if (state.rooms.some(room => room.id === event.data.roomId)) window.selectRoom(event.data.roomId);
  }
});

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
  const senderName = sender ? displayName(sender) : 'Пользователь';

  let title = senderName;
  let body = message._text || message.text || 'Новое сообщение';

  // Truncate long messages
  if (body.length > 100) {
    body = body.substring(0, 97) + '...';
  }

  // Add room context for groups/channels
  if (room.kind !== 'dm') {
    title = `${senderName} в ${room.title || 'группе'}`;
  }

  showNotification(title, body, {
    url: '/',
    roomId: room.id,
    messageId: message.id,
    tag: `message-${room.id}`
  });
}

async function loadAdminUsers() {
  if (!state.adminPanel) return;
  state.adminUsers = await api("/admin/users") || [];
  mount();
}

window.adminResetPassword = async (userId, username) => {
  const newPass = prompt(`Новый пароль для @${username}:`);
  if (!newPass || newPass.length < 6) { alert("Пароль слишком короткий"); return; }
  const res = await api(`/admin/users/${userId}/password`, { method: "PUT", body: JSON.stringify({ new_password: newPass }) });
  if (res?.ok) alert("Пароль изменен!");
  else alert("Ошибка: " + (res?.error || "неизвестная"));
};

window.adminToggleBan = async (userId, username, currentBan) => {
  const action = currentBan ? "разбанить" : "забанить";
  if (!confirm(`${action} пользователя @${username}?`)) return;
  const res = await api(`/admin/users/${userId}/ban`, { method: "PUT", body: JSON.stringify({ banned: !currentBan }) });
  if (res?.ok) { alert("Готово!"); loadAdminUsers(); }
  else alert("Ошибка: " + (res?.error || "неизвестная"));
};

window.adminDeleteUser = async (userId, username) => {
  if (!confirm(`ПОЛНОСТЬЮ удалить пользователя @${username}?\n\nЭто действие нельзя отменить!\nПользователь исчезнет из всех групп, каналов и чатов.`)) return;
  const res = await api(`/admin/users/${userId}`, { method: "DELETE" });
  if (res?.ok) { alert("Пользователь полностью удалён!"); loadAdminUsers(); }
  else alert("Ошибка: " + (res?.error || "неизвестная"));
};

window.updateMyName = async () => {
  const name = prompt("Отображаемое имя:", state.me?.display_name || "");
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
  const title = prompt("Название группы:", room?.title || "");
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
  const username = prompt("Username нового участника:");
  if (!username) return;
  const res = await api(`/rooms/${state.activeRoomId}/members`, { method: "POST", body: JSON.stringify({ username }) });
  if (res?.ok) { state.roomMembers = await api(`/rooms/${state.activeRoomId}/members`) || []; mount(); }
  else alert("Не удалось добавить участника");
};

window.removeMember = async userId => {
  if (!confirm("Удалить участника?")) return;
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
  if (!confirm("Удалить группу навсегда? Это действие нельзя отменить.")) return;
  await api(`/rooms/${state.activeRoomId}`, { method: "DELETE" });
  state.activeRoomId = null;
  state.messages = [];
  state.editingRoom = false;
  await loadRooms();
  mount();
};

window.leaveRoom = async () => {
  if (!confirm("Покинуть группу?")) return;
  await api(`/rooms/${state.activeRoomId}/members/${state.me.id}`, { method: "DELETE" });
  state.activeRoomId = null;
  state.messages = [];
  state.editingRoom = false;
  await loadRooms();
  mount();
};

window.deleteDMChat = async () => {
  if (!confirm("Удалить личную переписку? Все сообщения будут безвозвратно удалены.")) return;
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
      <h3>Добавить участников</h3>
      <div style="margin-bottom:15px;">
        <input type="text" id="memberSearch" placeholder="Поиск пользователей..."
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
        <button class="btn" onclick="closeAddMemberModal()">Отмена</button>
        <button class="btn primary" onclick="addSelectedMembers()">Добавить выбранных</button>
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
      memberList.innerHTML = '<div style="padding:20px;text-align:center;color:var(--muted);">Ошибка поиска</div>';
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
        alert(`Добавлено ${result.added || 0} пользователей`);
      } else {
        alert('Пользователь добавлен');
      }
    } catch (error) {
      console.error('Add member error:', error);
      alert('Ошибка добавления: ' + (error.message || 'неизвестная ошибка'));
    }
    return;
  }

  // Normal user selection flow
  const checkboxes = modal?.querySelectorAll('input[type="checkbox"]:checked');

  if (!checkboxes || checkboxes.length === 0) {
    alert('Выберите хотя бы одного пользователя');
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
    alert(`Добавлено: ${userIds.length} ${plural(userIds.length, 'участник', 'участника', 'участников')}`);
  } catch (error) {
    console.error('Add members error:', error);
    alert('Ошибка добавления: ' + (error.message || 'неизвестная ошибка'));
  }
};

window.handleMessageKeydown = (event) => {
  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault();
    doSend();
  }
};

window.startEditMessage = (msgId, event) => {
  if (event) event.stopPropagation();
  const msg = state.messages.find(m => m.id === msgId);
  if (!msg || String(msg.sender_id || "") !== String(state.me?.id || "") || msg.type !== "text") return;
  if (msg.encrypted_content) {
    alert("Сообщения со сквозным шифрованием нельзя редактировать. Удалите и отправьте заново.");
    return;
  }
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
  if (!confirm('Удалить сообщение у всех участников?')) return;
  const roomId = state.activeRoomId, account = state.me?.id;
  try {
    const result = await api(`/rooms/${roomId}/messages/${msgId}`, { method: 'DELETE' });
    if (!result?.ok) throw new Error('Не удалось удалить сообщение');
    await chatRuntime.local()?.removeMessage(roomId, msgId);
    if (account === state.me?.id && roomId === state.activeRoomId) { state.messages = state.messages.filter(m => m.id !== msgId); if (state.editMessage?.id === msgId) state.editMessage = null; render(); }
  } catch (error) { toast(userError(error)); }
};

// Global error handler
window.alert = message => toast(message);
window.addEventListener('error', (event) => {
  console.error('Global error:', event.error);
  alert('Произошла ошибка: ' + (event.error?.message || 'неизвестная ошибка'));
});

// Global unhandled promise rejection handler
window.addEventListener('unhandledrejection', (event) => {
  console.error('Unhandled promise rejection:', event.reason);
  alert('Произошла ошибка: ' + (event.reason?.message || 'неизвестная ошибка'));
});

const webDrafts = new Map();
const chatRuntime = createChatRuntime({ state, request: api, decode: decodeForWeb, buildText: buildTextMessage,
  mount, refresh: mountPreservingInput, render: renderMessages, report: toast, drafts: webDrafts, loadRooms });
async function decodeForWeb(roomId, message) {
  const decoded = { ...message };
  if(message.type!=='text' && message.meta?.caption_message)decoded.text=await decryptMsg(roomId,{...message.meta.caption_message,room_id:roomId,sender_id:message.sender_id,type:'text',id:message.id+'-caption'});
  if (message.type === 'text') decoded._text = await decryptMsg(roomId, message);
  if (message.reply_to) {
    const preview = message.meta?.reply_preview;
    const original = state.messages.find(m => m.id === message.reply_to.id);
    let text = original?._text;
    if (preview) text = await decryptMsg(roomId, { ...preview, type: 'text', room_id: roomId, sender_id: message.sender_id, id: message.id + '-reply' });
    decoded.reply_to = { ...message.reply_to, sender_name: message.reply_to.sender_name || message.reply_to.sender_display_name || message.reply_to.sender_username || 'Пользователь', text: text ?? (preview ? 'Ответ на сообщение' : message.reply_to.text || 'Вложение') };
  }
  return decoded;
}
const webMediaUrls = new Map();
const webMedia = createMediaController({ context: () => ({ userId: state.me?.id, roomId: state.activeRoomId, title: state.rooms.find(r => r.id === state.activeRoomId)?.other_display_name || state.rooms.find(r => r.id === state.activeRoomId)?.title || 'Переписка', store: chatRuntime.local() }),
  enqueue: options => chatRuntime.enqueue(options), report: toast,
  encryptFile: async (file, roomId) => {
    const key = await crypto.subtle.generateKey({ name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']), iv = crypto.getRandomValues(new Uint8Array(12));
    const data = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, await file.arrayBuffer());
    const wrapped = await buildTextMessage(roomId, state.rooms.find(r => r.id === roomId), JSON.stringify({ key: bytesToBase64(new Uint8Array(await crypto.subtle.exportKey('raw', key))), iv: bytesToBase64(iv) }));
    if (!wrapped.meta?.encrypted && !wrapped.meta?.e2e_group) throw new Error('Не удалось защитить ключ вложения. Повторите попытку.');
    return { file: new File([data], 'attachment.bin', { type: 'application/octet-stream' }), kind: 'file', meta: { media_encryption: { v: 2, key_message: wrapped, name: file.name, mime: file.type || 'application/octet-stream', size: file.size } } };
  }
});
async function resolveWebMedia(message) {
  const info = message.meta?.media_encryption, uri = '/api/media/' + encodeURIComponent(message.media_id);
  if (!info) return { url: uri, mime: message.media_mime, name: message.meta?.filename || '67chat-media' };
  const account = state.me?.id, cacheId = account + ':' + message.media_id;
  if (webMediaUrls.has(cacheId)) return webMediaUrls.get(cacheId);
  const text = await decryptMsg(message.room_id, { ...info.key_message, room_id: message.room_id, sender_id: message.sender_id, id: message.id + '-file-key', type: 'text' });
  let secret; try { secret = JSON.parse(text); } catch { throw new Error('Ключ вложения недоступен на этом устройстве'); }
  const response = await fetch(uri, { credentials: 'include' }); if (!response.ok) throw new Error('Не удалось загрузить вложение');
  const key = await crypto.subtle.importKey('raw', base64ToBytes(secret.key), { name: 'AES-GCM' }, false, ['decrypt']);
  const data = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: base64ToBytes(secret.iv) }, key, await response.arrayBuffer());
  if (account !== state.me?.id) throw new Error('Сессия завершена');
  const result = { url: URL.createObjectURL(new Blob([data], { type: info.mime })), mime: info.mime, name: info.name }; webMediaUrls.set(cacheId, result); return result;
}
window.openWebMedia = id => { const message = state.messages.find(m => m.id === id); if (message) void webMedia.openViewer(message, state.messages, resolveWebMedia).catch(e => toast(userError(e))); };
async function hydrateVisibleMedia() {
  if (!state.autoDownload) return;
  const account = state.me?.id;
  for (const node of document.querySelectorAll('[data-encrypted-media]')) {
    if (node.dataset.loading || node.getBoundingClientRect().top > innerHeight + 500) continue;
    const message = state.messages.find(m => m.id === node.dataset.encryptedMedia); if (!message) continue;
    node.dataset.loading = 'yes';
    try { const result = await resolveWebMedia(message); if (!node.isConnected || account !== state.me?.id) return;
      if (result.mime?.startsWith('image/')) { const image = document.createElement('img'); image.className = 'media-image'; image.src = result.url; image.alt = message.text || 'Фотография'; node.replaceChildren(image); }
    } catch { node.dataset.loading = ''; }
  }
}
const folderManager = createFolderManager({ state, request: api, refresh: async () => { await loadRooms(); await mountPreservingInput(); }, openRoom: (...args) => window.selectRoom(...args), report: toast });
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
window.selectMessageIfNeeded = (id, event) => { if (state.selectedMessages.size) { event.preventDefault(); event.stopPropagation(); chatActions.select(id); } };
window.copySelectedMessages = chatActions.copySelected;
window.deleteSelectedMessages = chatActions.removeSelected;
window.forwardSelectedMessages = () => chatActions.forward([...state.selectedMessages]);
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
    button.onclick = async e => { e.stopPropagation(); menu.remove(); try { await action(); } catch { alert('Не удалось выполнить действие. Повторите попытку.'); } };
    menu.appendChild(button);
  };
  add('Выделить', () => chatActions.select(id));
  add('Переслать', () => chatActions.forward([id]));
  add('Ответить', () => window.replyToMessage(id));
  add('Закрепить', () => window.pinMessage(id));
  add('Поставить реакцию', () => window.showReactionPicker(id, { target: bubble, stopPropagation() {} }));
  // A link that opens this exact message: ?room=<id>&msg=<id>
  add('Копировать ссылку', async () => {
    const url = new URL(location.href);
    url.searchParams.set('room', state.activeRoomId);
    url.searchParams.set('msg', id);
    url.hash = '';
    await navigator.clipboard.writeText(url.toString());
    toast('Ссылка на сообщение скопирована');
  });
  if (message._text || message.text) add('Копировать текст', () => navigator.clipboard.writeText(message._text || message.text));
  // Deleting only for yourself hides it locally and on your other devices,
  // leaving the message intact for everyone else.
  add('Удалить у себя', async () => {
    if (!confirm('Удалить сообщение только у себя? У других оно останется.')) return;
    const roomId=message.room_id || state.activeRoomId, store=chatRuntime.local();
    await api(`/rooms/${roomId}/messages/${id}/hide`, { method: 'DELETE' });
    await chatRuntime.hide(roomId,id);
    if(state.activeRoomId===roomId){state.messages=state.messages.filter(item=>item.id!==id);await mountPreservingInput();}
  }, true);
  if (message.sender_id === state.me.id) {
    if (message.type === 'text') add('Изменить', () => window.startEditMessage(id));
    add('Удалить у всех', () => window.deleteMessage(id), true);
  }
  menu.style.left = Math.max(8, Math.min(innerWidth - 238, event.clientX || rect.left)) + 'px';
  menu.style.top = Math.max(8, Math.min(innerHeight - menu.children.length * 44 - 20, event.clientY || rect.bottom)) + 'px';
  document.body.appendChild(menu);
  menu.querySelector('button')?.focus({ preventScroll: true });
};
function saveWebDraft() {
  if (state.activeRoomId && $('#inp')) chatRuntime.draft(state.activeRoomId, $('#inp').value);
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
window.setAutoDownload = value => { state.autoDownload = !!value; localStorage.setItem('autoDownload', value ? 'yes' : 'no'); void hydrateVisibleMedia(); };
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
    node.textContent = typing.length ? (typing.length === 1 ? `${typing[0]} печатает…` : 'Несколько человек печатают…') : room?.kind === 'dm' ? presenceLabel(state.activeDmPeer) : `${state.roomMembers.length} участников`;
    node.classList.toggle('online', !!typing.length || !!state.activeDmPeer?.online);
  }
  const connection = $('#webConnection');
  if (connection) { connection.hidden = state.wsAuthed; connection.textContent = navigator.onLine ? 'Подключение…' : 'Нет сети'; }
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
  const title = isGroup ? 'Создать группу' : 'Создать канал';

  return `
    <div class="modal-overlay create-room-modal" onclick="closeCreateRoom()">
      <div class="modal" onclick="event.stopPropagation()">
        <div class="modal-header">
          <h3>${title}</h3>
          <button class="btn" onclick="closeCreateRoom()">✕</button>
        </div>
        <div class="modal-body">
            <input class="input" id="createRoomTitleInput" placeholder="Название..." value="${state.createRoomTitle.replaceAll('"','&quot;')}" oninput="onCreateRoomTitleChange(this.value)">

          <div class="create-room-members">
            <div class="mini" style="margin-bottom:8px;">Добавить участников:</div>
            <input class="input" id="createRoomSearchInput" placeholder="Поиск пользователей..." value="${state.createRoomQuery.replaceAll('"','&quot;')}" oninput="onCreateRoomQueryChange(this.value)">

            <div id="createRoomResults" class="search-results" style="display:none;"></div>
          </div>

          <div id="createRoomSelectedContainer" style="margin-top:16px;${state.createRoomSelected.length ? '' : 'display:none;'}">
            <div class="mini" style="margin-bottom:8px;">Выбрано:</div>
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
          <button class="btn" onclick="closeCreateRoom()">Отмена</button>
          <button class="btn primary" onclick="submitCreateRoom()">Создать</button>
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
          <div class="status ${p.online ? 'online' : ''}">${p.online ? "онлайн" : (p.last_seen ? `был(а) ${new Date(p.last_seen).toLocaleString()}` : "")}</div>
        </div>
        <div class="profile-actions">
          ${!isMe ? `<button class="btn primary" onclick="startDM('${escapeJsSingleQuoted(p.username)}');closeProfile();">Написать</button>` : ""}
          ${!isMe && state.e2eReady ? `<button class="btn" onclick="showSafetyNumber('${escapeJsSingleQuoted(p.id)}')">🔐 Код безопасности</button>` : ""}
          ${!isMe ? `<button class="btn ${p.blocked_by_me ? '' : 'danger'}" onclick="toggleUserBlock('${escapeJsSingleQuoted(p.id)}')">${p.blocked_by_me ? 'Разбанить' : 'Забанить'}</button>` : ""}
          ${!isMe ? `<button class="btn danger" onclick="deleteConversationWithUser('${escapeJsSingleQuoted(p.id)}')">Удалить переписку</button>` : ""}
          ${isMe ? `<button class="btn primary" onclick="changePassword()">🔑 Сменить пароль</button>` : ""}
          ${isMe && state.me?.isAdmin ? `<button class="btn danger" onclick="openAdminPanel();closeProfile();">⚙️ Админ панель</button>` : ""}
          <button class="btn" onclick="closeProfile()">Закрыть</button>
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
            <h2>🔑 Бэкап ключей шифрования</h2>
            <div class="mini">Экспорт и восстановление E2E ключей</div>
          </div>
          <button class="btn" onclick="closeBackupPanel()">Закрыть</button>
        </div>
        <div class="modalBody">
          ${state.showRecoveryPhrase && state.recoveryPhrase ? `
            <div class="backup-success">
              <h3>✅ Бэкап создан успешно</h3>
              <p class="warning-text">⚠️ ВАЖНО: Сохраните эту информацию в безопасном месте!</p>

              <div class="backup-info">
                <div class="backup-field">
                  <label>Пользователь:</label>
                  <div class="backup-value">@${state.me?.username}</div>
                </div>
                <div class="backup-field">
                  <label>Дата создания:</label>
                  <div class="backup-value">${new Date(state.recoveryPhrase.timestamp).toLocaleString()}</div>
                </div>
                <div class="backup-field">
                  <label>Контрольная сумма:</label>
                  <div class="backup-value mono">${state.recoveryPhrase.checksum}</div>
                </div>
              </div>

              <div class="backup-phrase-container">
                <label>Recovery Phrase:</label>
                <textarea class="backup-phrase" readonly>${state.recoveryPhrase.phrase}</textarea>
              </div>

              <div class="backup-actions">
                <button class="btn primary" onclick="downloadBackup()">📥 Скачать файл</button>
                <button class="btn" onclick="copyBackupToClipboard()">📋 Копировать</button>
              </div>

              <div class="backup-warning">
                <p><strong>⚠️ Предупреждение:</strong></p>
                <ul>
                  <li>Любой, у кого есть эта фраза, может прочитать ваши сообщения</li>
                  <li>Храните бэкап в безопасном месте (менеджер паролей, зашифрованный диск)</li>
                  <li>Не отправляйте бэкап по незашифрованным каналам</li>
                  <li>При потере устройства используйте эту фразу для восстановления</li>
                </ul>
              </div>
            </div>
          ` : `
            <div class="backup-intro">
              <h3>Зачем нужен бэкап?</h3>
              <p>Ваши ключи E2E шифрования хранятся только на этом устройстве. Если вы потеряете доступ к устройству, вы потеряете доступ ко всем зашифрованным сообщениям.</p>

              <div class="backup-features">
                <div class="backup-feature">
                  <div class="backup-feature-icon">🔐</div>
                  <div class="backup-feature-text">
                    <strong>Безопасное восстановление</strong>
                    <p>Восстановите доступ к сообщениям на новом устройстве</p>
                  </div>
                </div>
                <div class="backup-feature">
                  <div class="backup-feature-icon">💾</div>
                  <div class="backup-feature-text">
                    <strong>Полный экспорт</strong>
                    <p>Все ключи и сессии сохраняются в одном файле</p>
                  </div>
                </div>
                <div class="backup-feature">
                  <div class="backup-feature-icon">✅</div>
                  <div class="backup-feature-text">
                    <strong>Проверка целостности</strong>
                    <p>Контрольная сумма защищает от повреждения данных</p>
                  </div>
                </div>
              </div>

              <div class="backup-actions-main">
                <button class="btn primary large" onclick="exportEncryptedBackup()">🔒 Экспорт в файл (с паролем)</button>
                <button class="btn large" onclick="importEncryptedBackup()">📂 Импорт из файла (с паролем)</button>
              </div>
              <div class="backup-info-box">
                <h4>🔒 Зашифрованный файл</h4>
                <p>Ключи шифруются вашим паролем (PBKDF2-SHA256 + AES-GCM). Пароль нигде не сохраняется: если вы его забудете, файл не восстановить.</p>
              </div>

              <div class="backup-actions-main">
                <button class="btn large" onclick="generateBackup()">🔑 Создать бэкап</button>
                <button class="btn large" onclick="restoreFromBackup()">📥 Восстановить из бэкапа</button>
              </div>

              <div class="backup-info-box">
                <h4>ℹ️ Автоматический бэкап</h4>
                <p>Ключи автоматически сохраняются в localStorage браузера. Однако при очистке данных браузера они будут удалены. Рекомендуем создать внешний бэкап.</p>
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
            <h2>🔐 Код безопасности</h2>
            <div class="mini">Верификация E2E шифрования с ${escapeHtml(data.user.username)}</div>
          </div>
          <button class="btn" onclick="closeSafetyNumber()">Закрыть</button>
        </div>
        <div class="modalBody">
          <div class="safety-number-intro">
            <h3>Что это?</h3>
            <p>Код безопасности (Safety Number) позволяет убедиться, что ваше E2E шифрование не скомпрометировано атакой "человек посередине" (MITM).</p>
          </div>

          <div class="safety-number-display">
            <div class="safety-number-code">
              ${formattedNumber.split('\n').map(line => `<div class="safety-number-line">${line}</div>`).join('')}
            </div>
          </div>

          <div class="safety-number-actions">
            <button class="btn primary" onclick="copySafetyNumber()">📋 Копировать код</button>
          </div>

          <div class="safety-number-instructions">
            <h3>Как проверить?</h3>
            <ol>
              <li>Попросите собеседника открыть ваш профиль и нажать "🔐 Код безопасности"</li>
              <li>Сравните коды - они должны полностью совпадать</li>
              <li>Сравнивайте коды через безопасный канал (лично, по телефону, видеозвонок)</li>
              <li>Если коды совпадают - ваше соединение защищено от MITM атак</li>
            </ol>
          </div>

          <div class="safety-number-warning">
            <p><strong>⚠️ Важно:</strong></p>
            <ul>
              <li>Код меняется при переустановке приложения или смене устройства</li>
              <li>Если код изменился без причины - возможна атака MITM</li>
              <li>Не сравнивайте коды через незашифрованные каналы (SMS, email)</li>
              <li>Этот код уникален для пары пользователей</li>
            </ul>
          </div>

          <div class="safety-number-technical">
            <details>
              <summary>Технические детали</summary>
              <div class="safety-number-tech-content">
                <p><strong>Алгоритм:</strong> SHA-256 хеш от комбинации публичных ключей обоих пользователей</p>
                <p><strong>Длина:</strong> 60 цифр (12 групп по 5)</p>
                <p><strong>Пользователи:</strong> @${escapeHtml(state.me.username)} ↔ @${escapeHtml(data.user.username)}</p>
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

    if (diffMins < 1) return 'только что';
    if (diffMins < 60) return `${diffMins} мин назад`;
    if (diffHours < 24) return `${diffHours} ч назад`;
    if (diffDays < 7) return `${diffDays} дн назад`;
    return date.toLocaleDateString();
  };

  return `
    <div class="modal-overlay" onclick="closeSessionsPanel()">
      <div class="modal-content sessions-panel" onclick="event.stopPropagation()">
        <div class="modalHead">
          <div>
            <h2>📱 Активные сессии</h2>
            <div class="mini">Управление устройствами</div>
          </div>
          <button class="btn" onclick="closeSessionsPanel()">Закрыть</button>
        </div>
        <div class="modalBody">
          ${sessions.length === 0 ? `
            <div class="sessions-empty">
              <p>Нет активных сессий</p>
            </div>
          ` : `
            ${currentSession ? `
              <div class="session-section">
                <h3>Текущая сессия</h3>
                <div class="session-item current">
                  <div class="session-icon">${getDeviceIcon(currentSession.deviceType)}</div>
                  <div class="session-info">
                    <div class="session-name">${escapeHtml(String(currentSession.deviceName || 'Устройство').replace(/^Unknown on /,'').replace(/^Unknown(?: Device)?$/,'Неизвестное устройство'))}</div>
                    <div class="session-details">
                      <span class="session-ip">${currentSession.ipAddress || 'IP не определён'}</span>
                      <span class="session-activity">${formatLastActivity(currentSession.lastActivity)}</span>
                    </div>
                    <div class="session-created">Создана: ${new Date(currentSession.createdAt).toLocaleString()}</div>
                  </div>
                  <div class="session-badge current-badge">Это устройство</div>
                </div>
              </div>
            ` : ''}

            ${otherSessions.length > 0 ? `
              <div class="session-section">
                <div class="session-section-header">
                  <h3>Другие сессии (${otherSessions.length})</h3>
                  ${otherSessions.length > 0 ? `
                    <button class="btn danger small" onclick="terminateOtherSessions()">Завершить все</button>
                  ` : ''}
                </div>
                ${otherSessions.map(s => `
                  <div class="session-item">
                    <div class="session-icon">${getDeviceIcon(s.deviceType)}</div>
                    <div class="session-info">
                      <div class="session-name">${escapeHtml(String(s.deviceName || 'Устройство').replace(/^Unknown on /,'').replace(/^Unknown(?: Device)?$/,'Неизвестное устройство'))}</div>
                      <div class="session-details">
                        <span class="session-ip">${s.ipAddress || 'IP не определён'}</span>
                        <span class="session-activity">${formatLastActivity(s.lastActivity)}</span>
                      </div>
                      <div class="session-created">Создана: ${new Date(s.createdAt).toLocaleString()}</div>
                    </div>
                    <button class="btn danger small" onclick="terminateSession('${s.id}')">Завершить</button>
                  </div>
                `).join('')}
              </div>
            ` : ''}

            <div class="sessions-actions">
              <button class="btn danger" onclick="terminateAllSessions()">Завершить все сессии (включая эту)</button>
            </div>

            <div class="sessions-info-box">
              <h4>ℹ️ О сессиях</h4>
              <ul>
                <li>Каждая сессия представляет вход с отдельного устройства или браузера</li>
                <li>Если вы видите незнакомое устройство, завершите его сессию</li>
                <li>При завершении сессии устройство будет разлогинено</li>
                <li>Рекомендуем периодически проверять активные сессии</li>
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
            <h2>⚙️ Админ панель</h2>
            <div class="mini">Пользователи: ${users.length}</div>
          </div>
          <button class="btn" onclick="closeAdminPanel()">Закрыть</button>
        </div>
        <div class="modalBody admin-panel-body">
          <div class="admin-users-list">
            ${users.map(u => `
              <div class="admin-user-row">
                ${u.avatar_id ? `<img class="avatar-sm" src="${avatarUrl(u.avatar_id)}">` : `<div class="avatar-placeholder"></div>`}
                <div class="admin-user-info">
                  <div class="admin-user-title"><b>${escapeHtml(displayName(u))}</b> @${escapeHtml(u.username)} ${u.banned ? '<span class="admin-user-bad">[ЗАБАНЕН]</span>' : ''} ${u.online ? '🟢' : ''}</div>
                  <div class="mini">Регистрация: ${new Date(u.created_at).toLocaleDateString()}</div>
                </div>
                <div class="admin-user-actions">
                  <button class="btn-sm" onclick="adminResetPassword('${u.id}', '${escapeJsSingleQuoted(u.username)}')">🔑 Пароль</button>
                  <button class="btn-sm ${u.banned ? '' : 'danger'}" onclick="adminToggleBan('${u.id}', '${escapeJsSingleQuoted(u.username)}', ${u.banned})">${u.banned ? '✅ Разбан' : '🚫 Бан'}</button>
                  ${u.banned ? `<button class="btn-sm danger" onclick="adminDeleteUser('${u.id}', '${escapeJsSingleQuoted(u.username)}')">🗑️ Удалить</button>` : ''}
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
          <h2>${isDM ? (members.find(m => m.id !== state.me.id)?.username || "Пользователь") : (room?.title || "Группа")} ${!isDM && canEdit ? `<button class="btn-sm" onclick="updateRoomTitle()">✏️</button>` : ""}</h2>
          <div class="status">${isDM ? "Личная переписка" : `${members.length} ${room?.kind === "channel" ? plural(members.length, 'подписчик', 'подписчика', 'подписчиков') : plural(members.length, 'участник', 'участника', 'участников')}`}</div>
        </div>

        ${!isDM ? `
        <div class="members-section">
          <div class="members-header">
            <h3>${room?.kind === "channel" ? "Подписчики" : "Участники"}</h3>
            ${canEdit ? `<button class="btn-sm" onclick="showAddMemberModal()">+ Добавить</button>` : ""}
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
                    <button class="btn-sm" onclick="toggleAdmin('${memberId}', '${escapeJsSingleQuoted(m.role)}')">${m.role === "admin" ? "Снять админа" : "Назначить админом"}</button>
                    <button class="btn-sm danger" onclick="removeMember('${memberId}')">✕</button>
                  </div>
                ` : ""}
              </div>
            `}).join("")}
          </div>
        </div>
        ` : ""}

        <div class="profile-actions">
          ${isDM ? `<button class="btn danger" onclick="deleteDMChat()">🗑️ Удалить чат</button>` : ""}
          ${!isDM && myRole === "owner" ? `<button class="btn danger" onclick="deleteRoom()">🗑️ Удалить группу</button>` : ""}
          ${!isDM && myRole !== "owner" ? `<button class="btn danger" onclick="leaveRoom()">🚪 Покинуть группу</button>` : ""}
          ${!isDM && (myRole === "owner" || myRole === "admin") ? `<button class="btn" onclick="toggleGroupE2e()">🔒 Сквозное шифрование: ${groupE2eRooms.get(state.activeRoomId) ? "вкл" : "выкл"}</button>` : ""}
          <button class="btn" onclick="closeRoomSettings()">Закрыть</button>
        </div>
      </div>
    </div>
  `;
}

function renderComposeArea(room) {
  if (!room) return "";
  if (state.roomLoading || state.roomLoadError) return '<div class="compose-hint">Переписка загружается</div>';

  // Reply preview
  const replyHtml = state.replyTo ? `
    <div class="reply-preview">
      <div class="reply-content">
        <div class="reply-name">${escapeHtml(state.replyTo.sender_display_name || state.replyTo.sender_username || 'Пользователь')}</div>
        <div class="reply-text">${escapeHtml((state.replyTo.text || '').slice(0, 100))}${(state.replyTo.text || '').length > 100 ? '...' : ''}</div>
      </div>
      <button class="btn-cancel-reply" onclick="cancelReply()">✕</button>
    </div>
  ` : '';
  const editHtml = state.editMessage ? `
    <div class="reply-preview edit-preview">
      <div class="reply-content">
        <div class="reply-name">Редактирование сообщения</div>
        <div class="reply-text">${escapeHtml((state.editMessage._text || state.editMessage.text || '').slice(0, 100))}</div>
      </div>
      <button class="btn-cancel-reply" onclick="cancelEdit()">✕</button>
    </div>
  ` : '';
  const dmBlocked = room.kind === "dm" && (state.activeDmPeer?.blocked_by_me || state.activeDmPeer?.blocks_me);
  if (dmBlocked) {
    return `<div class="compose" style="justify-content:center;color:var(--muted);">${state.activeDmPeer?.blocks_me ? 'Пользователь забанил вас, писать нельзя' : 'Вы забанили пользователя, переписка только для чтения'}</div>`;
  }

  if (room.kind !== "channel") {
    // Regular chat - full compose
    return `
      <div class="compose-wrapper">
        ${editHtml}
        ${replyHtml}
        <div class="compose">
          ${iconButton('attach', 'Прикрепить файл', 'triggerFileSelect()')}
          <input type="file" id="file" multiple hidden onchange="uploadSelectedFile()">
          <textarea class="textarea" id="inp" rows="1" aria-label="Сообщение" placeholder="Сообщение..." oninput="onComposerInput(this.value)" onkeydown="handleMessageKeydown(event)">${escapeHtml(state.composerText || "")}</textarea>
          <button class="icon-button" id="btn_voice" onclick="toggleVoice()" aria-label="Голосовое сообщение" title="Голосовое сообщение">${icon('mic')}</button>
          <button class="icon-button circle-note-button" id="btn_video_note" onclick="toggleVideoNote()" aria-label="Записать видеокружок" title="Видеокружок">${icon('video')}</button>
          <button class="icon-button send-button" onclick="doSend()" aria-label="Отправить сообщение" title="Отправить">${icon('send')}</button>
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
          ${iconButton('attach', 'Прикрепить файл', 'triggerFileSelect()')}
          <input type="file" id="file" multiple hidden onchange="uploadSelectedFile()">
          <textarea class="textarea" id="inp" rows="1" aria-label="Сообщение" placeholder="Сообщение канала..." oninput="onComposerInput(this.value)" onkeydown="handleMessageKeydown(event)">${escapeHtml(state.composerText || "")}</textarea>
          <button class="icon-button send-button" onclick="doSend()" aria-label="Отправить сообщение" title="Отправить">${icon('send')}</button>
        </div>
      </div>
    `;
  }
  return `<div class="compose" style="justify-content:center;color:var(--muted);">📢 Канал только для чтения</div>`;
}

function renderStatus(m) {
  if (m.delivery) {
    const labels = { sending: 'Отправляется…', queued: 'Ожидает соединения', failed: 'Не отправлено' };
    const id = escapeJsSingleQuoted(m.client_message_id);
    return `<span class="delivery-status" title="${escapeHtml(m.error || '')}">${labels[m.delivery] || ''}</span>${m.delivery === 'failed' ? `<button class="inline-action" onclick="retryQueuedMessage('${id}')">Повторить</button>` : ''}${m.delivery !== 'sending' ? `<button class="inline-action" onclick="cancelQueuedMessage('${id}')">Отменить</button>` : ''}`;
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
    return `<span class="msg-status" title="Отправлено">${icon('check')}</span>`;
  }

  if (readCount === 0) {
    // Not read by anyone yet - show single checkmark
    return `<span class="msg-status" title="Отправлено">${icon('check')}</span>`;
  }

  if (readCount >= totalOthers) {
    // Read by everyone - show blue double checkmarks
    return `<span class="msg-status read" title="Прочитано всеми">${icon('checks')}</span>`;
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
  const src = `/api/media/${m.media_id}`;
  const poster = m.thumb_path ? ` poster="/api/media/${m.thumb_path}"` : "";
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
      <button type="button" class="vn-mute" aria-label="Звук" aria-pressed="false"
              onclick="toggleVideoNoteMute(event, this)">${icon('speaker')}</button>
    </div>
  `;
}

function renderMessageBody(m) {
  if (m.meta?.media_encryption) return `<button class="encrypted-media" data-encrypted-media="${escapeHtml(m.id)}" onclick="openWebMedia('${escapeJsSingleQuoted(m.id)}')">${icon('attach')}<span>${escapeHtml(m.meta.media_encryption.name || 'Вложение')} · Открыть</span></button>`;
  if (m.type === "voice" && m.media_id) {
    return `<audio class="media-audio" src="/api/media/${m.media_id}" controls preload="metadata" playsinline></audio>`;
  }
  if (m.type === "video_note" && m.media_id) {
    return renderVideoNote(m);
  }
  if (m.type === "media" && m.media_id) {
    if (m.media_mime?.startsWith("image/")) {
      return `<img class="media-image" src="/api/media/${m.media_id}" onclick="openWebMedia('${escapeJsSingleQuoted(m.id)}')" alt="Фотография" loading="lazy">`;
    }
    if (m.media_mime?.startsWith("video/")) {
      return `<video class="media-video" src="/api/media/${m.media_id}" controls playsinline ${m.thumb_path ? `poster="/api/media/${m.media_id}/thumb"` : ""}></video>`;
    }
    return `<a class="media-file" href="/api/media/${m.media_id}" target="_blank">📎 Файл</a>`;
  }
  return `<div class="bText">${renderMentionText((m._text ?? m.text ?? "").toString())}</div>`;
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

  return state.messages.map((m, index) => {
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
    const editedBadge = meta.edited || meta.edited_at ? `<span class="msg-edited">изменено</span>` : "";

    const replyPreview = m.reply_to ? `
      <div class="reply-reference" onclick="scrollToMessage('${m.reply_to.id}', event)">
        <div class="reply-line"></div>
        <div class="reply-info">
          <div class="reply-name">${escapeHtml(m.reply_to.sender_name || 'Пользователь')}</div>
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
    const groupedBefore = groupWith(state.messages[index - 1], m);
    const groupedAfter = groupWith(m, state.messages[index + 1]);

    return `
      ${needsDaySeparator ? `<div class="day-separator" data-day-label="${escapeHtml(formatDayLabel(m.created_at))}"><span>${formatDayLabel(m.created_at)}</span></div>` : ""}
      <div class="bubble ${mine ? "me" : ""} ${isDM ? 'dm' : ''} ${isReplyingTo ? 'replying-to' : ''} ${groupedBefore ? 'group-before' : ''} ${groupedAfter ? 'group-after' : ''} ${state.selectedMessages?.has(msgId) ? 'selected-message' : ''}" onclick="selectMessageIfNeeded('${safeMsgId}',event)" id="msg-${escapeHtml(msgId)}" data-message-id="${escapeHtml(msgId)}"
           oncontextmenu="openMessageMenu('${safeMsgId}', event)"
           ontouchmove="cancelMessagePress()"
           ondblclick="replyToMessage('${safeMsgId}', event)"
           ontouchstart="handleTouchStart(event, '${safeMsgId}')"
           ontouchend="handleTouchEnd(event)">
        ${showSenderInfo && senderId ? `<div class="bAvatar">${senderAvatar ? `<img src="${senderAvatar}">` : `<div class="avatar-placeholder-sm"></div>`}</div>` : ""}
        <button class="icon-button message-menu-trigger" aria-label="Действия с сообщением" onclick="openMessageMenu('${safeMsgId}', event)">${icon('more')}</button>
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
  if (state.roomLoading || state.roomLoadError) { box.innerHTML = state.roomLoadError ? '<div class="list-hint">Не удалось загрузить переписку.<button class="btn" onclick="selectRoom(stateRoomId())">Повторить</button></div>' : '<div class="list-hint" role="status">Загрузка сообщений…</div>'; return; }
  const bottom = shouldStickToBottom(box);
  const older = state.chatHistory?.has_older !== false ? `<button id="loadOlderMessages" class="history-load" onclick="loadOlderMessages()" ${state.loadingOlder ? 'disabled' : ''}>${state.loadingOlder ? 'Загрузка…' : 'Загрузить предыдущие сообщения'}</button>` : '';
  reconcileMessages(box, `<div id="scrollDateIndicator" class="scroll-date-indicator" aria-hidden="true"></div>${older}${buildRenderedMessagesHtml()}`, bottom);
  bindMessagesScrollIndicator(); syncScrollDateIndicator(box, false); chatRuntime.bind(); bindAllVideoNotes(); void hydrateVisibleMedia();
}
function render() { renderMessages(); }

// --- Auth ---
function authReport(message, signedIn = false) {
  const node = $('#authError');
  if (node && !signedIn) { node.textContent = message; node.hidden = !message; }
  else if (message) toast(message);
}
const authenticate = createAuthFlow({ request: api, setUser: user => { state.me = user; }, render: () => mount(),
  initialize: async () => { localStorage.setItem('67chat-profile', JSON.stringify(state.me)); await chatRuntime.start(state.me); connectWS(); await loadRooms(); await syncProfileRoute(); void initE2E(); void initServiceWorker(); },
  report: authReport, busy: value => { document.querySelectorAll('.auth-card button,.auth-card input').forEach(node => node.disabled = value); }
});
window.login = () => authenticate('login', { username: $('#u').value.trim(), password: $('#p').value });
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
    authReport('Пройдите проверку и нажмите «Создать аккаунт».');
  } catch (error) { authReport(userError(error)); }
};
window.register = async () => {
  const username = $('#u').value.trim(), password = $('#p').value;
  if (!/^[a-zA-Z0-9_]{3,30}$/.test(username)) return authReport('Имя: 3–30 латинских букв, цифр или знаков подчёркивания.');
  if (password.length < 6) return authReport('Пароль должен содержать не менее 6 символов.');
  await authenticate('register', { username, password, captcha: captchaToken });
};

window.logout = async () => {
  try { await webNotifications.disable(); } catch {}
  try { await api("/auth/logout", { method: "POST", body: JSON.stringify({}) }); } catch {}
  resetClientSession();
  mount();
};

async function loadRooms() {
  const account = state.me?.id; if (!account) return;
  const rooms = await prepareRoomPreviews(await api("/rooms") || []);
  if (state.me?.id !== account) return;
  state.rooms = rooms;
  const folders = await api("/me/folders");
  if (state.me?.id !== account) return;
  state.folders = Array.isArray(folders) ? folders : [];
  await chatRuntime.local()?.set("folders", state.folders);
  await chatRuntime.saveRooms(rooms);
  updateRoomsList();
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
        return { ...room, _lastText: text };
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
      roomList.innerHTML = state.rooms.map(r => renderRoomItem(r)).join("");
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
  saveWebDraft(); ++roomRequestId; state.editingRoom = false;
  if (!options.fromRoute) { const url = new URL(location.href); url.searchParams.set('room', id); history[options.replace ? 'replaceState' : 'pushState']({ roomId: id }, '', url); }
  if (state.wsAuthed) state.ws?.send(JSON.stringify({ type: 'rooms.join', room_id: id }));
  try { await chatRuntime.open(id, options); } catch (error) { toast(userError(error)); }
};
async function markMessagesRead() { chatRuntime.bind(); }

async function buildTextMessage(roomId, currentRoom, text) {
  if (currentRoom?.kind === "group" && await roomE2eEnabled(roomId)) return encryptGroupText(roomId, text);
  const key = await getCryptoKey(roomId, { e2ee_v: 2 });
  if (key) {
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const enc = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, new TextEncoder().encode(text));
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
  const text = input?.value.trim(), reply = state.replyTo, editing = state.editMessage;
  if (!text || !roomId) return;
  if (text.length > 5000) return toast('Сообщение слишком длинное: максимум 5000 символов.');
  sendingDraft = true;
  try {
    if (editing) {
      const body = await buildTextMessage(roomId, state.rooms.find(r => r.id === roomId), text);
      if (state.me?.id !== account) return;
      const sent = await api(`/rooms/${roomId}/messages/${editing.id}`, { method: 'PUT', body: JSON.stringify({ text: body.text, meta: body.meta }) });
      await chatRuntime.received(sent);
    } else await chatRuntime.enqueue({ roomId, text, reply });
    if (state.me?.id === account && state.activeRoomId === roomId && $('#inp')?.value.trim() === text) {
      state.composerText = ''; $('#inp').value = ''; chatRuntime.draft(roomId, ''); state.replyTo = null; state.editMessage = null; resizeComposer();
      if (reply || editing) await mountPreservingInput();
    }
  } catch (error) { toast(userError(error)); } finally { sendingDraft = false; }
};

window.setE2EE = async () => {
  const k = prompt("Ключ шифрования:");
  if (k) await storeWrappedRoomPassphrase(state.activeRoomId, k);
  selectRoom(state.activeRoomId);
};

// Recording and uploads preserve the original account/recipient and require confirmation.
window.toggleVoice = () => void webMedia.compose('voice').catch(e => toast(userError(e)));
window.toggleVideoNote = () => void webMedia.compose('circle').catch(e => toast(userError(e)));
let selectedFileContext;
window.triggerFileSelect = () => {
  const dialog=document.createElement('dialog');dialog.className='actions-dialog';const title=document.createElement('h2');title.textContent='Вложения';dialog.append(title);
  const owner={userId:state.me?.id,roomId:state.activeRoomId,title:state.rooms.find(r=>r.id===state.activeRoomId)?.title || state.rooms.find(r=>r.id===state.activeRoomId)?.other_display_name || 'Переписка',store:chatRuntime.local()};
  const close=()=>{dialog.close();dialog.remove();};
  const add=(label,action)=>{const b=document.createElement('button');b.textContent=label;b.onclick=()=>{close();action();};dialog.append(b);};
  const choose=accept=>{selectedFileContext=owner;const input=$('#file');if(input){input.accept=accept;input.click();}};
  add('Фото и видео',()=>choose('image/*,video/*'));add('Файл',()=>choose(''));add('Черновики вложений',()=>void webMedia.compose('files',[],owner).catch(e=>toast(userError(e))));add('Голосовое сообщение',()=>void webMedia.compose('voice',[],owner).catch(e=>toast(userError(e))));add('Видеокружок',()=>void webMedia.compose('circle',[],owner).catch(e=>toast(userError(e))));add('Отмена',()=>{});document.body.append(dialog);dialog.showModal();
};
window.uploadSelectedFile = () => { const input = $('#file'), files = [...(input?.files || [])],owner=selectedFileContext; if (input) input.value = ''; if (files.length) void webMedia.compose('files', files,owner).catch(e => toast(userError(e))); };

// --- Calls ---
let currentCamera = 'user'; // 'user' for front, 'environment' for back

window.startRoomCall = async (isVideo = false) => {
  if (!state.wsAuthed) { alert("Нет подключения"); return; }

  if (state.callState !== 'idle') return;

  const roomId = state.activeRoomId;
  state.isVideoCall = !!isVideo; state.isVideoEnabled = !!isVideo; state.isAudioEnabled = true;
  state.callState = 'calling';
  state.currentCallRoomId = state.activeRoomId || null;
  state.currentCallPeerId = null;
  state.pendingIceCandidates = [];
  if (isVideo) showVideoCallUI(); else showAudioCallUI();

  let stream;
  try {
    // Audio only call
    stream = await navigator.mediaDevices.getUserMedia({ ...getAudioConstraints(), video: isVideo ? { facingMode: "user", width: { ideal: 1280 }, height: { ideal: 720 } } : false });
  }
  catch (e) { hangupCall(false); alert("Нет доступа к микрофону."); return; }

  if (state.callState !== "calling" || state.currentCallRoomId !== roomId) { stream.getTracks().forEach(track => track.stop()); return; }
  state.localStream = stream;
  await loadRtcIceServers();
  state.pc = new RTCPeerConnection({
    iceServers: getRtcIceServers(),
    iceTransportPolicy: "all",
    iceCandidatePoolSize: 4
  });
  if (isVideo) showVideoCallUI(); else showAudioCallUI();

  updateCallStatus("Вызов...", false);
  wirePeerConnectionHandlers({ roomId, peerUserId: null });

  stream.getTracks().forEach(t => state.pc.addTrack(t, stream));

  setTimeout(() => { if (state.callState === "calling" && state.currentCallRoomId === roomId) { toast("Абонент не ответил"); hangupCall(); } }, 45000);
  try {
    const offer = await state.pc.createOffer();
    await state.pc.setLocalDescription(offer);
    state.ws?.send(JSON.stringify({ type: "call.offer", toRoom: roomId, room_id: roomId, is_video: !!isVideo, sdp: { type: offer.type, sdp: offer.sdp } }));
  } catch (e) { console.error("Failed to start call:", e); alert("Ошибка звонка"); hangupCall(); }
};

window.jumpToDate = () => {
  const dialog = document.createElement('dialog');
  dialog.className = 'actions-dialog';
  dialog.innerHTML = '<h2>Перейти к дате</h2><label>Дата<input type="date" aria-label="Дата сообщения"></label><div class="row" style="margin-top:12px"><button type="button" class="btn primary">Перейти</button><button type="button" class="btn">Отмена</button></div>';
  const input = dialog.querySelector('input');
  const [go, cancel] = dialog.querySelectorAll('button');
  const close = () => { dialog.close(); dialog.remove(); };
  cancel.onclick = close;
  go.onclick = async () => {
    if (!input.value) return;
    const target = new Date(`${input.value}T00:00:00`);
    close();
    // Walk back through history until the target day is loaded (bounded so a
    // stray click cannot pull the whole archive).
    for (let page = 0; page < 40; page += 1) {
      const first = state.messages[0];
      if (!first || new Date(first.created_at) <= target || state.chatHistory?.has_older === false) break;
      await chatRuntime.older();
    }
    const hit = state.messages.find(message => new Date(message.created_at) >= target);
    if (!hit) { toast('Сообщений за эту дату не найдено'); return; }
    const node = document.getElementById('msg-' + hit.id);
    if (node) {
      node.scrollIntoView({ block: 'center' });
      node.classList.add('selected-message');
      setTimeout(() => node.classList.remove('selected-message'), 1600);
    } else {
      await chatRuntime.jump(hit.id);
    }
  };
  document.body.append(dialog);
  dialog.showModal();
  input.focus();
};

window.addEventListener("popstate", () => {
  const roomId = new URL(location.href).searchParams.get("room");
  if (roomId && state.me) void selectRoom(roomId, { fromRoute: true });
  else if (state.activeRoomId) window.backToList(true);
  syncProfileRoute().catch(err => console.error("Route sync error:", err));
});

// --- Init ---
(async () => {
  try {
    try {state.me=JSON.parse(localStorage.getItem('67chat-profile') || 'null');}catch{state.me=null;}
    if(state.me?.id) {
      await chatRuntime.start(state.me);await mount();
      const roomId=new URL(location.href).searchParams.get('room');
      if(roomId && state.rooms.some(room=>room.id===roomId))void selectRoom(roomId);
    } else await mount();
    const user=await api('/me').catch(error=>{if(error.status!==401)toast(userError(error));return state.me;});
    if(!user?.id){await mount();return;}
    state.me=user;localStorage.setItem('67chat-profile',JSON.stringify(user));await chatRuntime.start(user);
    connectWS();void initE2E();void initServiceWorker();
    await loadRooms().catch(error=>toast(userError(error)));await mount();
    const roomId=new URL(location.href).searchParams.get('room');if(roomId && !state.activeRoomId && state.rooms.some(room=>room.id===roomId))await selectRoom(roomId);
    // A shared message link (?room=…&msg=…) scrolls to that message once the
    // history has rendered.
    const linkedMessage = new URL(location.href).searchParams.get('msg');
    if (linkedMessage && state.activeRoomId) {
      requestAnimationFrame(() => window.scrollToMessage?.(linkedMessage, { highlight: true }));
    }
    await syncProfileRoute();
  }catch(error){console.error('Init error:',error);toast(userError(error));await mount();}
})();
