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
  darkTheme: localStorage.getItem("theme") !== "light",
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

window.handleTouchStart = (e, msgId) => {
  touchStartX = e.touches[0].clientX;
  touchStartY = e.touches[0].clientY;
  touchMsgId = msgId;
};

window.handleTouchEnd = (e) => {
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

window.scrollToMessage = (msgId, event) => {
  if (event) event.stopPropagation();
  const msgEl = $(`#msg-${msgId}`);
  if (msgEl) {
    msgEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
    msgEl.style.background = 'var(--accent)';
    setTimeout(() => { msgEl.style.background = ''; }, 1000);
  }
};

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
    picker.style.top = `${rect.top - 55}px`; // Above the message
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
  localStorage.setItem("theme", state.darkTheme ? "dark" : "light");
  applyTheme();
  mount();
};

function applyTheme() {
  document.body.classList.toggle("light-theme", !state.darkTheme);
}

// --- Utils ---
async function api(path, opts={}) {
  const r = await fetch(API + path, {
    ...opts,
    headers: { ...(opts.body instanceof FormData ? {} : { "Content-Type":"application/json" }), ...(opts.headers||{}) },
    credentials: "include"
  });
  const ct = r.headers.get("content-type") || "";
  const isJson = ct.includes("application/json");
  const body = isJson ? await r.json().catch(() => null) : await r.text().catch(() => "");
  if (r.status === 401 && !isAuthEntryPath(path)) handleUnauthorized();
  if (!r.ok) return null;
  return body;
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
  stopPreKeyMonitor();
  e2eSessionInflight.clear();
  e2ePeerDeviceIds.clear();
  state.e2eManager = null;
  state.e2eReady = false;

  closeWS();
  clearAllTyping();

  state.me = null;
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
  if (snapshot) state.composerText = snapshot.value;
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
async function initE2E() {
  // libsignal + Curve25519Wrapper load as ES modules (see index.html). Wait for
  // them if the module script hasn't finished yet.
  if (!window.E2EManager) return;
  if (!window.libsignal) {
    await new Promise((resolve) => {
      if (window.libsignal) return resolve();
      const onReady = () => { window.removeEventListener('libsignal-ready', onReady); resolve(); };
      window.addEventListener('libsignal-ready', onReady);
      setTimeout(onReady, 5000);
    });
  }
  if (!window.libsignal) return;

  try {
    state.e2eManager = new window.E2EManager();
    const deviceId = getE2EDeviceId();

    // Persistent backend: IndexedDB through the keystore module.
    const persistentStore = window.SigStore || null;

    // Try to restore previously generated keys first.
    const loaded = await state.e2eManager.loadFromIndexedDB(state.me.id);

    if (!loaded) {
      // Initialize and generate a fresh key bundle.
      const bundle = await state.e2eManager.init(state.me.id, deviceId, persistentStore);

      // Upload the public bundle (identity, signed pre-key + signature, pre-keys).
      const uploadRes = await api("/e2e/keys", {
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
          pre_keys: bundle.preKeys.map(pk => ({
            key_id: pk.keyId,
            public_key: pk.publicKey
          }))
        })
      });

      if (uploadRes?.ok) {
        state.e2eReady = true;
        // Persist locally only AFTER the server accepted the bundle, so a failed
        // upload doesn't leave us with keys nobody else can fetch.
        await state.e2eManager.saveToIndexedDB(state.me.id);
      } else {
        // Keys generated but not published — do not claim E2E is ready.
        state.e2eReady = false;
        console.warn('E2E key upload failed; staying on room-key fallback');
      }
    } else {
      state.e2eReady = true;
    }

    // Keep an eye on remaining one-time pre-keys; refill before running dry.
    if (state.e2eReady) {
      maybeReplenishPreKeys(deviceId, { force: true });
      startPreKeyMonitor();
    }
  } catch (e) {
    // Non-fatal: the app still works with room-key encryption for groups.
    console.error("E2E not available, using fallback encryption:", e?.message || e);
  }
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

// Single-flight: concurrent callers for the same peer share one in-flight session
// creation, so we never fetch a bundle / burn a pre-key / build a session twice.
const e2eSessionInflight = new Map();

function getE2ESessionForUser(userId) {
  if (!state.e2eReady || !state.e2eManager) return Promise.resolve(false);
  const key = String(userId);
  const pending = e2eSessionInflight.get(key);
  if (pending) return pending;
  const p = createE2ESessionForUser(userId).finally(() => {
    // Always release the lock (success or failure) so a later call can retry.
    if (e2eSessionInflight.get(key) === p) e2eSessionInflight.delete(key);
  });
  e2eSessionInflight.set(key, p);
  return p;
}

async function createE2ESessionForUser(userId) {
  try {
    // Check if session already exists
    const sessionExists = await state.e2eManager.store.loadSession(userId + '.1');
    if (sessionExists) return true;

    // Fetch user's key bundle from server
    const bundle = await api(`/e2e/keys/${userId}`);
    if (!bundle || !bundle.identity_key) return false;
    const peerDeviceId = bundle.device_id || '1';
    e2ePeerDeviceIds.set(String(userId), peerDeviceId);

    // Build session with user
    await state.e2eManager.buildSession(userId, peerDeviceId, {
      registrationId: bundle.registration_id || 0,
      identityKey: bundle.identity_key,
      signedPreKey: bundle.signed_pre_key,
      preKey: bundle.pre_key
    });

    return true;

    return true;
  } catch (e) {
    // Тихо игнорируем - будет работать без E2E
    return null;
  }
}

async function encryptMessageE2E(recipientId, plaintext) {
  if (!state.e2eReady || !state.e2eManager) return null;

  try {
    // Ensure session exists
    const hasSession = await getE2ESessionForUser(recipientId);
    if (!hasSession) {
      console.error('E2E session not available for user:', recipientId);
      return null;
    }

    // Encrypt message
    const ciphertext = await state.e2eManager.encryptMessage(recipientId, e2ePeerDeviceIds.get(String(recipientId)) || '1', plaintext);
    return ciphertext;
  } catch (e) {
    console.error('E2E encryption failed:', e);
    return null;
  }
}

async function decryptMessageE2E(senderId, ciphertext) {
  if (!state.e2eReady || !state.e2eManager) return null;

  try {
    // Ensure session exists (will be created on first message)
    await getE2ESessionForUser(senderId);

    const plaintext = await state.e2eManager.decryptMessage(senderId, e2ePeerDeviceIds.get(String(senderId)) || '1', ciphertext);
    // A PreKeyWhisperMessage (type 1) consumed one of our one-time pre-keys.
    if (ciphertext?.type === 1) maybeReplenishPreKeys();
    return plaintext;
  } catch (e) {
    console.error('E2E decryption failed:', e);
    return null;
  }
}

// --- Group E2E (Sender Keys) ---
// Opt-in per group (rooms.e2e_enabled). Each member's sender key reaches the others
// through the pairwise Signal sessions; the server only stores opaque payloads.
const GROUP_E2E_MAX_MEMBERS = 500;
const groupE2eRooms = new Map(); // roomId -> boolean, from GET /rooms/:id/e2e
const groupPlainCache = new Map(); // envelope JSON -> plaintext (own messages this session)
const groupInboxInflight = new Map();

function groupKey(roomId, suffix) {
  return `gk:${state.me?.id}:${roomId}:${suffix}`;
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
async function saveOwnSenderKey(roomId, rec) {
  await window.SigStore.put(groupKey(roomId, 'own'), rec);
  await window.SigStore.put(groupKey(roomId, `own:${rec.keyId}`), rec);
}

async function distributeOwnSenderKey(roomId, members, { rotate = false } = {}) {
  const myId = String(state.me.id);
  let rec = await window.SigStore.get(groupKey(roomId, 'own'), null);
  // A recipient has left (or a rekey was requested): a new key goes to current members only.
  const plan = window.GroupCrypto.planSend(rotate ? null : rec, members, myId);
  if (plan.rotate) rec = await window.GroupCrypto.createSenderKey();

  const pending = plan.pending;
  if (pending.length) {
    const dist = window.GroupCrypto.distributionFor(rec);
    const recipients = [];
    const unreachable = [];
    for (const uid of pending) {
      const ct = await encryptMessageE2E(uid, JSON.stringify({ ...dist, room_id: roomId }));
      if (!ct) { unreachable.push(uid); continue; }
      recipients.push({ user_id: uid, payload: JSON.stringify(ct) });
    }
    if (recipients.length) {
      const res = await api(`/rooms/${roomId}/sender-keys`, {
        method: 'POST',
        body: JSON.stringify({ key_id: rec.keyId, recipients })
      });
      if (!res) throw new Error('sender_key_upload_failed');
      rec.distributedTo = [...rec.distributedTo, ...recipients.map(r => r.user_id)];
    }
    // Warn once per change in the unreachable set, not on every message.
    const prev = (rec.unreachable || []).join(',');
    rec.unreachable = unreachable;
    if (unreachable.length && unreachable.join(',') !== prev) {
      alert(`Ключ не передан ${unreachable.length} участн. (нет E2E-ключей на устройстве). Они не смогут читать сообщения в этой группе.`);
    }
  }
  await saveOwnSenderKey(roomId, rec);
  return rec;
}

async function encryptGroupText(roomId, text) {
  if (!state.e2eReady || !state.e2eManager) throw new Error('E2E не готово на этом устройстве');
  const members = await fetchAllMemberIds(roomId);
  if (members.length > GROUP_E2E_MAX_MEMBERS) throw new Error(`В группе с E2E не более ${GROUP_E2E_MAX_MEMBERS} участников`);
  const rec = await distributeOwnSenderKey(roomId, members);
  const { envelope, state: next } = await window.GroupCrypto.encryptWithState(rec, roomId, String(state.me.id), text);
  await saveOwnSenderKey(roomId, next);
  const encrypted_content = JSON.stringify(envelope);
  groupPlainCache.set(encrypted_content, text);
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
    await distributeOwnSenderKey(roomId, members, { rotate: true });
  } catch (e) {
    console.error('Group re-key failed:', e.message);
  }
}

// Fetch pending sender-key distributions addressed to me; import and ack each one.
// Rows that fail to decrypt stay pending so a later attempt can retry them.
function syncGroupInbox(roomId) {
  if (groupInboxInflight.has(roomId)) return groupInboxInflight.get(roomId);
  const p = (async () => {
    const rows = await api(`/rooms/${roomId}/sender-keys`) || [];
    for (const row of rows) {
      try {
        const plain = await decryptMessageE2E(row.from_user_id, JSON.parse(row.payload));
        if (!plain) continue;
        const d = JSON.parse(plain);
        if (d.room_id !== roomId) continue;
        const key = groupKey(roomId, `in:${row.from_user_id}`);
        const cur = await window.SigStore.get(key, null);
        if (!cur || cur.keyId !== d.kid) {
          await window.SigStore.put(key, window.GroupCrypto.receiverStateFromDistribution(d));
        }
        await api(`/rooms/${roomId}/sender-keys/${row.id}`, { method: 'DELETE' });
      } catch (e) {
        console.error('Sender key import failed:', e.message);
      }
    }
  })().finally(() => groupInboxInflight.delete(roomId));
  groupInboxInflight.set(roomId, p);
  return p;
}

const GROUP_KEY_UNAVAILABLE = "🔐 [Ключ отправителя недоступен]";

async function decryptGroupMsg(roomId, msg) {
  const cached = groupPlainCache.get(msg.encrypted_content);
  if (cached !== undefined) return cached;
  let env;
  try { env = JSON.parse(msg.encrypted_content); } catch { return "❌ [Ошибка E2E расшифровки]"; }
  const senderId = String(msg.sender_id);
  const isOwn = senderId === String(state.me?.id);

  let readState = null;
  if (isOwn) {
    const own = await window.SigStore.get(groupKey(roomId, `own:${env.kid}`), null);
    if (own) readState = window.GroupCrypto.ownReceiverState(own);
  } else {
    const key = groupKey(roomId, `in:${senderId}`);
    let inState = await window.SigStore.get(key, null);
    if (!inState || inState.keyId !== env.kid) {
      await syncGroupInbox(roomId);
      inState = await window.SigStore.get(key, null);
    }
    if (inState && inState.keyId === env.kid) readState = inState;
  }
  if (!readState) return GROUP_KEY_UNAVAILABLE;

  try {
    const { plaintext, state: next } = await window.GroupCrypto.decryptWithState(readState, roomId, senderId, env);
    if (!isOwn) await window.SigStore.put(groupKey(roomId, `in:${senderId}`), next);
    groupPlainCache.set(msg.encrypted_content, plaintext);
    return plaintext;
  } catch (e) {
    console.error('Group E2E decrypt failed:', e.message);
    return "🔐 [Не удалось расшифровать E2E сообщение]";
  }
}

window.toggleGroupE2e = async () => {
  const roomId = state.activeRoomId;
  if (!roomId) return;
  const on = await roomE2eEnabled(roomId);
  const warning = on
    ? "Отключить сквозное шифрование? Новые сообщения пойдут без E2E."
    : "Включить сквозное шифрование группы?\n\nСообщения станут нечитаемы для участников, у которых нет веб-версии с поддержкой E2E (в т.ч. мобильного приложения). Вложения и медиа этим режимом НЕ шифруются. Сервер по-прежнему видит, кто и когда пишет.";
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
      if (!state.activeRoomId) render(); // Only render if not in chat

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

    if (data.type === "message.new" && data.message.room_id === state.activeRoomId) {
      const m = data.message;
      if (m.type === "text") m._text = await decryptMsg(state.activeRoomId, m);
      state.messages.push(m);
      renderMessages(); // Render only messages, not full UI
      // Mark as read after receiving
      setTimeout(() => markMessagesRead(), 500);
    }

    // Update room list on new message
    if (data.type === "message.new") {
      // The sender obviously stopped typing once their message arrived.
      if (data.message?.room_id && data.message?.sender_id) {
        const wasActive = String(data.message.room_id) === String(state.activeRoomId);
        clearTypingUser(String(data.message.room_id), String(data.message.sender_id), wasActive);
      }
      loadRooms();

      // Show notification if message is not in active room or tab is not focused
      if (data.message.room_id !== state.activeRoomId || !document.hasFocus()) {
        const room = state.rooms.find(r => r.id === data.message.room_id);
        if (room && data.message.sender_id !== state.me?.id) {
          // Decrypt message for notification
          const m = data.message;
          if (m.type === "text") m._text = await decryptMsg(m.room_id, m);
          notifyNewMessage(m, room);
        }
      }
    }
    
    // Handle messages read status
    if (data.type === "messages.read" && data.user_id !== state.me?.id) {
      // Update read status for messages from me
      const myId = String(state.me?.id || "");
      const readerId = String(data.user_id || "");
      state.messages.forEach(m => {
        if (String(m.sender_id || "") === myId) {
          if (!m.read_by) m.read_by = [];
          if (!m.read_by.includes(readerId)) {
            m.read_by.push(readerId);
          }
        }
      });
      // Force re-render of messages
      render();
    }
    
    // Handle message deletion
    if (data.type === "message.delete") {
      state.messages = state.messages.filter(m => m.id !== data.message_id);
      if (state.editMessage?.id === data.message_id) state.editMessage = null;
      render();
    }

    if (data.type === "message.update" && data.message) {
      const m = data.message;
      if (m.type === "text") m._text = await decryptMsg(m.room_id, m);
      state.messages = state.messages.map(item => item.id === m.id ? { ...item, ...m } : item);
      if (state.editMessage?.id === m.id) state.editMessage = null;
      render();
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

    if (data.type === "call.offer") {
      if (String(data.from_user_id || "") !== String(state.me?.id || "")) handleCall(data);
    }
    if (data.type === "call.answer" && state.pc && data.sdp) {
      try {
        await state.pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
        await flushPendingIceCandidates();
      } catch (e) { console.error("setRemoteDescription failed:", e); }
    }
    if (data.type === "call.ice" && state.pc && data.candidate) {
      try {
        if (state.pc.remoteDescription) await state.pc.addIceCandidate(new RTCIceCandidate(data.candidate));
        else state.pendingIceCandidates.push(data.candidate);
      } catch (e) { console.error("addIceCandidate failed:", e); }
    }
    if (data.type === "call.hangup") { if (typeof window.hangupCall === "function") window.hangupCall(false); }
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
  if (!data.sdp) { console.error("No SDP in call offer"); return; }
  if (state.callState !== 'idle') {
    try {
      state.ws?.send(JSON.stringify({ type: "call.hangup", room_id: data.room_id, to_user_id: data.from_user_id }));
    } catch {}
    return;
  }

  // Detect if it's a video call by checking SDP
  const isVideoCall = data.sdp.sdp && data.sdp.sdp.includes('m=video');

  state.callState = 'incoming';
  state.currentCallRoomId = data.room_id || state.activeRoomId || null;
  state.currentCallPeerId = data.from_user_id || null;
  state.pendingIceCandidates = [];
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
    $("#acceptCallBtn").onclick = () => resolve(true);
    $("#rejectCallBtn").onclick = () => resolve(false);
  });

  callDialog.remove();
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

    // Stop current track
    videoTrack.stop();

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
  const q = (state.searchQ || "").trim();
  if (!q) { state.searchResults = []; state.searching = false; mountPreservingInput(); return; }
  state.searching = true; mountPreservingInput();
  const res = await api(`/users/search?q=${encodeURIComponent(q)}`);
  state.searchResults = Array.isArray(res) ? res : [];
  state.searching = false; mountPreservingInput();
}

window.onSearchInput = v => {
  state.searchQ = String(v || "");
  if (searchTimer) clearTimeout(searchTimer);
  searchTimer = setTimeout(refreshSearch, 250);
};

window.onComposerInput = v => {
  state.composerText = String(v || "");
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

window.backToList = () => { state.activeRoomId = null; state.messages = []; state.editingRoom = false; mount(); };

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
async function initServiceWorker() {
  if (!('serviceWorker' in navigator)) {
    return;
  }

  try {
    const registration = await navigator.serviceWorker.register('/sw.js');
    state.serviceWorkerRegistration = registration;

    // Check notification permission
    state.notificationsPermission = Notification.permission;
    state.notificationsEnabled = Notification.permission === 'granted';

    // Update UI
    mount();
  } catch (err) {
    console.error('Service Worker registration failed:', err);
  }
}

window.requestNotificationPermission = async () => {
  if (!('Notification' in window)) {
    alert('Ваш браузер не поддерживает уведомления');
    return;
  }

  try {
    const permission = await Notification.requestPermission();
    state.notificationsPermission = permission;
    state.notificationsEnabled = permission === 'granted';

    if (permission === 'granted') {
      // Show test notification
      showNotification('Уведомления включены', 'Вы будете получать уведомления о новых сообщениях');
    } else if (permission === 'denied') {
      alert('Уведомления заблокированы. Разрешите их в настройках браузера.');
    }

    mount();
  } catch (err) {
    console.error('Failed to request notification permission:', err);
    alert('Ошибка запроса разрешения на уведомления');
  }
};

function showNotification(title, body, data = {}) {
  if (!state.notificationsEnabled || !state.serviceWorkerRegistration) return;

  // Don't show notification if tab is focused
  if (document.hasFocus()) return;

  state.serviceWorkerRegistration.showNotification(title, {
    body: body,
    icon: '/favicon.png',
    badge: '/favicon.png',
    tag: data.tag || 'message',
    data: data,
    requireInteraction: false,
    silent: false
  });
}

function notifyNewMessage(message, room) {
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
    alert(`Добавлено ${userIds.length} участников`);
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

window.deleteMessage = async (msgId) => {
  if (!confirm("Удалить сообщение?")) return;
  await api(`/rooms/${state.activeRoomId}/messages/${msgId}`, { method: "DELETE" });
  state.messages = state.messages.filter(m => m.id !== msgId);
  if (state.editMessage?.id === msgId) state.editMessage = null;
  render();
};

// Global error handler
window.addEventListener('error', (event) => {
  console.error('Global error:', event.error);
  alert('Произошла ошибка: ' + (event.error?.message || 'неизвестная ошибка'));
});

// Global unhandled promise rejection handler
window.addEventListener('unhandledrejection', (event) => {
  console.error('Unhandled promise rejection:', event.reason);
  alert('Произошла ошибка: ' + (event.reason?.message || 'неизвестная ошибка'));
});

// --- Main UI ---
async function mount() {
  applyTheme();
  const root = $("#app");
  
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
    root.innerHTML = `
      <div class="shell">
        <div class="card" style="margin:auto; max-width:520px;">
          <div class="topbar"><div class="brand"><span class="badge">67</span> <span>Вход</span></div></div>
          <div class="pad col">
            <input class="input" id="u" placeholder="Username" autocomplete="username">
            <input class="input" id="p" type="password" placeholder="Password" autocomplete="current-password">
            <div id="captcha-container" style="display:none;margin:10px 0;">
              <div class="h-captcha" data-sitekey="11dc04d6-78e8-44f8-9814-c5c032fc68be" data-callback="onCaptchaSuccess"></div>
            </div>
            <div class="row">
              <button class="btn primary" onclick="login()">Войти</button>
              <button class="btn" onclick="showRegister()">Регистрация</button>
            </div>
          </div>
        </div>
      </div>`;
    return;
  }

  // Prepare chat header
  let chatTitle = "Чат";
  let chatSubtitle = "";
  let chatAvatar = null;
  
  if (state.activeRoomId) {
    const room = state.rooms.find(r => r.id === state.activeRoomId);
    const members = state.roomMembers || [];
    const others = members.filter(m => m.id !== state.me.id);
    const typingMap = state.typingByRoom[state.activeRoomId] || {};
    const typingUsers = Object.values(typingMap);

    if (room) {
      if (room.kind === "dm" && others.length === 1) {
        chatTitle = displayName(others[0]);
        chatAvatar = avatarUrl(others[0].avatar_id);
      } else if (room.kind === "channel") {
        chatTitle = "📢 " + (room.title || "Канал");
        chatAvatar = avatarUrl(room.avatar_id);
      } else {
        chatTitle = room.title || "Группа";
        chatAvatar = avatarUrl(room.avatar_id);
      }
    }

    if (typingUsers.length) {
      chatSubtitle = typingUsers.length === 1 ? `${typingUsers[0]} печатает…` : "Несколько печатают…";
    } else if (room?.kind === "dm" && others.length === 1) {
      const u = others[0];
      if (u.online) chatSubtitle = "онлайн";
      else if (u.last_seen) {
        const d = new Date(u.last_seen);
        chatSubtitle = `был(а) ${d.toLocaleDateString()} ${d.toLocaleTimeString().slice(0,5)}`;
      }
    } else if (others.length > 0) {
      chatSubtitle = `${members.length} участников`;
    }
  }

  const shellClass = state.activeRoomId ? "show-chat" : "show-list";
  root.innerHTML = `
    <div class="shell ${shellClass}">
      <div class="card sidebar">
        <div class="topbar">
          <div class="brand clickable" onclick="showMyProfile()">
            ${state.me.avatar_id ? `<img class="avatar-sm" src="${avatarUrl(state.me.avatar_id)}" alt="">` : `<span class="badge">67</span>`}
            <div>
              <div>${escapeHtml(displayName(state.me))}</div>
              <div class="mini">@${state.me.username}</div>
            </div>
          </div>
          <div class="row">
            <button class="btn" onclick="toggleTheme()" title="Тема">${state.darkTheme ? '☀️' : '🌙'}</button>
            ${state.notificationsPermission === 'granted' ? `<button class="btn" title="Уведомления включены">🔔</button>` : state.notificationsPermission === 'denied' ? `<button class="btn" onclick="requestNotificationPermission()" title="Уведомления заблокированы">🔕</button>` : `<button class="btn" onclick="requestNotificationPermission()" title="Включить уведомления">🔔</button>`}
            <button class="btn" onclick="openBackupPanel()" title="Бэкап ключей">🔑</button>
            <button class="btn" onclick="openSessionsPanel()" title="Активные сессии">📱</button>
            <button class="btn" onclick="openCreateRoom('group')" title="Группа">+</button>
            <button class="btn" onclick="openCreateRoom('channel')" title="Канал">📢</button>
            <button class="btn danger" onclick="logout()">Выход</button>
          </div>
        </div>
        <div class="pad" style="border-bottom:1px solid var(--line);">
          <input class="input" id="searchInput" placeholder="Поиск..." value="${(state.searchQ||"").replaceAll('"','&quot;')}" oninput="onSearchInput(this.value)">
          ${state.searching ? `<div class="mini" style="margin-top:8px;">поиск...</div>` : ``}
          ${state.searchResults.length ? `
            <div class="list" style="padding:8px 0 0 0; gap:6px;">
              ${state.searchResults.map(u => `
                <div class="item" onclick="startDM('${escapeJsSingleQuoted(u.username)}')">
                  <div class="avatar-wrap">${u.avatar_id ? `<img class="avatar-sm" src="${avatarUrl(u.avatar_id)}">` : `<div class="avatar-placeholder"></div>`}</div>
                  <div class="itemText">
          <div class="title">${escapeHtml(displayName(u))}</div>
                    <div class="meta">@${escapeHtml(u.username)}</div>
                  </div>
                </div>
              `).join("")}
            </div>
          ` : ``}
        </div>
        <div class="list room-list" id="roomList">
          ${state.rooms.map(r => renderRoomItem(r)).join("")}
        </div>
      </div>

      <div class="card main">
        ${state.activeRoomId ? `
          <div class="chatHead">
            ${(() => {
              const room = state.rooms.find(r=>r.id===state.activeRoomId);
              const isGroup = room?.kind === 'group';
              const isChannel = room?.kind === 'channel';
              const otherMember = isGroup || isChannel ? null : (state.roomMembers || []).find(m => m.id !== state.me?.id);
              const onHeaderClick = isGroup || isChannel ? 'openRoomSettings()' : (otherMember ? `showProfile('${otherMember.id}')` : '');
              return `
            <div class="row clickable" onclick="${onHeaderClick}">
              <button class="btn back" onclick="backToList();event.stopPropagation();">←</button>
              ${chatAvatar ? `<img class="avatar-md" src="${chatAvatar}" alt="">` : `<div class="avatar-placeholder-md"></div>`}
              <div class="chatTitle">
                <div>${escapeHtml(chatTitle)}</div>
                ${chatSubtitle ? `<div class="mini">${escapeHtml(chatSubtitle)}</div>` : ``}
              </div>
            </div>`;
            })()}
            <div class="row">
              ${(() => {
                const room = state.rooms.find(r=>r.id===state.activeRoomId);
                // Only allow calls in DM, not in groups/channels
                return room?.kind === "dm" ? `
                  <button class="btn" onclick="startRoomCall(false)" title="Аудио звонок">📞</button>
                  <button class="btn" onclick="startRoomCall(true)" title="Видео звонок">📹</button>
                ` : "";
              })()}
              <button class="btn" onclick="toggleSearch()" title="Поиск по сообщениям">🔍</button>
            </div>
          </div>
          ${state.searchMode ? `
            <div class="searchBar">
              <input
                type="text"
                id="searchInput"
                class="input"
                placeholder="Поиск по сообщениям..."
                value="${escapeHtml(state.searchQuery)}"
                oninput="searchMessages(this.value)"
              />
              <div class="searchControls">
                ${state.searchResults.length > 0 ? `
                  <span class="searchCounter">${state.currentSearchIndex + 1} / ${state.searchResults.length}</span>
                  <button class="btn" onclick="prevSearchResult()" title="Предыдущий">↑</button>
                  <button class="btn" onclick="nextSearchResult()" title="Следующий">↓</button>
                ` : state.searchQuery.length >= 2 ? `
                  <span class="searchCounter">Ничего не найдено</span>
                ` : ''}
                <button class="btn" onclick="toggleSearch()" title="Закрыть">✕</button>
              </div>
            </div>
          ` : ''}
          <div id="msgs" class="messages"></div>
          ${(() => {
            const room = state.rooms.find(r=>r.id===state.activeRoomId);
            return renderComposeArea(room);
          })()}
        ` : `
          <div class="pad empty-state">
            <div class="title">Выбери чат</div>
            <div class="hint">или начни новый через поиск</div>
          </div>
        `}
      </div>
    </div>
    
    ${state.createRoomMode ? renderCreateRoomModal() : ''}
  `;

  if (state.activeRoomId) render();
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
  const isActive = r.id === state.activeRoomId;
  let title = r.title || "Чат";
  let avatar = avatarUrl(r.avatar_id);
  
  // For DM, show other user's name from other_* fields
  if (r.kind === "dm") {
    // Use the other person's info directly from the query
    title = r.other_display_name || r.other_username || "Личные сообщения";
    // Also use other user's avatar if no room avatar
    if (r.other_avatar_id && !r.avatar_id) {
      avatar = avatarUrl(r.other_avatar_id);
    }
  }
  
  let lastMsg = "";
  const lastMeta = r.last_msg_meta || {};
  const lastEncrypted = !!r.last_msg_encrypted_content || !!lastMeta.encrypted || !!lastMeta.e2e;
  if (r._lastText) {
    const sender = r.last_msg_sender_id === state.me?.id ? "Вы" : (r.last_msg_sender_name || r.last_msg_sender_username || "");
    const text = r._lastText.length > 30 ? r._lastText.slice(0,30) + "…" : r._lastText;
    lastMsg = sender ? `${sender}: ${text}` : text;
  } else if (lastEncrypted && r.last_msg_type === "text") {
    const sender = r.last_msg_sender_id === state.me?.id ? "Вы" : (r.last_msg_sender_name || r.last_msg_sender_username || "");
    const text = "🔐 Зашифрованное сообщение";
    lastMsg = sender ? `${sender}: ${text}` : text;
  } else if (r.last_msg_text) {
    const sender = r.last_msg_sender_id === state.me?.id ? "Вы" : (r.last_msg_sender_name || r.last_msg_sender_username || "");
    const text = r.last_msg_text.length > 30 ? r.last_msg_text.slice(0,30) + "…" : r.last_msg_text;
    lastMsg = sender ? `${sender}: ${text}` : text;
  } else if (r.last_msg_type === "voice") {
    lastMsg = "🎙️ Голосовое";
  } else if (r.last_msg_type === "video_note") {
    lastMsg = "⏺️ Кружок";
  } else if (r.last_msg_type === "media") {
    lastMsg = "📎 Медиа";
  }

  const kindLabel = r.kind === "channel" ? "📢 Канал" : r.kind === "dm" ? "Нет сообщений" : "Группа";

  // Unread counter
  const unreadBadge = r.unread_count > 0 ? `<span class="unread-badge">${r.unread_count > 99 ? '99+' : r.unread_count}</span>` : '';

  return `
    <div class="item ${isActive ? "active" : ""}" onclick="selectRoom('${r.id}')">
      <div class="avatar-wrap">${avatar ? `<img class="avatar-sm" src="${avatar}">` : `<div class="avatar-placeholder"></div>`}</div>
      <div class="itemText">
        <div class="title">${r.kind === "channel" ? "📢 " : ""}${title}</div>
        <div class="meta">${lastMsg || kindLabel}</div>
      </div>
      <div style="display:flex;flex-direction:column;align-items:flex-end;gap:2px;">
        ${r.last_msg_at ? `<div class="itemTime">${new Date(r.last_msg_at).toLocaleTimeString().slice(0,5)}</div>` : ""}
        ${unreadBadge}
      </div>
    </div>
  `;
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
                    <div class="session-name">${escapeHtml(currentSession.deviceName)}</div>
                    <div class="session-details">
                      <span class="session-ip">${currentSession.ipAddress || 'Unknown IP'}</span>
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
                      <div class="session-name">${escapeHtml(s.deviceName)}</div>
                      <div class="session-details">
                        <span class="session-ip">${s.ipAddress || 'Unknown IP'}</span>
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
          <div class="status">${isDM ? "Личная переписка" : `${members.length} ${room?.kind === "channel" ? "подписчиков" : "участников"}`}</div>
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
  
  // Reply preview
  const replyHtml = state.replyTo ? `
    <div class="reply-preview">
      <div class="reply-content">
        <div class="reply-name">${escapeHtml(state.replyTo.sender_display_name || state.replyTo.sender_username || 'Unknown')}</div>
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
          <button class="btn" onclick="triggerFileSelect()">📎</button>
          <input type="file" id="file" hidden onchange="uploadSelectedFile()">
          <textarea class="textarea" id="inp" placeholder="Сообщение..." oninput="onComposerInput(this.value)" onkeydown="handleMessageKeydown(event)">${escapeHtml(state.composerText || "")}</textarea>
          <button class="btn" id="btn_voice" onclick="toggleVoice()">🎙️</button>
          <button class="btn" id="btn_video_note" onclick="toggleVideoNote()">⏺️</button>
          <button class="btn primary" onclick="doSend()">➤</button>
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
          <button class="btn" onclick="triggerFileSelect()">📎</button>
          <input type="file" id="file" hidden onchange="uploadSelectedFile()">
          <textarea class="textarea" id="inp" placeholder="Сообщение канала..." oninput="onComposerInput(this.value)" onkeydown="handleMessageKeydown(event)">${escapeHtml(state.composerText || "")}</textarea>
          <button class="btn primary" onclick="doSend()">➤</button>
        </div>
      </div>
    `;
  }
  return `<div class="compose" style="justify-content:center;color:var(--muted);">📢 Канал только для чтения</div>`;
}

function renderStatus(m) {
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
    return `<span class="msg-status" title="Отправлено">✓</span>`;
  }
  
  if (readCount === 0) {
    // Not read by anyone yet - show single checkmark
    return `<span class="msg-status" title="Отправлено">✓</span>`;
  }
  
  if (readCount >= totalOthers) {
    // Read by everyone - show blue double checkmarks
    return `<span class="msg-status read" title="Прочитано всеми">✓✓</span>`;
  }
  
  // Read by some but not all - show gray double checkmarks
  return `<span class="msg-status delivered" title="Доставлено (${readCount}/${totalOthers})">✓✓</span>`;
}

function shouldStickToBottom(box) {
  if (!box) return true;
  const threshold = 80;
  return box.scrollHeight - box.scrollTop - box.clientHeight < threshold;
}

function legacyRenderMessages() {
  const box = $("#msgs");
  if (!box) return;
  const stickToBottom = shouldStickToBottom(box);
  
  // Check if current room is DM
  const currentRoom = state.rooms.find(r => r.id === state.activeRoomId);
  
  // Update only messages content, not full UI
  const messagesHtml = state.messages.map(m => {
    const senderId = String(m.sender_id || "");
    const msgId = String(m.id || "");
    const mine = senderId === String(state.me?.id || "");
    const time = new Date(m.created_at).toLocaleTimeString().slice(0,5);
    const senderName = m.sender_display_name || m.sender_username || "?";
    const senderAvatar = m.sender_avatar_id ? avatarUrl(m.sender_avatar_id) : null;
    const safeSenderName = escapeHtml(senderName);
    const safeMsgId = escapeJsSingleQuoted(msgId);
    
    let body = "";
    if (m.type === "voice" && m.media_id) {
      body = `<audio class="media-audio" src="/api/media/${m.media_id}" controls preload="metadata" playsinline></audio>`;
    } else if (m.type === "video_note" && m.media_id) {
      body = `
        <div class="video-note" onclick="toggleVideoNotePlayback(this)">
          <video src="/api/media/${m.media_id}" playsinline loop ${m.thumb_path ? `poster="/api/media/${m.media_id}/thumb"` : ""}></video>
        </div>
      `;
    } else if (m.type === "media" && m.media_id) {
      if (m.media_mime?.startsWith("image/")) {
        body = `<img class="media-image" src="/api/media/${m.media_id}" onclick="window.open(this.src)" alt="">`;
      } else if (m.media_mime?.startsWith("video/")) {
        body = `<video class="media-video" src="/api/media/${m.media_id}" controls playsinline ${m.thumb_path ? `poster="/api/media/${m.media_id}/thumb"` : ""}></video>`;
      } else {
        body = `<a class="media-file" href="/api/media/${m.media_id}" target="_blank">📎 Файл</a>`;
      }
    } else {
      body = `<div class="bText">${escapeHtml((m._text ?? m.text ?? "").toString())}</div>`;
    }
    
    // In DM: no avatars/names, just bubbles like Telegram
    // In groups: show avatars/names
    const showSenderInfo = !(currentRoom?.kind === "dm") && !mine;
    
    // Check if this message is being replied to
    const isReplyingTo = state.replyTo?.id === msgId;
    
    // Reply preview if this message is a reply
    const replyPreview = m.reply_to ? `
      <div class="reply-reference" onclick="scrollToMessage('${m.reply_to.id}', event)">
        <div class="reply-line"></div>
        <div class="reply-info">
          <div class="reply-name">${escapeHtml(m.reply_to.sender_name || 'Unknown')}</div>
          <div class="reply-text">${escapeHtml((m.reply_to.text || '').slice(0, 60))}${(m.reply_to.text || '').length > 60 ? '...' : ''}</div>
        </div>
      </div>
    ` : '';
    
    // Reactions
    const myId = String(state.me?.id || "");
    const reactions = m.reactions || {};
    const reactionsHtml = Object.keys(reactions).length > 0 ? `
      <div class="reactions">
        ${Object.entries(reactions).map(([emoji, users]) => {
          const count = users.length;
          const myReaction = users.includes(myId);
          return `<button class="reaction-tag ${myReaction ? 'my-reaction' : ''}" onclick="${myReaction ? `removeReaction('${safeMsgId}', '${escapeJsSingleQuoted(emoji)}', event)` : `addReaction('${safeMsgId}', '${escapeJsSingleQuoted(emoji)}')`}">${escapeHtml(emoji)} ${count}</button>`;
        }).join('')}
      </div>
    ` : '';
    
    // Check read status for my messages
    const isRead = mine && m.read_by && m.read_by.length > 0;
    const readStatus = isRead ? (m.read_by.length === 1 ? "✓✓" : "✓") : "";
    
    return `
      <div class="bubble ${mine ? "me" : ""} ${currentRoom?.kind === "dm" ? 'dm' : ''} ${isReplyingTo ? 'replying-to' : ''}" id="msg-${escapeHtml(msgId)}"
           ondblclick="replyToMessage('${safeMsgId}', event)"
           ontouchstart="handleTouchStart(event, '${safeMsgId}')"
           ontouchend="handleTouchEnd(event)">
        ${showSenderInfo && senderId ? `<div class="bAvatar">${senderAvatar ? `<img src="${senderAvatar}">` : `<div class="avatar-placeholder-sm"></div>`}</div>` : ""}
        <div class="bContent">
          ${showSenderInfo && senderId ? `<div class="bSender">${safeSenderName}</div>` : ""}
          ${replyPreview}
          ${body}
          <div class="bTime">${time}${mine && msgId ? ` <button class="btn-delete-msg" onclick="deleteMessage('${safeMsgId}')" title="Удалить">✕</button>` : ""} <button class="btn-reaction" onclick="showReactionPicker('${safeMsgId}', event)">😊</button>${readStatus ? ` <span class="msg-status ${isRead ? 'read' : 'delivered'}">${readStatus}</span>` : ""}</div>
          ${reactionsHtml}
        </div>
      </div>
    `;
  }).join("");
  
  box.innerHTML = messagesHtml;
  if (stickToBottom) box.scrollTop = box.scrollHeight;
}

function legacyRender() {
  const box = $("#msgs");
  if (!box) return;
  const stickToBottom = shouldStickToBottom(box);
  
  // Check if current room is DM
  const currentRoom = state.rooms.find(r => r.id === state.activeRoomId);
  const isDM = currentRoom?.kind === "dm";
  
  box.innerHTML = state.messages.map(m => {
    const senderId = String(m.sender_id || "");
    const msgId = String(m.id || "");
    const mine = senderId === String(state.me?.id || "");
    const time = new Date(m.created_at).toLocaleTimeString().slice(0,5);
    const senderName = m.sender_display_name || m.sender_username || "?";
    const senderAvatar = m.sender_avatar_id ? avatarUrl(m.sender_avatar_id) : null;
    const safeSenderName = escapeHtml(senderName);
    const safeMsgId = escapeJsSingleQuoted(msgId);
    
    let body = "";
    if (m.type === "voice" && m.media_id) {
      body = `<audio class="media-audio" src="/api/media/${m.media_id}" controls preload="metadata" playsinline></audio>`;
    } else if (m.type === "video_note" && m.media_id) {
      body = `
        <div class="video-note" onclick="toggleVideoNotePlayback(this)">
          <video src="/api/media/${m.media_id}" playsinline loop ${m.thumb_path ? `poster="/api/media/${m.media_id}/thumb"` : ""}></video>
        </div>
      `;
    } else if (m.type === "media" && m.media_id) {
      if (m.media_mime?.startsWith("image/")) {
        body = `<img class="media-image" src="/api/media/${m.media_id}" onclick="window.open(this.src)" alt="">`;
      } else if (m.media_mime?.startsWith("video/")) {
        body = `<video class="media-video" src="/api/media/${m.media_id}" controls playsinline ${m.thumb_path ? `poster="/api/media/${m.media_id}/thumb"` : ""}></video>`;
      } else {
        body = `<a class="media-file" href="/api/media/${m.media_id}" target="_blank">📎 Файл</a>`;
      }
    } else {
      body = `<div class="bText">${escapeHtml((m._text ?? m.text ?? "").toString())}</div>`;
    }
    
    // In DM: no avatars/names, just bubbles like Telegram
    // In groups: show avatars/names
    const showSenderInfo = !isDM && !mine;
    
    // Check if this message is being replied to
    const isReplyingTo = state.replyTo?.id === msgId;
    
    // Reply preview if this message is a reply
    const replyPreview = m.reply_to ? `
      <div class="reply-reference" onclick="scrollToMessage('${m.reply_to.id}', event)">
        <div class="reply-line"></div>
        <div class="reply-info">
          <div class="reply-name">${escapeHtml(m.reply_to.sender_name || 'Unknown')}</div>
          <div class="reply-text">${escapeHtml((m.reply_to.text || '').slice(0, 60))}${(m.reply_to.text || '').length > 60 ? '...' : ''}</div>
        </div>
      </div>
    ` : '';
    
    // Reactions
    const myId = String(state.me?.id || "");
    const reactions = m.reactions || {};
    const reactionsHtml = Object.keys(reactions).length > 0 ? `
      <div class="reactions">
        ${Object.entries(reactions).map(([emoji, users]) => {
          const count = users.length;
          const myReaction = users.includes(myId);
          return `<button class="reaction-tag ${myReaction ? 'my-reaction' : ''}" onclick="${myReaction ? `removeReaction('${safeMsgId}', '${escapeJsSingleQuoted(emoji)}', event)` : `addReaction('${safeMsgId}', '${escapeJsSingleQuoted(emoji)}')`}">${escapeHtml(emoji)} ${count}</button>`;
        }).join('')}
      </div>
    ` : '';
    
    // Check read status for my messages
    const isRead = mine && m.read_by && m.read_by.length > 0;
    const readStatus = isRead ? (m.read_by.length === 1 ? "✓✓" : "✓") : "";
    
    return `
      <div class="bubble ${mine ? "me" : ""} ${isDM ? 'dm' : ''} ${isReplyingTo ? 'replying-to' : ''}" id="msg-${escapeHtml(msgId)}"
           ondblclick="replyToMessage('${safeMsgId}', event)"
           ontouchstart="handleTouchStart(event, '${safeMsgId}')"
           ontouchend="handleTouchEnd(event)">
        ${showSenderInfo && senderId ? `<div class="bAvatar">${senderAvatar ? `<img src="${senderAvatar}">` : `<div class="avatar-placeholder-sm"></div>`}</div>` : ""}
        <div class="bContent">
          ${showSenderInfo && senderId ? `<div class="bSender">${safeSenderName}</div>` : ""}
          ${replyPreview}
          ${body}
          <div class="bTime">${time}${mine && msgId ? ` <button class="btn-delete-msg" onclick="deleteMessage('${safeMsgId}')" title="Удалить">✕</button>` : ""} <button class="btn-reaction" onclick="showReactionPicker('${safeMsgId}', event)">😊</button>${readStatus ? ` <span class="msg-status ${isRead ? 'read' : 'delivered'}">${readStatus}</span>` : ""}</div>
          ${reactionsHtml}
        </div>
      </div>
    `;
  }).join("");
  if (stickToBottom) box.scrollTop = box.scrollHeight;
}

function renderMessageBody(m) {
  if (m.type === "voice" && m.media_id) {
    return `<audio class="media-audio" src="/api/media/${m.media_id}" controls preload="metadata" playsinline></audio>`;
  }
  if (m.type === "video_note" && m.media_id) {
    return `
      <div class="video-note" onclick="toggleVideoNotePlayback(this)">
        <video src="/api/media/${m.media_id}" playsinline loop ${m.thumb_path ? `poster="/api/media/${m.media_id}/thumb"` : ""}></video>
      </div>
    `;
  }
  if (m.type === "media" && m.media_id) {
    if (m.media_mime?.startsWith("image/")) {
      return `<img class="media-image" src="/api/media/${m.media_id}" onclick="window.open(this.src)" alt="">`;
    }
    if (m.media_mime?.startsWith("video/")) {
      return `<video class="media-video" src="/api/media/${m.media_id}" controls playsinline ${m.thumb_path ? `poster="/api/media/${m.media_id}/thumb"` : ""}></video>`;
    }
    return `<a class="media-file" href="/api/media/${m.media_id}" target="_blank">📎 Файл</a>`;
  }
  return `<div class="bText">${renderMentionText((m._text ?? m.text ?? "").toString())}</div>`;
}

function buildRenderedMessagesHtml() {
  const currentRoom = state.rooms.find(r => r.id === state.activeRoomId);
  const isDM = currentRoom?.kind === "dm";
  let lastRenderedDay = null;

  return state.messages.map(m => {
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
          <div class="reply-name">${escapeHtml(m.reply_to.sender_name || 'Unknown')}</div>
          <div class="reply-text">${escapeHtml((m.reply_to.text || '').slice(0, 60))}${(m.reply_to.text || '').length > 60 ? '...' : ''}</div>
        </div>
      </div>
    ` : '';

    const myId = String(state.me?.id || "");
    const reactions = m.reactions || {};
    const reactionsHtml = Object.keys(reactions).length > 0 ? `
      <div class="reactions">
        ${Object.entries(reactions).map(([emoji, users]) => {
          const count = users.length;
          const myReaction = users.includes(myId);
          return `<button class="reaction-tag ${myReaction ? 'my-reaction' : ''}" onclick="${myReaction ? `removeReaction('${safeMsgId}', '${escapeJsSingleQuoted(emoji)}', event)` : `addReaction('${safeMsgId}', '${escapeJsSingleQuoted(emoji)}')`}">${escapeHtml(emoji)} ${count}</button>`;
        }).join('')}
      </div>
    ` : '';

    const editButton = mine && m.type === "text"
      ? ` <button class="btn-delete-msg" onclick="startEditMessage('${safeMsgId}', event)" title="Редактировать">✏️</button>`
      : "";
    const deleteButton = mine && msgId
      ? ` <button class="btn-delete-msg" onclick="deleteMessage('${safeMsgId}')" title="Удалить">✖</button>`
      : "";

    return `
      ${needsDaySeparator ? `<div class="day-separator" data-day-label="${escapeHtml(formatDayLabel(m.created_at))}"><span>${formatDayLabel(m.created_at)}</span></div>` : ""}
      <div class="bubble ${mine ? "me" : ""} ${isDM ? 'dm' : ''} ${isReplyingTo ? 'replying-to' : ''}" id="msg-${escapeHtml(msgId)}"
           ondblclick="replyToMessage('${safeMsgId}', event)"
           ontouchstart="handleTouchStart(event, '${safeMsgId}')"
           ontouchend="handleTouchEnd(event)">
        ${showSenderInfo && senderId ? `<div class="bAvatar">${senderAvatar ? `<img src="${senderAvatar}">` : `<div class="avatar-placeholder-sm"></div>`}</div>` : ""}
        <div class="bContent">
          ${showSenderInfo && senderId ? `<div class="bSender">${safeSenderName}</div>` : ""}
          ${replyPreview}
          ${renderMessageBody(m)}
          <div class="bTime">${formatMessageTime(m.created_at)} ${editedBadge}${editButton}${deleteButton} <button class="btn-reaction" onclick="showReactionPicker('${safeMsgId}', event)">😊</button>${renderStatus(m)}</div>
          ${reactionsHtml}
        </div>
      </div>
    `;
  }).join("");
}

function renderMessages() {
  const box = $("#msgs");
  if (!box) return;
  const stickToBottom = shouldStickToBottom(box);
  box.innerHTML = `<div id="scrollDateIndicator" class="scroll-date-indicator" aria-hidden="true"></div>${buildRenderedMessagesHtml()}`;
  if (stickToBottom) box.scrollTop = box.scrollHeight;
  bindMessagesScrollIndicator();
  syncScrollDateIndicator(box, false);
}

function render() {
  const box = $("#msgs");
  if (!box) return;
  const stickToBottom = shouldStickToBottom(box);
  box.innerHTML = `<div id="scrollDateIndicator" class="scroll-date-indicator" aria-hidden="true"></div>${buildRenderedMessagesHtml()}`;
  if (stickToBottom) box.scrollTop = box.scrollHeight;
  bindMessagesScrollIndicator();
  syncScrollDateIndicator(box, false);
}

// --- Auth ---
window.login = async () => {
  const res = await api("/auth/login", { method:"POST", body: JSON.stringify({username:$("#u").value, password:$("#p").value}) });
  if (res?.user) {
    state.me = await api("/me");
    if (!state.me?.id) { alert("Ошибка авторизации"); return; }

    // Initialize E2E encryption
    await initE2E();

    // Initialize Service Worker and notifications
    await initServiceWorker();

    connectWS();
    await loadRooms();
    await syncProfileRoute();
  } else {
    const message = res?.error === "bad_credentials"
      ? "Неверный логин или пароль"
      : "Не удалось войти. Попробуйте еще раз.";
    alert(message);
  }
};

// Captcha handling
let captchaToken = "";
window.onCaptchaSuccess = (token) => { captchaToken = token; };

window.showRegister = () => {
  const container = $("#captcha-container");
  if (container) container.style.display = "block";
  // Load hCaptcha script if not loaded
  if (!window.hcaptcha) {
    const script = document.createElement("script");
    script.src = "https://js.hcaptcha.com/1/api.js";
    script.async = true;
    script.defer = true;
    document.head.appendChild(script);
  }
  // Change button to actual register
  const btn = event.target;
  btn.textContent = "Создать аккаунт";
  btn.onclick = () => window.register();
};

window.register = async () => {
  if (!captchaToken) {
    alert("Пройдите капчу");
    return;
  }
  const res = await api("/auth/register", { 
    method:"POST", 
    body: JSON.stringify({
      username: $("#u").value, 
      password: $("#p").value,
      captcha: captchaToken
    }) 
  });
  if (res?.user) {
    state.me = await api("/me");
    if (!state.me?.id) { alert("Ошибка регистрации"); return; }

    // Initialize E2E encryption
    await initE2E();

    // Initialize Service Worker and notifications
    await initServiceWorker();

    connectWS();
    await loadRooms();
    await syncProfileRoute();
  } else alert(res?.error === "captcha_failed" ? "Капча не пройдена" : "Ошибка регистрации");
};

window.logout = async () => {
  try { await api("/auth/logout", { method: "POST", body: JSON.stringify({}) }); } catch {}
  resetClientSession();
  mount();
};

async function loadRooms() {
  state.rooms = await prepareRoomPreviews(await api("/rooms") || []);
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

  // Update page title with total unread count
  updatePageTitle();
}

function updatePageTitle() {
  const totalUnread = state.rooms.reduce((sum, r) => sum + (r.unread_count || 0), 0);

  if (totalUnread > 0) {
    document.title = `(${totalUnread > 99 ? '99+' : totalUnread}) 67`;
  } else {
    document.title = '67';
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

window.searchMessages = (query) => {
  state.searchQuery = query;

  if (!query || query.length < 2) {
    state.searchResults = [];
    state.currentSearchIndex = 0;
    clearSearchHighlights();
    mount();
    return;
  }

  const lowerQuery = query.toLowerCase();

  // Search through messages
  state.searchResults = state.messages
    .map((msg, index) => ({ msg, index }))
    .filter(({ msg }) => {
      const text = msg._text || msg.text || '';
      return text.toLowerCase().includes(lowerQuery);
    });

  state.currentSearchIndex = 0;

  // Highlight and scroll to first result
  if (state.searchResults.length > 0) {
    scrollToSearchResult(0);
  }

  mount();
};

window.nextSearchResult = () => {
  if (state.searchResults.length === 0) return;

  state.currentSearchIndex = (state.currentSearchIndex + 1) % state.searchResults.length;
  scrollToSearchResult(state.currentSearchIndex);
  mount();
};

window.prevSearchResult = () => {
  if (state.searchResults.length === 0) return;

  state.currentSearchIndex = (state.currentSearchIndex - 1 + state.searchResults.length) % state.searchResults.length;
  scrollToSearchResult(state.currentSearchIndex);
  mount();
};

function scrollToSearchResult(index) {
  if (!state.searchResults[index]) return;

  const msgId = state.searchResults[index].msg.id;
  const msgEl = document.getElementById(`msg-${msgId}`);

  if (msgEl) {
    msgEl.scrollIntoView({ behavior: 'smooth', block: 'center' });

    // Highlight current result
    clearSearchHighlights();
    msgEl.classList.add('search-highlight-current');

    // Highlight all other results
    state.searchResults.forEach((result, i) => {
      if (i !== index) {
        const el = document.getElementById(`msg-${result.msg.id}`);
        if (el) el.classList.add('search-highlight');
      }
    });
  }
}

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

window.selectRoom = async id => {
  state.activeRoomId = id;
  state.editingRoom = false;
  state.editMessage = null;
  state.activeDmPeer = null;

  // Reset search when changing rooms
  state.searchMode = false;
  state.searchQuery = '';
  state.searchResults = [];
  state.currentSearchIndex = 0;
  clearSearchHighlights();

  if (state.wsAuthed) joinActiveRoom();

  // Load members first
  state.roomMembers = await api(`/rooms/${id}/members`) || [];
  groupE2eRooms.delete(id);
  await roomE2eEnabled(id);
  const room = state.rooms.find(r => r.id === id);
  if (room?.kind === "dm" && room.other_user_id) {
    state.activeDmPeer = await api(`/users/${room.other_user_id}`);

    // Initialize E2E session for DM chat
    if (state.e2eReady) {
      await getE2ESessionForUser(room.other_user_id);
    }
  }

  const msgs = await api(`/rooms/${id}/messages`) || [];
  for (let m of msgs) {
    if (m.type === "text") m._text = await decryptMsg(id, m);
  }
  state.messages = msgs;
  
  // Mark messages as read when opening room
  await markMessagesRead();
  
  // Re-render everything after marking as read
  mountPreservingInput();
};

async function markMessagesRead() {
  if (!state.activeRoomId) return;
  try {
    await api(`/rooms/${state.activeRoomId}/read`, { 
      method: "POST",
      body: JSON.stringify({}) // Send empty JSON object instead of no body
    });
    // Reload rooms to update unread counters
    await loadRooms();
  } catch (e) {
    console.error("Failed to mark messages as read:", e);
  }
}

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
window.doSend = async () => {
  try {
    const input = $("#inp");
    if (!input) return;
    const text = input.value.trim();
    if (!text) return;
    if (text.length > 5000) { alert("Сообщение слишком длинное (макс 5000 символов)"); return; }
    if (!state.ws || state.ws.readyState !== 1) {
      alert("Нет подключения к серверу");
      return;
    }
    const currentRoom = state.rooms.find(r => r.id === state.activeRoomId);
    let message = await buildTextMessage(state.activeRoomId, currentRoom, text);

    if (state.editMessage) {
      const updated = await api(`/rooms/${state.activeRoomId}/messages/${state.editMessage.id}`, {
        method: "PUT",
        body: JSON.stringify({ text: message.text, meta: message.meta })
      });
      if (!updated?.id) {
        alert("Не удалось отредактировать сообщение");
        return;
      }
      if (updated.type === "text") updated._text = await decryptMsg(state.activeRoomId, updated);
      state.messages = state.messages.map(m => m.id === updated.id ? updated : m);
      input.value = "";
      state.composerText = "";
      state.editMessage = null;
      await mount();
      setTimeout(() => {
        const nextInput = $("#inp");
        if (nextInput) nextInput.focus();
      }, 50);
      return;
    }
    // Add reply info if present
    if (state.replyTo) {
      message.reply_to = {
        id: state.replyTo.id,
        text: state.replyTo.text?.slice(0, 100),
        sender_name: state.replyTo.sender_display_name || state.replyTo.sender_username
      };
    }
    state.ws.send(JSON.stringify({ type: "message.send", room_id: state.activeRoomId, message }));
    input.value = "";
    state.composerText = "";
    state.replyTo = null;
    state.editMessage = null;
    
    // Preserve focus and cursor during render
    const activeElement = document.activeElement;
    const wasInputFocused = activeElement === input;
    const cursorPosition = input.selectionStart;
    const cursorEnd = input.selectionEnd;
    
    render(); // Use render() instead of mount() to preserve focus
    
    // Restore focus and cursor if it was on input
    if (wasInputFocused) {
      setTimeout(() => {
        input.focus();
        input.setSelectionRange(cursorPosition, cursorEnd);
      }, 0);
    }
  } catch (error) {
    console.error("Error sending message:", error);
    alert("Ошибка отправки сообщения: " + error.message);
  }
};

window.setE2EE = async () => {
  const k = prompt("Ключ шифрования:");
  if (k) await storeWrappedRoomPassphrase(state.activeRoomId, k);
  selectRoom(state.activeRoomId);
};

// --- Voice ---
const isVoiceSupported = typeof MediaRecorder !== "undefined" && typeof MediaRecorder.isTypeSupported === "function";

window.toggleVoice = async () => {
  if (!isVoiceSupported) { alert("Голосовые сообщения не поддерживаются в этом браузере."); return; }
  if (state.recorder) {
    try { state.recorder.stop(); } catch {}
    state.recorder = null;
    $("#btn_voice").textContent = "🎙️";
  } else {
    let stream;
    try { stream = await navigator.mediaDevices.getUserMedia({ audio: true }); }
    catch (e) { alert("Нет доступа к микрофону."); return; }
    
    const mimeType = MediaRecorder.isTypeSupported("audio/mp4") ? "audio/mp4"
      : MediaRecorder.isTypeSupported("audio/webm;codecs=opus") ? "audio/webm;codecs=opus"
      : MediaRecorder.isTypeSupported("audio/ogg;codecs=opus") ? "audio/ogg;codecs=opus" : "";
    
    const recorder = new MediaRecorder(stream, mimeType ? { mimeType } : undefined);
    const chunks = [];
    recorder.ondataavailable = e => chunks.push(e.data);
    recorder.onstop = async () => {
      stream.getTracks().forEach(t => t.stop());
      
      // Show upload progress for voice
      const ext = recorder.mimeType?.includes("mp4") ? "m4a" : recorder.mimeType?.includes("ogg") ? "ogg" : "webm";
      showUploadProgress("voice." + ext);
      
      const fd = new FormData();
      fd.append("file", new Blob(chunks, { type: recorder.mimeType || "application/octet-stream" }), `voice.${ext}`);
      
      try {
        // Use XMLHttpRequest for upload progress tracking
        const xhr = new XMLHttpRequest();
        xhr.open("POST", "/api/media/upload?kind=voice", true);
        xhr.withCredentials = true;
        
        // Track upload progress
        xhr.upload.addEventListener("progress", (e) => {
          if (e.lengthComputable) {
            const percentComplete = Math.round((e.loaded / e.total) * 100);
            const progressEl = $("#uploadProgress");
            if (progressEl) {
              const bar = progressEl.querySelector('.upload-progress-bar');
              const text = progressEl.querySelector('.upload-progress-text');
              if (bar) bar.style.width = percentComplete + '%';
              if (text) text.textContent = `${percentComplete}%`;
            }
          }
        });
        
        xhr.onload = async () => {
          if (xhr.status === 200) {
            try {
              const data = JSON.parse(xhr.responseText);
              if (data?.id) {
                state.ws?.send(JSON.stringify({ type: "message.send", room_id: state.activeRoomId, message: { type: "voice", media_id: data.id, meta: {} } }));
              } else {
                alert("Ошибка отправки");
              }
              hideUploadProgress();
            } catch (e) {
              console.error("Voice upload error:", e);
              alert("Ошибка отправки: " + e.message);
              hideUploadProgress();
            }
          } else {
            alert("Ошибка отправки: " + xhr.responseText);
            hideUploadProgress();
          }
        };
        
        xhr.onerror = () => {
          alert("Ошибка сети при отправке");
          hideUploadProgress();
        };
        
        xhr.send(fd);
      } catch (e) {
        console.error("Voice upload error:", e);
        alert("Ошибка отправки: " + e.message);
        hideUploadProgress();
      }
    };
    state.recorder = recorder;
    recorder.start();
    $("#btn_voice").textContent = "⏹️";
  }
};

// --- Video Note (Kruzhki) ---
window.toggleVideoNotePlayback = (container) => {
  const video = container.querySelector("video");
  if (video.paused) {
    video.play();
  } else {
    video.pause();
  }
};

window.toggleVideoNote = async () => {
  
  if (state.recordingVideoNote) {
    // Stop recording
    if (state.recorder) {
      state.recorder.stop();
    }
    state.recorder = null;
    state.recordingVideoNote = false;
    $("#btn_video_note").textContent = "⏺️";
    const preview = $("#videoNotePreview");
    if (preview) preview.remove();
    // Clear timer reference
    window.currentVideoNoteTimer = null;
  } else {
    // Start recording
    let stream;
    try { stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: { facingMode: "user", width: 480, height: 480 } }); }
    catch (e) { alert("Нет доступа к камере/микрофону."); return; }
    
    // Show preview
    const preview = document.createElement("div");
    preview.id = "videoNotePreview";
    preview.innerHTML = '<video autoplay playsinline muted></video><div class="vn-timer">0:00</div><button class="vn-switch-camera" onclick="switchVideoNoteCamera()">🔄</button>';
    document.body.appendChild(preview);
    const video = preview.querySelector("video");
    video.srcObject = stream;
    
    let seconds = 0;
    const timer = setInterval(() => {
      seconds++;
      if (seconds >= 60) { window.toggleVideoNote(); return; }
      preview.querySelector(".vn-timer").textContent = `0:${String(seconds).padStart(2,"0")}`;
    }, 1000);
    
    // Store timer reference globally for camera switch access
    window.currentVideoNoteTimer = timer;
    
    const mimeType = MediaRecorder.isTypeSupported("video/mp4") ? "video/mp4"
      : MediaRecorder.isTypeSupported("video/webm;codecs=vp9") ? "video/webm;codecs=vp9"
      : MediaRecorder.isTypeSupported("video/webm") ? "video/webm" : "";
    
    
    // Initialize global chunks array for video note recording
    state.videoNoteChunks = [];
    
    const recorder = new MediaRecorder(stream, mimeType ? { mimeType } : undefined);
    recorder.ondataavailable = e => {
      if (e.data && e.data.size > 0) {
        state.videoNoteChunks.push(e.data);
      }
    };
    recorder.onstop = async () => {
      clearInterval(timer);
      stream.getTracks().forEach(t => t.stop());
      const previewEl = $("#videoNotePreview");
      if (previewEl) previewEl.remove();
      
      // Show upload progress for video note
      showUploadProgress("video_note." + (recorder.mimeType?.includes("mp4") ? "mp4" : "webm"));
      
      // Use global chunks array
      const fd = new FormData();
      const ext = recorder.mimeType?.includes("mp4") ? "mp4" : "webm";
      fd.append("file", new Blob(state.videoNoteChunks, { type: recorder.mimeType || "video/webm" }), `video_note.${ext}`);
      
      try {
        const res = checkUnauthorized(await fetch("/api/media/upload?kind=video_note", { method: "POST", body: fd, credentials: "include" }));
        const data = await res.json().catch(() => null);
        if (data?.id) {
          state.ws?.send(JSON.stringify({ type: "message.send", room_id: state.activeRoomId, message: { type: "video_note", media_id: data.id, meta: {} } }));
        } else {
          console.error("Failed to upload video note");
          alert("Ошибка отправки");
        }
      } catch (e) {
        console.error("Upload error:", e);
        alert("Ошибка отправки: " + e.message);
      } finally {
        hideUploadProgress();
      }
      
      // Clear chunks after upload
      state.videoNoteChunks = [];
      // Clear timer reference
      window.currentVideoNoteTimer = null;
    };
    state.recorder = recorder;
    state.recordingVideoNote = true;
    state.currentStream = stream;
    recorder.start();
    $("#btn_video_note").textContent = "⏹️";
  }
};

window.triggerFileSelect = () => {
  const input = $("#file");
  if (input) {
    input.value = ""; // Clear previous selection
    input.click();
  }
};

// --- Upload progress indicator ---
window.showUploadProgress = (filename) => {
  const progress = document.createElement("div");
  progress.id = "uploadProgress";
  progress.style.cssText = `
    position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.5);z-index:10000;
    display:flex;align-items:center;justify-content:center;flex-direction:column;gap:15px;
  `;
  progress.innerHTML = `
    <div style="background:var(--bg);border-radius:12px;padding:20px;max-width:300px;width:90%;text-align:center;">
      <div style="font-size:16px;margin-bottom:10px;">Загрузка файла...</div>
      <div style="font-size:14px;opacity:0.7;word-break:break-all;">${filename}</div>
      <div style="margin-top:15px;">
        <div style="width:100%;height:4px;background:var(--line);border-radius:2px;overflow:hidden;">
          <div id="uploadProgressBar" style="width:0%;height:100%;background:var(--accent);transition:width 0.3s;"></div>
        </div>
      </div>
    </div>
  `;
  document.body.appendChild(progress);
};

window.hideUploadProgress = () => {
  const progress = $("#uploadProgress");
  if (progress) progress.remove();
};

window.updateUploadProgress = (percent) => {
  const bar = $("#uploadProgressBar");
  if (bar) bar.style.width = percent + "%";
};

// --- File upload ---
window.uploadSelectedFile = async () => {
  const input = $("#file");
  const f = input?.files?.[0];
  if (!f) { alert("Файл не выбран"); return; }
  const MAX_SIZE = 1024 * 1024 * 1024; // 1GB
  if (f.size > MAX_SIZE) { alert("Файл слишком большой (макс 1GB)"); input.value = ""; return; }
  const isImage = f.type.startsWith("image/");
  const isVideo = f.type.startsWith("video/");
  const kind = isImage ? "image" : isVideo ? "video" : "file";

  showUploadProgress(f.name);

  const currentRoom = state.rooms.find(r => r.id === state.activeRoomId);
  let encryptedFile = null;
  let mediaKeyData = null;

  // Encrypt file for DM chats
  if (currentRoom?.kind === "dm" && currentRoom.other_user_id && state.e2eReady) {
    try {
      const mediaEncryption = new window.MediaEncryption();

      // Encrypt file
      encryptedFile = await mediaEncryption.encryptFile(f);

      // Encrypt media key with E2E
      const encryptedKey = await mediaEncryption.encryptMediaKey(
        state.e2eManager,
        currentRoom.other_user_id,
        encryptedFile.key,
        encryptedFile.iv
      );

      mediaKeyData = {
        encrypted_key: JSON.stringify(encryptedKey),
        original_name: encryptedFile.originalName,
        original_type: encryptedFile.originalType,
        original_size: encryptedFile.originalSize
      };

      // Create blob from encrypted data
      const encryptedBlob = new Blob([encryptedFile.encryptedData], { type: 'application/octet-stream' });
      f = new File([encryptedBlob], 'encrypted_' + encryptedFile.originalName, { type: 'application/octet-stream' });

    } catch (e) {
      console.error('Media encryption failed:', e);
      alert("Не удалось зашифровать файл: " + e.message);
      hideUploadProgress();
      return;
    }
  }

  const fd = new FormData();
  fd.append("file", f, f.name || "file");

  // Add encryption metadata if encrypted
  if (mediaKeyData) {
    fd.append("encrypted", "true");
    fd.append("media_key_data", JSON.stringify(mediaKeyData));
  }

  try {
    // Use XMLHttpRequest for upload progress tracking
    const xhr = new XMLHttpRequest();
    xhr.open("POST", `/api/media/upload?kind=${kind}`, true);
    xhr.withCredentials = true;

    // Track upload progress
    xhr.upload.addEventListener("progress", (e) => {
      if (e.lengthComputable) {
        const percentComplete = Math.round((e.loaded / e.total) * 100);
        const progressEl = $("#uploadProgress");
        if (progressEl) {
          const bar = progressEl.querySelector('.upload-progress-bar');
          const text = progressEl.querySelector('.upload-progress-text');
          if (bar) bar.style.width = percentComplete + '%';
          if (text) text.textContent = `${percentComplete}%`;
        }
      }
    });

    xhr.onload = async () => {
      if (xhr.status === 200) {
        try {
          const data = JSON.parse(xhr.responseText);
          input.value = "";
          if (!data?.id) {
            alert("Ошибка загрузки: нет ID файла");
            hideUploadProgress();
            return;
          }

          const messageMeta = { kind };
          if (mediaKeyData) {
            messageMeta.encrypted_media = true;
          }

          state.ws?.send(JSON.stringify({
            type: "message.send",
            room_id: state.activeRoomId,
            message: {
              type: "media",
              media_id: data.id,
              meta: messageMeta
            }
          }));
          hideUploadProgress();
        } catch (e) {
          alert("Ошибка ответа сервера: " + e.message);
          hideUploadProgress();
        }
      } else {
        const err = xhr.responseText || "unknown error";
        alert("Ошибка загрузки: " + err);
        input.value = "";
        hideUploadProgress();
      }
    };

    xhr.onerror = () => {
      alert("Ошибка сети при загрузке");
      input.value = "";
      hideUploadProgress();
    };

    xhr.send(fd);
  } catch (e) {
    alert("Ошибка сети при загрузке: " + e.message);
    input.value = "";
    hideUploadProgress();
  }
};

// --- Calls ---
let currentCamera = 'user'; // 'user' for front, 'environment' for back
let currentVideoNoteCamera = 'user'; // For video notes recording
let videoNoteRecorder = null; // Single recorder for entire session
let videoNoteStream = null; // Current stream

window.startRoomCall = async () => {
  if (!state.wsAuthed) { alert("Нет подключения"); return; }

  if (state.callState !== 'idle') return;

  state.callState = 'calling';
  state.currentCallRoomId = state.activeRoomId || null;
  state.currentCallPeerId = null;
  state.pendingIceCandidates = [];
  showAudioCallUI();

  let stream;
  try { 
    // Audio only call
    stream = await navigator.mediaDevices.getUserMedia(getAudioConstraints()); 
  }
  catch (e) { hangupCall(false); alert("Нет доступа к микрофону."); return; }

  state.localStream = stream;
  await loadRtcIceServers();
  state.pc = new RTCPeerConnection({ 
    iceServers: getRtcIceServers(),
    iceTransportPolicy: "all",
    iceCandidatePoolSize: 4
  });
  showAudioCallUI();
  updateCallStatus("Р’С‹Р·РѕРІ...", false);
  updateCallStatus("Вызов...", false);
  wirePeerConnectionHandlers({ roomId: state.activeRoomId, peerUserId: null });

  stream.getTracks().forEach(t => state.pc.addTrack(t, stream));

  try {
    const offer = await state.pc.createOffer();
    await state.pc.setLocalDescription(offer);
    state.ws?.send(JSON.stringify({ type: "call.offer", toRoom: state.activeRoomId, room_id: state.activeRoomId, sdp: { type: offer.type, sdp: offer.sdp } }));
  } catch (e) { console.error("Failed to start call:", e); alert("Ошибка звонка"); hangupCall(); }
};

// Switch camera for video notes (mobile) - BACK TO WORKING APPROACH
window.switchVideoNoteCamera = async () => {
  
  if (!state.recordingVideoNote) {
    return;
  }
  
  // Switch camera
  currentVideoNoteCamera = currentVideoNoteCamera === 'user' ? 'environment' : 'user';
  
  try {
    // Get new stream FIRST to minimize delay
    const newStream = await navigator.mediaDevices.getUserMedia({
      audio: true,
      video: { facingMode: currentVideoNoteCamera, width: 480, height: 480 }
    });
    
    // Update preview video IMMEDIATELY
    const preview = $("#videoNotePreview");
    if (preview) {
      const video = preview.querySelector("video");
      if (video) {
        video.srcObject = newStream;
      }
    }
    
    // Update recorder if exists
    if (state.recorder) {
      
      // Create new recorder with new stream FIRST
      const mimeType = MediaRecorder.isTypeSupported("video/mp4") ? "video/mp4"
        : MediaRecorder.isTypeSupported("video/webm;codecs=vp9") ? "video/webm;codecs=vp9"
        : MediaRecorder.isTypeSupported("video/webm") ? "video/webm" : "";
      
      const newRecorder = new MediaRecorder(newStream, mimeType ? { mimeType } : undefined);
      
      // Continue using the global chunks array
      newRecorder.ondataavailable = e => {
        if (e.data && e.data.size > 0) {
          state.videoNoteChunks.push(e.data);
        }
      };
      
      // Store the original timer reference
      const timerRef = window.currentVideoNoteTimer;
      
      newRecorder.onstop = async () => {
        // This will be called when user actually stops recording
        if (timerRef) clearInterval(timerRef);
        newStream.getTracks().forEach(t => t.stop());
        const previewEl = $("#videoNotePreview");
        if (previewEl) previewEl.remove();
        
        // Show upload progress for video note
        showUploadProgress("video_note." + (newRecorder.mimeType?.includes("mp4") ? "mp4" : "webm"));
        
        // Use global chunks array
        const fd = new FormData();
        const ext = newRecorder.mimeType?.includes("mp4") ? "mp4" : "webm";
        fd.append("file", new Blob(state.videoNoteChunks, { type: newRecorder.mimeType || "video/webm" }), `video_note.${ext}`);
        
        try {
          const res = checkUnauthorized(await fetch("/api/media/upload?kind=video_note", { method: "POST", body: fd, credentials: "include" }));
          const data = await res.json().catch(() => null);
          if (data?.id) {
            state.ws?.send(JSON.stringify({ type: "message.send", room_id: state.activeRoomId, message: { type: "video_note", media_id: data.id, meta: {} } }));
          } else {
            console.error("Failed to upload video note");
            alert("Ошибка отправки");
          }
        } catch (e) {
          console.error("Upload error:", e);
          alert("Ошибка отправки: " + e.message);
        } finally {
          hideUploadProgress();
        }
        
        // Clear chunks after upload
        state.videoNoteChunks = [];
      };
      
      // MINIMIZE DELAY: Start new recorder BEFORE stopping old one
      newRecorder.start();
      
      // Wait for new recorder to actually start recording
      await new Promise(resolve => {
        const checkStarted = () => {
          if (newRecorder.state === 'recording') {
            // Wait a bit more to ensure it's actually capturing data
            setTimeout(resolve, 300);
          } else {
            setTimeout(checkStarted, 10);
          }
        };
        checkStarted();
      });
      
      // Now stop old recorder with minimal delay
      const oldRecorder = state.recorder;
      state.recorder = newRecorder;
      
      if (oldRecorder.state === 'recording') {
        // CRITICAL: Replace onstop with empty function to prevent upload
        oldRecorder.onstop = () => {
          // intentionally ignored
        };
        oldRecorder.stop();
      }
      
      // Stop old stream tracks AFTER new recorder is running
      if (state.currentStream) {
        state.currentStream.getTracks().forEach(track => {
          track.stop();
        });
      }
      state.currentStream = newStream;
      
    }
    
  } catch (e) {
    console.error("Failed to switch camera:", e);
    // Fallback to front camera
    currentVideoNoteCamera = 'user';
  }
};

// Switch camera for mobile devices
window.switchCamera = async () => {
  if (!state.localStream) return;
  
  // Stop current video track
  const videoTrack = state.localStream.getVideoTracks()[0];
  if (videoTrack) {
    videoTrack.stop();
  }
  
  // Switch camera
  currentCamera = currentCamera === 'user' ? 'environment' : 'user';
  
  try {
    const newStream = await navigator.mediaDevices.getUserMedia({
      audio: true,
      video: { facingMode: currentCamera }
    });
    
    // Replace track in peer connection
    if (state.pc && videoTrack) {
      const newVideoTrack = newStream.getVideoTracks()[0];
      await state.pc.replaceTrack(videoTrack, newVideoTrack, state.localStream);
    }
    
    // Update local stream
    state.localStream = newStream;
    
    // Update local video if exists
    const localVideo = $("#localVideo");
    if (localVideo) {
      localVideo.srcObject = newStream;
    }
    
  } catch (e) {
    console.error("Failed to switch camera:", e);
    // Fallback to audio only
    currentCamera = 'user';
  }
};

window.addEventListener("popstate", () => {
  syncProfileRoute().catch(err => console.error("Route sync error:", err));
});

// --- Init ---
(async () => {
  try {
    state.me = await api("/me");
    if (state.me?.id) await initE2E(); // restores keys from IndexedDB, starts pre-key monitor
    await loadRooms();
    if (state.me?.id) connectWS();
    mount();
    await syncProfileRoute();
  } catch (e) {
    console.error("Init error:", e);
    mount();
  }
})();
