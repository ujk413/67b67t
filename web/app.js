const $ = (q, el=document) => el.querySelector(q);
const $$ = (q, el=document) => Array.from(el.querySelectorAll(q));
const API = "/api";

let state = {
  me: null,
  rooms: [],
  activeRoomId: null,
  messages: [],
  ws: null,
  wsAuthed: false,
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
  replyTo: null,
  createRoomMode: null, // 'group' | 'channel' | null
  createRoomQuery: "",
  createRoomResults: [],
  createRoomSelected: [],
  createRoomTitle: "",
  callState: 'idle', // 'idle', 'calling', 'connected', 'incoming'
  callTimer: null,
  callStartTime: null
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
  state.replyTo = msg;
  mount();
  // Focus input
  setTimeout(() => {
    const inp = $("#inp");
    if (inp) inp.focus();
  }, 100);
};

window.cancelReply = () => {
  state.replyTo = null;
  mount();
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
      <div class="search-item" onclick='toggleSelectUser(${JSON.stringify(u).replaceAll("'","&#39;")})'>
        ${u.avatar_id ? `<img class="avatar-sm" src="${avatarUrl(u.avatar_id)}">` : `<div class="avatar-placeholder-sm"></div>`}
        <div class="search-item-text">
          <div>${displayName(u)}</div>
          <div class="meta">@${u.username}</div>
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
        <div class="selected-chip" onclick='toggleSelectUser(${JSON.stringify(u).replaceAll("'","&#39;")})'>
          ${displayName(u)}
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
  if (!r.ok) return null;
  return body;
}

function avatarUrl(id) {
  return id ? `/api/media/${id}` : null;
}

function displayName(user) {
  return user?.display_name || user?.username || "User";
}

const keyStorageId = (id) => `k_${id}`;
let searchTimer = null;
let typingTimer = null;
let typingActive = false;

// --- E2EE Logic ---
async function getCryptoKey(roomId) {
  const pass = localStorage.getItem(keyStorageId(roomId));
  if (!pass) return null;
  const enc = new TextEncoder();
  const baseKey = await crypto.subtle.importKey("raw", enc.encode(pass), "PBKDF2", false, ["deriveKey"]);
  return crypto.subtle.deriveKey(
    { name: "PBKDF2", salt: enc.encode(roomId), iterations: 100000, hash: "SHA-256" },
    baseKey, { name: "AES-GCM", length: 256 }, false, ["encrypt", "decrypt"]
  );
}

async function decryptMsg(roomId, msg) {
  if (!msg || msg.text == null) return "";
  if (!msg.meta?.encrypted) return String(msg.text);
  try {
    const key = await getCryptoKey(roomId);
    if (!key) return "🔐 [Зашифровано]";
    const iv = Uint8Array.from(atob(msg.meta.iv), c => c.charCodeAt(0));
    const data = Uint8Array.from(atob(msg.text), c => c.charCodeAt(0));
    const dec = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, data);
    return new TextDecoder().decode(dec);
  } catch (e) { return "❌ Ошибка ключа"; }
}

// --- WebSocket & WebRTC ---
function connectWS() {
  const prot = location.protocol === "https:" ? "wss:" : "ws:";
  state.ws = new WebSocket(`${prot}//${location.host}/ws`);
  state.ws.onopen = () => { state.ws.send(JSON.stringify({ type: "auth" })); };
  state.ws.onmessage = async (e) => {
    let data;
    try { data = JSON.parse(e.data); } catch { return; }

    if (data.type === "auth.ok") {
      state.wsAuthed = true;
      if (state.activeRoomId) joinActiveRoom();
      if (!state.activeRoomId) render(); // Only render if not in chat
      return;
    }
    if (data.type === "auth.required") { state.wsAuthed = false; mount(); return; }

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
      loadRooms();
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

    if (data.type === "call.offer") handleCall(data);
    if (data.type === "call.answer" && state.pc && data.sdp) {
      try { 
        await state.pc.setRemoteDescription(new RTCSessionDescription(data.sdp)); 
        state.callState = 'connected';
        const statusEl = $("#callStatus");
        const timerEl = $("#callTimer");
        if (statusEl) statusEl.textContent = 'На связи';
        if (timerEl) timerEl.style.display = 'block';
        // Play ringtone stop
        const audio = $("#remoteAudio");
        if (audio && audio.srcObject) {
          audio.play().catch(e => console.log("Play failed:", e));
        }
      } catch (e) { console.error("setRemoteDescription failed:", e); }
    }
    if (data.type === "call.ice" && state.pc && data.candidate) {
      try { await state.pc.addIceCandidate(new RTCIceCandidate(data.candidate)); } catch (e) { console.error("addIceCandidate failed:", e); }
    }
    if (data.type === "call.hangup") { if (typeof window.hangupCall === "function") window.hangupCall(); }
    
    if (data.type === "typing") {
      const roomId = String(data.room_id || "");
      if (!roomId) return;
      const current = state.typingByRoom[roomId] || {};
      const uId = String(data.user_id || "");
      const next = { ...current };
      if (data.is_typing) next[uId] = data.username || "user";
      else delete next[uId];
      state.typingByRoom = { ...state.typingByRoom, [roomId]: next };
      if (roomId === state.activeRoomId) render(); // Use render() instead of mount() to preserve focus
    }
    
    // Update room list in real-time
    if (data.type === "rooms.update" && Array.isArray(data.rooms)) {
      state.rooms = data.rooms;
      updateRoomsList();
    }
  };
  state.ws.onclose = () => { state.wsAuthed = false; setTimeout(connectWS, 3000); };
}

async function handleCall(data) {
  console.log("Incoming call:", data);
  const fromName = data.from_username || data.from_user_id || "unknown";
  if (!data.sdp) { console.error("No SDP in call offer"); return; }
  
  state.callState = 'incoming';
  
  // Show incoming call UI with avatar
  const caller = state.roomMembers?.find(m => m.id === data.from_user_id);
  const avatarHtml = caller?.avatar_id ? '<img src="' + avatarUrl(caller.avatar_id) + '" style="width:120px;height:120px;border-radius:50%;object-fit:cover;">' : '<div style="width:120px;height:120px;border-radius:50%;background:var(--accent);display:flex;align-items:center;justify-content:center;font-size:60px;">👤</div>';
  
  const callDialog = document.createElement("div");
  callDialog.id = "incomingCallDialog";
  callDialog.innerHTML = 
    '<div style="position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.8);z-index:10000;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:20px;">' +
      '<div style="text-align:center;color:white;">' +
        '<div style="margin-bottom:20px;">' + avatarHtml + '</div>' +
        '<div style="font-size:24px;font-weight:bold;">' + (displayName(caller) || fromName) + '</div>' +
        '<div style="font-size:16px;opacity:0.8;">Входящий звонок...</div>' +
      '</div>' +
      '<div style="display:flex;gap:20px;">' +
        '<button id="acceptCallBtn" style="width:80px;height:80px;border-radius:50%;background:#42d392;border:none;font-size:40px;cursor:pointer;">📞</button>' +
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
    return;
  }

  let stream;
  try { 
    // Audio only
    stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: false }); 
  }
  catch (e) { alert("Нет доступа к микрофону."); state.callState = 'idle'; return; }

  state.localStream = stream;
  state.pc = new RTCPeerConnection({ 
    iceServers: [
      { urls: "stun:stun.l.google.com:19302" },
      { 
        urls: ["turn:67b67t.ru:3478", "turns:67b67t.ru:5349"],
        username: "messenger",
        credential: "messenger67pass"
      }
    ],
    iceTransportPolicy: "all"
  });
  showAudioCallUI();

  state.pc.onicecandidate = ev => {
    if (ev.candidate) state.ws?.send(JSON.stringify({ type: "call.ice", to_user_id: data.from_user_id, room_id: data.room_id, candidate: ev.candidate }));
  };
  state.pc.ontrack = e => {
    const audio = $("#remoteAudio");
    if (audio) { 
      audio.srcObject = e.streams[0]; 
      audio.muted = false;
      audio.volume = 1.0;
      // Force play with user interaction workaround
      const playAudio = () => {
        audio.play().then(() => {
          console.log("Remote audio playing successfully");
          // Update status when audio starts playing
          const statusEl = $("#callStatus");
          if (statusEl) statusEl.textContent = 'На связи';
          const timerEl = $("#callTimer");
          if (timerEl) timerEl.style.display = 'block';
        }).catch(e => {
          console.log("Audio play failed:", e);
          // Try muted first, then unmute
          audio.muted = true;
          audio.play().then(() => {
            setTimeout(() => {
              audio.muted = false;
              console.log("Audio unmuted successfully");
            }, 100);
          }).catch(() => {
            console.log("Even muted play failed");
          });
        });
      };
      
      // Try immediately
      playAudio();
      
      // Also try on any user interaction
      const tryPlayOnInteraction = () => {
        document.addEventListener('click', playAudio, { once: true });
        document.addEventListener('touchstart', playAudio, { once: true });
      };
      tryPlayOnInteraction();
    }
  };

  stream.getTracks().forEach(t => state.pc.addTrack(t, stream));

  try {
    await state.pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
    const answer = await state.pc.createAnswer();
    await state.pc.setLocalDescription(answer);
    state.ws?.send(JSON.stringify({ type: "call.answer", to_user_id: data.from_user_id, room_id: data.room_id, sdp: { type: answer.type, sdp: answer.sdp } }));
    state.callState = 'connected';
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
        '<div class="callStatus" id="callStatus">Вызов...</div>' +
        '<div class="callTimer" id="callTimer" style="display:none;">00:00</div>' +
        '<audio id="remoteAudio" autoplay></audio>' +
      '</div>' +
      '<div class="callControls">' +
        '<button class="btn danger" onclick="hangupCall()">Завершить</button>' +
      '</div>';
    document.body.appendChild(hud);
  }
  
  // Start call timer
  if (!state.callTimer) {
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
}

// Check if mobile device
function isMobile() {
  return /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
}

window.hangupCall = () => {
  // Clear timer
  if (state.callTimer) {
    clearInterval(state.callTimer);
    state.callTimer = null;
  }
  state.callStartTime = null;
  state.callState = 'idle';
  
  // Close peer connection
  try { state.pc?.close(); } catch(e) { console.log("PC close error:", e); }
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
    if (state.ws?.readyState === 1) {
      state.ws.send(JSON.stringify({ type: "call.hangup", room_id: state.activeRoomId })); 
    }
  } catch {}
  
  // Only re-render call UI, not full page
  const callContainer = $("#callContainer");
  if (callContainer) callContainer.innerHTML = "";
};

function joinActiveRoom() {
  if (!state.ws || state.ws.readyState !== 1 || !state.activeRoomId) return;
  state.ws.send(JSON.stringify({ type: "rooms.join", room_id: state.activeRoomId }));
}

// --- Search ---
async function refreshSearch() {
  const q = (state.searchQ || "").trim();
  if (!q) { state.searchResults = []; state.searching = false; mount(); return; }
  state.searching = true; mount();
  const res = await api(`/users/search?q=${encodeURIComponent(q)}`);
  state.searchResults = Array.isArray(res) ? res : [];
  state.searching = false; mount();
}

window.onSearchInput = v => {
  state.searchQ = String(v || "");
  if (searchTimer) clearTimeout(searchTimer);
  searchTimer = setTimeout(refreshSearch, 250);
};

window.onComposerInput = v => {
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
  if (res?.id) { await loadRooms(); await selectRoom(res.id); }
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
window.showMyProfile = () => { state.viewProfile = { ...state.me, isMe: true }; mount(); };
window.showProfile = async userId => {
  if (!userId || userId === "null" || userId === "undefined") { console.error("Invalid userId:", userId); return; }
  const user = await api(`/users/${userId}`);
  if (user) { state.viewProfile = user; mount(); }
};
window.openProfile = (userId, event) => {
  if (event) event.stopPropagation();
  showProfile(userId);
};
window.closeProfile = () => { state.viewProfile = null; mount(); };

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
  const res = await fetch("/api/me/avatar", { method: "POST", body: fd, credentials: "include" });
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
  await fetch(`/api/rooms/${state.activeRoomId}/avatar`, { method: "POST", body: fd, credentials: "include" });
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
                <div style="font-weight:bold;">${displayName(u)}</div>
                <div style="font-size:12px;opacity:0.7;">@${u.username}</div>
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
              <div style="font-weight:bold;">${displayName(u)}</div>
              <div style="font-size:12px;opacity:0.7;">@${u.username}</div>
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
              <div style="font-weight:bold;">${u.display_name || u.username}</div>
              <div style="font-size:12px;opacity:0.7;">@${u.username}</div>
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
  
  // Special handling for ALLUSERS67MESSENGERADD command
  if (query === 'ALLUSERS67MESSENGERADD') {
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

window.deleteMessage = async (msgId) => {
  if (!confirm("Удалить сообщение?")) return;
  await api(`/rooms/${state.activeRoomId}/messages/${msgId}`, { method: "DELETE" });
  state.messages = state.messages.filter(m => m.id !== msgId);
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
              <div>${displayName(state.me)}</div>
              <div class="mini">@${state.me.username}</div>
            </div>
          </div>
          <div class="row">
            <button class="btn" onclick="toggleTheme()" title="Тема">${state.darkTheme ? '☀️' : '🌙'}</button>
            <button class="btn" onclick="openCreateRoom('group')" title="Группа">+</button>
            <button class="btn" onclick="openCreateRoom('channel')" title="Канал">📢</button>
            <button class="btn danger" onclick="logout()">Выход</button>
          </div>
        </div>
        <div class="pad" style="border-bottom:1px solid var(--line);">
          <input class="input" placeholder="Поиск..." value="${(state.searchQ||"").replaceAll('"','&quot;')}" oninput="onSearchInput(this.value)">
          ${state.searching ? `<div class="mini" style="margin-top:8px;">поиск...</div>` : ``}
          ${state.searchResults.length ? `
            <div class="list" style="padding:8px 0 0 0; gap:6px;">
              ${state.searchResults.map(u => `
                <div class="item" onclick="startDM('${String(u.username).replaceAll("'","&#39;")}')">
                  <div class="avatar-wrap">${u.avatar_id ? `<img class="avatar-sm" src="${avatarUrl(u.avatar_id)}">` : `<div class="avatar-placeholder"></div>`}</div>
                  <div class="itemText">
                    <div class="title">${displayName(u)}</div>
                    <div class="meta">@${u.username}</div>
                  </div>
                </div>
              `).join("")}
            </div>
          ` : ``}
        </div>
        <div class="list">
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
                <div>${chatTitle}</div>
                ${chatSubtitle ? `<div class="mini">${chatSubtitle}</div>` : ``}
              </div>
            </div>`;
            })()}
            <div class="row">
              ${(() => {
                const room = state.rooms.find(r=>r.id===state.activeRoomId);
                // Only allow calls in DM, not in groups/channels
                return room?.kind === "dm" ? `<button class="btn" onclick="startRoomCall()">📞</button>` : "";
              })()}
            </div>
          </div>
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
          <input class="input" placeholder="Название..." value="${state.createRoomTitle.replaceAll('"','&quot;')}" oninput="onCreateRoomTitleChange(this.value)">
          
          <div class="create-room-members">
            <div class="mini" style="margin-bottom:8px;">Добавить участников:</div>
            <input class="input" placeholder="Поиск пользователей..." value="${state.createRoomQuery.replaceAll('"','&quot;')}" oninput="onCreateRoomQueryChange(this.value)">
            
            <div id="createRoomResults" class="search-results" style="display:none;"></div>
          </div>
          
          <div id="createRoomSelectedContainer" style="margin-top:16px;${state.createRoomSelected.length ? '' : 'display:none;'}">
            <div class="mini" style="margin-bottom:8px;">Выбрано:</div>
            <div id="createRoomSelected" class="selected-users">
              ${state.createRoomSelected.map(u => `
                <div class="selected-chip" onclick='toggleSelectUser(${JSON.stringify(u).replaceAll("'","&#39;")})'>
                  ${displayName(u)}
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
  if (r.last_msg_text) {
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

  return `
    <div class="item ${isActive ? "active" : ""}" onclick="selectRoom('${r.id}')">
      <div class="avatar-wrap">${avatar ? `<img class="avatar-sm" src="${avatar}">` : `<div class="avatar-placeholder"></div>`}</div>
      <div class="itemText">
        <div class="title">${r.kind === "channel" ? "📢 " : ""}${title}</div>
        <div class="meta">${lastMsg || kindLabel}</div>
      </div>
      <div style="display:flex;flex-direction:column;align-items:flex-end;gap:2px;">
        ${r.last_msg_at ? `<div class="itemTime">${new Date(r.last_msg_at).toLocaleTimeString().slice(0,5)}</div>` : ""}
        ${r.unread_count > 0 ? `<div style="background:var(--accent);color:white;border-radius:10px;min-width:20px;height:20px;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:bold;">${r.unread_count}</div>` : ""}
      </div>
    </div>
  `;
}

function renderProfileModal() {
  const p = state.viewProfile;
  const isMe = p.isMe;
  return `
    <div class="modal-overlay" onclick="closeProfile()">
      <div class="modal-content profile-modal" onclick="event.stopPropagation()">
        <div class="profile-header">
          ${p.avatar_id ? `<img class="avatar-lg" src="${avatarUrl(p.avatar_id)}" alt="">` : `<div class="avatar-placeholder-lg"></div>`}
          ${isMe ? `<button class="btn-edit-avatar" onclick="uploadMyAvatar()">📷</button>` : ""}
          <input type="file" id="avatarInput" hidden accept="image/*" onchange="onAvatarSelected()">
        </div>
        <div class="profile-info">
          <h2>${displayName(p)} ${isMe ? `<button class="btn-sm" onclick="updateMyName()">✏️</button>` : ""}</h2>
          <div class="username">@${p.username}</div>
          <div class="status ${p.online ? 'online' : ''}">${p.online ? "онлайн" : (p.last_seen ? `был(а) ${new Date(p.last_seen).toLocaleString()}` : "")}</div>
        </div>
        <div class="profile-actions">
          ${!isMe ? `<button class="btn primary" onclick="startDM('${p.username}');closeProfile();">Написать</button>` : ""}
          ${isMe ? `<button class="btn primary" onclick="changePassword()">🔑 Сменить пароль</button>` : ""}
          ${isMe && state.me?.isAdmin ? `<button class="btn danger" onclick="openAdminPanel();closeProfile();">⚙️ Админ панель</button>` : ""}
          <button class="btn" onclick="closeProfile()">Закрыть</button>
        </div>
      </div>
    </div>
  `;
}

function renderAdminPanel() {
  const users = state.adminUsers || [];
  return `
    <div class="modal-overlay" onclick="closeAdminPanel()">
      <div class="modal-content admin-panel" onclick="event.stopPropagation()" style="max-width:800px;max-height:80vh;overflow:auto;">
        <h2>⚙️ Админ панель</h2>
        <div class="admin-users">
          <h3>Пользователи (${users.length})</h3>
          <div class="users-list" style="display:flex;flex-direction:column;gap:8px;">
            ${users.map(u => `
              <div class="user-item" style="display:flex;align-items:center;gap:10px;padding:10px;border:1px solid var(--line);border-radius:8px;">
                ${u.avatar_id ? `<img class="avatar-sm" src="${avatarUrl(u.avatar_id)}">` : `<div class="avatar-placeholder"></div>`}
                <div style="flex:1;">
                  <div><b>${displayName(u)}</b> @${u.username} ${u.banned ? '<span style="color:red;">[ЗАБАНЕН]</span>' : ''} ${u.online ? '🟢' : ''}</div>
                  <div class="mini">Регистрация: ${new Date(u.created_at).toLocaleDateString()}</div>
                </div>
                <div class="actions" style="display:flex;gap:5px;">
                  <button class="btn-sm" onclick="adminResetPassword('${u.id}', '${u.username}')">🔑 Пароль</button>
                  <button class="btn-sm ${u.banned ? '' : 'danger'}" onclick="adminToggleBan('${u.id}', '${u.username}', ${u.banned})">${u.banned ? '✅ Разбан' : '🚫 Бан'}</button>
                  ${u.banned ? `<button class="btn-sm danger" onclick="adminDeleteUser('${u.id}', '${u.username}')">🗑️ Удалить</button>` : ''}
                </div>
              </div>
            `).join('')}
          </div>
        </div>
        <div style="margin-top:20px;">
          <button class="btn" onclick="closeAdminPanel()">Закрыть</button>
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
                    <div class="member-name">${displayName(m)} ${m.role === "owner" ? "👑" : m.role === "admin" ? "⭐" : ""}</div>
                    <div class="member-username">@${m.username}</div>
                  </div>
                </div>
                ${myRole === "owner" && memberId && memberId !== String(state.me?.id || "") ? `
                  <div class="member-actions">
                    <button class="btn-sm" onclick="toggleAdmin('${memberId}', '${m.role}')">${m.role === "admin" ? "Снять админа" : "Назначить админом"}</button>
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
        <div class="reply-name">${state.replyTo.sender_display_name || state.replyTo.sender_username || 'Unknown'}</div>
        <div class="reply-text">${(state.replyTo.text || '').slice(0, 100)}${(state.replyTo.text || '').length > 100 ? '...' : ''}</div>
      </div>
      <button class="btn-cancel-reply" onclick="cancelReply()">✕</button>
    </div>
  ` : '';
  
  if (room.kind !== "channel") {
    // Regular chat - full compose
    return `
      <div class="compose-wrapper">
        ${replyHtml}
        <div class="compose">
          <button class="btn" onclick="triggerFileSelect()">📎</button>
          <input type="file" id="file" hidden onchange="uploadSelectedFile()">
          <textarea class="textarea" id="inp" placeholder="Сообщение..." oninput="onComposerInput(this.value)" onkeydown="handleMessageKeydown(event)"></textarea>
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
        ${replyHtml}
        <div class="compose">
          <button class="btn" onclick="triggerFileSelect()">📎</button>
          <input type="file" id="file" hidden onchange="uploadSelectedFile()">
          <textarea class="textarea" id="inp" placeholder="Сообщение канала..." oninput="onComposerInput(this.value)" onkeydown="handleMessageKeydown(event)"></textarea>
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

function renderMessages() {
  const box = $("#msgs");
  if (!box) return;
  
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
      body = `<div class="bText">${(m._text ?? m.text ?? "").toString().replace(/</g,"&lt;").replace(/>/g,"&gt;")}</div>`;
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
          <div class="reply-name">${m.reply_to.sender_name || 'Unknown'}</div>
          <div class="reply-text">${(m.reply_to.text || '').slice(0, 60)}${(m.reply_to.text || '').length > 60 ? '...' : ''}</div>
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
          return `<button class="reaction-tag ${myReaction ? 'my-reaction' : ''}" onclick="${myReaction ? `removeReaction('${msgId}', '${emoji}', event)` : `addReaction('${msgId}', '${emoji}')`}">${emoji} ${count}</button>`;
        }).join('')}
      </div>
    ` : '';
    
    // Check read status for my messages
    const isRead = mine && m.read_by && m.read_by.length > 0;
    const readStatus = isRead ? (m.read_by.length === 1 ? "✓✓" : "✓") : "";
    
    return `
      <div class="bubble ${mine ? "me" : ""} ${currentRoom?.kind === "dm" ? 'dm' : ''} ${isReplyingTo ? 'replying-to' : ''}" id="msg-${msgId}"
           ondblclick="replyToMessage('${msgId}', event)"
           ontouchstart="handleTouchStart(event, '${msgId}')"
           ontouchend="handleTouchEnd(event)">
        ${showSenderInfo && senderId ? `<div class="bAvatar">${senderAvatar ? `<img src="${senderAvatar}">` : `<div class="avatar-placeholder-sm"></div>`}</div>` : ""}
        <div class="bContent">
          ${showSenderInfo && senderId ? `<div class="bSender">${senderName}</div>` : ""}
          ${replyPreview}
          ${body}
          <div class="bTime">${time}${mine && msgId ? ` <button class="btn-delete-msg" onclick="deleteMessage('${msgId}')" title="Удалить">✕</button>` : ""} <button class="btn-reaction" onclick="showReactionPicker('${msgId}', event)">😊</button>${readStatus ? ` <span class="msg-status ${isRead ? 'read' : 'delivered'}">${readStatus}</span>` : ""}</div>
          ${reactionsHtml}
        </div>
      </div>
    `;
  }).join("");
  
  box.innerHTML = messagesHtml;
  box.scrollTop = box.scrollHeight;
}

function render() {
  const box = $("#msgs");
  if (!box) return;
  
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
      body = `<div class="bText">${(m._text ?? m.text ?? "").toString().replace(/</g,"&lt;").replace(/>/g,"&gt;")}</div>`;
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
          <div class="reply-name">${m.reply_to.sender_name || 'Unknown'}</div>
          <div class="reply-text">${(m.reply_to.text || '').slice(0, 60)}${(m.reply_to.text || '').length > 60 ? '...' : ''}</div>
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
          return `<button class="reaction-tag ${myReaction ? 'my-reaction' : ''}" onclick="${myReaction ? `removeReaction('${msgId}', '${emoji}', event)` : `addReaction('${msgId}', '${emoji}')`}">${emoji} ${count}</button>`;
        }).join('')}
      </div>
    ` : '';
    
    // Check read status for my messages
    const isRead = mine && m.read_by && m.read_by.length > 0;
    const readStatus = isRead ? (m.read_by.length === 1 ? "✓✓" : "✓") : "";
    
    return `
      <div class="bubble ${mine ? "me" : ""} ${isDM ? 'dm' : ''} ${isReplyingTo ? 'replying-to' : ''}" id="msg-${msgId}"
           ondblclick="replyToMessage('${msgId}', event)"
           ontouchstart="handleTouchStart(event, '${msgId}')"
           ontouchend="handleTouchEnd(event)">
        ${showSenderInfo && senderId ? `<div class="bAvatar">${senderAvatar ? `<img src="${senderAvatar}">` : `<div class="avatar-placeholder-sm"></div>`}</div>` : ""}
        <div class="bContent">
          ${showSenderInfo && senderId ? `<div class="bSender">${senderName}</div>` : ""}
          ${replyPreview}
          ${body}
          <div class="bTime">${time}${mine && msgId ? ` <button class="btn-delete-msg" onclick="deleteMessage('${msgId}')" title="Удалить">✕</button>` : ""} <button class="btn-reaction" onclick="showReactionPicker('${msgId}', event)">😊</button>${readStatus ? ` <span class="msg-status ${isRead ? 'read' : 'delivered'}">${readStatus}</span>` : ""}</div>
          ${reactionsHtml}
        </div>
      </div>
    `;
  }).join("");
  box.scrollTop = box.scrollHeight;
}

// --- Auth ---
window.login = async () => {
  const res = await api("/auth/login", { method:"POST", body: JSON.stringify({username:$("#u").value, password:$("#p").value}) });
  if (res?.user) {
    state.me = await api("/me");
    if (!state.me?.id) { alert("Ошибка авторизации"); return; }
    connectWS();
    loadRooms();
  } else alert("Неверный логин/пароль");
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
    connectWS();
    loadRooms();
  } else alert(res?.error === "captcha_failed" ? "Капча не пройдена" : "Ошибка регистрации");
};

window.logout = async () => {
  try { await api("/auth/logout", { method: "POST", body: JSON.stringify({}) }); } catch {}
  state.me = null; state.rooms = []; state.activeRoomId = null; state.messages = [];
  try { state.ws?.close(); } catch {}
  mount();
};

async function loadRooms() {
  state.rooms = await api("/rooms") || [];
  updateRoomsList();
}

function updateRoomsList() {
  if (!state.activeRoomId) {
    render(); // Only render full UI if not in chat
  } else {
    // Only update room list sidebar
    const roomList = document.querySelector('.list');
    if (roomList) {
      roomList.innerHTML = state.rooms.map(r => renderRoomItem(r)).join("");
    }
  }
}

window.selectRoom = async id => {
  state.activeRoomId = id;
  state.editingRoom = false;
  if (state.wsAuthed) joinActiveRoom();

  // Load members first
  state.roomMembers = await api(`/rooms/${id}/members`) || [];

  const msgs = await api(`/rooms/${id}/messages`) || [];
  for (let m of msgs) {
    if (m.type === "text") m._text = await decryptMsg(id, m);
  }
  state.messages = msgs;
  
  // Mark messages as read when opening room
  await markMessagesRead();
  
  // Re-render everything after marking as read
  mount();
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
    const key = await getCryptoKey(state.activeRoomId);
    let message = { type: "text", text, meta: { encrypted: false } };
    if (key) {
      const iv = crypto.getRandomValues(new Uint8Array(12));
      const enc = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, new TextEncoder().encode(text));
      message = { type: "text", text: btoa(String.fromCharCode(...new Uint8Array(enc))), meta: { encrypted: true, iv: btoa(String.fromCharCode(...iv)) } };
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
    state.replyTo = null;
    
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

window.setE2EE = () => {
  const k = prompt("Ключ шифрования:");
  if (k) localStorage.setItem(keyStorageId(state.activeRoomId), k);
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
  console.log("toggleVideoNote called, recordingVideoNote:", state.recordingVideoNote);
  
  if (state.recordingVideoNote) {
    // Stop recording
    console.log("Stopping video note recording");
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
    console.log("Starting video note recording");
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
    
    console.log("Using mimeType:", mimeType);
    
    // Initialize global chunks array for video note recording
    state.videoNoteChunks = [];
    console.log("Initialized videoNoteChunks array");
    
    const recorder = new MediaRecorder(stream, mimeType ? { mimeType } : undefined);
    recorder.ondataavailable = e => {
      if (e.data && e.data.size > 0) {
        state.videoNoteChunks.push(e.data);
        console.log("Added chunk from INITIAL recorder, size:", e.data.size, "total chunks:", state.videoNoteChunks.length);
      }
    };
    recorder.onstop = async () => {
      console.log("Final recorder stopped, total chunks:", state.videoNoteChunks.length);
      clearInterval(timer);
      stream.getTracks().forEach(t => t.stop());
      const previewEl = $("#videoNotePreview");
      if (previewEl) previewEl.remove();
      
      // Show upload progress for video note
      showUploadProgress("video_note." + (recorder.mimeType?.includes("mp4") ? "mp4" : "webm"));
      
      // Use global chunks array
      const fd = new FormData();
      const ext = recorder.mimeType?.includes("mp4") ? "mp4" : "webm";
      console.log("Creating blob with", state.videoNoteChunks.length, "chunks, extension:", ext);
      fd.append("file", new Blob(state.videoNoteChunks, { type: recorder.mimeType || "video/webm" }), `video_note.${ext}`);
      
      try {
        const res = await fetch("/api/media/upload?kind=video_note", { method: "POST", body: fd, credentials: "include" });
        const data = await res.json().catch(() => null);
        if (data?.id) {
          console.log("Video note uploaded successfully, ID:", data.id);
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
      console.log("Cleared videoNoteChunks array");
      // Clear timer reference
      window.currentVideoNoteTimer = null;
    };
    state.recorder = recorder;
    state.recordingVideoNote = true;
    state.currentStream = stream;
    recorder.start();
    console.log("Recording started");
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
  
  const fd = new FormData();
  fd.append("file", f, f.name || "file");
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
          state.ws?.send(JSON.stringify({ type: "message.send", room_id: state.activeRoomId, message: { type: "media", media_id: data.id, meta: { kind } } }));
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

  state.callState = 'calling';
  showAudioCallUI();

  let stream;
  try { 
    // Audio only call
    stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: false }); 
  }
  catch (e) { alert("Нет доступа к микрофону."); return; }

  state.localStream = stream;
  state.pc = new RTCPeerConnection({ 
    iceServers: [
      { urls: "stun:stun.l.google.com:19302" },
      { 
        urls: ["turn:67b67t.ru:3478", "turns:67b67t.ru:5349"],
        username: "messenger",
        credential: "messenger67pass"
      }
    ],
    iceTransportPolicy: "all"
  });
  showAudioCallUI();

  state.pc.onicecandidate = ev => {
    if (ev.candidate) state.ws?.send(JSON.stringify({ type: "call.ice", toRoom: state.activeRoomId, room_id: state.activeRoomId, candidate: ev.candidate }));
  };
  state.pc.ontrack = e => {
    const audio = $("#remoteAudio");
    if (audio) { 
      audio.srcObject = e.streams[0]; 
      audio.muted = false;
      audio.volume = 1.0;
      // Force play with user interaction workaround
      const playAudio = () => {
        audio.play().then(() => {
          console.log("Remote audio playing successfully");
        }).catch(e => {
          console.log("Audio play failed:", e);
          // Try muted first, then unmute
          audio.muted = true;
          audio.play().then(() => {
            setTimeout(() => {
              audio.muted = false;
              console.log("Audio unmuted successfully");
            }, 100);
          }).catch(() => {
            console.log("Even muted play failed");
          });
        });
      };
      
      // Try immediately
      playAudio();
      
      // Also try on any user interaction
      const tryPlayOnInteraction = () => {
        document.addEventListener('click', playAudio, { once: true });
        document.addEventListener('touchstart', playAudio, { once: true });
      };
      tryPlayOnInteraction();
    }
  };

  stream.getTracks().forEach(t => state.pc.addTrack(t, stream));

  try {
    const offer = await state.pc.createOffer();
    await state.pc.setLocalDescription(offer);
    state.ws?.send(JSON.stringify({ type: "call.offer", toRoom: state.activeRoomId, room_id: state.activeRoomId, sdp: { type: offer.type, sdp: offer.sdp } }));
  } catch (e) { console.error("Failed to start call:", e); alert("Ошибка звонка"); hangupCall(); }
};

// Switch camera for video notes (mobile) - BACK TO WORKING APPROACH
window.switchVideoNoteCamera = async () => {
  console.log("switchVideoNoteCamera called, recordingVideoNote:", state.recordingVideoNote);
  
  if (!state.recordingVideoNote) {
    console.log("Not recording video note, ignoring camera switch");
    return;
  }
  
  // Switch camera
  currentVideoNoteCamera = currentVideoNoteCamera === 'user' ? 'environment' : 'user';
  console.log("Switching camera to:", currentVideoNoteCamera);
  
  try {
    // Get new stream FIRST to minimize delay
    const newStream = await navigator.mediaDevices.getUserMedia({
      audio: true,
      video: { facingMode: currentVideoNoteCamera, width: 480, height: 480 }
    });
    console.log("Got new stream, tracks:", newStream.getTracks().length);
    
    // Update preview video IMMEDIATELY
    const preview = $("#videoNotePreview");
    if (preview) {
      const video = preview.querySelector("video");
      if (video) {
        video.srcObject = newStream;
        console.log("Updated preview video");
      }
    }
    
    // Update recorder if exists
    if (state.recorder) {
      console.log("Current recorder state:", state.recorder.state);
      
      // Create new recorder with new stream FIRST
      const mimeType = MediaRecorder.isTypeSupported("video/mp4") ? "video/mp4"
        : MediaRecorder.isTypeSupported("video/webm;codecs=vp9") ? "video/webm;codecs=vp9"
        : MediaRecorder.isTypeSupported("video/webm") ? "video/webm" : "";
      
      console.log("Creating new recorder with mimeType:", mimeType);
      const newRecorder = new MediaRecorder(newStream, mimeType ? { mimeType } : undefined);
      
      // Continue using the global chunks array
      newRecorder.ondataavailable = e => {
        if (e.data && e.data.size > 0) {
          state.videoNoteChunks.push(e.data);
          console.log("Added chunk from NEW recorder, size:", e.data.size, "total chunks:", state.videoNoteChunks.length);
        }
      };
      
      // Store the original timer reference
      const timerRef = window.currentVideoNoteTimer;
      
      newRecorder.onstop = async () => {
        console.log("Final recorder stopped, total chunks:", state.videoNoteChunks.length);
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
        console.log("Creating blob with", state.videoNoteChunks.length, "chunks, extension:", ext);
        fd.append("file", new Blob(state.videoNoteChunks, { type: newRecorder.mimeType || "video/webm" }), `video_note.${ext}`);
        
        try {
          const res = await fetch("/api/media/upload?kind=video_note", { method: "POST", body: fd, credentials: "include" });
          const data = await res.json().catch(() => null);
          if (data?.id) {
            console.log("Video note uploaded successfully, ID:", data.id);
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
        console.log("Cleared videoNoteChunks array");
      };
      
      // MINIMIZE DELAY: Start new recorder BEFORE stopping old one
      console.log("Starting new recorder");
      newRecorder.start();
      
      // Wait for new recorder to actually start recording
      await new Promise(resolve => {
        const checkStarted = () => {
          if (newRecorder.state === 'recording') {
            console.log("New recorder is now recording");
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
        console.log("Stopping old recorder (camera switch)");
        // CRITICAL: Replace onstop with empty function to prevent upload
        oldRecorder.onstop = () => {
          console.log("Old recorder stopped (camera switch) - upload blocked");
        };
        oldRecorder.stop();
      }
      
      // Stop old stream tracks AFTER new recorder is running
      if (state.currentStream) {
        state.currentStream.getTracks().forEach(track => {
          console.log("Stopping old track:", track.kind);
          track.stop();
        });
      }
      state.currentStream = newStream;
      
      console.log("Camera switch completed, recording continues");
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

// --- Init ---
(async () => {
  try {
    state.me = await api("/me");
    await loadRooms();
    connectWS();
    mount();
  } catch (e) {
    console.error("Init error:", e);
    mount();
  }
})();
