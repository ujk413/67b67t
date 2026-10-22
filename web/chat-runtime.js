import { createAccountStore, DurableOutbox, mergeMessages } from './local-data.js';
import { userError } from './errors.js';
export function createChatRuntime({ state, request, decode, buildText, mount, refresh, render, report, drafts, loadRooms }) {
  let store, queue, owner, ready, generation = 0, opening = 0, olderBusy = false, newerBusy = false, searching = 0, observer, scrollTimer, readTimer, heartbeat;
  const pending = item => ({ ...item, id: 'local-' + item.id, client_message_id: item.id, sender_id: owner, type: item.body?.type || 'text', _text: item.text, text: item.text });
  const current = id => owner && state.me?.id === owner && (!id || state.activeRoomId === id);
  function changed(item, message) {
    if (!current(item.room_id)) return;
    if (item.cancelled) state.messages = state.messages.filter(m => m.client_message_id !== item.id);
    else if (message) state.messages = mergeMessages(state.messages, [message]);
    else if (!state.messages.some(m => !m.delivery && m.client_message_id === item.id)) state.messages = mergeMessages(state.messages, [pending(item)]);
    render();
  }
  async function page(roomId, options = {}) {
    const result = await request(`/rooms/${roomId}/history?${new URLSearchParams(options)}`);
    const messages = await Promise.all(result.messages.map(m => decode(roomId, m)));
    return { ...result, messages };
  }
  function start(user) { if (owner === user.id && store) return ready; stop(); ready = startAccount(user); return ready; }
  async function startAccount(user) {
    const mine = ++generation; owner = user.id; store = createAccountStore(owner); const local = store;
    const [cached, savedDrafts, folders] = await Promise.all([local.get('rooms', []), local.entries('draft:'),local.get('folders',[])]);
    if (mine !== generation) return;
    if (!state.rooms.length && cached.length) state.rooms = cached;
    state.folders=folders;
    for (const [name, text] of savedDrafts) if (!drafts.has(name.slice(6))) drafts.set(name.slice(6), text);
    queue = new DurableOutbox({ store: local, changed, report: e => report(userError(e)), send: async item => {
      if (mine !== generation) throw new Error('Сессия завершена');
      if (!item.body) {
        item.body = await buildText(item.room_id, state.rooms.find(r => r.id === item.room_id), item.text);
        if (item.reply) item.body.reply_to = { id: item.reply.id, sender_name: item.reply.sender_display_name || item.reply.sender_username, text: 'Ответ на сообщение' };
        if (item.reply) { const preview = await buildText(item.room_id, state.rooms.find(r => r.id === item.room_id), item.reply._text ?? item.reply.text ?? 'Вложение'); item.body.meta = { ...item.body.meta, reply_preview: preview }; }
        await local.set('outbox:' + item.id, item);
      }
      if (mine !== generation) throw new Error('Сессия завершена');
      if(item.body.type!=='text' && item.text && !item.body.meta?.caption_message) {item.body={...item.body,text:null,meta:{...item.body.meta,caption_message:await buildText(item.room_id,state.rooms.find(r=>r.id===item.room_id),item.text)}};await local.set('outbox:'+item.id,item);}
      if(mine!==generation)throw Error('Сессия завершена');
      const sent = await request(`/rooms/${item.room_id}/messages`, { method: 'POST', body: JSON.stringify({ ...item.body, client_message_id: item.id }) });
      return decode(item.room_id, sent);
    } });
    heartbeat = setInterval(() => { if (!document.hidden) void queue?.flush(); }, 15000);
    void queue.flush();
  }
  function stop() { ++generation; ++opening; ++searching; queue?.stop(); queue = null; store = null; owner = null; ready = null; observer?.disconnect(); clearTimeout(scrollTimer); clearTimeout(readTimer); clearInterval(heartbeat); }
  async function saveRooms(rooms) { if (!current()) return; await store?.set('rooms', rooms); }
  function draft(roomId, text) { drafts.set(roomId, text); void store?.set('draft:' + roomId, text).catch(e => report(userError(e))); }
  async function enqueue({ roomId, text = '', body, reply }) {
    if (!queue || !store || !current()) throw new Error('Локальное хранилище ещё загружается. Повторите попытку.');
    const item = { id: crypto.randomUUID(), room_id: roomId, text, body, reply, delivery: 'queued', created_at: new Date().toISOString() };
    await queue.enqueue(item); return item;
  }
  async function open(id, options = {}) {
    await start(state.me); const mine = ++opening, local = store, account = owner;
    const valid = () => mine === opening && account === owner && current(id);
    state.selectedMessages?.clear();
    state.activeRoomId = id; state.messages = []; state.roomMembers = []; state.activeDmPeer = null;
    state.roomLoading = true; state.roomLoadError = false; state.replyTo = null; state.editMessage = null;
    state.searchMode = false; state.searchResults = []; state.composeOpen = false; state.settingsOpen = false; state.uiMenu = null;
    state.composerText = drafts.get(id) || ''; state.newMessages = 0; state.chatHistory = null; state.lastReadId = null;
    await mount();
    const [cached, outgoing] = await Promise.all([local.get('messages:' + id, []), queue.items()]);
    if (!valid()) return;
    state.messages = mergeMessages(cached.slice(-100), outgoing.filter(x => x.room_id === id).map(pending));
    if (state.messages.length) { state.roomLoading = false; render(); }
    try {
      const memberTask = request(`/rooms/${id}/members`).then(members => { if (valid()) state.roomMembers = members; }).catch(()=>{});
      const room = state.rooms.find(r => r.id === id);
      const peerTask = room?.other_user_id ? request(`/users/${room.other_user_id}`).then(peer => { if (valid()) state.activeDmPeer = peer; }).catch(()=>{}) : Promise.resolve();
      let result = await page(id);
      const saved = await local.get('position:' + id), anchor = options.latest ? null : options.around || saved || result.first_unread_id;
      if (anchor && !result.messages.some(m => m.id === anchor)) result = await page(id, { around: anchor }).catch(() => result);
      if (!valid()) return;
      await local.saveMessages(id, result.messages); await Promise.allSettled([memberTask, peerTask]);
      if (!valid()) return;
      state.chatHistory = result; state.messages = mergeMessages(result.messages, state.messages.filter(m => m.delivery || (!m.delivery && m.room_id === id && new Date(m.created_at) > new Date(result.messages.at(-1)?.created_at || 0))));
      state.roomLoading = false; await mount();
      const target = anchor && document.getElementById('msg-' + anchor), box = document.getElementById('msgs');
      if (target) target.scrollIntoView({ block: 'center' }); else if (box) box.scrollTop = box.scrollHeight;
      bind();
    } catch (error) { if (valid()) { state.roomLoading = false; state.roomLoadError = !state.messages.length; state.chatHistory ||= { has_older: true, has_newer: false }; await mount(); report(userError(error)); bind(); } }
  }
  async function older() {
    if (olderBusy || !current(state.activeRoomId) || state.chatHistory?.has_older === false) return;
    const id = state.activeRoomId, before = state.messages.find(m => !m.delivery)?.id, mine = opening; if (!before) return;
    olderBusy = true; state.loadingOlder = true; render();
    try { const result = await page(id, { before }); if (mine !== opening || !current(id)) return; await store.saveMessages(id, result.messages); state.messages = mergeMessages(result.messages, state.messages); const trimmed=state.messages.length>300;state.messages=state.messages.slice(0,300); state.chatHistory = { ...state.chatHistory, has_older: result.has_older,has_newer:trimmed || state.chatHistory?.has_newer }; }
    catch (error) { report(userError(error)); } finally { olderBusy = false; state.loadingOlder = false; if (current(id)) render(); }
  }
  async function newer() {
    if (newerBusy || !current(state.activeRoomId)) return;
    const id = state.activeRoomId, after = state.messages.filter(m => !m.delivery).at(-1)?.id, mine = opening; if (!after) return;
    newerBusy = true;
    try { const result = await page(id, { after }); if (mine !== opening || !current(id)) return; await store.saveMessages(id, result.messages); state.messages = mergeMessages(state.messages, result.messages); const trimmed=state.messages.length>300;state.messages=state.messages.slice(-300); state.chatHistory = { ...state.chatHistory, has_newer: result.has_newer,has_older:trimmed || state.chatHistory?.has_older }; render(); }
    catch (error) { report(userError(error)); } finally { newerBusy = false; }
  }
  async function jump(id) {
    if (!current()) return; const roomId = state.activeRoomId, mine = opening;
    if (!state.messages.some(m => m.id === id)) { const result = await page(roomId, { around: id }); if (mine !== opening || !current(roomId)) return; state.chatHistory = result; state.messages = result.messages; await store.saveMessages(roomId, result.messages); render(); }
    const target = document.getElementById('msg-' + id); target?.scrollIntoView({ behavior: 'smooth', block: 'center' }); target?.classList.add('search-highlight-current'); setTimeout(() => target?.classList.remove('search-highlight-current'), 1800);
  }
  async function received(message) {
    const account = owner, local = store; if (!account) return;
    const prepared = await decode(message.room_id, message); if (owner !== account || state.me?.id !== account) return;
    await local.saveMessages(message.room_id, [prepared]);
    if (current(message.room_id)) {
      if (state.chatHistory?.has_newer && !state.messages.some(m => m.id === message.id)) state.newMessages = (state.newMessages || 0) + 1;
      else {
        const box=document.getElementById('msgs'),atBottom=box && box.scrollTop+box.clientHeight>=box.scrollHeight-150;
        if(state.messages.length>=300 && !atBottom && !state.messages.some(m=>m.id===message.id)) {state.chatHistory={...state.chatHistory,has_newer:true};state.newMessages=(state.newMessages||0)+1;}
        else {const all=mergeMessages(state.messages,[prepared]);if(all.length>300)state.chatHistory={...state.chatHistory,has_older:true};state.messages=all.slice(-300);}
      } render();
    }
    return prepared;
  }
  function bind() {
    observer?.disconnect(); const box = document.getElementById('msgs'); if (!box) return;
    const roomId = state.activeRoomId, mine = opening;
    box.onscroll = () => {
      clearTimeout(scrollTimer); scrollTimer = setTimeout(() => {
        if (mine !== opening || !current(roomId)) return;
        const top = box.getBoundingClientRect().top, nodes = [...box.querySelectorAll('[data-message-id]')], first = nodes.find(n => n.getBoundingClientRect().bottom > top);
        if (first) void store?.set('position:' + roomId, first.dataset.messageId).catch(() => {});
        const down = document.getElementById('chatJumpDown'); if (down) down.hidden = box.scrollTop + box.clientHeight >= box.scrollHeight - 150 && !state.chatHistory?.has_newer;
      }, 150);
    };
    if (typeof IntersectionObserver === 'undefined') return;
    const visible = new Set();
    observer = new IntersectionObserver(entries => {
      for (const entry of entries) { const id = entry.target.dataset.messageId; if (entry.isIntersecting && entry.intersectionRatio >= 0.6) visible.add(id); else visible.delete(id); }
      clearTimeout(readTimer); readTimer = setTimeout(async () => {
        if (mine !== opening || !current(roomId) || document.hidden || !document.hasFocus()) return;
        const last = state.messages.filter(m => visible.has(m.id) && !m.delivery).at(-1); if (!last || last.id === state.lastReadId) return;
        try { await request(`/rooms/${roomId}/read`, { method: 'POST', body: JSON.stringify({ up_to_message_id: last.id }) }); if (current(roomId)) state.lastReadId = last.id; }
        catch { /* A later visibility/reconnect event retries read state. */ }
      }, 500);
    }, { root: box, threshold: [0, 0.6] });
    box.querySelectorAll('[data-message-id]').forEach(node => observer.observe(node));
  }
  async function search(query) {
    const mine = ++searching, roomId = state.activeRoomId, account = owner;
    state.searchQuery = query; state.searchResults = []; state.searchProgress = ''; if (query.trim().length < 2 || !roomId) { await refresh(); return; }
    const known = new Map((await store.get('messages:' + roomId, [])).map(m => [m.id, m]));
    const update = async () => { if (mine !== searching || roomId !== state.activeRoomId || owner !== account) return; state.searchResults = [...known.values()].filter(m => (m._text ?? m.text ?? '').toLowerCase().includes(query.toLowerCase())); state.currentSearchIndex = 0; await refresh(); };
    await update(); let before, count = 0;
    try {
      do { const result = await page(roomId, before ? { before, limit: 100 } : { limit: 100 }); if (mine !== searching || owner !== account || roomId !== state.activeRoomId) return;
        await store.saveMessages(roomId, result.messages); for (const message of result.messages) known.set(message.id, message); count += result.messages.length;
        state.searchProgress = `Проверено: ${count}`; await update(); before = result.has_older && result.messages[0]?.id;
      } while (before);
      state.searchProgress = 'Вся доступная история проверена'; await update();
    } catch (error) { if (mine === searching) report(userError(error)); }
  }
  window.addEventListener('online', () => { void queue?.flush(); if (current()) void loadRooms().catch(() => {}); });
  document.addEventListener('visibilitychange', () => { if (!document.hidden) { void queue?.flush(); bind(); } });
  return { start, stop, saveRooms, draft, enqueue, open, older, newer, jump, received, bind, search, cancelSearch: () => { ++searching; },
    retry: id => queue?.retry(id), cancel: id => queue?.cancel(id), flush: () => queue?.flush(), local: () => store };
}
