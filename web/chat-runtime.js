import { createAccountStore, DurableOutbox, mergeMessages } from './local-data.js';
import { userError } from './errors.js';
import { uploadQueuedFile } from './queued-upload.js';
export function createChatRuntime({ state, request, decode, buildText, mount, refresh, render, report, drafts, loadRooms }) {
  let store, queue, owner, ready, generation = 0, opening = 0, olderBusy = false, newerBusy = false, searching = 0, observer, scrollTimer, readTimer, heartbeat;
  let hidden=new Set(), syncingHidden=null;
  const pending = item => ({ ...item, id: 'local-' + item.id, client_message_id: item.id, sender_id: owner, type: item.body?.type || 'text', _text: item.text, text: item.text });
  const current = id => owner && state.me?.id === owner && (!id || state.activeRoomId === id);
  function changed(item, message) {
    if (!current(item.room_id) || (message && hidden.has(message.id))) return;
    if (item.cancelled) state.messages = state.messages.filter(m => m.client_message_id !== item.id);
    else if (message) state.messages = mergeMessages(state.messages, [message]);
    else if (!state.messages.some(m => !m.delivery && m.client_message_id === item.id)) state.messages = mergeMessages(state.messages, [pending(item)]);
    render();
  }
  async function page(roomId, options = {}) {
    const result = await request(`/rooms/${roomId}/history?${new URLSearchParams(options)}`);
    const messages = await Promise.all(result.messages.map(m => decode(roomId, m)));
    return { ...result, messages:messages.filter(m=>!hidden.has(m.id)) };
  }
  function start(user) { if (owner === user.id && store) return ready; stop(); ready = startAccount(user); return ready; }
  async function startAccount(user) {
    const mine = ++generation; owner = user.id; store = createAccountStore(owner); const local = store;
    const [cached, savedDrafts, folders,tombstones] = await Promise.all([local.get('rooms', []), local.entries('draft:'),local.get('folders',[]),local.entries('hidden:')]);
    if (mine !== generation) return;
    hidden=new Set(tombstones.map(([key])=>key.slice(7)));
    if (!state.rooms.length && cached.length) state.rooms = cached;
    state.folders=folders;
    for (const [name, text] of savedDrafts) if (!drafts.has(name.slice(6))) drafts.set(name.slice(6), text);
    queue = new DurableOutbox({ store: local, changed:(...args)=>{if(mine===generation)changed(...args);}, report: e => {if(mine===generation)report(userError(e));}, send: async (item,signal) => {
      if (mine !== generation) throw new Error('Сессия завершена');
      if(item.upload && !item.body?.media_id) {
        const blob=await local.getBlob(item.upload.blobKey);
        if(!blob)throw Object.assign(Error('Файл очереди больше недоступен'),{status:410});
        const media=await uploadQueuedFile(blob,item.upload.kind,signal);
        if(mine!==generation || signal?.aborted)throw Error('Сессия завершена');
        item.body={...item.body,media_id:media.id};await local.set('outbox:'+item.id,item);
      }
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
  function stop() { hidden=new Set();syncingHidden=null;++generation; ++opening; ++searching; queue?.stop(); queue = null; store = null; owner = null; ready = null; observer?.disconnect(); clearTimeout(scrollTimer); clearTimeout(readTimer); clearInterval(heartbeat); }
  async function hide(roomId,messageId) {
    const account=owner,local=store,bag=hidden;if(!account || !local)return;
    bag.add(messageId);
    if(owner===account && current(roomId)){state.messages=state.messages.filter(m=>m.id!==messageId);state.searchResults=state.searchResults?.filter(m=>(m.msg || m).id!==messageId) || [];render();}
    await local.set('hidden:'+messageId,roomId);await local.removeMessage(roomId,messageId);
  }
  function syncHidden() {
    if(syncingHidden)return syncingHidden;
    const account=owner,local=store;if(!account || !local)return Promise.resolve();
    const task=(async()=>{try{let after;
      do {const page=await request('/me/hidden-messages'+(after?'?after='+encodeURIComponent(after):''));
        if(owner!==account || store!==local)return;
        for(const item of page.items || []){if(owner!==account || store!==local)return;await hide(item.room_id,item.message_id);}
        after=page.next_cursor;
      }while(after);
    }catch{/* Offline cache remains usable; reconnect retries authoritative tombstones. */}})();
    syncingHidden=task;void task.finally(()=>{if(syncingHidden===task)syncingHidden=null;});return task;
  }
  async function saveRooms(rooms) { if (!current()) return; await store?.set('rooms', rooms); }
  function draft(roomId, text) { drafts.set(roomId, text); void store?.set('draft:' + roomId, text).catch(e => report(userError(e))); }
  async function enqueue({ roomId, text = '', body, reply, upload, clientId }) {
    if (!queue || !store || !current()) throw new Error('Локальное хранилище ещё загружается. Повторите попытку.');
    const local=store,account=owner,box=queue;
    const item = { id: clientId || crypto.randomUUID(), room_id: roomId, text, body, reply, delivery: 'queued', created_at: new Date().toISOString() };
    if(clientId){const existing=await local.get('outbox:'+clientId);if(existing)return existing;}
    if(account!==owner)throw Error('Сессия завершена');
    if(upload){const blobKey='outbox-blob:'+item.id;await local.setBlob(blobKey,upload.file);item.upload={blobKey,kind:upload.kind};}
    if(account!==owner)throw Error('Сессия завершена');
    await box.enqueue(item); return item;
  }
  async function open(id, options = {}) {
    await start(state.me); const mine = ++opening, local = store, account = owner;
    const valid = () => mine === opening && account === owner && current(id);
    state.selectedMessages?.clear();
    state.activeRoomId = id; state.messages = []; state.roomMembers = []; state.activeDmPeer = null;
    state.roomLoading = true; state.syncing=true;state.roomLoadError = false; state.replyTo = null; state.editMessage = null;
    state.searchMode = false; state.searchResults = []; state.composeOpen = false; state.settingsOpen = false; state.uiMenu = null;
    state.composerText = drafts.get(id) || ''; state.newMessages = 0; state.chatHistory = null; state.lastReadId = null;
    await mount();
    const [cached, outgoing] = await Promise.all([local.get('messages:' + id, []), queue.items()]);
    if (!valid()) return;
    state.messages = mergeMessages(cached.filter(m=>!hidden.has(m.id)).slice(-100), outgoing.filter(x => x.room_id === id).map(pending));
    if (state.messages.length) { state.roomLoading = false; render(); }
    try {
      await syncHidden();if(!valid())return;
      const memberTask = request(`/rooms/${id}/members`).then(members => { if (valid()) state.roomMembers = members; }).catch(()=>{});
      const room = state.rooms.find(r => r.id === id);
      const peerTask = room?.other_user_id ? request(`/users/${room.other_user_id}`).then(peer => { if (valid()) state.activeDmPeer = peer; }).catch(()=>{}) : Promise.resolve();
      let result = await page(id);
      const saved = await local.get('position:' + id), anchor = options.latest ? null : options.around || saved || result.first_unread_id;
      if (anchor && !result.messages.some(m => m.id === anchor)) result = await page(id, { around: anchor }).catch(() => result);
      if (!valid()) return;
      await local.saveMessages(id, result.messages);result.messages=result.messages.filter(m=>!hidden.has(m.id)); await Promise.allSettled([memberTask, peerTask]);
      if (!valid()) return;
      state.chatHistory = result; state.messages = mergeMessages(result.messages, state.messages.filter(m => !hidden.has(m.id) && (m.delivery || (!m.delivery && m.room_id === id && new Date(m.created_at) > new Date(result.messages.at(-1)?.created_at || 0)))));
      state.roomLoading = false;state.syncing=false; await mount();
      const target = anchor && document.getElementById('msg-' + anchor), box = document.getElementById('msgs');
      if (target) target.scrollIntoView({ block: 'center' }); else if (box) box.scrollTop = box.scrollHeight;
      bind();
    } catch (error) { if (valid()) { state.roomLoading = false;state.syncing=false; state.roomLoadError = !state.messages.length; state.chatHistory ||= { has_older: true, has_newer: false }; await mount(); report(userError(error)); bind(); } }
  }
  async function older() {
    if (olderBusy || !current(state.activeRoomId) || state.chatHistory?.has_older === false) return;
    const id = state.activeRoomId, before = state.messages.find(m => !m.delivery)?.id, mine = opening; if (!before) return;
    olderBusy = true; state.loadingOlder = true; render();
    try { const result = await page(id, { before }); if (mine !== opening || !current(id)) return; await store.saveMessages(id, result.messages); state.messages = mergeMessages(result.messages, state.messages).filter(m=>!hidden.has(m.id)); const trimmed=state.messages.length>300;state.messages=state.messages.slice(0,300); state.chatHistory = { ...state.chatHistory, has_older: result.has_older,has_newer:trimmed || state.chatHistory?.has_newer }; }
    catch (error) { report(userError(error)); } finally { olderBusy = false; state.loadingOlder = false; if (current(id)) render(); }
  }
  async function newer() {
    if (newerBusy || !current(state.activeRoomId)) return;
    const id = state.activeRoomId, after = state.messages.filter(m => !m.delivery).at(-1)?.id, mine = opening; if (!after) return;
    newerBusy = true;
    try { const result = await page(id, { after }); if (mine !== opening || !current(id)) return; await store.saveMessages(id, result.messages); state.messages = mergeMessages(state.messages, result.messages).filter(m=>!hidden.has(m.id)); const trimmed=state.messages.length>300;state.messages=state.messages.slice(-300); state.chatHistory = { ...state.chatHistory, has_newer: result.has_newer,has_older:trimmed || state.chatHistory?.has_older }; render(); }
    catch (error) { report(userError(error)); } finally { newerBusy = false; }
  }
  async function jump(id) {
    if (!current()) return; const roomId = state.activeRoomId, mine = opening;
    if (!state.messages.some(m => m.id === id)) { const result = await page(roomId, { around: id }); if (mine !== opening || !current(roomId)) return; state.chatHistory = result; state.messages = result.messages; await store.saveMessages(roomId, result.messages); render(); }
    const target = document.getElementById('msg-' + id); target?.scrollIntoView({ behavior: 'smooth', block: 'center' }); target?.classList.add('search-highlight-current'); setTimeout(() => target?.classList.remove('search-highlight-current'), 1800);
  }
  async function received(message) {
    const account = owner, local = store,bag=hidden; if (!account || bag.has(message.id)) return;
    const prepared = await decode(message.room_id, message); if (owner !== account || state.me?.id !== account || bag.has(message.id)) return;
    await local.saveMessages(message.room_id, [prepared]);
    if(bag.has(message.id)){await local.removeMessage(message.room_id,message.id);return;}
    if(owner!==account || state.me?.id!==account)return;
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
    await syncHidden();if(owner!==account || roomId!==state.activeRoomId)return;
    const known = new Map((await store.get('messages:' + roomId, [])).map(m => [m.id, m]));
    const update = async () => { if (mine !== searching || roomId !== state.activeRoomId || owner !== account) return; state.searchResults = [...known.values()].filter(m => !hidden.has(m.id) && (m._text ?? m.text ?? '').toLowerCase().includes(query.toLowerCase())); state.currentSearchIndex = 0; await refresh(); };
    await update(); let before, count = 0;
    try {
      do { const result = await page(roomId, before ? { before, limit: 100 } : { limit: 100 }); if (mine !== searching || owner !== account || roomId !== state.activeRoomId) return;
        await store.saveMessages(roomId, result.messages); for (const message of result.messages) known.set(message.id, message); count += result.messages.length;
        state.searchProgress = `Проверено: ${count}`; await update(); before = result.has_older && result.messages[0]?.id;
      } while (before);
      state.searchProgress = 'Вся доступная история проверена'; await update();
    } catch (error) { if (mine === searching) report(userError(error)); }
  }
  window.addEventListener('online', () => { void syncHidden();void queue?.flush(); if (current()) void loadRooms().catch(() => {}); });
  document.addEventListener('visibilitychange', () => { if (!document.hidden) { void syncHidden();void queue?.flush(); bind(); } });
  return { hide,syncHidden,start, stop, saveRooms, draft, enqueue, open, older, newer, jump, received, bind, search, cancelSearch: () => { ++searching; },
    retry: id => queue?.retry(id), cancel: id => queue?.cancel(id), flush: () => queue?.flush(), local: () => store };
}
