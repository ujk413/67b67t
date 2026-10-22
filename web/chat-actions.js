import { html } from './ui.js';
import { userError } from './errors.js';
export function createChatActions({ state, request, runtime, report, mount, buildText, decrypt, openViewer }) {
  const label = room => room.is_saved ? 'Избранное' : room.title || room.other_display_name || room.other_username || 'Чат';
  const close = node => { node.close(); node.remove(); };
  async function preference(id, changes) { try { await request(`/rooms/${id}/preferences`, { method: 'PUT', body: JSON.stringify(changes) }); const room = state.rooms.find(r => r.id === id); if (room) room.preferences = { ...room.preferences, ...changes }; state.uiMenu = null; await runtime.saveRooms(state.rooms); await mount(); } catch (e) { report(userError(e)); } }
  function roomMenu(id, event) {
    event.preventDefault(); event.stopPropagation(); document.getElementById('roomActions')?.remove();
    const room = state.rooms.find(r => r.id === id); if (!room) return;
    const node = document.createElement('dialog'); node.id = 'roomActions'; node.className = 'actions-dialog';
    const title = document.createElement('h2'); title.textContent = label(room); node.append(title);
    const add = (text, changes) => { const b = document.createElement('button'); b.textContent = text; b.onclick = () => { close(node); void preference(id, changes); }; node.append(b); };
    add(room.preferences?.pinned ? 'Открепить' : 'Закрепить', { pinned: !room.preferences?.pinned });
    add(room.preferences?.muted ? 'Включить уведомления' : 'Отключить уведомления', { muted: !room.preferences?.muted });
    add(room.preferences?.archived ? 'Вернуть из архива' : 'В архив', { archived: !room.preferences?.archived });
    add('Отметить непрочитанным', { marked_unread: true });
    const done = document.createElement('button'); done.textContent = 'Закрыть'; done.onclick = () => close(node); node.append(done); document.body.append(node); node.showModal();
  }
  function select(id) { state.selectedMessages ||= new Set(); if (state.selectedMessages.has(id)) state.selectedMessages.delete(id); else state.selectedMessages.add(id); void mount(); }
  async function copySelected() { try { await navigator.clipboard.writeText(state.messages.filter(m => state.selectedMessages?.has(m.id)).map(m => m._text ?? m.text ?? 'Вложение').join('\n')); report('Сообщения скопированы'); } catch (e) { report(userError(e)); } }
  async function removeSelected() {
    const roomId = state.activeRoomId, items = state.messages.filter(m => state.selectedMessages?.has(m.id) && m.sender_id === state.me.id);
    if (!items.length || !confirm(`Удалить ${items.length} сообщений у всех участников?`)) return;
    try { for (const message of items) { await request(`/rooms/${roomId}/messages/${message.id}`, { method: 'DELETE' }); await runtime.local()?.removeMessage(roomId, message.id); if (state.activeRoomId === roomId) { state.messages = state.messages.filter(m => m.id !== message.id); state.selectedMessages.delete(message.id); } } await mount(); }
    catch (e) { report(userError(e)); await mount(); }
  }
  function forward(ids) {
    const messages = state.messages.filter(m => ids.includes(m.id)), owner = state.me?.id;
    const dialog = document.createElement('dialog'); dialog.className = 'actions-dialog'; const title = document.createElement('h2'); title.textContent = 'Переслать в чат'; dialog.append(title);
    for (const room of state.rooms.filter(r => !r.preferences?.hidden)) {
      const b = document.createElement('button'); b.textContent = label(room); b.onclick = async () => {
        b.disabled = true;
        try {
          for (const message of messages) {
            if (owner !== state.me?.id) throw new Error('Сессия завершена');
            if (message.type === 'text') await runtime.enqueue({ roomId: room.id, text: message._text ?? message.text ?? '' });
            else {
              const meta = { ...message.meta, forwarded: true }, info = meta.media_encryption;
              if (info) { const secret = await decrypt(message.room_id, { ...info.key_message, type: 'text', sender_id: message.sender_id, room_id: message.room_id, id: message.id + '-key' }); const wrapped = await buildText(room.id, room, secret); meta.media_encryption = { ...info, key_message: wrapped }; }
              delete meta.caption_message;
              await runtime.enqueue({ roomId: room.id, text: message.text || '', body: { type: message.type, media_id: message.media_id, text: message.text, meta } });
            }
          }
          state.selectedMessages?.clear(); close(dialog); report('Сообщения добавлены в очередь отправки'); await mount();
        } catch (e) { b.disabled = false; report(userError(e)); }
      }; dialog.append(b);
    }
    const cancel = document.createElement('button'); cancel.textContent = 'Отмена'; cancel.onclick = () => close(dialog); dialog.append(cancel); document.body.append(dialog); dialog.showModal();
  }
  async function sharedMedia() {
    const roomId = state.activeRoomId, dialog = document.createElement('dialog'); dialog.className = 'media-dialog';
    dialog.innerHTML = '<h2>Общие вложения</h2><label>Показать <select aria-label="Тип вложений"><option value="all">Все</option><option value="image">Фото</option><option value="video">Видео</option><option value="file">Файлы</option></select></label><div class="shared-media-list"></div>';
    const list = dialog.querySelector('.shared-media-list'), filter = dialog.querySelector('select'); let items = [], before, busy = false, requestId = 0;
    const more = document.createElement('button'); more.textContent = 'Загрузить ещё'; more.className = 'btn';
    const done = document.createElement('button'); done.textContent = 'Закрыть'; done.className = 'btn'; done.onclick = () => { ++requestId; close(dialog); }; dialog.append(more, done); document.body.append(dialog); dialog.showModal();
    async function load(reset = false) {
      if (busy && !reset) return; const mine = ++requestId; busy = true; more.disabled = true;
      if (reset) { before = null; items = []; list.replaceChildren(); }
      try { const next = await request(`/rooms/${roomId}/media?${new URLSearchParams({ kind: filter.value, ...(before ? { before } : {}) })}`); if (mine !== requestId) return;
        items.push(...next); before = next.at(-1)?.id; more.hidden = next.length < 100;
        for (const message of next) { const row = document.createElement('button'); row.className = 'shared-media-item'; row.textContent = `${message.meta?.media_encryption?.name || message.meta?.filename || message.text || message.media_mime || 'Вложение'} · ${new Date(message.created_at).toLocaleDateString('ru-RU')}`; row.onclick = () => void openViewer(message, items); list.append(row); }
      } catch (e) { report(userError(e)); } finally { if (mine === requestId) { busy = false; more.disabled = false; } }
    }
    filter.onchange = () => void load(true); more.onclick = () => void load(); await load();
  }
  return { preference, roomMenu, select, copySelected, removeSelected, forward, sharedMedia };
}
