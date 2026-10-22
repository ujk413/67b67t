import { userError } from './errors.js';
export function createFolderManager({ state, request, refresh, openRoom, report }) {
  async function saved() { try { const room = await request('/me/saved', { method: 'POST', body: '{}' }); await refresh(); await openRoom(room.id); } catch (e) { report(userError(e)); } }
  function edit(id) {
    const owner = state.me?.id, current = state.folders?.find(f => f.id === id);
    const dialog = document.createElement('dialog'); dialog.className = 'media-dialog';
    const heading = document.createElement('h2'); heading.textContent = current ? 'Изменить папку' : 'Новая папка';
    const label = document.createElement('label'); label.textContent = 'Название папки'; const input = document.createElement('input'); input.className = 'input'; input.maxLength = 40; input.value = current?.title || ''; label.append(input);
    const list = document.createElement('div'); list.className = 'folder-chat-options'; const selected = new Set(current?.room_ids || []);
    for (const room of state.rooms.filter(r => !r.preferences?.hidden)) { const row = document.createElement('label'), check = document.createElement('input'); check.type = 'checkbox'; check.checked = selected.has(room.id); check.onchange = () => check.checked ? selected.add(room.id) : selected.delete(room.id); row.append(check, document.createTextNode(room.is_saved ? 'Избранное' : room.title || room.other_display_name || room.other_username || 'Чат')); list.append(row); }
    const controls = document.createElement('div'); controls.className = 'recording-controls';
    const done = document.createElement('button'); done.className = 'btn primary'; done.textContent = 'Сохранить'; done.onclick = async () => { if (owner !== state.me?.id) return; done.disabled = true; try { await request('/me/folders' + (id ? '/' + id : ''), { method: id ? 'PUT' : 'POST', body: JSON.stringify({ title: input.value, room_ids: [...selected] }) }); await refresh(); dialog.close(); dialog.remove(); } catch (e) { report(userError(e)); done.disabled = false; } };
    const cancel = document.createElement('button'); cancel.className = 'btn'; cancel.textContent = 'Отмена'; cancel.onclick = () => { dialog.close(); dialog.remove(); };
    controls.append(cancel, done);
    if (current) { const remove = document.createElement('button'); remove.className = 'btn'; remove.textContent = 'Удалить папку'; remove.onclick = async () => { if (owner !== state.me?.id || !confirm('Удалить папку? Переписки сохранятся.')) return; try { await request('/me/folders/' + id, { method: 'DELETE' }); if (state.selectedFolder === id) state.selectedFolder = null; await refresh(); dialog.close(); dialog.remove(); } catch (e) { report(userError(e)); } }; controls.prepend(remove); }
    dialog.append(heading, label, list, controls); document.body.append(dialog); dialog.showModal(); input.focus();
  }
  return { saved, edit };
}
