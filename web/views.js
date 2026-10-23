import {html as escapeUi} from './ui.js';
import {t as uiText} from "./i18n.js";
import { html, icon, avatar, presenceLabel, plural, listTime } from './ui.js';

const mediaUrl = id => id ? `/api/media/${encodeURIComponent(id)}` : '';
const name = person => person?.display_name || person?.username || uiText("Пользователь");
export const iconButton = (glyph, label, action, extra = '') => `<button type="button" class="icon-button ${extra}" title="${html(label)}" aria-label="${html(label)}" onclick="${action}">${icon(glyph)}</button>`;
const menuItem = (label, action, glyph = '') => `<button type="button" role="menuitem" onclick="${action}">${glyph ? icon(glyph) : ''}<span>${html(label)}</span></button>`;

export function renderLogin() {
  return `<main class="auth-screen"><section class="auth-card card" aria-labelledby="authTitle">
    <img class="brand-logo" src="/icons/app-192.png" width="72" height="72" alt="67">
    <h1 id="authTitle">${escapeUi(uiText("Вход в 67"))}</h1><p class="auth-caption">${escapeUi(uiText("Ваши люди. Ваши разговоры."))}</p>
    <form class="col" onsubmit="event.preventDefault();login()">
      <label for="u">${escapeUi(uiText("Имя пользователя"))}</label><input class="input" id="u" placeholder="username" autocomplete="username" required>
      <label for="p">${escapeUi(uiText("Пароль"))}</label><input class="input" id="p" type="password" placeholder="${escapeUi(uiText("Введите пароль"))}" autocomplete="current-password" required>
      <div id="captcha-container" style="display:none"><div class="h-captcha" data-sitekey="11dc04d6-78e8-44f8-9814-c5c032fc68be" data-callback="onCaptchaSuccess"></div></div>
      <p id="authError" class="form-error" role="alert" hidden></p><button class="btn primary auth-submit" type="submit">${escapeUi(uiText("Войти"))}</button>
      <button class="btn auth-register" type="button" onclick="showRegister(event)">${escapeUi(uiText("Создать аккаунт"))}</button>
      <button class="btn" type="button" onclick="openBrowserAccounts()">${escapeUi(uiText("Аккаунты"))}</button>
      <button class="btn" type="button" onclick="startQrLogin()">${escapeUi(uiText("Войти по QR"))}</button>
      <button class="btn" type="button" onclick="importEncryptedBackup()">${escapeUi(uiText("Восстановить ключи и войти"))}</button>
      <button class="btn" type="button" onclick="cancelWebBackupRestore()">${escapeUi(uiText("Отменить восстановление"))}</button>
    </form>
    <button class="theme-option" onclick="toggleTheme()">${escapeUi(uiText("Сменить оформление"))}</button>
  </section></main>`;
}

export function renderRoomRow(room, me) {
  const direct = room.kind === 'dm' && !room.is_saved;
  const title = room.is_saved ? uiText("Избранное") : direct ? (room.other_display_name || room.other_username || uiText("Личные сообщения")) : (room.title || (room.kind === 'channel' ? uiText("Канал") : uiText("Группа")));
  let preview = room._lastText || room.last_msg_text || '';
  const encrypted = room.last_msg_encrypted_content || room.last_msg_meta?.encrypted || room.last_msg_meta?.e2e;
  if (!preview && encrypted) preview = uiText("Зашифрованное сообщение");
  if (!preview) preview = ({ voice: uiText("Голосовое сообщение"), video_note: uiText("Видеокружок"), media: uiText("Вложение"), call: uiText("Звонок") })[room.last_msg_type] || uiText("Нет сообщений");
  if (room.last_msg_sender_id === me?.id && (room._lastText || room.last_msg_text)) preview = (""+uiText("Вы:")+" ") + preview;
  else if (!direct && room.last_msg_sender_username) preview = (room.last_msg_sender_name || room.last_msg_sender_username) + ': ' + preview;
  const unread = Math.max(room.preferences?.marked_unread ? 1 : 0, Number(room.unread_count) || 0);
  const muted = !!room.preferences?.muted;
  const time = listTime(room.last_msg_at);
  return `<button type="button" class="item chat-row ${room.id === me?.activeRoomId ? 'active' : ''} ${muted ? 'chat-row-muted' : ''}" data-room-id="${html(room.id)}" onclick="selectRoom('${html(room.id)}')" oncontextmenu="openRoomActions('${html(room.id)}',event)">
    ${avatar(direct ? room.other_user_id : room.id, title, mediaUrl(room.avatar_id || (direct && room.other_avatar_id)), 'chat-avatar')}
    <span class="itemText"><span class="title">${html(title)}</span><span class="meta">${html(preview)}</span></span>
    <span class="chat-row-meta"><time class="itemTime">${html(time)}</time>${unread ? `<span class="unread-badge${muted ? ' muted' : ''}" aria-label="Непрочитанных: ${unread}${muted ? uiText(", чат без звука") : ''}">${unread > 99 ? '99+' : unread}</span>` : ''}</span>
  </button>`;
}

function renderSettings(state) {
  return `<section class="web-settings" aria-label="${escapeUi(uiText("Настройки"))}">
    <button class="profile-entry" onclick="showMyProfile()">${avatar(state.me.id, name(state.me), mediaUrl(state.me.avatar_id))}<span><strong>${html(name(state.me))}</strong><small>@${html(state.me.username)}</small></span></button>
    <h2>${escapeUi(uiText("Оформление"))}</h2><div class="theme-choices" role="group" aria-label="${escapeUi(uiText("Тема"))}">
      <button class="${!state.darkTheme ? 'selected' : ''}" aria-pressed="${!state.darkTheme}" onclick="setWebTheme('light')"><span class="theme-sample theme-light"></span>${escapeUi(uiText("Светлая"))}</button>
      <button class="${state.darkTheme ? 'selected' : ''}" aria-pressed="${state.darkTheme}" onclick="setWebTheme('dark')"><span class="theme-sample theme-dark"></span>${escapeUi(uiText("Тёмная"))}</button>
    </div>
    <button class="settings-action" onclick="setWebTheme('system')">${state.themeMode === 'system' ? '✓ ' : ''}${escapeUi(uiText("Как в системе"))}</button>
    <button class="settings-action" onclick="openQualityTool()">${escapeUi(uiText("Качество, язык и оформление"))}</button>
    <label class="settings-action">${escapeUi(uiText("Размер сообщений"))} <input type="range" min="12" max="24" value="${state.fontSize}" oninput="setMessageFontSize(this.value)" aria-label="${escapeUi(uiText("Размер текста сообщений"))}"></label>
    <label class="settings-action"><input type="checkbox" ${state.autoDownload ? 'checked' : ''} onchange="setAutoDownload(this.checked)"> ${escapeUi(uiText("Автозагрузка фото"))}</label>
    <h2>${escapeUi(uiText("Звонки"))}</h2><button class="settings-action" onclick="openCallTools('history')">${escapeUi(uiText("История и настройки звонков"))}</button><h2>${escapeUi(uiText("Уведомления"))}</h2><button class="settings-action" onclick="requestNotificationPermission()">${icon('chats')}<span>${escapeUi(uiText("Уведомления"))}<small id="notificationSettingStatus">${state.notificationsEnabled ? uiText("Включены") : uiText("Включить в этом браузере")}</small></span></button>
    <button class="settings-action" onclick="testWebNotification()">${escapeUi(uiText("Проверить уведомления"))}</button>
    <button class="settings-action" onclick="checkConnection()">${escapeUi(uiText("Проверить соединение и звонки"))}</button>
    <h2>${escapeUi(uiText("Папки чатов"))}</h2><button class="settings-action" onclick="manageFolderOrder()">${escapeUi(uiText("Изменить порядок и счётчики"))}</button><button class="settings-action" onclick="editChatFolder()">${escapeUi(uiText("Создать папку"))}</button>${(state.folders || []).map(folder => `<button class="settings-action" onclick="editChatFolder('${html(folder.id)}')">${html(folder.title)}${folder.unread_count?' · '+html(folder.unread_count):''}</button>`).join('')}
    <h2>${escapeUi(uiText("Приватность и аккаунт"))}</h2>${[['contacts',uiText("Контакты")],['privacy',uiText("Настройки приватности")],['blocked',uiText("Заблокированные")],['security',uiText("Двухэтапная защита")],['aliases',uiText("Имя пользователя и алиасы")],['backup',uiText("Защищённый бэкап")],['qr',uiText("Мой QR")],['transport',uiText("Сеть и прокси")],['secret',uiText("Секретные чаты")],['delete',uiText("Удалить аккаунт")]].map(([mode,label])=>`<button class="settings-action" onclick="openAccountTool('${mode}')">${label}</button>`).join('')}<button class="settings-action" onclick="openHiddenChats()">${escapeUi(uiText("Скрытые чаты"))}</button><button class="settings-action" onclick="openConversationTool('communities')">${escapeUi(uiText("Публичные сообщества"))}</button>
    <button class="settings-action" onclick="openSessionsPanel()">${icon('settings')}<span>${escapeUi(uiText("Активные устройства"))}</span></button>
    <button class="settings-action" onclick="openBackupPanel()">${icon('attach')}<span>${escapeUi(uiText("Резервная копия ключей"))}</span></button>
    ${state.me.is_admin ? `<button class="settings-action" onclick="openAdminPanel()">${escapeUi(uiText("Управление пользователями"))}</button>` : ''}
    <button class="settings-action danger-text" onclick="logout()">${escapeUi(uiText("Выйти из аккаунта"))}</button>
  </section>`;
}

function renderNewChat(state) {
  return `<section class="new-chat-panel" aria-label="${escapeUi(uiText("Новая переписка"))}"><div class="people-search"><label class="sr-only" for="peopleSearch">${escapeUi(uiText("Поиск людей"))}</label>${icon('search')}<input class="input" id="peopleSearch" placeholder="${escapeUi(uiText("Имя пользователя"))}" value="${html(state.searchQ)}" oninput="onSearchInput(this.value)" autocomplete="off"></div>
    <div class="create-options"><button onclick="openCreateRoom('group')">${escapeUi(uiText("Новая группа"))}</button><button onclick="openCreateRoom('channel')">${escapeUi(uiText("Новый канал"))}</button></div>
    <div class="people-results">${state.searching ? `<p class="list-hint" role="status">${escapeUi(uiText("Поиск…"))}</p>` : state.peopleResults.length ? state.peopleResults.map(person => `<button class="item chat-row" onclick="startDM('${html(person.username)}')">${avatar(person.id, name(person), mediaUrl(person.avatar_id))}<span class="itemText"><strong>${html(name(person))}</strong><small>@${html(person.username)}</small></span></button>`).join('') : `<p class="list-hint">${state.searchQ.trim() ? uiText("Пользователи не найдены") : uiText("Найдите человека по имени пользователя")}</p>`}</div></section>`;
}

export function renderMessengerShell(state, compose) {
  const room = state.rooms.find(item => item.id === state.activeRoomId);
  const direct = room?.kind === 'dm' && !room.is_saved;
  const peer = state.activeDmPeer || state.roomMembers.find(person => person.id !== state.me.id);
  const title = room?.is_saved ? uiText("Избранное") : direct ? (name(peer) === uiText("Пользователь") ? room.other_display_name || room.other_username || uiText("Личные сообщения") : name(peer)) : room?.title || uiText("Чат");
  const subtitle = direct ? presenceLabel(state.activeDmPeer) : `${state.roomMembers.length} ${plural(state.roomMembers.length, uiText("участник"), uiText("участника"), uiText("участников"))}`;
  const profileAction = direct ? (peer ? `showProfile('${html(peer.id)}')` : '') : 'openRoomSettings()';
  const roomRows = state.rooms.filter(r => !r.preferences?.hidden && (!!r.preferences?.archived === !!state.archiveOpen) && (!state.selectedFolder || state.folders?.find(f => f.id === state.selectedFolder)?.room_ids.includes(r.id))).sort((a,b) => Number(!!b.preferences?.pinned)-Number(!!a.preferences?.pinned)).map(item => renderRoomRow(item, { ...state.me, activeRoomId: state.activeRoomId })).join('');
  return `<div class="shell ${state.activeRoomId ? 'show-chat' : 'show-list'}">
    <aside class="card sidebar" aria-label="${escapeUi(uiText("Переписки"))}"><header class="topbar sidebar-header"><h1>${state.settingsOpen ? uiText("Настройки") : state.composeOpen ? uiText("Новое сообщение") : state.archiveOpen ? uiText("Архив") : uiText("Чаты")}</h1>
      ${state.composeOpen ? iconButton('close', uiText("Закрыть поиск"), 'openChatList()') : `<button class="icon-button" id="accountMenuToggle" aria-label="${escapeUi(uiText("Меню чатов"))}" aria-expanded="${state.uiMenu === 'account'}" onclick="toggleWebMenu('account',event)">${icon('more')}</button>`}
      ${state.uiMenu === 'account' ? `<div class="web-popover" role="menu">${menuItem(uiText("Избранное"), 'closeWebMenu();openSavedMessages()')}${menuItem(uiText("Создать папку"),'closeWebMenu();editChatFolder()')}${menuItem(uiText("Порядок папок"),'closeWebMenu();manageFolderOrder()')}${menuItem(uiText("Поиск по всем сообщениям"),'closeWebMenu();openGlobalHistorySearch()')}${menuItem(uiText("Прочитать все чаты"),'closeWebMenu();markAllRoomsRead()')}${menuItem(uiText("Скрытые чаты"),'closeWebMenu();openHiddenChats()')}${menuItem(uiText("Публичные сообщества"),"closeWebMenu();openConversationTool('communities')")}${menuItem(state.archiveOpen ? uiText("Все чаты") : uiText("Архив"), 'closeWebMenu();openArchive()', 'chats')}${menuItem(uiText("Новая группа"), "closeWebMenu();openCreateRoom('group')", 'chats')}${menuItem(uiText("Новый канал"), "closeWebMenu();openCreateRoom('channel')", 'compose')}${menuItem(uiText("Сменить тему"), 'closeWebMenu();toggleTheme()', 'settings')}${menuItem(uiText("Мой профиль"), 'closeWebMenu();showMyProfile()')}</div>` : ''}
    </header>
    ${!state.settingsOpen && !state.composeOpen && state.folders?.length ? `<div class="folder-tabs" role="tablist" aria-label="${escapeUi(uiText("Папки чатов"))}"><button role="tab" aria-selected="${!state.selectedFolder}" onclick="selectChatFolder(null)">${escapeUi(uiText("Все чаты"))}</button>${state.folders.map(folder => `<button role="tab" aria-selected="${state.selectedFolder === folder.id}" onclick="selectChatFolder('${html(folder.id)}')" oncontextmenu="event.preventDefault();editChatFolder('${html(folder.id)}')">${html(folder.title)}${folder.unread_count?' · '+html(folder.unread_count):''}</button>`).join('')}</div>` : ''}
    ${state.settingsOpen ? renderSettings(state) : state.composeOpen ? renderNewChat(state) : `<div class="list room-list" id="roomList">${roomRows || `<div class="empty-list"><strong>${escapeUi(uiText("Здесь будут ваши чаты"))}</strong><p>${escapeUi(uiText("Начните разговор с помощью кнопки «Написать»."))}</p></div>`}</div><button class="compose-fab" onclick="openNewChat()" aria-label="${escapeUi(uiText("Написать сообщение"))}">${icon('compose')}<span>${escapeUi(uiText("Написать"))}</span></button>`}
    <nav class="sidebar-nav" aria-label="${escapeUi(uiText("Основная навигация"))}"><button class="${!state.settingsOpen ? 'active' : ''}" aria-current="${!state.settingsOpen ? 'page' : 'false'}" onclick="openChatList()">${icon('chats')}<span>${escapeUi(uiText("Чаты"))}</span></button><button class="${state.settingsOpen ? 'active' : ''}" aria-current="${state.settingsOpen ? 'page' : 'false'}" onclick="openWebSettings()">${icon('settings')}<span>${escapeUi(uiText("Настройки"))}</span></button></nav>
    </aside>
    <main class="card main" aria-label="${escapeUi(uiText("Переписка"))}">${state.activeRoomId ? `<header class="chatHead"><div class="chat-identity">${iconButton('back', uiText("К списку чатов"), 'backToList()', 'back')}<button class="peer-profile" onclick="${profileAction}">${avatar(direct ? peer?.id || room?.other_user_id : room?.id, title, mediaUrl(direct ? peer?.avatar_id || room?.other_avatar_id : room?.avatar_id), 'header-avatar')}<span class="chatTitle"><strong>${html(title)}</strong><small class="mini" id="chatPresence">${html(subtitle)}</small></span></button></div><div class="chat-actions">${direct ? iconButton('phone', uiText("Аудиозвонок"), 'startRoomCall(false)') + iconButton('video', uiText("Видеозвонок"), 'startRoomCall(true)') : ''}<button class="icon-button" id="chatMenuToggle" aria-label="${escapeUi(uiText("Меню переписки"))}" aria-expanded="${state.uiMenu === 'chat'}" onclick="toggleWebMenu('chat',event)">${icon('more')}</button></div>
      ${state.uiMenu === 'chat' ? `<div class="web-popover" role="menu">${menuItem(room?.preferences?.pinned ? uiText("Открепить") : uiText("Закрепить"), `setChatPreference('${html(room.id)}',{pinned:${!room?.preferences?.pinned}})`)}${menuItem(room?.preferences?.muted ? uiText("Включить уведомления") : uiText("Отключить уведомления"), `setChatPreference('${html(room.id)}',{muted:${!room?.preferences?.muted}})`)}${menuItem(room?.preferences?.archived ? uiText("Вернуть из архива") : uiText("В архив"), `setChatPreference('${html(room.id)}',{archived:${!room?.preferences?.archived}})`)}${menuItem(uiText("Общие вложения"), 'closeWebMenu();openSharedMedia()')}${menuItem(uiText("Поиск в переписке"), 'closeWebMenu();toggleSearch()', 'search')}${menuItem(uiText("Перейти к дате"),'closeWebMenu();jumpToDate()')}${menuItem(uiText("К непрочитанным"),'closeWebMenu();jumpToUnread()')}${menuItem(uiText("Прочитать чат"),'closeWebMenu();markRoomRead()')}${!direct?menuItem(uiText("Групповой звонок"),"closeWebMenu();openCallTools('group')"):''}${menuItem(uiText("Все закрепления"),"closeWebMenu();openConversationTool('pins')")}${menuItem(uiText("Черновики и конфликты"),'closeWebMenu();openDraftConflict()')}${menuItem(uiText("Отложенные и напоминания"),"closeWebMenu();openConversationTool('scheduled')")}${menuItem(uiText("Экспорт чата"),"closeWebMenu();openExpressiveTool('export')")}${!direct?menuItem(uiText("Участники и подписчики"),"closeWebMenu();openConversationTool('members')")+menuItem(uiText("E2E, медленный режим и публичность"),"closeWebMenu();openConversationTool('settings')"):''}${menuItem(room?.preferences?.hidden?uiText("Показать чат"):uiText("Скрыть чат"),`setChatPreference('${html(room.id)}',{hidden:${!room?.preferences?.hidden}})`)}${menuItem(direct ? uiText("Профиль собеседника") : uiText("Информация"), `closeWebMenu();${profileAction}`, 'settings')}${direct ? menuItem(uiText("Удалить переписку"), 'closeWebMenu();deleteDMChat()') : ''}</div>` : ''}</header>
      ${(() => {
        // Pinned message bar, like Telegram's: one line, tap to jump, ✕ to unpin.
        const pinnedId = room?.pinned_message_id;
        const pinned = pinnedId ? state.messages?.find(m => m.id === pinnedId) : null;
        if (!pinnedId) return '';
        const text = pinned ? (pinned._text || pinned.text || uiText("Вложение")) : uiText("Закреплённое сообщение");
        const canUnpin = direct || ['owner', 'admin'].includes(room?.my_role);
        return `<div class="pinned-bar" role="status"><span class="pinned-bar-line" aria-hidden="true"></span><button type="button" class="pinned-bar-text" onclick="scrollToMessage('${html(pinnedId)}', event)"><strong>${escapeUi(uiText("Закреплённое сообщение"))}</strong><span>${html(String(text).slice(0, 120))}</span></button><button type="button" class="pinned-bar-all" onclick="openConversationTool('pins')" aria-label="${escapeUi(uiText("Все закрепления"))}">${escapeUi(uiText("Все"))}</button>${canUnpin ? `<button type="button" class="icon-button" aria-label="${escapeUi(uiText("Открепить"))}" onclick="unpinMessage()">${icon('close')}</button>` : ''}</div>`;
      })()}
      <div class="connection-strip" id="webConnection" role="status" ${state.wsAuthed && !state.syncing ? 'hidden' : ''}>${!navigator.onLine?uiText("Нет сети"):!state.wsAuthed?uiText("Подключение…"):uiText("Синхронизация…")}</div>
      ${state.searchMode ? `<div class="searchBar"><label class="sr-only" for="searchInput">${escapeUi(uiText("Поиск по сообщениям"))}</label><input id="searchInput" class="input" placeholder="${escapeUi(uiText("Поиск в переписке"))}" value="${html(state.searchQuery)}" oninput="searchMessages(this.value)"><div class="searchControls"><span class="searchCounter">${state.searchResults.length ? `${state.currentSearchIndex + 1} / ${state.searchResults.length}` : state.searchQuery.length >= 2 ? uiText("Ничего не найдено") : ''}</span><button class="icon-button" onclick="prevSearchResult()" aria-label="${escapeUi(uiText("Предыдущий результат"))}">↑</button><button class="icon-button" onclick="nextSearchResult()" aria-label="${escapeUi(uiText("Следующий результат"))}">↓</button><span role="status">${html(state.searchProgress || '')}</span><button onclick="cancelHistorySearch()">${escapeUi(uiText("Остановить"))}</button>${iconButton('close', uiText("Закрыть поиск"), 'toggleSearch()')}</div></div>` : ''}
      ${state.selectedMessages?.size ? `<div class="selection-bar"><button onclick="clearMessageSelection()">${escapeUi(uiText("Отмена"))}</button><button onclick="selectAllMessages()">${escapeUi(uiText("Выбрать все"))}</button><strong>${escapeUi(uiText("Выбрано:"))} ${state.selectedMessages.size}</strong><button onclick="copySelectedMessages()">${escapeUi(uiText("Копировать"))}</button><button onclick="forwardSelectedMessages()">${escapeUi(uiText("Переслать"))}</button><button onclick="deleteSelectedMessages()">${escapeUi(uiText("Удалить свои"))}</button></div>` : ''}<div class="history-container"><div id="msgs" class="messages" aria-label="${escapeUi(uiText("Сообщения"))}" aria-busy="${state.roomLoading}"></div><button id="chatJumpDown" class="chat-jump-down" onclick="jumpToLatest()" aria-label="${escapeUi(uiText("К новым сообщениям"))}" ${!state.chatHistory?.has_newer ? 'hidden' : ''}>↓${state.newMessages ? ` · ${state.newMessages}` : ''}</button></div>${compose(room)}
    ` : `<section class="conversation-empty"><img src="/icons/app-192.png" width="64" height="64" alt="67"><h2>${escapeUi(uiText("Ваши разговоры — здесь"))}</h2><p>${escapeUi(uiText("Выберите чат или начните новую переписку."))}</p><button class="btn primary" onclick="openNewChat()">${escapeUi(uiText("Написать сообщение"))}</button></section>`}</main>
  </div>`;
}
