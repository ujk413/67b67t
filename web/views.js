import { html, icon, avatar, presenceLabel, plural, listTime } from './ui.js';

const mediaUrl = id => id ? `/api/media/${encodeURIComponent(id)}` : '';
const name = person => person?.display_name || person?.username || 'Пользователь';
export const iconButton = (glyph, label, action, extra = '') => `<button type="button" class="icon-button ${extra}" title="${html(label)}" aria-label="${html(label)}" onclick="${action}">${icon(glyph)}</button>`;
const menuItem = (label, action, glyph = '') => `<button type="button" role="menuitem" onclick="${action}">${glyph ? icon(glyph) : ''}<span>${html(label)}</span></button>`;

export function renderLogin() {
  return `<main class="auth-screen"><section class="auth-card card" aria-labelledby="authTitle">
    <img class="brand-logo" src="/icons/app-192.png" width="72" height="72" alt="67">
    <h1 id="authTitle">Вход в 67</h1><p class="auth-caption">Ваши люди. Ваши разговоры.</p>
    <form class="col" onsubmit="event.preventDefault();login()">
      <label for="u">Имя пользователя</label><input class="input" id="u" placeholder="username" autocomplete="username" required>
      <label for="p">Пароль</label><input class="input" id="p" type="password" placeholder="Введите пароль" autocomplete="current-password" required>
      <div id="captcha-container" style="display:none"><div class="h-captcha" data-sitekey="11dc04d6-78e8-44f8-9814-c5c032fc68be" data-callback="onCaptchaSuccess"></div></div>
      <p id="authError" class="form-error" role="alert" hidden></p><button class="btn primary auth-submit" type="submit">Войти</button>
      <button class="btn auth-register" type="button" onclick="showRegister(event)">Создать аккаунт</button>
    </form>
    <button class="theme-option" onclick="toggleTheme()">Сменить оформление</button>
  </section></main>`;
}

export function renderRoomRow(room, me) {
  const direct = room.kind === 'dm' && !room.is_saved;
  const title = room.is_saved ? 'Избранное' : direct ? (room.other_display_name || room.other_username || 'Личные сообщения') : (room.title || (room.kind === 'channel' ? 'Канал' : 'Группа'));
  let preview = room._lastText || room.last_msg_text || '';
  const encrypted = room.last_msg_encrypted_content || room.last_msg_meta?.encrypted || room.last_msg_meta?.e2e;
  if (!preview && encrypted) preview = 'Зашифрованное сообщение';
  if (!preview) preview = ({ voice: 'Голосовое сообщение', video_note: 'Видеокружок', media: 'Вложение', call: 'Звонок' })[room.last_msg_type] || 'Нет сообщений';
  if (room.last_msg_sender_id === me?.id && (room._lastText || room.last_msg_text)) preview = 'Вы: ' + preview;
  else if (!direct && room.last_msg_sender_username) preview = (room.last_msg_sender_name || room.last_msg_sender_username) + ': ' + preview;
  const unread = Math.max(room.preferences?.marked_unread ? 1 : 0, Number(room.unread_count) || 0);
  const muted = !!room.preferences?.muted;
  const time = listTime(room.last_msg_at);
  return `<button type="button" class="item chat-row ${room.id === me?.activeRoomId ? 'active' : ''} ${muted ? 'chat-row-muted' : ''}" data-room-id="${html(room.id)}" onclick="selectRoom('${html(room.id)}')" oncontextmenu="openRoomActions('${html(room.id)}',event)">
    ${avatar(direct ? room.other_user_id : room.id, title, mediaUrl(room.avatar_id || (direct && room.other_avatar_id)), 'chat-avatar')}
    <span class="itemText"><span class="title">${html(title)}</span><span class="meta">${html(preview)}</span></span>
    <span class="chat-row-meta"><time class="itemTime">${html(time)}</time>${unread ? `<span class="unread-badge${muted ? ' muted' : ''}" aria-label="Непрочитанных: ${unread}${muted ? ', чат без звука' : ''}">${unread > 99 ? '99+' : unread}</span>` : ''}</span>
  </button>`;
}

function renderSettings(state) {
  return `<section class="web-settings" aria-label="Настройки">
    <button class="profile-entry" onclick="showMyProfile()">${avatar(state.me.id, name(state.me), mediaUrl(state.me.avatar_id))}<span><strong>${html(name(state.me))}</strong><small>@${html(state.me.username)}</small></span></button>
    <h2>Оформление</h2><div class="theme-choices" role="group" aria-label="Тема">
      <button class="${!state.darkTheme ? 'selected' : ''}" aria-pressed="${!state.darkTheme}" onclick="setWebTheme('light')"><span class="theme-sample theme-light"></span>Светлая</button>
      <button class="${state.darkTheme ? 'selected' : ''}" aria-pressed="${state.darkTheme}" onclick="setWebTheme('dark')"><span class="theme-sample theme-dark"></span>Тёмная</button>
    </div>
    <button class="settings-action" onclick="setWebTheme('system')">${state.themeMode === 'system' ? '✓ ' : ''}Как в системе</button>
    <label class="settings-action">Размер сообщений <input type="range" min="12" max="24" value="${state.fontSize}" oninput="setMessageFontSize(this.value)" aria-label="Размер текста сообщений"></label>
    <label class="settings-action"><input type="checkbox" ${state.autoDownload ? 'checked' : ''} onchange="setAutoDownload(this.checked)"> Автозагрузка фото</label>
    <h2>Уведомления</h2><button class="settings-action" onclick="requestNotificationPermission()">${icon('chats')}<span>Уведомления<small id="notificationSettingStatus">${state.notificationsEnabled ? 'Включены' : 'Включить в этом браузере'}</small></span></button>
    <button class="settings-action" onclick="testWebNotification()">Проверить уведомления</button>
    <h2>Папки чатов</h2><button class="settings-action" onclick="editChatFolder()">Создать папку</button>${(state.folders || []).map(folder => `<button class="settings-action" onclick="editChatFolder('${html(folder.id)}')">${html(folder.title)}</button>`).join('')}
    <h2>Аккаунт</h2>
    <button class="settings-action" onclick="openSessionsPanel()">${icon('settings')}<span>Активные устройства</span></button>
    <button class="settings-action" onclick="openBackupPanel()">${icon('attach')}<span>Резервная копия ключей</span></button>
    ${state.me.is_admin ? '<button class="settings-action" onclick="openAdminPanel()">Управление пользователями</button>' : ''}
    <button class="settings-action danger-text" onclick="logout()">Выйти из аккаунта</button>
  </section>`;
}

function renderNewChat(state) {
  return `<section class="new-chat-panel" aria-label="Новая переписка"><div class="people-search"><label class="sr-only" for="peopleSearch">Поиск людей</label>${icon('search')}<input class="input" id="peopleSearch" placeholder="Имя пользователя" value="${html(state.searchQ)}" oninput="onSearchInput(this.value)" autocomplete="off"></div>
    <div class="create-options"><button onclick="openCreateRoom('group')">Новая группа</button><button onclick="openCreateRoom('channel')">Новый канал</button></div>
    <div class="people-results">${state.searching ? '<p class="list-hint" role="status">Поиск…</p>' : state.peopleResults.length ? state.peopleResults.map(person => `<button class="item chat-row" onclick="startDM('${html(person.username)}')">${avatar(person.id, name(person), mediaUrl(person.avatar_id))}<span class="itemText"><strong>${html(name(person))}</strong><small>@${html(person.username)}</small></span></button>`).join('') : `<p class="list-hint">${state.searchQ.trim() ? 'Пользователи не найдены' : 'Найдите человека по имени пользователя'}</p>`}</div></section>`;
}

export function renderMessengerShell(state, compose) {
  const room = state.rooms.find(item => item.id === state.activeRoomId);
  const direct = room?.kind === 'dm' && !room.is_saved;
  const peer = state.activeDmPeer || state.roomMembers.find(person => person.id !== state.me.id);
  const title = room?.is_saved ? 'Избранное' : direct ? (name(peer) === 'Пользователь' ? room.other_display_name || room.other_username || 'Личные сообщения' : name(peer)) : room?.title || 'Чат';
  const subtitle = direct ? presenceLabel(state.activeDmPeer) : `${state.roomMembers.length} ${plural(state.roomMembers.length, 'участник', 'участника', 'участников')}`;
  const profileAction = direct ? (peer ? `showProfile('${html(peer.id)}')` : '') : 'openRoomSettings()';
  const roomRows = state.rooms.filter(r => !r.preferences?.hidden && (!!r.preferences?.archived === !!state.archiveOpen) && (!state.selectedFolder || state.folders?.find(f => f.id === state.selectedFolder)?.room_ids.includes(r.id))).sort((a,b) => Number(!!b.preferences?.pinned)-Number(!!a.preferences?.pinned)).map(item => renderRoomRow(item, { ...state.me, activeRoomId: state.activeRoomId })).join('');
  return `<div class="shell ${state.activeRoomId ? 'show-chat' : 'show-list'}">
    <aside class="card sidebar" aria-label="Переписки"><header class="topbar sidebar-header"><h1>${state.settingsOpen ? 'Настройки' : state.composeOpen ? 'Новое сообщение' : state.archiveOpen ? 'Архив' : 'Чаты'}</h1>
      ${state.composeOpen ? iconButton('close', 'Закрыть поиск', 'openChatList()') : `<button class="icon-button" id="accountMenuToggle" aria-label="Меню чатов" aria-expanded="${state.uiMenu === 'account'}" onclick="toggleWebMenu('account',event)">${icon('more')}</button>`}
      ${state.uiMenu === 'account' ? `<div class="web-popover" role="menu">${menuItem('Избранное', 'closeWebMenu();openSavedMessages()')}${menuItem('Создать папку', 'closeWebMenu();editChatFolder()')}${menuItem(state.archiveOpen ? 'Все чаты' : 'Архив', 'closeWebMenu();openArchive()', 'chats')}${menuItem('Новая группа', "closeWebMenu();openCreateRoom('group')", 'chats')}${menuItem('Новый канал', "closeWebMenu();openCreateRoom('channel')", 'compose')}${menuItem('Сменить тему', 'closeWebMenu();toggleTheme()', 'settings')}${menuItem('Мой профиль', 'closeWebMenu();showMyProfile()')}</div>` : ''}
    </header>
    ${!state.settingsOpen && !state.composeOpen && state.folders?.length ? `<div class="folder-tabs" role="tablist" aria-label="Папки чатов"><button role="tab" aria-selected="${!state.selectedFolder}" onclick="selectChatFolder(null)">Все чаты</button>${state.folders.map(folder => `<button role="tab" aria-selected="${state.selectedFolder === folder.id}" onclick="selectChatFolder('${html(folder.id)}')" oncontextmenu="event.preventDefault();editChatFolder('${html(folder.id)}')">${html(folder.title)}</button>`).join('')}</div>` : ''}
    ${state.settingsOpen ? renderSettings(state) : state.composeOpen ? renderNewChat(state) : `<div class="list room-list" id="roomList">${roomRows || '<div class="empty-list"><strong>Здесь будут ваши чаты</strong><p>Начните разговор с помощью кнопки «Написать».</p></div>'}</div><button class="compose-fab" onclick="openNewChat()" aria-label="Написать сообщение">${icon('compose')}<span>Написать</span></button>`}
    <nav class="sidebar-nav" aria-label="Основная навигация"><button class="${!state.settingsOpen ? 'active' : ''}" aria-current="${!state.settingsOpen ? 'page' : 'false'}" onclick="openChatList()">${icon('chats')}<span>Чаты</span></button><button class="${state.settingsOpen ? 'active' : ''}" aria-current="${state.settingsOpen ? 'page' : 'false'}" onclick="openWebSettings()">${icon('settings')}<span>Настройки</span></button></nav>
    </aside>
    <main class="card main" aria-label="Переписка">${state.activeRoomId ? `<header class="chatHead"><div class="chat-identity">${iconButton('back', 'К списку чатов', 'backToList()', 'back')}<button class="peer-profile" onclick="${profileAction}">${avatar(direct ? peer?.id || room?.other_user_id : room?.id, title, mediaUrl(direct ? peer?.avatar_id || room?.other_avatar_id : room?.avatar_id), 'header-avatar')}<span class="chatTitle"><strong>${html(title)}</strong><small class="mini" id="chatPresence">${html(subtitle)}</small></span></button></div><div class="chat-actions">${direct ? iconButton('phone', 'Аудиозвонок', 'startRoomCall(false)') + iconButton('video', 'Видеозвонок', 'startRoomCall(true)') : ''}<button class="icon-button" id="chatMenuToggle" aria-label="Меню переписки" aria-expanded="${state.uiMenu === 'chat'}" onclick="toggleWebMenu('chat',event)">${icon('more')}</button></div>
      ${state.uiMenu === 'chat' ? `<div class="web-popover" role="menu">${menuItem(room?.preferences?.pinned ? 'Открепить' : 'Закрепить', `setChatPreference('${html(room.id)}',{pinned:${!room?.preferences?.pinned}})`)}${menuItem(room?.preferences?.muted ? 'Включить уведомления' : 'Отключить уведомления', `setChatPreference('${html(room.id)}',{muted:${!room?.preferences?.muted}})`)}${menuItem(room?.preferences?.archived ? 'Вернуть из архива' : 'В архив', `setChatPreference('${html(room.id)}',{archived:${!room?.preferences?.archived}})`)}${menuItem('Общие вложения', 'closeWebMenu();openSharedMedia()')}${menuItem('Поиск в переписке', 'closeWebMenu();toggleSearch()', 'search')}${menuItem('Перейти к дате', 'closeWebMenu();jumpToDate()')}${menuItem(direct ? 'Профиль собеседника' : 'Информация', `closeWebMenu();${profileAction}`, 'settings')}${direct ? menuItem('Удалить переписку', 'closeWebMenu();deleteDMChat()') : ''}</div>` : ''}</header>
      ${(() => {
        // Pinned message bar, like Telegram's: one line, tap to jump, ✕ to unpin.
        const pinnedId = room?.pinned_message_id;
        const pinned = pinnedId ? state.messages?.find(m => m.id === pinnedId) : null;
        if (!pinnedId) return '';
        const text = pinned ? (pinned._text || pinned.text || 'Вложение') : 'Закреплённое сообщение';
        const canUnpin = direct || ['owner', 'admin'].includes(room?.my_role);
        return `<div class="pinned-bar" role="status"><span class="pinned-bar-line" aria-hidden="true"></span><button type="button" class="pinned-bar-text" onclick="scrollToMessage('${html(pinnedId)}', event)"><strong>Закреплённое сообщение</strong><span>${html(String(text).slice(0, 120))}</span></button>${canUnpin ? `<button type="button" class="icon-button" aria-label="Открепить" onclick="unpinMessage()">${icon('close')}</button>` : ''}</div>`;
      })()}
      <div class="connection-strip" id="webConnection" role="status" ${state.wsAuthed ? 'hidden' : ''}>${navigator.onLine ? 'Подключение…' : 'Нет сети'}</div>
      ${state.searchMode ? `<div class="searchBar"><label class="sr-only" for="searchInput">Поиск по сообщениям</label><input id="searchInput" class="input" placeholder="Поиск в переписке" value="${html(state.searchQuery)}" oninput="searchMessages(this.value)"><div class="searchControls"><span class="searchCounter">${state.searchResults.length ? `${state.currentSearchIndex + 1} / ${state.searchResults.length}` : state.searchQuery.length >= 2 ? 'Ничего не найдено' : ''}</span><button class="icon-button" onclick="prevSearchResult()" aria-label="Предыдущий результат">↑</button><button class="icon-button" onclick="nextSearchResult()" aria-label="Следующий результат">↓</button><span role="status">${html(state.searchProgress || '')}</span><button onclick="cancelHistorySearch()">Остановить</button>${iconButton('close', 'Закрыть поиск', 'toggleSearch()')}</div></div>` : ''}
      ${state.selectedMessages?.size ? `<div class="selection-bar"><button onclick="clearMessageSelection()">Отмена</button><button onclick="selectAllMessages()">Выбрать все</button><strong>Выбрано: ${state.selectedMessages.size}</strong><button onclick="copySelectedMessages()">Копировать</button><button onclick="forwardSelectedMessages()">Переслать</button><button onclick="deleteSelectedMessages()">Удалить свои</button></div>` : ''}<div class="history-container"><div id="msgs" class="messages" aria-label="Сообщения" aria-busy="${state.roomLoading}"></div><button id="chatJumpDown" class="chat-jump-down" onclick="jumpToLatest()" aria-label="К новым сообщениям" ${!state.chatHistory?.has_newer ? 'hidden' : ''}>↓${state.newMessages ? ` · ${state.newMessages}` : ''}</button></div>${compose(room)}
    ` : `<section class="conversation-empty"><img src="/icons/app-192.png" width="64" height="64" alt="67"><h2>Ваши разговоры — здесь</h2><p>Выберите чат или начните новую переписку.</p><button class="btn primary" onclick="openNewChat()">Написать сообщение</button></section>`}</main>
  </div>`;
}
