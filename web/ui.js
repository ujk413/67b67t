// Presentation helpers shared by the web messenger and its UI checks.
export const html = value => String(value ?? '').replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
const icons = new Set(['attach','back','chats','close','compose','down','mic','more','phone','search','send','settings','video','check','checks']);
export function icon(name) {
  if (!icons.has(name)) throw new Error('Unknown UI icon');
  return `<span class="ui-icon" style="--icon:url('/icons/${name}@2x.png')" aria-hidden="true"></span>`;
}
const colors = ['#FF845E','#FEBB5B','#B694F9','#9AD164','#5BCBE3','#5CAFFA','#FF8AAC'];
export function avatar(id, name, url = '', extraClass = '') {
  let hash = 0;
  for (const char of String(id || name)) hash = ((hash * 31) + char.codePointAt(0)) | 0;
  const initials = String(name || '?').trim().split(/\s+/).slice(0, 2).map(word => Array.from(word)[0] || '').join('').toLocaleUpperCase('ru');
  return `<span class="person-avatar ${html(extraClass)}" style="--avatar-color:${colors[Math.abs(hash) % colors.length]}" aria-hidden="true"><span>${html(initials)}</span>${url ? `<img src="${html(url)}" alt="" loading="lazy" onerror="this.remove()">` : ''}</span>`;
}
export function groupWith(previous, message) {
  if (!previous || !message) return false;
  const a = new Date(previous.created_at), b = new Date(message.created_at);
  return previous.room_id === message.room_id && previous.sender_id === message.sender_id &&
    a.toDateString() === b.toDateString() && b - a >= 0 && b - a < 300000;
}
export function presenceLabel(peer) {
  if (!peer) return 'Обновление статуса…';
  if (peer.online) return 'онлайн';
  const seen = new Date(peer.last_seen);
  if (!peer.last_seen || Number.isNaN(seen.getTime())) return 'не в сети';
  const today = seen.toDateString() === new Date().toDateString();
  const time = seen.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
  return `был(а) ${today ? 'сегодня' : seen.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' })} в ${time}`;
}
// Russian plural form: plural(5,'участник','участника','участников') -> 'участников'.
export function plural(count, one, few, many) {
  const n = Math.abs(Number(count) || 0) % 100, last = n % 10;
  if (n > 10 && n < 20) return many;
  if (last > 1 && last < 5) return few;
  if (last === 1) return one;
  return many;
}

// Chat-list timestamp, like Telegram: time today, «Вчера», weekday this week, date older.
export function listTime(value, now = new Date()) {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  const startOfDay = d => new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
  const days = Math.round((startOfDay(now) - startOfDay(date)) / 86400000);
  if (days <= 0) return date.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
  if (days === 1) return 'Вчера';
  if (days < 7) return date.toLocaleDateString('ru-RU', { weekday: 'short' });
  return date.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' });
}

export function reactionPeople(users, participants) {
  const byId = new Map(participants.filter(Boolean).map(person => [String(person.id || person.user_id), person]));
  return [...new Set((Array.isArray(users) ? users : []).map(user => String(user?.id || user)))].map(id => byId.get(id) || { id, username: 'Участник' });
}
