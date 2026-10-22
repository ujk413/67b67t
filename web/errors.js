const messages = {
  username_taken: 'Имя уже занято. Выберите другое или войдите в существующий аккаунт.',
  username_too_short: 'Имя пользователя: не менее 3 символов.', username_too_long: 'Имя пользователя: не более 30 символов.',
  username_invalid: 'Используйте латинские буквы, цифры и подчёркивание.', password_too_short: 'Пароль: не менее 6 символов.',
  bad_credentials: 'Неверное имя пользователя или пароль.', captcha_failed: 'Проверка не пройдена. Пройдите капчу ещё раз.',
  account_banned: 'Аккаунт заблокирован.', forbidden: 'Недостаточно прав для этого действия.',
  blocked: 'Писать этому пользователю сейчас нельзя.', not_found: 'Данные больше недоступны. Обновите список.',
  message_not_found: 'Сообщение удалено или больше недоступно.',
};
export function userError(error) {
  const raw = error?.code || error?.message || String(error || '');
  if (messages[raw]) return messages[raw];
  if (/network|fetch|offline/i.test(raw)) return 'Нет соединения. Проверьте интернет и повторите попытку.';
  if (/abort|timeout|timed out/i.test(raw)) return 'Сервер не ответил вовремя. Повторите попытку.';
  if (error?.status === 429) return 'Слишком много попыток. Подождите минуту.';
  if (error?.status >= 500) return 'Сервер временно недоступен. Повторите попытку позже.';
  return raw || 'Не удалось выполнить действие. Повторите попытку.';
}
export function toast(message) {
  let host = document.getElementById('toastHost');
  if (!host) { host = document.createElement('div'); host.id = 'toastHost'; host.setAttribute('aria-live', 'polite'); document.body.append(host); }
  const item = document.createElement('div'); item.className = 'app-toast';
  const text = document.createElement('span'); text.textContent = String(message);
  const close = document.createElement('button'); close.type = 'button'; close.textContent = 'Закрыть'; close.onclick = () => item.remove();
  item.append(text, close); host.append(item); setTimeout(() => item.remove(), 9000);
}
