import {t as uiText,isUiCopy,getLanguage} from "./i18n.js";
const messages = {
  username_taken: 'Имя уже занято. Выберите другое или войдите в существующий аккаунт.',
  username_too_short: 'Имя пользователя: не менее 3 символов.', username_too_long: 'Имя пользователя: не более 30 символов.',
  username_invalid: 'Используйте латинские буквы, цифры и подчёркивание.', password_too_short: 'Пароль: не менее 6 символов.',
  bad_credentials: "Неверное имя пользователя или пароль.", captcha_failed: 'Проверка не пройдена. Пройдите капчу ещё раз.',
  account_banned: 'Аккаунт заблокирован.', forbidden: "Недостаточно прав для этого действия.",
  blocked: 'Писать этому пользователю сейчас нельзя.', not_found: "Данные больше недоступны. Обновите список.",
  message_not_found: "Сообщение удалено или больше недоступно.",
  bad_factor: 'Неверный или уже использованный код подтверждения.',
  invalid_challenge: 'Подтверждение истекло или исчерпаны попытки. Войдите снова.',
  device_approval_required: 'Подтвердите вход на действующем устройстве.',
  account_disabled: 'Этот аккаунт больше недоступен.',
  device_identity_changed: 'Сеанс связан с другим ключом. Восстановите копию через новый вход.',
  restore_requires_fresh_session: 'Для восстановления нужен новый вход без запуска синхронизации.',
  device_proof_required: 'Обновите приложение для подтверждения ключа устройства.',
  invalid_device_proof: 'Подтверждение ключа устройства отклонено. Повторите вход.',
};
export function userError(error) {
  const raw = error?.code || error?.message || String(error || '');
  if (messages[raw]) return uiText(messages[raw]);
  if (/network|fetch|offline/i.test(raw)) return uiText("Нет соединения. Проверьте интернет и повторите попытку.");
  if (/abort|timeout|timed out/i.test(raw)) return uiText("Сервер не ответил вовремя. Повторите попытку.");
  if (error?.status === 429) return uiText("Слишком много попыток. Подождите минуту.");
  if (error?.status >= 500) return uiText("Сервер временно недоступен. Повторите попытку позже.");
  if(isUiCopy(raw))return uiText(raw);
  if(getLanguage()==='ru'&&/[А-Яа-яЁё]/.test(raw))return raw;
  return uiText("Не удалось выполнить действие. Повторите попытку.");
}
export function toast(message) {
  let host = document.getElementById('toastHost');
  if (!host) { host = document.createElement('div'); host.id = 'toastHost'; host.setAttribute('aria-live', 'polite'); document.body.append(host); }
  const item = document.createElement('div'); item.className = 'app-toast';
  const text = document.createElement('span'); text.textContent = String(message);
  const close = document.createElement('button'); close.type = 'button'; close.textContent = uiText("Закрыть"); close.onclick = () => item.remove();
  item.append(text, close); host.append(item); setTimeout(() => item.remove(), 9000);
}
