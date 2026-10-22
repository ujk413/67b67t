const messages: Record<string, string> = {
  username_taken: "Это имя пользователя уже занято. Выберите другое или войдите в существующий аккаунт.",
  username_too_short: "Имя пользователя должно содержать не менее 3 символов.",
  username_too_long: "Имя пользователя должно содержать не более 30 символов.",
  username_invalid: "В имени пользователя разрешены латинские буквы, цифры и знак подчёркивания.",
  password_too_short: "Пароль должен содержать не менее 6 символов.",
  bad_credentials: "Неверное имя пользователя или пароль.",
  captcha_failed: "Не удалось проверить регистрацию. Повторите попытку.",
  forbidden: "Недостаточно прав для этого действия.",
  blocked: "Отправить сообщение этому пользователю сейчас нельзя.",
  not_found: "Данные больше недоступны. Обновите список.",
  message_not_found: "Сообщение удалено или больше недоступно.",
  request_timeout: "Сервер не ответил вовремя. Проверьте соединение и повторите попытку.",
};
export function userError(value: unknown): string {
  const raw = value instanceof Error ? value.message : String(value || "");
  if (messages[raw]) return messages[raw];
  if (/network|failed to fetch|network request failed|offline/i.test(raw))
    return "Нет соединения. Проверьте интернет и повторите попытку.";
  if (/timeout|timed out/i.test(raw)) return messages.request_timeout;
  if (/HTTP 429/.test(raw)) return "Слишком много попыток. Подождите минуту.";
  if (/HTTP 5\d\d/.test(raw)) return "Сервер временно недоступен. Повторите попытку позже.";
  return raw || "Не удалось выполнить действие. Повторите попытку.";
}
