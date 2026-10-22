import type { Message, Room, User } from "./types";

export function displayName(user?: Partial<User> | null) {
  return user?.display_name || user?.username || "Пользователь";
}

export function roomTitle(room: Room, meId?: string | null) {
  if (room.is_saved) return "Избранное";
  if (room.kind === "dm") {
    return room.other_display_name || room.other_username || "Переписка";
  }
  return room.title || "Чат";
}

export function roomPreview(room: Room, meId?: string | null) {
  const sender =
    room.last_msg_sender_id === meId
      ? "Вы"
      : room.last_msg_sender_name || room.last_msg_sender_username || "";
  if (room.last_msg_text) {
    // No manual truncation: the row renders one line and ellipsizes in layout,
    // which also keeps surrogate pairs (emoji) intact.
    return sender ? `${sender}: ${room.last_msg_text}` : room.last_msg_text;
  }
  if (
    room.last_msg_type === "text" &&
    (room.last_msg_meta?.encrypted || room.last_msg_encrypted_content)
  ) {
    return sender
      ? `${sender}: Зашифрованное сообщение`
      : "Зашифрованное сообщение";
  }
  if (room.last_msg_type === "voice") return "Голосовое сообщение";
  if (room.last_msg_type === "video_note") return "Видеокружок";
  if (room.last_msg_type === "media") return "Вложение";
  return room.kind === "channel"
    ? "Канал"
    : room.kind === "group"
      ? "Группа"
      : "Нет сообщений";
}

export function messageText(message: Message) {
  if (message.type !== "text") return `[${message.type}]`;
  if (message.text) return message.text;
  if (message.meta?.encrypted || message.encrypted_content)
    return "Зашифрованное сообщение";
  return "";
}

export function shortTime(value?: string | null) {
  if (!value) return "";
  const d = new Date(value);
  return d.toLocaleTimeString("ru-RU", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

/**
 * Timestamp in a chat-list row, the way Telegram shows it: time for today,
 * «Вчера» for yesterday, a short weekday within the last week, date beyond that.
 */
export function listTime(value?: string | null, now: Date = new Date()) {
  if (!value) return "";
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return "";
  const startOfDay = (d: Date) =>
    new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
  const days = Math.round((startOfDay(now) - startOfDay(date)) / 86400000);
  if (days <= 0) return shortTime(value);
  if (days === 1) return "Вчера";
  if (days < 7)
    return date.toLocaleDateString("ru-RU", { weekday: "short" });
  return date.toLocaleDateString("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "2-digit",
  });
}

/** Day separator inside a conversation: «Сегодня» / «Вчера» / «5 октября». */
export function dayLabel(value: string, now: Date = new Date()) {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return "";
  const startOfDay = (d: Date) =>
    new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
  const days = Math.round((startOfDay(now) - startOfDay(date)) / 86400000);
  if (days === 0) return "Сегодня";
  if (days === 1) return "Вчера";
  return date.toLocaleDateString("ru-RU", { day: "numeric", month: "long" });
}

/** Russian plural: plural(5,'участник','участника','участников'). */
export function plural(count: number, one: string, few: string, many: string) {
  const n = Math.abs(count) % 100;
  const last = n % 10;
  if (n > 10 && n < 20) return many;
  if (last > 1 && last < 5) return few;
  if (last === 1) return one;
  return many;
}

export function participantsLabel(count: number) {
  return `${count} ${plural(count, "участник", "участника", "участников")}`;
}
