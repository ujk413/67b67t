import {t as uiText,locale,getLanguage} from "../../web/i18n.js";
import type { Message, User } from "./types";

export function presenceText(user?: User | null, now = Date.now()) {
  if (!user || typeof user.online !== "boolean") return uiText("статус недоступен");
  if (user.online) return uiText("в сети");
  const seen = user.last_seen ? new Date(user.last_seen) : null;
  if (!seen || !Number.isFinite(seen.getTime())) return uiText("не в сети");
  const minutes = Math.max(0, Math.floor((now - seen.getTime()) / 60000));
  if (minutes < 1) return uiText("был(а) только что");
  if (minutes < 60) return uiText('был(а) {0} мин. назад',minutes);
  const date = new Date(now);
  const clock = seen.toLocaleTimeString(locale(), {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  if (date.toDateString() === seen.toDateString()) return uiText('был(а) в {0}',clock);
  date.setDate(date.getDate() - 1);
  if (date.toDateString() === seen.toDateString())
    return uiText('был(а) вчера в {0}',clock);
  return uiText('был(а) {0}',seen.toLocaleDateString(locale(), { day: "numeric", month: "short" }));
}

export function reactionPeople(ids: string[], people: User[]) {
  const known = new Map(people.map((user) => [user.id, user]));
  return [...new Set(ids)].map(
    (id) => known.get(id) || { id, username: "", display_name: uiText("Участник") },
  );
}

export function sameMessageGroup(first?: Message, second?: Message) {
  if (
    !first ||
    !second ||
    first.room_id !== second.room_id ||
    first.sender_id !== second.sender_id
  )
    return false;
  const a = new Date(first.created_at),
    b = new Date(second.created_at);
  const gap = b.getTime() - a.getTime();
  return gap >= 0 && gap < 5 * 60000 && a.toDateString() === b.toDateString();
}
