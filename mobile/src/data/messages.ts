import type { Message } from "../types";
export function mergeMessages(...groups: Message[][]) {
  const map = new Map<string, Message>();
  for (const m of groups.flat()) {
    if (m.client_message_id)
      for (const [id, prior] of map)
        if (
          prior.sender_id === m.sender_id &&
          prior.client_message_id === m.client_message_id &&
          id !== m.id
        )
          map.delete(id);
    map.set(m.id, m);
  }
  return [...map.values()].sort(
    (a, b) =>
      a.created_at.localeCompare(b.created_at) || a.id.localeCompare(b.id),
  );
}
export function messageStatus(m: Message) {
  return m.delivery === "failed"
    ? "Ошибка"
    : m.delivery === "sending"
      ? "Отправляется…"
      : m.delivery === "queued"
        ? "В очереди"
        : m.read_by?.some((id) => id !== m.sender_id)
          ? "Прочитано"
          : "Отправлено";
}
