import type { Message, Room, User } from "./types";

export function displayName(user?: Partial<User> | null) {
  return user?.display_name || user?.username || "User";
}

export function roomTitle(room: Room, meId?: string | null) {
  if (room.kind === "dm") {
    return room.other_display_name || room.other_username || "Dialog";
  }
  return room.title || "Chat";
}

export function roomPreview(room: Room, meId?: string | null) {
  const sender = room.last_msg_sender_id === meId ? "You" : (room.last_msg_sender_name || room.last_msg_sender_username || "");
  if (room.last_msg_text) {
    const text = room.last_msg_text.length > 42 ? `${room.last_msg_text.slice(0, 42)}...` : room.last_msg_text;
    return sender ? `${sender}: ${text}` : text;
  }
  if (room.last_msg_type === "text" && (room.last_msg_meta?.encrypted || room.last_msg_encrypted_content)) {
    return sender ? `${sender}: Encrypted message` : "Encrypted message";
  }
  if (room.last_msg_type === "voice") return "Voice message";
  if (room.last_msg_type === "video_note") return "Video note";
  if (room.last_msg_type === "media") return "Media";
  return room.kind === "channel" ? "Channel" : room.kind === "group" ? "Group" : "No messages";
}

export function messageText(message: Message) {
  if (message.type !== "text") return `[${message.type}]`;
  if (message.text) return message.text;
  if (message.meta?.encrypted || message.encrypted_content) return "Encrypted message";
  return "";
}

export function shortTime(value?: string | null) {
  if (!value) return "";
  const d = new Date(value);
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}
