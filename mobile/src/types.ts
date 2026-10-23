export type User = {
  id: string;
  username: string;
  display_name?: string | null;
  avatar_id?: string | null;
  created_at?: string;
  last_seen?: string | null;
  online?: boolean;
  isAdmin?: boolean;
};

export type Room = {
  pinned_message_id?: string | null;
  is_saved?: boolean;
  my_role?: string;
  preferences?: {
    pinned?: boolean;
    muted?: boolean;
    hidden?: boolean;
    archived?: boolean;
    marked_unread?: boolean;
  };
  id: string;
  kind: "dm" | "group" | "channel";
  title?: string | null;
  avatar_id?: string | null;
  created_by?: string | null;
  created_at?: string;
  last_msg_id?: string | null;
  last_msg_type?: string | null;
  last_msg_text?: string | null;
  last_msg_meta?: Record<string, unknown> | null;
  last_msg_encrypted_content?: string | null;
  last_msg_sender_id?: string | null;
  last_msg_sender_username?: string | null;
  last_msg_sender_name?: string | null;
  last_msg_at?: string | null;
  other_user_id?: string | null;
  other_username?: string | null;
  other_display_name?: string | null;
  other_avatar_id?: string | null;
  unread_count?: number;
};

export type Message = {
  client_message_id?: string | null;
  edited_at?: string | null;
  delivery?: "queued" | "sending" | "failed";
  delivery_error?: string;
  id: string;
  room_id: string;
  sender_id: string;
  sender_username?: string | null;
  sender_display_name?: string | null;
  sender_avatar_id?: string | null;
  type: "text" | "media" | "voice" | "video_note" | "call";
  text?: string | null;
  media_id?: string | null;
  meta?: Record<string, unknown> | null;
  created_at: string;
  read_by?: string[];
  reply_to?: {
    id?: string;
    text?: string | null;
    sender_id?: string;
    sender_username?: string | null;
    sender_display_name?: string | null;
  } | null;
  reactions?: Record<string, string[]>;
  media_kind?: string | null;
  media_mime?: string | null;
  media_size?: number | null;
  thumb_path?: string | null;
  duration_sec?: number | null;
  width?: number | null;
  height?: number | null;
  encrypted_content?: string | null;
  _decrypted?: boolean;
};

export type AuthResponse = {
  user: User;
  token?: string;
};
