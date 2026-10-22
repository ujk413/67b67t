import { request } from "./api";
import type { Message, Room, User } from "./types";
export type History = {
  messages: Message[];
  has_older: boolean;
  has_newer: boolean;
  first_unread_id: string | null;
};
export type RoomPreference = {
  room_id: string;
  pinned: boolean;
  muted: boolean;
  archived: boolean;
  hidden: boolean;
  marked_unread: boolean;
};
export function mobileApi(base: string, token: string) {
  const call = <T>(path: string, method = "GET", body?: unknown) =>
    request<T>(base, path, { method, body, token });
  return {
    folders: () => call<{ id: string; title: string; room_ids: string[] }[]>("/me/folders"),
    saveFolder: (title: string, room_ids: string[], id?: string) => call(`/me/folders${id ? '/' + id : ''}`, id ? "PUT" : "POST", { title, room_ids }),
    removeFolder: (id: string) => call(`/me/folders/${id}`, "DELETE"),
    saved: () => call<{ id: string }>("/me/saved", "POST", {}),
    capabilities: () =>
      call<{ version: number; idempotent_send: boolean; sync: boolean }>(
        "/mobile/capabilities",
      ),
    users: (q: string) =>
      call<User[]>(`/users/search?q=${encodeURIComponent(q)}`),
    createRoom: (kind: string, members: string[], title?: string) =>
      call<{ id: string }>("/rooms", "POST", { kind, members, title }),
    room: (id: string) => call<Room>(`/rooms/${id}`),
    history: (id: string, options: Record<string, string | number> = {}) =>
      call<History>(
        `/rooms/${id}/history?${new URLSearchParams(Object.entries(options).map(([k, v]) => [k, String(v)]))}`,
      ),
    edit: (roomId: string, id: string, body: unknown) =>
      call<Message>(`/rooms/${roomId}/messages/${id}`, "PUT", body),
    remove: (roomId: string, id: string) =>
      call(`/rooms/${roomId}/messages/${id}`, "DELETE"),
    read: (id: string, upTo?: string) =>
      call(`/rooms/${id}/read`, "POST", upTo ? { up_to_message_id: upTo } : {}),
    preferences: () => call<RoomPreference[]>("/me/room-preferences"),
    preference: (id: string, body: Partial<RoomPreference>) =>
      call(`/rooms/${id}/preferences`, "PUT", body),
    sync: (cursor: string) =>
      call<{
        cursor: string;
        has_more: boolean;
        room_ids: string[];
        changes: {
          operation: string;
          message_id: string;
          room_id: string;
          message: Message | null;
        }[];
      }>(`/sync?cursor=${cursor}`),
    sessions: () => call<{ sessions: any[] } | any[]>("/auth/sessions"),
    terminate: (id: string) => call(`/auth/sessions/${id}`, "DELETE"),
    profile: (id: string) => call<User>(`/users/${id}`),
    block: (id: string, blocked: boolean) =>
      call(`/users/${id}/block`, blocked ? "POST" : "DELETE"),
    media: (id: string, options: Record<string, string> = {}) => call<Message[]>(`/rooms/${id}/media?${new URLSearchParams(options)}`),
    member: (id: string, username: string) =>
      call(`/rooms/${id}/members`, "POST", { username }),
    kick: (id: string, userId: string) =>
      call(`/rooms/${id}/members/${userId}`, "DELETE"),
    role: (id: string, userId: string, role: string) =>
      call(`/rooms/${id}/members/${userId}/role`, "PUT", { role }),
    updateRoom: (id: string, title: string) =>
      call(`/rooms/${id}`, "PUT", { title }),
    invite: (id: string) =>
      call<{ token: string }>(`/rooms/${id}/invites`, "POST", {}),
    revokeInvite: (id: string, invite: string) =>
      call(`/rooms/${id}/invites/${invite}`, "DELETE"),
    joinInvite: (invite: string) =>
      call<{ id: string }>(
        `/invites/${encodeURIComponent(invite)}/join`,
        "POST",
        {},
      ),
    registerPush: (id: string, pushToken: string, roomId: string | null) =>
      call(`/me/push-devices/${id}`, "PUT", {
        token: pushToken,
        active_room_id: roomId,
      }),
    unregisterPush: (id: string) => call(`/me/push-devices/${id}`, "DELETE"),
    pushStatus: () => call<{ available: boolean; registered: boolean }>("/me/push-status"),
    testPush: () => call<{ sent: number }>("/me/push-test", "POST", {}),
  };
}
