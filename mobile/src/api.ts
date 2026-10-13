import type { AuthResponse, Message, Room, User } from "./types";

type UnauthorizedHandler = (failedToken: string) => void;

let unauthorizedHandler: UnauthorizedHandler | null = null;
let unauthorizedFired = false;

// App registers a callback that clears the stored session and routes to login.
export function setUnauthorizedHandler(handler: UnauthorizedHandler | null) {
  unauthorizedHandler = handler;
}

// Re-arm the "fire once" guard (call after a successful login/register).
export function resetUnauthorizedGuard() {
  unauthorizedFired = false;
}

function notifyUnauthorized(path: string, failedToken: string | null | undefined) {
  if (!failedToken || unauthorizedFired) return;
  if (path.startsWith("/auth/login") || path.startsWith("/auth/register") || path.startsWith("/auth/logout")) return;
  unauthorizedFired = true;
  try {
    unauthorizedHandler?.(failedToken);
  } catch (e) {
    console.error("Unauthorized handler failed:", e);
  }
}

type ApiOptions = {
  method?: string;
  token?: string | null;
  body?: unknown;
};

async function request<T>(baseUrl: string, path: string, options: ApiOptions = {}): Promise<T> {
  const headers: Record<string, string> = {
    Accept: "application/json",
  };
  if (options.body !== undefined) {
    headers["Content-Type"] = "application/json";
  }
  if (options.token) {
    headers.Authorization = `Bearer ${options.token}`;
  }

  const res = await fetch(`${baseUrl}${path}`, {
    method: options.method || "GET",
    headers,
    body: options.body !== undefined ? JSON.stringify(options.body) : undefined,
  });

  const text = await res.text();
  const data = text ? JSON.parse(text) : null;
  if (!res.ok) {
    if (res.status === 401) notifyUnauthorized(path, options.token);
    throw new Error(data?.error || `HTTP ${res.status}`);
  }
  return data as T;
}

export const api = {
  login(baseUrl: string, username: string, password: string) {
    return request<AuthResponse>(baseUrl, "/auth/login", {
      method: "POST",
      body: { username, password },
      token: null,
    });
  },
  mobileLogin(baseUrl: string, username: string, password: string) {
    return fetch(`${baseUrl}/auth/login`, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        "x-session-transport": "bearer",
      },
      body: JSON.stringify({ username, password }),
    }).then(async (res) => {
      const text = await res.text();
      const data = text ? JSON.parse(text) : null;
      if (!res.ok) throw new Error(data?.error || `HTTP ${res.status}`);
      return data as AuthResponse;
    });
  },
  mobileRegister(baseUrl: string, username: string, password: string) {
    return fetch(`${baseUrl}/auth/register`, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        "x-session-transport": "bearer",
      },
      body: JSON.stringify({ username, password, captcha: "" }),
    }).then(async (res) => {
      const text = await res.text();
      const data = text ? JSON.parse(text) : null;
      if (!res.ok) throw new Error(data?.error || `HTTP ${res.status}`);
      return data as AuthResponse;
    });
  },
  me(baseUrl: string, token: string) {
    return request<User>(baseUrl, "/me", { token });
  },
  updateMe(baseUrl: string, token: string, display_name: string | null) {
    return request<{ ok: boolean }>(baseUrl, "/me", {
      method: "PUT",
      token,
      body: { display_name },
    });
  },
  changePassword(baseUrl: string, token: string, new_password: string) {
    return request<{ ok: boolean }>(baseUrl, "/me/password", {
      method: "PUT",
      token,
      body: { new_password },
    });
  },
  rooms(baseUrl: string, token: string) {
    return request<Room[]>(baseUrl, "/rooms", { token });
  },
  roomMembers(baseUrl: string, token: string, roomId: string) {
    return request<User[]>(baseUrl, `/rooms/${roomId}/members`, { token });
  },
  roomMessages(baseUrl: string, token: string, roomId: string) {
    return request<Message[]>(baseUrl, `/rooms/${roomId}/messages`, { token });
  },
  roomKey(baseUrl: string, token: string, roomId: string) {
    return request<{ room_id: string; key: string }>(baseUrl, `/rooms/${roomId}/key`, { token });
  },
  markRead(baseUrl: string, token: string, roomId: string) {
    return request<{ ok: boolean }>(baseUrl, `/rooms/${roomId}/read`, { method: "POST", token, body: {} });
  },
  uploadE2EKeys(baseUrl: string, token: string, deviceId: string, keys: {
    identityKey: string;
    signedPreKey: { keyId: number; publicKey: string; signature: string };
    preKeys: Array<{ keyId: number; publicKey: string }>;
  }) {
    return request<{ ok: boolean }>(baseUrl, "/e2e/keys", {
      method: "POST",
      token,
      body: {
        device_id: deviceId,
        identity_key: keys.identityKey,
        signed_pre_key: {
          key_id: keys.signedPreKey.keyId,
          public_key: keys.signedPreKey.publicKey,
          signature: keys.signedPreKey.signature,
        },
        pre_keys: keys.preKeys.map((key) => ({
          key_id: key.keyId,
          public_key: key.publicKey,
        })),
      },
    });
  },
  roomE2e(baseUrl: string, token: string, roomId: string) {
    return request<{ enabled: boolean }>(baseUrl, `/rooms/${roomId}/e2e`, { token });
  },
  e2ePrekeyCount(baseUrl: string, token: string, deviceId: string) {
    return request<{ device_id: string; unused: number }>(baseUrl, `/e2e/prekeys/count?device_id=${encodeURIComponent(deviceId)}`, { token });
  },
  iceServers(baseUrl: string, token: string) {
    return request<{ iceServers: any[]; ttl: number | null }>(baseUrl, `/rtc/ice-servers`, { token });
  },
  sendMessage(baseUrl: string, token: string, roomId: string, text: string) {
    return request<Message>(baseUrl, `/rooms/${roomId}/messages`, {
      method: "POST",
      token,
      body: { type: "text", text, meta: { encrypted: false } },
    });
  },
  sendMediaMessage(baseUrl: string, token: string, roomId: string, body: Record<string, unknown>) {
    return request<Message>(baseUrl, `/rooms/${roomId}/messages`, {
      method: "POST",
      token,
      body,
    });
  },
  addReaction(baseUrl: string, token: string, messageId: string, emoji: string) {
    return request<{ ok: boolean }>(baseUrl, `/messages/${messageId}/reactions`, {
      method: "POST",
      token,
      body: { emoji },
    });
  },
  removeReaction(baseUrl: string, token: string, messageId: string, emoji: string) {
    return request<{ ok: boolean }>(baseUrl, `/messages/${messageId}/reactions/${encodeURIComponent(emoji)}`, {
      method: "DELETE",
      token,
    });
  },
  async upload(baseUrl: string, token: string, kind: string, file: { uri: string; name: string; type: string }) {
    const form = new FormData();
    form.append("file", {
      uri: file.uri,
      name: file.name,
      type: file.type,
    } as never);

    const res = await fetch(`${baseUrl}/media/upload?kind=${encodeURIComponent(kind)}`, {
      method: "POST",
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${token}`,
      },
      body: form,
    });
    const text = await res.text();
    const data = text ? JSON.parse(text) : null;
    if (!res.ok) {
      if (res.status === 401) notifyUnauthorized("/media/upload", token);
      throw new Error(data?.error || `HTTP ${res.status}`);
    }
    return data as { id: string; kind: string; mime: string; duration_sec?: number | null };
  },
  async uploadMyAvatar(baseUrl: string, token: string, file: { uri: string; name: string; type: string }) {
    const form = new FormData();
    form.append("file", {
      uri: file.uri,
      name: file.name,
      type: file.type,
    } as never);

    const res = await fetch(`${baseUrl}/me/avatar`, {
      method: "POST",
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${token}`,
      },
      body: form,
    });
    const text = await res.text();
    const data = text ? JSON.parse(text) : null;
    if (!res.ok) {
      if (res.status === 401) notifyUnauthorized("/me/avatar", token);
      throw new Error(data?.error || `HTTP ${res.status}`);
    }
    return data as { avatar_id: string };
  },
  logout(baseUrl: string, token: string) {
    return request<{ ok: boolean }>(baseUrl, "/auth/logout", { method: "POST", token });
  },
  registerFCMToken(baseUrl: string, token: string, fcmToken: string) {
    return request<{ ok: boolean }>(baseUrl, "/me/fcm-token", {
      method: "POST",
      token,
      body: { fcm_token: fcmToken },
    });
  },
};
