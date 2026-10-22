import { useEffect, useState } from "react";
import { AppState } from "react-native";
import { mobileApi } from "../mobileApi";
import type { User } from "../types";

// Presence is read from the server. A profile from another chat/account is never reused.
export function useChatPresence(
  base: string,
  token: string | null,
  userId?: string | null,
) {
  const [value, setValue] = useState<{
    base: string;
    token: string;
    userId: string;
    user: User;
  } | null>(null);
  useEffect(() => {
    if (!token || !userId) {
      setValue(null);
      return;
    }
    let active = true,
      pending = false;
    const refresh = async () => {
      if (!active || pending || AppState.currentState !== "active") return;
      pending = true;
      try {
        const user = await mobileApi(base, token).profile(userId);
        if (active) setValue({ base, token, userId, user });
      } catch {
        if (active) setValue(null);
      } finally {
        pending = false;
      }
    };
    void refresh();
    const timer = setInterval(() => void refresh(), 20000);
    const listener = AppState.addEventListener("change", (state) => {
      if (state === "active") void refresh();
    });
    return () => {
      active = false;
      clearInterval(timer);
      listener.remove();
    };
  }, [base, token, userId]);
  return value?.base === base &&
    value.token === token &&
    value.userId === userId
    ? value.user
    : null;
}
