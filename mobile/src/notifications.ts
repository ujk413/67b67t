import * as Notifications from "expo-notifications";
import Constants from "expo-constants";
import { AppState, Platform } from "react-native";
import { addNetworkStateListener } from "expo-network";
import { storage } from "./storage";
import { newId } from "./data/repository";
import { mobileApi } from "./mobileApi";
import { requestWithTimeout } from "./requestTimeout";

export type PushStatus = { status: "unconfigured" | "permission" | "connecting" | "enabled" | "error"; message: string };
class PushNotifications {
  private callback: ((data: any) => void) | null = null;
  private token: string | null = null;
  private id: string | null = null;
  private dispose: (() => void) | null = null;
  private activate: ((prompt?: boolean) => Promise<void>) | null = null;
  private refreshRegistration: (() => Promise<void>) | null = null;
  private generation = 0;
  private writes: Promise<unknown> = Promise.resolve();
  private opened = new Set<string>();
  private listeners = new Set<(state: PushStatus) => void>();
  private state: PushStatus = { status: "connecting", message: "Подключение уведомлений…" };
  get configured() { return Platform.OS === "android" && !!Constants.expoConfig?.extra?.fcmConfigured; }
  get status() { return this.state; }
  subscribe(listener: (state: PushStatus) => void) {
    this.listeners.add(listener); listener(this.state);
    return () => { this.listeners.delete(listener); };
  }
  private report(status: PushStatus["status"], message: string) {
    this.state = { status, message };
    for (const listener of this.listeners) listener(this.state);
  }
  async initialize(prompt = true): Promise<string | null> {
    if (!this.configured) return null;
    await Notifications.setNotificationChannelAsync("messages", {
      name: "Сообщения", importance: Notifications.AndroidImportance.HIGH,
      sound: "default", enableVibrate: true, showBadge: true,
      vibrationPattern: [0, 250, 150, 250],
    });
    let permission = await Notifications.getPermissionsAsync();
    if (permission.status !== "granted" && permission.canAskAgain !== false && prompt)
      permission = await Notifications.requestPermissionsAsync();
    if (permission.status !== "granted") return null;
    const value = await requestWithTimeout(() => Notifications.getDevicePushTokenAsync(), 20000);
    if (typeof value.data !== "string" || !value.data) throw new Error("push_token_missing");
    return value.data;
  }
  async installation() {
    this.id ||= await storage.get("push.installation");
    if (!this.id) { this.id = newId(); await storage.set("push.installation", this.id); }
    return this.id;
  }
  async connect(base: string, auth: string, activeRoomId: () => string | null) {
    this.dispose?.();
    const generation = ++this.generation;
    if (!this.configured) {
      this.report("unconfigured", "Установите обновление приложения для уведомлений");
      return () => {};
    }
    let stopped = false, foreground = AppState.currentState === "active";
    let initializing: Promise<void> | null = null;
    const current = () => !stopped && generation === this.generation;
    const api = mobileApi(base, auth);
    const register = () => {
      const task = this.writes.catch(() => {}).then(async () => {
        if (!current() || !this.token) return;
        const id = await this.installation();
        if (!current()) return;
        await api.registerPush(id, this.token, foreground ? activeRoomId() : null);
        if (current()) this.report("enabled", "Включены · звук и вибрация");
      });
      this.writes = task.catch(() => {
        if (current()) this.report("error", "Не удалось подключить уведомления. Повторите при подключении к сети.");
      });
      return task;
    };
    const activate = (prompt = false) => {
      if (initializing) return initializing;
      initializing = (async () => {
        try {
          const token = await this.initialize(prompt);
          if (!current()) return;
          this.token = token;
          if (!token) {
            this.report("permission", "Разрешите уведомления в настройках телефона");
            await api.unregisterPush(await this.installation());
            return;
          }
          await register();
        } catch (error) {
          if (current()) this.report("error", "Не удалось подключить уведомления. Повторите при подключении к сети.");
          throw error;
        }
      })().finally(() => { initializing = null; });
      return initializing;
    };
    this.activate = activate;
    this.refreshRegistration = register;
    this.report("connecting", "Подключение уведомлений…");
    Notifications.setNotificationHandler({
      handleNotification: async notification => {
        const active = current() && foreground && !!activeRoomId() &&
          notification.request.content.data?.room_id === activeRoomId();
        return { shouldPlaySound: !active, shouldSetBadge: true, shouldShowBanner: !active, shouldShowList: !active };
      },
    });
    const open = (response: Notifications.NotificationResponse) => {
      if (!current()) return;
      const key = response.notification.request.identifier;
      if (this.opened.has(key)) return;
      if (this.opened.size >= 128) this.opened.delete(this.opened.values().next().value!);
      this.opened.add(key);
      this.callback?.(response.notification.request.content.data);
      void Notifications.clearLastNotificationResponseAsync().catch(() => {});
    };
    const responses = Notifications.addNotificationResponseReceivedListener(open);
    const tokens = Notifications.addPushTokenListener(value => {
      if (!current() || typeof value.data !== "string") return;
      this.token = value.data;
      void register().catch(() => {});
    });
    void activate(true).catch(() => {});
    const appState = AppState.addEventListener("change", state => {
      foreground = state === "active";
      if (foreground) void activate(false).catch(() => {});
      else void register().catch(() => {});
    });
    const network = addNetworkStateListener(state => {
      if (state.isConnected && foreground && current()) void activate(false).catch(() => {});
    });
    const timer = setInterval(() => { if (foreground && current()) void activate(false).catch(() => {}); }, 30000);
    const dispose = () => {
      stopped = true; responses.remove(); tokens.remove(); appState.remove(); network.remove(); clearInterval(timer);
      if (generation === this.generation) { this.activate = null; this.refreshRegistration = null; }
    };
    this.dispose = dispose;
    void Notifications.getLastNotificationResponseAsync().then(initial => { if (initial) open(initial); }).catch(() => {});
    return dispose;
  }
  async refresh() { await this.refreshRegistration?.(); }
  async retry() { await this.activate?.(true); }
  async test(base: string, auth: string) {
    await this.retry();
    if (this.state.status !== "enabled") throw new Error(this.state.message);
    const result = await mobileApi(base, auth).testPush();
    if (!result.sent) throw new Error("Проверочное уведомление не отправлено. Повторите подключение.");
  }
  async disconnect(base: string, auth: string) {
    this.dispose?.(); this.dispose = null; ++this.generation;
    this.activate = null; this.refreshRegistration = null; this.token = null;
    await this.writes.catch(() => {});
    await mobileApi(base, auth).unregisterPush(await this.installation()).catch(() => {});
    await Notifications.setBadgeCountAsync(0).catch(() => {});
  }
  getFCMToken() { return this.token; }
  onMessage(callback: (data: any) => void) { this.callback = callback; }
  handleMessage(data: any) { this.callback?.(data); }
  async showLocalNotification() {
    await Notifications.scheduleNotificationAsync({
      content: { title: "67chat", body: "Новое сообщение", sound: "default" },
      trigger: { channelId: "messages" },
    });
  }
  async setBadgeCount(count = 0) { if (this.configured) await Notifications.setBadgeCountAsync(count); }
  async clearBadge() { await this.setBadgeCount(0); }
}
export const pushNotifications = new PushNotifications();
