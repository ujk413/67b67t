export class PushNotifications {
  private static instance: PushNotifications;
  private onMessageCallback: ((data: any) => void) | null = null;

  private constructor() {}

  static getInstance(): PushNotifications {
    if (!PushNotifications.instance) {
      PushNotifications.instance = new PushNotifications();
    }
    return PushNotifications.instance;
  }

  async initialize(): Promise<string | null> {
    return null;
  }

  getFCMToken(): string | null {
    return null;
  }

  onMessage(callback: (data: any) => void): void {
    this.onMessageCallback = callback;
  }

  handleMessage(data: any): void {
    this.onMessageCallback?.(data);
  }

  async showLocalNotification(): Promise<void> {}

  async setBadgeCount(): Promise<void> {}

  async clearBadge(): Promise<void> {}
}

export const pushNotifications = PushNotifications.getInstance();
