// Browser Web Push uses its standard push service; no Firebase project is needed.
export function createWebNotifications({ api, activeRoom, status }) {
  let registration, subscription, timer, stopped = true;
  const supported = () => 'Notification' in window && 'serviceWorker' in navigator && 'PushManager' in window;
  const report = enabled => status({ enabled, permission: supported() ? Notification.permission : 'unsupported', registration });
  async function register() {
    registration ||= await navigator.serviceWorker.register('/sw.js');
    await navigator.serviceWorker.ready;
    return registration;
  }
  async function heartbeat(inactive = false) {
    if (stopped || !subscription) return;
    const result = await api('/me/web-push', { method: 'PUT', keepalive: true, body: JSON.stringify({ subscription: subscription.toJSON(), active_room_id: inactive || document.hidden || !document.hasFocus() ? null : activeRoom() }) });
    if (!result) throw new Error('Не удалось сохранить подписку на уведомления');
  }
  async function enable(prompt = true) {
    try {
    if (!supported()) throw new Error('Уведомления недоступны. На iPhone добавьте сайт на главный экран и откройте оттуда.');
    if (prompt && Notification.permission === 'default') await Notification.requestPermission();
    if (Notification.permission !== 'granted') throw new Error('Разрешите уведомления для 67chat в настройках браузера.');
    stopped = false;
    const config = await api('/web-push/config');
    if (!config?.enabled) throw new Error('Сервер уведомлений временно недоступен');
    const worker = await register();
    subscription = await worker.pushManager.getSubscription();
    if (!subscription) {
      const raw = atob(config.public_key.replace(/-/g, '+').replace(/_/g, '/'));
      subscription = await worker.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: Uint8Array.from(raw, char => char.charCodeAt(0)) });
    }
    await heartbeat();
    clearInterval(timer); timer = setInterval(() => heartbeat().catch(() => {}), 25000);
    localStorage.removeItem('web-push-disabled'); report(true);
    } catch (error) {
      stopped = true; clearInterval(timer); report(false);
      throw error;
    }
  }
  async function disable() {
    stopped = true; clearInterval(timer);
    const current = subscription || (registration && await registration.pushManager.getSubscription());
    if (current) {
      await api('/me/web-push', { method: 'DELETE', body: JSON.stringify({ endpoint: current.endpoint }) });
      await current.unsubscribe();
    }
    subscription = null; report(false);
  }
  document.addEventListener('visibilitychange', () => heartbeat().catch(() => {}));
  window.addEventListener('focus', () => heartbeat().catch(() => {}));
  window.addEventListener('blur', () => heartbeat().catch(() => {}));
  window.addEventListener('pagehide', () => heartbeat(true).catch(() => {}));
  return {
    async start() {
      if (!supported()) { report(false); return; }
      stopped = false;
      await register();
      report(false);
      if (Notification.permission === 'granted' && localStorage.getItem('web-push-disabled') !== 'yes') await enable(false);
    },
    enable,
    async toggle() {
      if (subscription && !stopped) { localStorage.setItem('web-push-disabled','yes'); await disable(); }
      else await enable();
    },
    disable,
    async test() {
      if (!subscription) await enable();
      const result = await api('/me/web-push/test', { method: 'POST', body: '{}' });
      if (!result?.sent) throw new Error('Проверочное уведомление не отправлено. Обновите подписку и повторите.');
    },
    subscribed: () => !!subscription && !stopped,
  };
}
