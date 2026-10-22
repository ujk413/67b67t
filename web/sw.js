// Service Worker for Push Notifications and PWA
const CACHE_NAME = '67chat-v20261023-001';
const urlsToCache = [
  '/manifest.webmanifest', '/icons/app-192.png', '/icons/notification-96.png'
];

// Install event - cache static assets
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(urlsToCache))
      .then(() => self.skipWaiting())
  );
});

// Activate event - clean old caches
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames.map(cacheName => {
          if (cacheName !== CACHE_NAME) {
            return caches.delete(cacheName);
          }
        })
      );
    }).then(() => self.clients.claim()).then(() => {
      return self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    }).then(clients => {
      clients.forEach(client => {
        client.postMessage({ type: 'UPDATE_AVAILABLE' });
      });
    })
  );
});

// Fetch event - network first for app shell/assets, cache fallback for offline.
self.addEventListener('fetch', event => {
  if (event.request.method !== 'GET') return;
  if (event.request.url.includes('/api/')) return;

  event.respondWith(
    fetch(event.request).then(response => {
      if (!response || response.status !== 200 || response.type !== 'basic') {
        return response;
      }
      const responseToCache = response.clone();
      caches.open(CACHE_NAME).then(cache => {
        cache.put(event.request, responseToCache);
      });
      return response;
    }).catch(() => {
      return caches.match(event.request).then(response => {
        if (response) return response;
        if (event.request.mode === 'navigate') {
          return caches.match('/');
        }
        return Response.error();
      });
    })
  );
});

// Push payloads deliberately contain no message text or sender name.
self.addEventListener('push', event => {
  let data = {};
  try { data = event.data ? event.data.json() : {}; } catch {}
  event.waitUntil(self.registration.showNotification(data.title || '67chat', {
    body: data.body || 'Новое сообщение', icon: '/icons/app-192.png', badge: '/icons/notification-96.png',
    tag: data.roomId ? 'room-' + data.roomId : data.tag || '67chat-test',
    renotify: true, vibrate: [120,60,120], silent: false,
    data: { url: data.url || '/', roomId: data.roomId, messageId: data.messageId },
  }));
});
self.addEventListener('notificationclick', event => {
  event.notification.close();
  let target = new URL(event.notification.data?.url || '/', self.location.origin);
  if (target.origin !== self.location.origin) target = new URL('/', self.location.origin);
  event.waitUntil((async () => {
    const windows = await clients.matchAll({ type: 'window', includeUncontrolled: true });
    const client = windows.find(item => new URL(item.url).origin === self.location.origin);
    if (client) {
      await client.focus();
      client.postMessage({ type: 'OPEN_ROOM', roomId: event.notification.data?.roomId });
      return;
    }
    return clients.openWindow(target.href);
  })());
});
self.addEventListener('pushsubscriptionchange', event => {
  event.waitUntil(clients.matchAll({ type: 'window' }).then(windows => {
    windows.forEach(client => client.postMessage({ type: 'PUSH_SUBSCRIPTION_CHANGED' }));
  }));
});
self.addEventListener('message', event => {
  if (event.data?.type === 'SKIP_WAITING') self.skipWaiting();
});
