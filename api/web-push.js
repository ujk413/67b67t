import crypto from 'node:crypto';
import webpush from 'web-push';

export const webPushSchema = `CREATE TABLE IF NOT EXISTS web_push_subscriptions (
  endpoint text PRIMARY KEY, user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  session_id uuid NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  subscription jsonb NOT NULL, active_room_id uuid, updated_at timestamptz NOT NULL DEFAULT now()
); CREATE INDEX IF NOT EXISTS web_push_user ON web_push_subscriptions(user_id);`;
export function validateSubscription(value) {
  try {
    const url = new URL(value?.endpoint);
    const trusted = url.hostname === 'fcm.googleapis.com' || url.hostname === 'web.push.apple.com' ||
      url.hostname.endsWith('.push.services.mozilla.com') || url.hostname.endsWith('.notify.windows.com');
    return trusted && url.protocol === 'https:' && (!url.port || url.port === '443') && !url.username && !url.password &&
      !url.hash && value.endpoint.length < 4096 && /^[A-Za-z0-9_-]{86,90}$/.test(value.keys?.p256dh || '') &&
      /^[A-Za-z0-9_-]{20,25}$/.test(value.keys?.auth || '');
  } catch { return false; }
}
function vapid() {
  const { WEB_PUSH_PUBLIC_KEY: publicKey, WEB_PUSH_PRIVATE_KEY: privateKey, WEB_PUSH_SUBJECT: subject = 'https://67chat.ru' } = process.env;
  return publicKey && privateKey ? { publicKey, privateKey, subject } : null;
}
export function createWebPushSender(pool, logger, transport = webpush) {
  return async (userId, data = {}, sessionId = null) => {
    const keys = vapid();
    if (!keys) return { sent: 0 };
    const { rows } = await pool.query(`SELECT d.endpoint,d.subscription FROM web_push_subscriptions d
      JOIN sessions s ON s.id=d.session_id AND s.expires_at>now()
      LEFT JOIN room_preferences p ON p.user_id=d.user_id AND p.room_id=$2
      WHERE d.user_id=$1 AND ($3::uuid IS NULL OR d.session_id=$3)
      AND coalesce(p.muted,false)=false
      AND ($2::uuid IS NULL OR EXISTS(SELECT 1 FROM room_members rm WHERE rm.room_id=$2 AND rm.user_id=d.user_id))
      AND NOT coalesce(d.active_room_id=$2 AND d.updated_at>now()-interval '60 seconds',false)`, [userId, data.room_id || null, sessionId]);
    let sent = 0;
    for (const device of rows) {
      if (!validateSubscription(device.subscription)) continue;
      try {
        const payload = JSON.stringify({ title: '67chat', body: data.test ? 'Уведомления работают' : 'Новое сообщение',
          roomId: data.room_id || null, messageId: data.message_id || null,
          tag: data.message_id || crypto.randomUUID(), url: data.room_id ? '/?room=' + encodeURIComponent(data.room_id) : '/',
        });
        await transport.sendNotification(device.subscription, payload, { vapidDetails: keys, TTL: 3600, urgency: 'high', timeout: 10000 });
        sent++;
      } catch (error) {
        if ([404,410].includes(error.statusCode)) await pool.query('DELETE FROM web_push_subscriptions WHERE endpoint=$1', [device.endpoint]);
        else logger.warn('Web push delivery failed', { code: error.statusCode || error.code || 'network' });
      }
    }
    return { sent };
  };
}
export async function installWebPush(app, { pool, requireAuth, isRoomMember, logger, transport }) {
  await pool.query(webPushSchema);
  const sender = createWebPushSender(pool, logger, transport);
  app.get('/web-push/config', async req => {
    await requireAuth(req);
    const keys = vapid();
    return { enabled: !!keys, public_key: keys?.publicKey || null };
  });
  app.put('/me/web-push', async (req, reply) => {
    const me = await requireAuth(req), subscription = req.body?.subscription;
    if (!validateSubscription(subscription)) return reply.code(400).send({ error: 'bad_subscription' });
    const room = req.body.active_room_id || null;
    if (room && !(await isRoomMember(room, me.id))) return reply.code(404).send({ error: 'not_found' });
    await pool.query(`INSERT INTO web_push_subscriptions(endpoint,user_id,session_id,subscription,active_room_id)
      VALUES($1,$2,$3,$4::jsonb,$5) ON CONFLICT(endpoint) DO UPDATE SET user_id=$2,session_id=$3,subscription=$4::jsonb,active_room_id=$5,updated_at=now()`,
      [subscription.endpoint, me.id, me.sessionId, JSON.stringify({ endpoint: subscription.endpoint, keys: subscription.keys }), room]);
    return { ok: true };
  });
  app.delete('/me/web-push', async req => {
    const me = await requireAuth(req);
    await pool.query('DELETE FROM web_push_subscriptions WHERE endpoint=$1 AND user_id=$2 AND session_id=$3', [req.body?.endpoint || '', me.id, me.sessionId]);
    return { ok: true };
  });
  app.post('/me/web-push/test', { config: { rateLimit: { max: 3, timeWindow: '1 minute' } } }, async req => {
    const me = await requireAuth(req);
    return sender(me.id, { test: true }, me.sessionId);
  });
}
