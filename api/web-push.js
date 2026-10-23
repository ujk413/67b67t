import crypto from 'node:crypto';
import webpush from 'web-push';
import {isQuietNow,readQualityPreference} from './quality-features.js';

export const webPushSchema = `CREATE TABLE IF NOT EXISTS web_push_subscriptions (
  endpoint text NOT NULL, user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  session_id uuid NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  subscription jsonb NOT NULL, active_room_id uuid, updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(user_id,endpoint)
);
DO $web_push_accounts$
DECLARE previous_key text;
BEGIN
  LOCK TABLE web_push_subscriptions IN ACCESS EXCLUSIVE MODE;
  IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='web_push_subscriptions'::regclass AND contype='p' AND pg_get_constraintdef(oid)='PRIMARY KEY (user_id, endpoint)') THEN
    SELECT conname INTO previous_key FROM pg_constraint WHERE conrelid='web_push_subscriptions'::regclass AND contype='p';
    IF previous_key IS NOT NULL THEN EXECUTE format('ALTER TABLE web_push_subscriptions DROP CONSTRAINT %I',previous_key);END IF;
    ALTER TABLE web_push_subscriptions ADD PRIMARY KEY(user_id,endpoint);
  END IF;
END $web_push_accounts$;
CREATE INDEX IF NOT EXISTS web_push_user ON web_push_subscriptions(user_id);`;
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
export function createWebPushSender(pool, logger, transport = webpush, options = {}) {
  return async (userId, data = {}, sessionId = null) => {
    const keys = vapid();
    if (!keys) return { sent: 0 };
    const quality=await readQualityPreference(pool,userId);
    if(data.room_id&&!data.test&&isQuietNow(quality.quiet,options.clock?.()||new Date()))return{sent:0,quiet:true};
    const { rows } = await pool.query(`SELECT d.endpoint,d.subscription FROM web_push_subscriptions d
      JOIN sessions s ON s.id=d.session_id AND s.user_id=d.user_id AND s.expires_at>now()
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
          recipientId:String(userId),recipient_id:String(userId),roomId: data.room_id || null, messageId: data.message_id || null,
          tag: String(userId)+":"+(quality.group_notifications?(data.room_id || crypto.randomUUID()):(data.message_id || crypto.randomUUID())), url: '/?account_id='+encodeURIComponent(userId)+(data.room_id?'&room='+encodeURIComponent(data.room_id):''),
        });
        await transport.sendNotification(device.subscription, payload, { vapidDetails: keys, TTL: 3600, urgency: 'high', timeout: 10000 });
        sent++;
      } catch (error) {
        if ([404,410].includes(error.statusCode)) await pool.query('DELETE FROM web_push_subscriptions WHERE user_id=$1 AND endpoint=$2 AND subscription=$3::jsonb', [userId,device.endpoint,JSON.stringify(device.subscription)]);
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
      VALUES($1,$2,$3,$4::jsonb,$5) ON CONFLICT(user_id,endpoint) DO UPDATE SET session_id=$3,subscription=$4::jsonb,active_room_id=$5,updated_at=now()`,
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
