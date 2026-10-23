import { initializeApp, applicationDefault, getApps } from "firebase-admin/app";
import { getMessaging } from "firebase-admin/messaging";
import crypto from 'node:crypto';
import { createWebPushSender } from "./web-push.js";
import { withPushDelivery } from "./account-push-policy.js";
import { isQuietNow,readQualityPreference } from './quality-features.js';

export const nativePushConfigured = () => !!(process.env.GOOGLE_APPLICATION_CREDENTIALS || process.env.FIREBASE_CONFIG);
export function createNativePushSender(pool, logger, options = {}) {
  let firebase;
  const configured = options.configured || nativePushConfigured;
  const messaging = options.messaging || (() => {
    firebase ||= getApps()[0] || initializeApp({ credential: applicationDefault() });
    return getMessaging(firebase);
  });
  return async (userId, _title, _body, data = {}, sessionId = null) => {
    if (!configured()) return { available: false, sent: 0 };
    const quality=await readQualityPreference(pool,userId);
    if (data.room_id && !data.call_event && !data.test && isQuietNow(quality.quiet,options.clock?.()||new Date())) return {available:true,sent:0,quiet:true};
    let sent = 0, failed = 0;
    try {
      const { rows } = await pool.query(
        `SELECT d.installation_id,d.token,d.quick_reply,d.session_id FROM push_devices d
        JOIN sessions s ON s.id=d.session_id AND s.user_id=d.user_id AND s.expires_at>now()
        LEFT JOIN room_preferences p ON p.user_id=d.user_id AND p.room_id=$2
        WHERE d.user_id=$1 AND ($3::uuid IS NULL OR d.session_id=$3)
        AND coalesce(p.muted,false)=false
        AND ($2::uuid IS NULL OR EXISTS(SELECT 1 FROM room_members rm WHERE rm.room_id=$2 AND rm.user_id=d.user_id))
        AND NOT coalesce(d.active_room_id=$2 AND d.updated_at>now()-interval '90 seconds',false)`,
        [userId, data.room_id || null, sessionId],
      );
      const { rows: unread } = await pool.query(
        `SELECT count(*)::int AS n FROM messages m
        JOIN room_members rm ON rm.room_id=m.room_id AND rm.user_id=$1
        LEFT JOIN room_preferences p ON p.room_id=m.room_id AND p.user_id=$1
        WHERE m.sender_id<>$1 AND coalesce(p.muted,false)=false
        AND NOT coalesce(m.read_by,'[]'::jsonb) @> $2::jsonb`,
        [userId, JSON.stringify([userId])],
      );
      for (const device of rows) {
        try {
          const call = !!data.call_event;
          const quickReply = !!device.quick_reply && !call && !!data.room_id && !data.test;
          const delivery=await withPushDelivery(pool,device.installation_id,async client=>{
            const live=await client.query(`SELECT 1 FROM push_devices d
              JOIN sessions s ON s.id=d.session_id AND s.user_id=d.user_id AND s.expires_at>now()
              LEFT JOIN room_preferences p ON p.user_id=d.user_id AND p.room_id=$2
              WHERE d.user_id=$1 AND d.installation_id=$3 AND d.token=$4 AND d.session_id=$5
              AND coalesce(p.muted,false)=false
              AND ($2::uuid IS NULL OR EXISTS(SELECT 1 FROM room_members rm WHERE rm.room_id=$2 AND rm.user_id=d.user_id))
              AND NOT coalesce(d.active_room_id=$2 AND d.updated_at>now()-interval '90 seconds',false)`,[userId,data.room_id||null,device.installation_id,device.token,device.session_id]);
            if(!live.rows.length)return false;
            // Firebase Admin bounds each HTTP attempt (15 seconds, bounded SDK retries).
            // Keep the installation fence held until the provider operation settles.
            try{await messaging().send({
            token: device.token,
            ...(call || quickReply ? {} : {notification: { title: "67chat", body: data.test ? "Уведомления работают" : "Новое сообщение" }}),
            data: {...Object.fromEntries(Object.entries(data).map(([key, value]) => [key, String(value)])),recipient_id:String(userId),recipientId:String(userId),group_notifications:String(quality.group_notifications),...(quickReply?{quality_message:'true'}:{})},
            android: {
              priority: "high", ttl: call ? 45000 : 3600000,
              ...(call || quickReply ? {} : {notification: {
                channelId: "messages", sound: "default", defaultVibrateTimings: true,
                tag: String(userId)+":"+(quality.group_notifications?(data.room_id || "67chat-test"):(data.message_id || crypto.randomUUID())), notificationCount: unread[0]?.n || 0,
              }}),
            },
            });return true;}catch(error){return{error};}
          });
          if(delivery?.error)throw delivery.error;
          if(delivery===true)sent++;
        } catch (error) {
          failed++;
          if (["messaging/registration-token-not-registered", "messaging/invalid-registration-token"].includes(error.code))
            await pool.query("DELETE FROM push_devices WHERE user_id=$1 AND installation_id=$2 AND token=$3", [userId, device.installation_id, device.token]);
          else logger.warn("Push delivery failed", { code: error.code || "unavailable" });
        }
      }
    } catch (error) {
      failed++;
      logger.warn("Push unavailable", { code: error.code || "unavailable" });
    }
    return { available: true, sent, failed };
  };
}

export function installNativePush(app, { pool, requireAuth, logger, options }) {
  const configured = options?.configured || nativePushConfigured;
  const send = createNativePushSender(pool, logger, options);
  app.get("/me/push-status", async req => {
    const me = await requireAuth(req);
    const { rows } = await pool.query("SELECT count(*)::int AS n FROM push_devices WHERE user_id=$1 AND session_id=$2", [me.id, me.sessionId]);
    return { available: configured(), registered: rows[0].n > 0 };
  });
  app.post("/me/push-test", { config: { rateLimit: { max: 3, timeWindow: "1 minute" } } }, async (req, reply) => {
    const me = await requireAuth(req);
    if (!configured()) return reply.code(503).send({ error: "push_unavailable" });
    return send(me.id, "", "", { test: true }, me.sessionId);
  });
}

export function createPushSender(pool, logger) {
  const native = createNativePushSender(pool, logger), browser = createWebPushSender(pool, logger);
  return async (userId, title, body, data) => {
    const results = await Promise.allSettled([native(userId,title,body,data), ...(data.call_event ? [] : [browser(userId,data)])]);
    for (const result of results) if (result.status === "rejected") logger.warn("Push dispatch failed", { code: result.reason?.code || "unavailable" });
  };
}
