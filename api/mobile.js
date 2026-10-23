import fs from "node:fs";
import crypto from "node:crypto";
import { installOrganization } from "./organization.js";
import { withPushRegistration } from "./account-push-policy.js";

export const messageSelect = `SELECT m.*,u.username AS sender_username,u.display_name AS sender_display_name,
  u.avatar_id AS sender_avatar_id,md.kind AS media_kind,md.mime AS media_mime,md.size_bytes AS media_size,
  md.thumb_path,md.duration_sec,md.width,md.height FROM messages m JOIN users u ON u.id=m.sender_id
  LEFT JOIN media md ON md.id=m.media_id`;

export async function getMobileMessage(
  pool,
  id,
  decodeMetadata = (value) => value,
  viewerId = null,
) {
  const { rows } = await pool.query(`${messageSelect} WHERE m.id=$1 AND ($2::uuid IS NULL OR NOT EXISTS(SELECT 1 FROM hidden_messages h WHERE h.message_id=m.id AND h.user_id=$2))`, [id,viewerId]);
  if (!rows[0]) return null;
  const { rows: reactions } = await pool.query(
    "SELECT user_id,emoji FROM reactions WHERE message_id=$1",
    [id],
  );
  const grouped = {};
  for (const r of reactions) (grouped[r.emoji] ||= []).push(r.user_id);
  return {
    ...rows[0],
    sender_display_name: decodeMetadata(rows[0].sender_display_name),
    reactions: grouped,
  };
}

export async function createDirectRoom(pool, a, b) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", [
      [a, b].sort().join(":"),
    ]);
    const { rows: existing } = await client.query(
      "SELECT r.id FROM rooms r JOIN room_members a ON a.room_id=r.id AND a.user_id=$1 JOIN room_members b ON b.room_id=r.id AND b.user_id=$2 WHERE r.kind='dm' LIMIT 1",
      [a, b],
    );
    if (existing.length) {
      await client.query("COMMIT");
      return existing[0].id;
    }
    const { rows } = await client.query(
      "INSERT INTO rooms(kind,title,created_by) VALUES('dm',NULL,$1) RETURNING id",
      [a],
    );
    await client.query(
      "INSERT INTO room_members(room_id,user_id,role) VALUES($1,$2,'owner'),($1,$3,'member')",
      [rows[0].id, a, b],
    );
    await client.query("COMMIT");
    return rows[0].id;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function installMobileRoutes(app, deps) {
  const {
    pool,
    requireAuth,
    isRoomMember,
    getRoomRole,
    notifyRoomListUpdate,
    broadcastToRoom,
    decryptMetadata = (value) => value,
  } = deps;
  await pool.query(
    fs.readFileSync(new URL("./mobile-schema.sql", import.meta.url), "utf8"),
  );
  await installOrganization(app, deps);
  const member = async (req, reply) => {
    const me = await requireAuth(req);
    if (!(await isRoomMember(req.params.id, me.id))) {
      reply.code(404).send({ error: "not_found" });
      return null;
    }
    return me;
  };
  app.get("/mobile/capabilities", async (req) => {
    await requireAuth(req);
    return { version: 1, idempotent_send: true, sync: true };
  });
  app.post('/rooms/:id/messages/:messageId/listened',async(req,reply)=>{
    const me=await member(req,reply);if(!me)return;
    const {rows}=await pool.query("SELECT m.id,m.type FROM messages m WHERE m.id=$1 AND m.room_id=$2 AND NOT EXISTS(SELECT 1 FROM hidden_messages h WHERE h.user_id=$3 AND h.message_id=m.id)",[req.params.messageId,req.params.id,me.id]);
    if(!rows.length)return reply.code(404).send({error:'message_not_found'});
    if(!['voice','video_note'].includes(rows[0].type))return reply.code(400).send({error:'not_playable'});
    const result=await pool.query("UPDATE messages SET listened_by=listened_by || $2::jsonb WHERE id=$1 AND NOT listened_by @> $2::jsonb RETURNING listened_by",[rows[0].id,JSON.stringify([me.id])]);
    if(result.rows.length)broadcastToRoom(req.params.id,{type:'message.listened',room_id:req.params.id,message_id:rows[0].id,user_id:me.id,listened_by:result.rows[0].listened_by});
    return {ok:true};
  });
  app.get("/rooms/:id/history", async (req, reply) => {
    const me = await member(req, reply);
    if (!me) return;
    const { id } = req.params;
    // Apply personal tombstones to every page, cursor, unread anchor and count.
    const visibleQuery=(sql,args=[])=>pool.query(
      `WITH visible_messages AS (SELECT m.* FROM messages m WHERE NOT EXISTS(SELECT 1 FROM hidden_messages h WHERE h.message_id=m.id AND h.user_id=$${args.length+1})) ` + sql.replace(/\bmessages\b/g,'visible_messages'),
      [...args,me.id],
    );
    const limit = Math.max(1, Math.min(100, Number(req.query.limit) || 50));
    const anchor = req.query.around || req.query.before || req.query.after;
    let target = null;
    if (anchor) {
      const { rows } = await visibleQuery(
        // JS Date truncates PostgreSQL microseconds and can repeat/skip a cursor.
        "SELECT id,created_at::text AS created_at FROM messages WHERE id=$1 AND room_id=$2",
        [anchor, id],
      );
      target = rows[0];
      if (!target) return reply.code(404).send({ error: "message_not_found" });
    }
    let messages;
    if (req.query.around) {
      const half = Math.floor(limit / 2);
      const { rows: left } = await visibleQuery(
        `${messageSelect} WHERE m.room_id=$1 AND (m.created_at,m.id)<=($2,$3) ORDER BY m.created_at DESC,m.id DESC LIMIT $4`,
        [id, target.created_at, target.id, half + 1],
      );
      const { rows: right } = await visibleQuery(
        `${messageSelect} WHERE m.room_id=$1 AND (m.created_at,m.id)>($2,$3) ORDER BY m.created_at,m.id LIMIT $4`,
        [id, target.created_at, target.id, half],
      );
      messages = [...left.reverse(), ...right];
    } else {
      const args = [id];
      let clause = "";
      if (target) {
        args.push(target.created_at, target.id);
        clause = ` AND (m.created_at,m.id)${req.query.after ? ">" : "<"}($2,$3)`;
      }
      args.push(limit);
      const { rows } = await visibleQuery(
        `${messageSelect} WHERE m.room_id=$1${clause} ORDER BY m.created_at ${req.query.after ? "ASC" : "DESC"},m.id ${req.query.after ? "ASC" : "DESC"} LIMIT $${args.length}`,
        args,
      );
      messages = req.query.after ? rows : rows.reverse();
    }
    const first = messages[0],
      last = messages.at(-1);
    const { rows: more } = await visibleQuery(
      `SELECT EXISTS(SELECT 1 FROM messages WHERE room_id=$1 AND (created_at,id)<
        (SELECT created_at,id FROM messages WHERE room_id=$1 AND id=$2)) AS older,
      EXISTS(SELECT 1 FROM messages WHERE room_id=$1 AND (created_at,id)>
        (SELECT created_at,id FROM messages WHERE room_id=$1 AND id=$3)) AS newer`,
      [
        id,
        first?.id || null,
        last?.id || null,
      ],
    );
    const { rows: unread } = await visibleQuery(
      `SELECT id FROM messages WHERE room_id=$1 AND sender_id<>$2 AND NOT coalesce(read_by,'[]') @> $3::jsonb ORDER BY created_at,id LIMIT 1`,
      [id, me.id, JSON.stringify([me.id])],
    );
    const reactionsByMessage=new Map();
    if(messages.length){
      const {rows}=await pool.query('SELECT message_id,user_id,emoji FROM reactions WHERE message_id=ANY($1::uuid[])',[messages.map(m=>m.id)]);
      for(const row of rows){let grouped=reactionsByMessage.get(row.message_id);if(!grouped){grouped={};reactionsByMessage.set(row.message_id,grouped);}(grouped[row.emoji]||=[]).push(row.user_id);}
    }
    for(const m of messages){m.sender_display_name=decryptMetadata(m.sender_display_name);m.reactions=reactionsByMessage.get(m.id)||{};}
    return {
      messages,
      has_older: more[0].older,
      has_newer: more[0].newer,
      first_unread_id: unread[0]?.id || null,
    };
  });
  app.get("/sync", async (req) => {
    const me = await requireAuth(req);
    const cursor = String(req.query.cursor || "0");
    if (!/^\d{1,19}$/.test(cursor))
      throw Object.assign(new Error("bad_cursor"), { statusCode: 400 });
    const { rows: rooms } = await pool.query(
      "SELECT room_id FROM room_members WHERE user_id=$1",
      [me.id],
    );
    const { rows } = await pool.query(
      `SELECT c.* FROM message_changes c JOIN room_members rm ON rm.room_id=c.room_id AND rm.user_id=$1 WHERE c.seq>$2::bigint ORDER BY c.seq LIMIT 200`,
      [me.id, cursor],
    );
    const changes = [];
    for (const row of rows) {
      const message =
        row.operation === "delete"
          ? null
          : await getMobileMessage(pool, row.message_id, decryptMetadata, me.id);
      changes.push({
        seq: String(row.seq),
        room_id: row.room_id,
        message_id: row.message_id,
        operation: message ? "upsert" : "delete",
        message,
      });
    }
    return {
      cursor: rows.length ? String(rows.at(-1).seq) : cursor,
      has_more: rows.length === 200,
      room_ids: rooms.map((r) => r.room_id),
      changes,
    };
  });
  app.get("/me/room-preferences", async (req) => {
    const me = await requireAuth(req);
    return (
      await pool.query("SELECT * FROM room_preferences WHERE user_id=$1", [
        me.id,
      ])
    ).rows;
  });
  app.put("/rooms/:id/preferences", async (req, reply) => {
    const me = await member(req, reply);
    if (!me) return;
    const fields = ["pinned", "muted", "archived", "hidden", "marked_unread"];
    await pool.query(
      "INSERT INTO room_preferences(user_id,room_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
      [me.id, req.params.id],
    );
    for (const key of fields)
      if (typeof req.body?.[key] === "boolean")
        await pool.query(
          `UPDATE room_preferences SET ${key}=$3 WHERE user_id=$1 AND room_id=$2`,
          [me.id, req.params.id, req.body[key]],
        );
    await notifyRoomListUpdate(me.id);
    return { ok: true };
  });
  app.put("/me/push-devices/:installationId", async (req, reply) => {
    const me = await requireAuth(req);
    const installation = String(req.params.installationId);
    const token = String(req.body?.token || "");
    if (
      !/^[a-zA-Z0-9_-]{8,100}$/.test(installation) ||
      !token ||
      token.length > 4096
    )
      return reply.code(400).send({ error: "bad_device" });
    const active = req.body?.active_room_id || null;
    const quickReply=req.body?.quick_reply===true;
    if (active && !(await isRoomMember(active, me.id)))
      return reply.code(404).send({ error: "not_found" });
    await withPushRegistration(pool,me,installation,req.body,client=>client.query(
      `INSERT INTO push_devices(installation_id,user_id,session_id,token,active_room_id,quick_reply) VALUES($1,$2,$3,$4,$5,$6)
      ON CONFLICT(user_id,installation_id) DO UPDATE SET session_id=$3,token=$4,active_room_id=$5,quick_reply=$6,updated_at=now()`,
      [installation, me.id, me.sessionId, token, active,quickReply],
    ));
    return { ok: true };
  });
  app.delete("/me/push-devices/:installationId", async (req) => {
    const me = await requireAuth(req);
    await pool.query(
      "DELETE FROM push_devices WHERE installation_id=$1 AND user_id=$2 AND session_id=$3",
      [req.params.installationId, me.id, me.sessionId],
    );
    return { ok: true };
  });
  app.get("/rooms/:id/media", async (req, reply) => {
    const me=await member(req,reply);if(!me)return;
    const before = req.query.before || null;
    const kind = ["image", "video", "file"].includes(req.query.kind) ? req.query.kind : "all";
    return (
      await pool.query(
        `${messageSelect} WHERE m.room_id=$1 AND m.media_id IS NOT NULL
        AND NOT EXISTS(SELECT 1 FROM hidden_messages h WHERE h.message_id=m.id AND h.user_id=$4)
        AND ($2::uuid IS NULL OR (m.created_at,m.id)<(SELECT created_at,id FROM messages WHERE id=$2 AND room_id=$1))
        AND ($3='all' OR ($3='image' AND coalesce(m.meta#>>'{media_encryption,mime}',md.mime) LIKE 'image/%')
          OR ($3='video' AND (m.type='video_note' OR coalesce(m.meta#>>'{media_encryption,mime}',md.mime) LIKE 'video/%'))
          OR ($3='file' AND m.type<>'voice' AND m.type<>'video_note' AND coalesce(m.meta#>>'{media_encryption,mime}',md.mime,'') NOT LIKE 'image/%' AND coalesce(m.meta#>>'{media_encryption,mime}',md.mime,'') NOT LIKE 'video/%'))
        ORDER BY m.created_at DESC,m.id DESC LIMIT 100`,
        [req.params.id, before, kind,me.id],
      )
    ).rows;
  });
  app.get('/me/hidden-messages',async(req)=>{
    const me=await requireAuth(req),after=req.query.after || null;
    if(after && !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(after))throw Object.assign(Error('bad_cursor'),{statusCode:400});
    const {rows}=await pool.query(`SELECT h.message_id,m.room_id FROM hidden_messages h JOIN messages m ON m.id=h.message_id
      WHERE h.user_id=$1 AND ($2::uuid IS NULL OR h.message_id>$2) ORDER BY h.message_id LIMIT 200`,[me.id,after]);
    return {items:rows,next_cursor:rows.length===200?rows.at(-1).message_id:null};
  });
  app.post("/rooms/:id/invites", async (req, reply) => {
    const me = await member(req, reply);
    if (!me) return;
    if (!["owner", "admin"].includes(await getRoomRole(req.params.id, me.id)))
      return reply.code(403).send({ error: "forbidden" });
    const { rows } = await pool.query("SELECT kind FROM rooms WHERE id=$1", [
      req.params.id,
    ]);
    if (rows[0]?.kind !== "group")
      return reply.code(400).send({ error: "group_required" });
    const token = crypto.randomBytes(24).toString("base64url");
    await pool.query(
      "INSERT INTO room_invites(token,room_id,created_by) VALUES($1,$2,$3)",
      [token, req.params.id, me.id],
    );
    return { token };
  });
  app.delete("/rooms/:id/invites/:token", async (req, reply) => {
    const me = await member(req, reply);
    if (!me) return;
    if (!["owner", "admin"].includes(await getRoomRole(req.params.id, me.id)))
      return reply.code(403).send({ error: "forbidden" });
    await pool.query("DELETE FROM room_invites WHERE token=$1 AND room_id=$2", [
      req.params.token,
      req.params.id,
    ]);
    return { ok: true };
  });
  app.post("/invites/:token/join", async (req, reply) => {
    const me = await requireAuth(req);
    const { rows } = await pool.query(
      `SELECT i.room_id FROM room_invites i JOIN rooms r ON r.id=i.room_id WHERE i.token=$1 AND r.kind='group'`,
      [req.params.token],
    );
    if (!rows[0]) return reply.code(404).send({ error: "invite_invalid" });
    const id = rows[0].room_id;
    await pool.query(
      "INSERT INTO room_members(room_id,user_id,role) VALUES($1,$2,'member') ON CONFLICT DO NOTHING",
      [id, me.id],
    );
    broadcastToRoom(id, { type: "room.members_changed", room_id: id });
    await notifyRoomListUpdate(me.id);
    return { id };
  });
}
