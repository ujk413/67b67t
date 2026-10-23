import { ed25519 } from "@noble/curves/ed25519";
import { readFileSync } from "node:fs";
import { createHash, randomBytes } from "node:crypto";
import { installAccountFeatures, canReadReceipts } from "./account-features.js";
import { installPushPolicy } from "./account-push-policy.js";
const fail = (statusCode, message) => {
  throw Object.assign(Error(message), { statusCode });
};
const hash = (value) => createHash("sha256").update(value).digest("hex");
const ident = (value) => '"' + String(value).replaceAll('"', '""') + '"';
// Signal Curve25519 signatures encode the Edwards sign bit in signature[63].
// Convert Montgomery u to Edwards y=(u-1)/(u+1); strict Ed25519 verifies the signature.
export function verifyDeviceSignature(publicKey, message, encodedSignature) {
  try {
    const bytes = Buffer.from(publicKey, "base64"),
      signature = Buffer.from(encodedSignature, "base64");
    if (
      signature.length !== 64 ||
      signature.toString("base64") !== encodedSignature
    )
      return false;
    const mont =
      bytes.length === 33 && bytes[0] === 5
        ? bytes.subarray(1)
        : bytes.length === 32
          ? bytes
          : null;
    if (!mont || bytes.toString("base64") !== publicKey) return false;
    const p = (1n << 255n) - 19n,
      u = BigInt("0x" + Buffer.from(mont).reverse().toString("hex"));
    if (u >= p || u === 0n || u === 1n || u === p - 1n) return false;
    const mod = (x) => ((x % p) + p) % p;
    let exponent = p - 2n,
      base = mod(u + 1n),
      inverse = 1n;
    while (exponent) {
      if (exponent & 1n) inverse = mod(inverse * base);
      base = mod(base * base);
      exponent >>= 1n;
    }
    const y = mod((u - 1n) * inverse),
      ed = Buffer.from(y.toString(16).padStart(64, "0"), "hex").reverse();
    ed[31] |= signature[63] & 128;
    signature[63] &= 127;
    return ed25519.verify(signature, Buffer.from(message, "utf8"), ed, {
      zip215: false,
    });
  } catch {
    return false;
  }
}
export async function applyAccountMigrations(pool) {
  for (const file of ["account-features.sql", "account-integration.sql", "account-push-policy.sql"])
    await pool.query(
      readFileSync(new URL("./" + file, import.meta.url), "utf8"),
    );
}
export function createSessionIssuer({
  pool,
  ttlDays = 30,
  wantsBearer,
  parseDevice,
  getClientIp,
  setCookie,
}) {
  return async (user, req, reply) => {
    const client = await pool.connect(),
      token = randomBytes(32).toString("hex"),
      days = Math.max(1, Math.min(365, Number(ttlDays) || 30));
    try {
      await client.query("BEGIN");
      const found = (
        await client.query(
          "SELECT id,username,banned,login_disabled_at,merged_into_user_id FROM users WHERE id=$1 FOR UPDATE",
          [user.id],
        )
      ).rows[0];
      if (
        !found ||
        found.banned ||
        found.login_disabled_at ||
        found.merged_into_user_id
      )
        fail(403, "account_disabled");
      const ua = String(req.headers["user-agent"] || "").slice(0, 512),
        device = parseDevice(ua);
      await client.query(
        `INSERT INTO sessions(user_id,token_hash,expires_at,device_name,device_type,ip_address,user_agent,last_activity) VALUES($1,$2,now()+($3::int*interval '1 day'),$4,$5,$6,$7,now())`,
        [
          found.id,
          hash(token),
          days,
          device.deviceName,
          device.deviceType,
          getClientIp(req) || null,
          ua,
        ],
      );
      await client.query("COMMIT");
      setCookie(req, reply, token, days, found);
      const result = { user: { id: found.id, username: found.username } };
      if (wantsBearer(req)) result.token = token;
      return result;
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  };
}
export async function installAccountIntegration(app, deps) {
  installPushPolicy(app,deps);
  const { pool } = deps;
  async function requireSession(me) {
    const row = (
      await pool.query(
        "SELECT 1 FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.id=$1 AND s.user_id=$2 AND s.expires_at>now() AND coalesce(u.banned,false)=false AND u.login_disabled_at IS NULL",
        [me.sessionId, me.id],
      )
    ).rows[0];
    if (!row) fail(401, "unauthorized");
  }
  async function getSessionDevice(me) {
    const row = (
      await pool.query(
        "SELECT b.device_id,b.public_key_hash,d.public_key FROM account_session_devices b JOIN sessions s ON s.id=b.session_id AND s.user_id=b.user_id AND s.expires_at>now() JOIN user_devices d ON d.user_id=b.user_id AND d.device_id=b.device_id WHERE b.session_id=$1 AND b.user_id=$2 AND b.proof_verified_at IS NOT NULL",
        [me.sessionId, me.id],
      )
    ).rows[0];
    return row && row.public_key_hash === hash(row.public_key)
      ? row.device_id
      : null;
  }
  async function validatePublishedDevice(
    me,
    deviceId,
    publicKey,
    client = pool,
  ) {
    const old = (
      await client.query(
        "SELECT device_id,public_key_hash FROM account_session_devices WHERE session_id=$1 AND user_id=$2",
        [me.sessionId, me.id],
      )
    ).rows[0];
    if (
      old &&
      (old.device_id !== deviceId || old.public_key_hash !== hash(publicKey))
    )
      fail(409, "session_device_already_bound");
    const existing = (
      await client.query(
        "SELECT public_key FROM user_devices WHERE user_id=$1 AND device_id=$2",
        [me.id, deviceId],
      )
    ).rows[0];
    if (!old && existing && hash(existing.public_key) !== hash(publicKey))
      fail(409, "device_identity_mismatch");
  }
  app.get("/me/account/capabilities", async (req) => {
    await deps.requireAuth(req);
    return {
      device_proof: true,
      backup_staging: true,
      secret_device_proof_required: true,
    };
  });
  app.post('/me/account/push-disable',async req=>{
    const me=await deps.requireAuth(req),id=req.body?.installation_id;
    await requireSession(me);
    if(typeof id!=='string'||!/^[A-Za-z0-9_-]{8,100}$/.test(id))fail(400,'invalid_installation');
    // A restored login may replace the local token while an older session owns
    // this same installation. Remove only this user's matching installation.
    const result=await pool.query('DELETE FROM push_devices WHERE user_id=$1 AND installation_id=$2 RETURNING installation_id',[me.id,id]);
    return{ok:true,removed:result.rows.length};
  });
  app.post(
    "/e2e/device-proof",
    { config: { rateLimit: { max: 30, timeWindow: "1 minute" } } },
    async (req) => {
      const me = await deps.requireAuth(req),
        { device_id: deviceId, identity_key: publicKey } = req.body || {};
      if (
        typeof deviceId !== "string" ||
        !/^[A-Za-z0-9._-]{1,64}$/.test(deviceId) ||
        typeof publicKey !== "string" ||
        publicKey.length > 100
      )
        fail(400, "invalid_device_proof_request");
      await requireSession(me);
      await validatePublishedDevice(me, deviceId, publicKey);
      const challenge = [
        "67chat-device-proof-v1",
        me.id,
        me.sessionId,
        deviceId,
        hash(publicKey),
        randomBytes(32).toString("base64url"),
      ].join(String.fromCharCode(10));
      await pool.query(
        "DELETE FROM account_device_proofs WHERE expires_at<=now()",
      );
      const row = (
        await pool.query(
          "INSERT INTO account_device_proofs(session_id,user_id,device_id,public_key_hash,challenge)VALUES($1,$2,$3,$4,$5)RETURNING id,expires_at",
          [me.sessionId, me.id, deviceId, hash(publicKey), challenge],
        )
      ).rows[0];
      return { ...row, user_id: me.id, device_id: deviceId, challenge };
    },
  );
  app.post("/me/account/backup/preflight", async (req) => {
    const me = await deps.requireAuth(req),
      b = req.body || {};
    await requireSession(me);
    if (
      typeof b.device_id !== "string" ||
      !/^[0-9]{1,10}$/.test(b.device_id) ||
      typeof b.identity_key !== "string" ||
      b.identity_key.length > 100
    )
      fail(400, "invalid_restore_device");
    if (
      (
        await pool.query(
          "SELECT 1 FROM account_session_devices WHERE session_id=$1",
          [me.sessionId],
        )
      ).rows.length
    )
      fail(409, "restore_requires_fresh_session");
    if (
      (
        await pool.query(
          "SELECT 1 FROM user_devices WHERE user_id=$1 AND device_id=$2",
          [me.id, b.device_id],
        )
      ).rows.length
    )
      fail(409, "restore_requires_new_device");
    await pool.query(
      "INSERT INTO account_backup_restores(session_id,user_id,device_id,public_key_hash)VALUES($1,$2,$3,$4)ON CONFLICT(session_id)DO UPDATE SET device_id=EXCLUDED.device_id,public_key_hash=EXCLUDED.public_key_hash",
      [me.sessionId, me.id, b.device_id, hash(b.identity_key)],
    );
    return { ok: true, restore_id: me.sessionId, device_id: b.device_id };
  });
  app.post("/me/account/backup/abort", async (req) => {
    const me = await deps.requireAuth(req),
      restoreId = req.body?.restore_id;
    if (typeof restoreId !== "string" || !/^[-a-f0-9]{36}$/i.test(restoreId))
      fail(400, "invalid_restore");
    if(!deps.revokeSession)fail(503,'session_revocation_unavailable');
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const row = (
        await client.query(
          "SELECT * FROM account_backup_restores WHERE session_id=$1 AND user_id=$2 FOR UPDATE",
          [restoreId, me.id],
        )
      ).rows[0];
      if (row) {
        const device = (
          await client.query(
            "SELECT public_key FROM user_devices WHERE user_id=$1 AND device_id=$2",
            [me.id, row.device_id],
          )
        ).rows[0];
        if (device && hash(device.public_key) === row.public_key_hash)
          await client.query(
            "DELETE FROM user_devices WHERE user_id=$1 AND device_id=$2",
            [me.id, row.device_id],
          );
        await client.query("DELETE FROM sessions WHERE id=$1 AND user_id=$2", [
          restoreId,
          me.id,
        ]);
      }
      await client.query("COMMIT");
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
    await deps.revokeSession(me.id,restoreId);
    return { ok: true };
  });
  app.post("/me/account/backup/finalize", async (req) => {
    const me = await deps.requireAuth(req);
    await pool.query(
      "DELETE FROM account_backup_restores WHERE session_id=$1 AND user_id=$2",
      [me.sessionId, me.id],
    );
    return { ok: true };
  });
  async function consumeDeviceProof(client, me, deviceId, publicKey, proof) {
    if (
      !proof ||
      typeof proof.id !== "string" ||
      !/^[-a-f0-9]{36}$/i.test(proof.id) ||
      typeof proof.signature !== "string"
    )
      fail(403, "device_proof_required");
    const row = (
      await client.query(
        "SELECT challenge FROM account_device_proofs WHERE id=$1 AND session_id=$2 AND user_id=$3 AND device_id=$4 AND public_key_hash=$5 AND expires_at>now() FOR UPDATE",
        [proof.id, me.sessionId, me.id, deviceId, hash(publicKey)],
      )
    ).rows[0];
    if (
      !row ||
      !verifyDeviceSignature(publicKey, row.challenge, proof.signature)
    )
      fail(403, "invalid_device_proof");
    await client.query("DELETE FROM account_device_proofs WHERE id=$1", [
      proof.id,
    ]);
  }
  async function bindPublishedDevice(
    me,
    deviceId,
    transactionClient = null,
    proof = null,
  ) {
    const client = transactionClient || (await pool.connect());
    try {
      if (!transactionClient) await client.query("BEGIN");
      await client.query("SELECT id FROM sessions WHERE id=$1 FOR UPDATE", [
        me.sessionId,
      ]);
      const session = (
        await client.query(
          "SELECT 1 FROM sessions WHERE id=$1 AND user_id=$2 AND expires_at>now()",
          [me.sessionId, me.id],
        )
      ).rows[0];
      if (!session) fail(401, "unauthorized");
      const device = (
        await client.query(
          "SELECT public_key FROM user_devices WHERE user_id=$1 AND device_id=$2",
          [me.id, deviceId],
        )
      ).rows[0];
      if (!device) fail(403, "device_not_owned");
      const old = (
        await client.query(
          "SELECT device_id,public_key_hash FROM account_session_devices WHERE session_id=$1",
          [me.sessionId],
        )
      ).rows[0];
      if (
        old &&
        (old.device_id !== deviceId ||
          old.public_key_hash !== hash(device.public_key))
      )
        fail(409, "session_device_already_bound");
      await consumeDeviceProof(client, me, deviceId, device.public_key, proof);
      await client.query(
        "INSERT INTO account_session_devices(session_id,user_id,device_id,public_key_hash,proof_verified_at)VALUES($1,$2,$3,$4,now())ON CONFLICT(session_id)DO UPDATE SET proof_verified_at=now()",
        [me.sessionId, me.id, deviceId, hash(device.public_key)],
      );
      if (!transactionClient) await client.query("COMMIT");
    } catch (e) {
      if (!transactionClient) await client.query("ROLLBACK");
      throw e;
    } finally {
      if (!transactionClient) client.release();
    }
  }
  async function filterPayload(viewer, payload) {
    if (!payload || typeof payload !== "object") return payload;
    const ids = new Set([viewer]);
    let needed = false;
    function scan(value) {
      if (
        !value ||
        typeof value !== "object" ||
        value instanceof Date ||
        Buffer.isBuffer(value)
      )
        return;
      if (Array.isArray(value)) {
        for (const v of value) scan(v);
        return;
      }
      if (value.id && ("last_seen" in value || "online" in value)) {
        ids.add(value.id);
        needed = true;
      }
      for (const field of ["read_by", "listened_by"])
        if (Array.isArray(value[field])) {
          for (const id of value[field])
            if (typeof id === "string") ids.add(id);
          needed = true;
        }
      if (
        [
          "messages.read",
          "message.listened",
          "typing",
          "presence",
          "user.presence",
        ].includes(value.type) &&
        value.user_id
      ) {
        ids.add(value.user_id);
        needed = true;
      }
      for (const [key, v] of Object.entries(value))
        if (!["meta", "reply_to"].includes(key)) scan(v);
    }
    scan(payload);
    if (!needed) return payload;
    const validIds = [...ids].filter(
      (x) => typeof x === "string" && /^[a-f0-9-]{36}$/i.test(x),
    );
    const rows = (
      await pool.query(
        `SELECT u.id,coalesce(p.last_seen,'everyone') AS visibility,coalesce(p.read_receipts,true) AS receipts,EXISTS(SELECT 1 FROM account_contacts c WHERE c.user_id=u.id AND c.contact_id=$1)AS contact,EXISTS(SELECT 1 FROM user_blocks b WHERE(b.blocker_id=u.id AND b.blocked_id=$1)OR(b.blocked_id=u.id AND b.blocker_id=$1))AS blocked FROM users u LEFT JOIN account_privacy p ON p.user_id=u.id WHERE u.id=ANY($2::uuid[])`,
        [viewer, validIds],
      )
    ).rows;
    const map = new Map(rows.map((r) => [r.id, r])),
      own = map.get(viewer);
    const receipt = (id) =>
      id === viewer ||
      !!(own?.receipts && map.get(id)?.receipts && !map.get(id)?.blocked);
    const visible = (id) =>
      id === viewer ||
      !!(
        map.get(id) &&
        !map.get(id).blocked &&
        (map.get(id).visibility === "everyone" ||
          (map.get(id).visibility === "contacts" && map.get(id).contact))
      );
    function transform(value) {
      if (
        !value ||
        typeof value !== "object" ||
        value instanceof Date ||
        Buffer.isBuffer(value)
      )
        return value;
      if (Array.isArray(value))
        return value.map(transform).filter((x) => x !== null);
      if (
        ["messages.read", "message.listened"].includes(value.type) &&
        !receipt(value.user_id)
      )
        return null;
      if (
        ["typing", "presence", "user.presence"].includes(value.type) &&
        !visible(value.user_id)
      )
        return null;
      const out = {};
      for (const [k, v] of Object.entries(value)) {
        if ((k === "read_by" || k === "listened_by") && Array.isArray(v))
          out[k] = v.filter(receipt);
        else if (k === "meta" || k === "reply_to") out[k] = v;
        else out[k] = transform(v);
      }
      if (
        value.id &&
        ("last_seen" in value || "online" in value) &&
        !visible(value.id)
      ) {
        if ("last_seen" in value) out.last_seen = null;
        if ("online" in value) out.online = false;
      }
      return out;
    }
    return transform(payload);
  }
  app.addHook("preSerialization", async (req, _reply, payload) =>
    req.accountViewer ? filterPayload(req.accountViewer.id, payload) : payload,
  );
  async function purgeAccountData(client, userId) {
    // Snapshot shared ownership before removing any memberships. Keep rooms alive.
    const rooms = (
      await client.query("SELECT id FROM rooms WHERE created_by=$1", [userId])
    ).rows;
    for (const room of rooms) {
      const successor = (
        await client.query(
          "SELECT rm.user_id FROM room_members rm JOIN users u ON u.id=rm.user_id WHERE rm.room_id=$1 AND rm.user_id<>$2 AND coalesce(u.banned,false)=false AND u.login_disabled_at IS NULL ORDER BY CASE rm.role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 ELSE 2 END,rm.user_id LIMIT 1",
          [room.id, userId],
        )
      ).rows[0];
      if (successor) {
        await client.query("UPDATE rooms SET created_by=$2 WHERE id=$1", [
          room.id,
          successor.user_id,
        ]);
        await client.query(
          "UPDATE room_members SET role='owner' WHERE room_id=$1 AND user_id=$2",
          [room.id, successor.user_id],
        );
      }
    }
    await client.query("UPDATE rooms SET pinned_by=NULL WHERE pinned_by=$1", [
      userId,
    ]);
    const existing = (
      await client.query(
        "SELECT tablename FROM pg_tables WHERE schemaname='public'",
      )
    ).rows.map((r) => r.tablename);
    if (existing.includes("message_edit_history"))
      await client.query(
        "DELETE FROM message_edit_history WHERE message_id IN(SELECT id FROM messages WHERE sender_id=$1)",
        [userId],
      );
    if (existing.includes("room_pins"))
      await client.query(
        "UPDATE room_pins SET pinned_by=NULL WHERE pinned_by=$1",
        [userId],
      );
    const references = (
      await client.query(
        `SELECT DISTINCT cl.relname AS table_name,a.attname AS column_name FROM pg_constraint co JOIN pg_class cl ON cl.oid=co.conrelid JOIN pg_namespace ns ON ns.oid=cl.relnamespace JOIN LATERAL unnest(co.conkey)AS cols(num)ON true JOIN pg_attribute a ON a.attrelid=cl.oid AND a.attnum=cols.num WHERE co.contype='f' AND co.confrelid='users'::regclass AND ns.nspname='public'`,
        [],
      )
    ).rows;
    const skipped = new Set([
      "users",
      "rooms",
      "messages",
      "media",
      "room_members",
      "room_pins",
      // Keep the durable kick record until post-commit SFU removal succeeds.
      "group_call_leases",
    ]);
    if(existing.includes('group_call_leases'))await client.query('UPDATE group_call_leases SET revoking_at=COALESCE(revoking_at,now()) WHERE user_id=$1',[userId]);
    for (const ref of references) {
      if (skipped.has(ref.table_name) || ref.table_name.startsWith("account_"))
        continue;
      await client.query(
        `DELETE FROM ${ident(ref.table_name)} WHERE ${ident(ref.column_name)}=$1`,
        [userId],
      );
    }
    await client.query(
      "UPDATE messages SET read_by=coalesce(read_by,'[]'::jsonb)-$1::text,listened_by=coalesce(listened_by,'[]'::jsonb)-$1::text WHERE coalesce(read_by,'[]'::jsonb)?$1::text OR coalesce(listened_by,'[]'::jsonb)?$1::text",
      [userId],
    );
    const messageColumns = (
      await client.query(
        "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='messages'",
      )
    ).rows.map((r) => r.column_name);
    if (messageColumns.includes("reply_to"))
      await client.query(
        "UPDATE messages SET reply_to=jsonb_build_object('id',reply_to->'id','deleted',true) WHERE reply_to->>'id' IN(SELECT id::text FROM messages WHERE sender_id=$1)",
        [userId],
      );
    // Preserve attachments already shared in somebody else's message, and shared room avatars.
    const media = (
      await client.query(
        `SELECT m.* FROM media m WHERE m.owner_id=$1 AND NOT EXISTS(SELECT 1 FROM messages x WHERE x.media_id=m.id AND x.sender_id<>$1)AND NOT EXISTS(SELECT 1 FROM rooms r WHERE r.avatar_id=m.id)`,
        [userId],
      )
    ).rows;
    for (const item of media) {
      await client.query(
        "INSERT INTO account_media_purge_jobs(media_id,local_path,thumb_path,s3_path)VALUES($1,$2,$3,$4)ON CONFLICT(media_id)DO NOTHING",
        [item.id, item.path, item.thumb_path, item.s3_path],
      );
      await client.query(
        "UPDATE messages SET media_id=NULL WHERE media_id=$1",
        [item.id],
      );
      await client.query("DELETE FROM media WHERE id=$1", [item.id]);
    }
  }
  async function runMediaPurge() {
    if (!deps.deleteMediaFiles) return;
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const jobs = (
        await client.query(
          "SELECT * FROM account_media_purge_jobs WHERE next_attempt_at<=now() ORDER BY created_at LIMIT 20 FOR UPDATE SKIP LOCKED",
        )
      ).rows;
      for (const job of jobs) {
        try {
          await deps.deleteMediaFiles(job);
          await client.query(
            "DELETE FROM account_media_purge_jobs WHERE id=$1",
            [job.id],
          );
        } catch (e) {
          await client.query(
            "UPDATE account_media_purge_jobs SET attempts=attempts+1,next_attempt_at=now()+interval '5 minutes',last_error=$2 WHERE id=$1",
            [job.id, String(e.code || "delete_failed").slice(0, 80)],
          );
        }
      }
      await client.query("COMMIT");
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  }
  async function afterAccountTombstone(client, userId) {
    if (
      (
        await client.query(
          "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='message_edit_history'",
        )
      ).rows.length
    )
      await client.query(
        "DELETE FROM message_edit_history WHERE message_id IN(SELECT id FROM messages WHERE sender_id=$1)",
        [userId],
      );
  }
  const services = await installAccountFeatures(app, {
    ...deps,
    getSessionDevice,
    purgeAccountData,
    afterAccountTombstone,
    revokeUserSessions: deps.revokeUserSessions,
  });
  const timer = setInterval(
    () =>
      runMediaPurge().catch((e) =>
        app.log.error({ code: e.code }, "Media purge retry failed"),
      ),
    30000,
  );
  timer.unref();
  app.addHook("onClose", async () => clearInterval(timer));
  return {
    ...services,
    getSessionDevice,
    validatePublishedDevice,
    bindPublishedDevice,
    filterPayload,
    requireSession,
    purgeAccountData,
    runMediaPurge,
  };
}
