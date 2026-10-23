import {
  createHash,
  createHmac,
  randomBytes,
  timingSafeEqual,
  createCipheriv,
  createDecipheriv,
} from "node:crypto";
const fail = (statusCode, message) => {
  throw Object.assign(new Error(message), { statusCode });
};
const hash = (x) => createHash("sha256").update(String(x)).digest("hex");
const nonce = () => randomBytes(32).toString("base64url");
const uuid = (x) => {
  if (
    typeof x !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(x)
  )
    fail(400, "invalid_id");
  return x;
};
const text = (x, max = 200) => {
  if (typeof x !== "string" || !x.trim() || x.length > max)
    fail(400, "invalid_text");
  return x.trim();
};
const equal = (a, b) => {
  const x = Buffer.from(String(a)),
    y = Buffer.from(String(b));
  return x.length === y.length && timingSafeEqual(x, y);
};
async function transaction(pool, fn) {
  const c = await pool.connect();
  try {
    await c.query("BEGIN");
    const v = await fn(c);
    await c.query("COMMIT");
    return v;
  } catch (e) {
    await c.query("ROLLBACK");
    throw e;
  } finally {
    c.release();
  }
}
const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
function base32(bytes) {
  let bits = 0,
    value = 0,
    result = "";
  for (const byte of bytes) {
    value = (value << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      result += alphabet[(value >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }
  if (bits) result += alphabet[(value << (5 - bits)) & 31];
  return result;
}
function unbase32(s) {
  let bits = 0,
    value = 0,
    output = [];
  for (const c of s.replace(/=/g, "").toUpperCase()) {
    const n = alphabet.indexOf(c);
    if (n < 0) fail(400, "invalid_secret");
    value = (value << 5) | n;
    bits += 5;
    if (bits >= 8) {
      output.push((value >>> (bits - 8)) & 255);
      bits -= 8;
    }
  }
  return Buffer.from(output);
}
export function totpCode(secret, now = Date.now(), digits = 6) {
  const step = Buffer.alloc(8);
  step.writeBigUInt64BE(BigInt(Math.floor(now / 30000)));
  const h = createHmac("sha1", unbase32(secret)).update(step).digest(),
    o = h[19] & 15;
  return String((h.readUInt32BE(o) & 0x7fffffff) % 10 ** digits).padStart(
    digits,
    "0",
  );
}
function key(k) {
  if (!Buffer.isBuffer(k) || k.length !== 32)
    fail(503, "security_key_unavailable");
  return k;
}
function seal(value, k, user) {
  const iv = randomBytes(12),
    c = createCipheriv("aes-256-gcm", key(k), iv);
  c.setAAD(Buffer.from(user));
  return Buffer.concat([
    iv,
    c.update(value),
    c.final(),
    c.getAuthTag(),
  ]).toString("base64");
}
function unseal(value, k, user) {
  const b = Buffer.from(value, "base64"),
    c = createDecipheriv("aes-256-gcm", key(k), b.subarray(0, 12));
  c.setAAD(Buffer.from(user));
  c.setAuthTag(b.subarray(-16));
  return Buffer.concat([c.update(b.subarray(12, -16)), c.final()]).toString();
}
async function settings(pool, id) {
  return (
    (await pool.query("SELECT * FROM account_privacy WHERE user_id=$1", [id]))
      .rows[0] || {
      last_seen: "everyone",
      write: "everyone",
      read_receipts: true,
    }
  );
}
async function blocked(pool, a, b) {
  return (
    (
      await pool.query(
        "SELECT 1 FROM user_blocks WHERE (blocker_id=$1 AND blocked_id=$2) OR (blocker_id=$2 AND blocked_id=$1)",
        [a, b],
      )
    ).rows.length > 0
  );
}
async function visibility(pool, owner, viewer, value) {
  if (owner === viewer) return true;
  if (await blocked(pool, owner, viewer)) return false;
  if (value === "everyone") return true;
  if (value !== "contacts") return false;
  return (
    (
      await pool.query(
        "SELECT 1 FROM account_contacts WHERE user_id=$1 AND contact_id=$2",
        [owner, viewer],
      )
    ).rows.length > 0
  );
}
export async function canWrite(pool, sender, recipient) {
  return visibility(
    pool,
    recipient,
    sender,
    (await settings(pool, recipient)).write,
  );
}
export async function canReadReceipts(pool, a, b) {
  return (
    !(await blocked(pool, a, b)) &&
    (await settings(pool, a)).read_receipts &&
    (await settings(pool, b)).read_receipts
  );
}
export async function filterProfile(pool, profile, viewer) {
  return {
    ...profile,
    last_seen: (await visibility(
      pool,
      profile.id,
      viewer,
      (await settings(pool, profile.id)).last_seen,
    ))
      ? profile.last_seen
      : null,
  };
}
async function factor(pool, user, body, securityKey) {
  return transaction(pool, async (c) => {
    const s = (
      await c.query(
        "SELECT * FROM account_security WHERE user_id=$1 FOR UPDATE",
        [user],
      )
    ).rows[0];
    if (!s?.enabled) return true;
    if (s.locked_until && new Date(s.locked_until) > new Date()) return false;
    let valid = false,
      step = Number(s.last_step);
    if (body.backup_code) {
      const codes = s.backup_hashes,
        found = codes.findIndex((h) => equal(h, hash(body.backup_code)));
      if (found >= 0) {
        codes.splice(found, 1);
        await c.query(
          "UPDATE account_security SET backup_hashes=$2 WHERE user_id=$1",
          [user, JSON.stringify(codes)],
        );
        valid = true;
      }
    } else if (/^\d{6}$/.test(String(body.code || body.totp || ""))) {
      const secret = unseal(s.secret_sealed, securityKey, user),
        now = Date.now();
      for (const delta of [-1, 0, 1]) {
        const candidate = Math.floor(now / 30000) + delta;
        if (
          candidate > step &&
          equal(totpCode(secret, candidate * 30000), body.code || body.totp)
        ) {
          valid = true;
          step = candidate;
          break;
        }
      }
    }
    if (valid) {
      await c.query(
        "UPDATE account_security SET last_step=$2,failures=0,locked_until=NULL WHERE user_id=$1",
        [user, step],
      );
      return true;
    }
    await c.query(
      `UPDATE account_security SET failures=failures+1,locked_until=CASE WHEN failures>=4 THEN now()+interval '5 minutes' ELSE locked_until END WHERE user_id=$1`,
      [user],
    );
    return false;
  });
}
async function audit(pool, user, ctx, outcome) {
  await pool.query(
    "INSERT INTO account_login_audit(user_id,device_key,device_name,ip,outcome) VALUES($1,$2,$3,$4,$5)",
    [
      user,
      ctx.device_key ? hash(ctx.device_key) : null,
      String(ctx.device_name || "").slice(0, 160),
      String(ctx.ip || "").slice(0, 64),
      outcome,
    ],
  );
  await pool.query(
    `DELETE FROM account_login_audit WHERE user_id=$1 AND id NOT IN (SELECT id FROM account_login_audit WHERE user_id=$1 ORDER BY created_at DESC LIMIT 100)`,
    [user],
  );
}
/** Must run AFTER password verification and BEFORE minting any session. */
export async function authGate(user, ctx) {
  const { pool, securityKey } = ctx;
  const security = (
    await pool.query("SELECT * FROM account_security WHERE user_id=$1", [
      user.id,
    ])
  ).rows[0];
  const factorVerified =
    !security?.enabled ||
    (ctx.code || ctx.totp || ctx.backup_code
      ? await factor(pool, user.id, ctx, securityKey)
      : false);
  const deviceHash = ctx.device_key ? hash(text(ctx.device_key, 512)) : null;
  const trusted =
    deviceHash &&
    (
      await pool.query(
        "SELECT 1 FROM account_trusted_devices WHERE user_id=$1 AND device_hash=$2",
        [user.id, deviceHash],
      )
    ).rows.length > 0;
  if (factorVerified && (!security?.approval_required || trusted)) {
    await audit(pool, user.id, ctx, "login");
    if (!trusted)
      await ctx.sendToUser?.(user.id, {
        type: "account.new_login",
        device_name: String(ctx.device_name || "Неизвестное устройство").slice(
          0,
          160,
        ),
      });
    return { allowed: true };
  }
  const secret = nonce(),
    kind = factorVerified ? "device" : "factor";
  const row = (
    await pool.query(
      `INSERT INTO account_auth_challenges(user_id,secret_hash,kind,device_hash,device_name,factor_verified)VALUES($1,$2,$3,$4,$5,$6) RETURNING id,expires_at`,
      [
        user.id,
        hash(secret),
        kind,
        deviceHash,
        String(ctx.device_name || "").slice(0, 160),
        factorVerified,
      ],
    )
  ).rows[0];
  await audit(pool, user.id, ctx, "challenge");
  await ctx.sendToUser?.(user.id, {
    type: "account.login_approval",
    id: row.id,
    device_name: ctx.device_name,
    expires_at: row.expires_at,
  });
  return { allowed: false, challenge: { ...row, secret, kind } };
}
export async function installAccountFeatures(app, deps) {
  const { pool, requireAuth, securityKey } = deps;
  const ensure = async (id) =>
    pool.query(
      "INSERT INTO account_security(user_id)VALUES($1) ON CONFLICT DO NOTHING",
      [id],
    );
  const password = async (me, body) => {
    const u = (await pool.query("SELECT * FROM users WHERE id=$1", [me.id]))
      .rows[0];
    if (
      !u ||
      !(await deps.verifyPassword(u.password_hash, String(body.password || "")))
    )
      fail(403, "bad_password");
    return u;
  };
  const sensitive = async (me, body) => {
    const u = await password(me, body);
    if (!(await factor(pool, me.id, body, securityKey)))
      fail(403, "factor_required");
    return u;
  };
  const clean = async () => {
    await pool.query(
      "DELETE FROM account_secret_mailbox WHERE expires_at<=now()",
    );
    await pool.query(
      "DELETE FROM account_auth_challenges WHERE expires_at<=now()",
    );
  };
  app.get("/me/account/privacy", async (req) =>
    settings(pool, (await requireAuth(req)).id),
  );
  app.put("/me/account/privacy", async (req) => {
    const me = await requireAuth(req),
      b = req.body || {},
      old = await settings(pool, me.id);
    for (const field of ["write", "last_seen"])
      if (
        b[field] !== undefined &&
        !["everyone", "contacts", "nobody"].includes(b[field])
      )
        fail(400, "invalid_privacy");
    if (b.read_receipts !== undefined && typeof b.read_receipts !== "boolean")
      fail(400, "invalid_privacy");
    await pool.query(
      `INSERT INTO account_privacy(user_id,last_seen,write,read_receipts)VALUES($1,$2,$3,$4) ON CONFLICT(user_id)DO UPDATE SET last_seen=EXCLUDED.last_seen,write=EXCLUDED.write,read_receipts=EXCLUDED.read_receipts`,
      [
        me.id,
        b.last_seen ?? old.last_seen,
        b.write ?? old.write,
        b.read_receipts ?? old.read_receipts,
      ],
    );
    return settings(pool, me.id);
  });
  app.get(
    "/me/account/contacts",
    async (req) =>
      (
        await pool.query(
          "SELECT u.id,u.username,u.display_name,c.name FROM account_contacts c JOIN users u ON u.id=c.contact_id WHERE c.user_id=$1 ORDER BY c.name,u.username",
          [(await requireAuth(req)).id],
        )
      ).rows,
  );
  app.put("/me/account/contacts/:id", async (req) => {
    const me = await requireAuth(req),
      id = uuid(req.params.id);
    if (id === me.id) fail(400, "self_contact");
    if (
      !(
        await pool.query(
          "SELECT 1 FROM users WHERE id=$1 AND login_disabled_at IS NULL",
          [id],
        )
      ).rows.length
    )
      fail(404, "user_not_found");
    const name = String(req.body?.name || "").trim();
    if (name.length > 100) fail(400, "name_too_long");
    await pool.query(
      "INSERT INTO account_contacts(user_id,contact_id,name) VALUES($1,$2,$3) ON CONFLICT(user_id,contact_id)DO UPDATE SET name=EXCLUDED.name",
      [me.id, id, name],
    );
    return { ok: true };
  });
  app.delete("/me/account/contacts/:id", async (req) => {
    await pool.query(
      "DELETE FROM account_contacts WHERE user_id=$1 AND contact_id=$2",
      [(await requireAuth(req)).id, uuid(req.params.id)],
    );
    return { ok: true };
  });
  app.get(
    "/me/account/blocks",
    async (req) =>
      (
        await pool.query(
          "SELECT u.id,u.username,u.display_name FROM user_blocks b JOIN users u ON u.id=b.blocked_id WHERE b.blocker_id=$1",
          [(await requireAuth(req)).id],
        )
      ).rows,
  );
  app.put("/me/account/blocks/:id", async (req) => {
    const me = await requireAuth(req),
      id = uuid(req.params.id);
    if (id === me.id) fail(400, "self_block");
    if (
      !(await pool.query("SELECT 1 FROM users WHERE id=$1", [id])).rows.length
    )
      fail(404, "user_not_found");
    await pool.query(
      "INSERT INTO user_blocks(blocker_id,blocked_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
      [me.id, id],
    );
    return { ok: true };
  });
  app.delete("/me/account/blocks/:id", async (req) => {
    await pool.query(
      "DELETE FROM user_blocks WHERE blocker_id=$1 AND blocked_id=$2",
      [(await requireAuth(req)).id, uuid(req.params.id)],
    );
    return { ok: true };
  });
  app.get("/me/account/security", async (req) => {
    const me = await requireAuth(req);
    await ensure(me.id);
    const s = (
      await pool.query(
        "SELECT enabled,approval_required FROM account_security WHERE user_id=$1",
        [me.id],
      )
    ).rows[0];
    return {
      ...s,
      devices: (
        await pool.query(
          "SELECT device_hash,name,created_at FROM account_trusted_devices WHERE user_id=$1",
          [me.id],
        )
      ).rows,
      audit: (
        await pool.query(
          "SELECT id,device_name,ip,outcome,created_at FROM account_login_audit WHERE user_id=$1 ORDER BY created_at DESC LIMIT 100",
          [me.id],
        )
      ).rows,
      challenges: (
        await pool.query(
          `SELECT id,device_name,expires_at,kind FROM account_auth_challenges WHERE user_id=$1 AND expires_at>now() AND kind<>'handoff'`,
          [me.id],
        )
      ).rows,
    };
  });
  app.post(
    "/me/account/totp/setup",
    { config: { rateLimit: { max: 5, timeWindow: "1 minute" } } },
    async (req) => {
      const me = await requireAuth(req);
      await sensitive(me, req.body || {});
      key(securityKey);
      const secret = base32(randomBytes(20));
      await ensure(me.id);
      await pool.query(
        `UPDATE account_security SET pending_sealed=$2,pending_until=now()+interval '10 minutes' WHERE user_id=$1`,
        [me.id, seal(secret, securityKey, me.id)],
      );
      return {
        secret,
        uri: `otpauth://totp/67chat:${encodeURIComponent(me.username || me.id)}?secret=${secret}&issuer=67chat&digits=6&period=30`,
      };
    },
  );
  app.post(
    "/me/account/totp/enable",
    { config: { rateLimit: { max: 5, timeWindow: "1 minute" } } },
    async (req) => {
      const me = await requireAuth(req);
      return transaction(pool, async (c) => {
        const s = (
          await c.query(
            "SELECT * FROM account_security WHERE user_id=$1 FOR UPDATE",
            [me.id],
          )
        ).rows[0];
        if (!s?.pending_sealed || new Date(s.pending_until) <= new Date())
          fail(400, "setup_expired");
        const secret = unseal(s.pending_sealed, securityKey, me.id);
        if (!equal(totpCode(secret), String(req.body?.code || "")))
          fail(403, "bad_factor");
        const codes = Array.from({ length: 10 }, () =>
          randomBytes(10).toString("hex"),
        );
        await c.query(
          `UPDATE account_security SET secret_sealed=pending_sealed,pending_sealed=NULL,pending_until=NULL,enabled=true,last_step=$2,backup_hashes=$3,failures=0,locked_until=NULL WHERE user_id=$1`,
          [
            me.id,
            Math.floor(Date.now() / 30000),
            JSON.stringify(codes.map(hash)),
          ],
        );
        return { ok: true, backup_codes: codes };
      });
    },
  );
  app.post(
    "/me/account/totp/disable",
    { config: { rateLimit: { max: 5, timeWindow: "1 minute" } } },
    async (req) => {
      const me = await requireAuth(req);
      await sensitive(me, req.body || {});
      await pool.query(
        `UPDATE account_security SET enabled=false,approval_required=false,secret_sealed=NULL,pending_sealed=NULL,backup_hashes='[]' WHERE user_id=$1`,
        [me.id],
      );
      return { ok: true };
    },
  );
  app.put(
    "/me/account/device-approval",
    { config: { rateLimit: { max: 5, timeWindow: "1 minute" } } },
    async (req) => {
      const me = await requireAuth(req),
        b = req.body || {};
      await sensitive(me, b);
      if (typeof b.enabled !== "boolean") fail(400, "invalid_enabled");
      await ensure(me.id);
      if (b.enabled) {
        const s = (
          await pool.query(
            "SELECT enabled FROM account_security WHERE user_id=$1",
            [me.id],
          )
        ).rows[0];
        if (!s.enabled) fail(409, "enroll_totp_first");
        await pool.query(
          "INSERT INTO account_trusted_devices(user_id,device_hash,name)VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
          [
            me.id,
            hash(text(b.device_key, 512)),
            String(b.device_name || "Текущее устройство").slice(0, 160),
          ],
        );
      }
      await pool.query(
        "UPDATE account_security SET approval_required=$2 WHERE user_id=$1",
        [me.id, b.enabled],
      );
      return { ok: true };
    },
  );
  app.post("/me/account/challenges/:id/approve", async (req) => {
    const me = await requireAuth(req);
    const row = (
      await pool.query(
        `UPDATE account_auth_challenges SET approved=true WHERE id=$1 AND user_id=$2 AND kind<>'handoff' AND expires_at>now() RETURNING id`,
        [uuid(req.params.id), me.id],
      )
    ).rows[0];
    if (!row) fail(404, "challenge_not_found");
    return { ok: true };
  });
  app.post(
    "/auth/account-challenge/complete",
    { config: { rateLimit: { max: 10, timeWindow: "1 minute" } } },
    async (req, reply) => {
      const b = req.body || {},
        id = uuid(b.id),
        s = (
          await pool.query(
            "SELECT * FROM account_auth_challenges WHERE id=$1 AND expires_at>now()",
            [id],
          )
        ).rows[0];
      if (
        !s ||
        s.kind === "handoff" ||
        s.failures >= 5 ||
        !equal(s.secret_hash, hash(b.secret))
      )
        fail(403, "invalid_challenge");
      if (
        !s.factor_verified &&
        !(await factor(pool, s.user_id, b, securityKey))
      ) {
        await pool.query(
          "UPDATE account_auth_challenges SET failures=failures+1 WHERE id=$1",
          [id],
        );
        fail(403, "bad_factor");
      }
      await pool.query(
        "UPDATE account_auth_challenges SET factor_verified=true WHERE id=$1",
        [id],
      );
      const sec = (
        await pool.query(
          "SELECT approval_required FROM account_security WHERE user_id=$1",
          [s.user_id],
        )
      ).rows[0];
      const trusted =
        s.device_hash &&
        (
          await pool.query(
            "SELECT 1 FROM account_trusted_devices WHERE user_id=$1 AND device_hash=$2",
            [s.user_id, s.device_hash],
          )
        ).rows.length;
      if (sec?.approval_required && !trusted && !s.approved)
        return reply.code(409).send({ error: "device_approval_required", id });
      if (!deps.issueSession) fail(503, "session_issuer_unavailable");
      const consumed = (
        await pool.query(
          "DELETE FROM account_auth_challenges WHERE id=$1 RETURNING user_id",
          [id],
        )
      ).rows[0];
      if (!consumed) fail(403, "invalid_challenge");
      const user = (
        await pool.query(
          "SELECT * FROM users WHERE id=$1 AND banned=false AND login_disabled_at IS NULL",
          [s.user_id],
        )
      ).rows[0];
      if (!user) fail(403, "account_disabled");
      if (s.device_hash && s.approved)
        await pool.query(
          "INSERT INTO account_trusted_devices(user_id,device_hash,name)VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
          [s.user_id, s.device_hash, s.device_name || "Устройство"],
        );
      await audit(
        pool,
        user.id,
        { device_name: s.device_name, ip: req.ip },
        "login",
      );
      await deps.sendToUser?.(user.id, {
        type: "account.new_login",
        device_name: s.device_name,
      });
      return deps.issueSession(user, req, reply);
    },
  );
  app.post(
    "/auth/account-handoff",
    { config: { rateLimit: { max: 10, timeWindow: "1 minute" } } },
    async () => {
      const secret = nonce(),
        approval = nonce(),
        r = (
          await pool.query(
            `INSERT INTO account_auth_challenges(secret_hash,approval_hash,kind)VALUES($1,$2,'handoff')RETURNING id,expires_at`,
            [hash(secret), hash(approval)],
          )
        ).rows[0];
      return {
        ...r,
        secret,
        approval,
        qr: JSON.stringify({
          v: 1,
          type: "67chat-handoff",
          id: r.id,
          approval,
        }),
      };
    },
  );
  app.post("/me/account/handoff/:id/approve", async (req) => {
    const me = await requireAuth(req),
      b = req.body || {},
      r = (
        await pool.query(
          `SELECT * FROM account_auth_challenges WHERE id=$1 AND kind='handoff' AND user_id IS NULL AND expires_at>now()`,
          [uuid(req.params.id)],
        )
      ).rows[0];
    if (!r || !equal(r.approval_hash, hash(b.approval)))
      fail(403, "invalid_handoff");
    await pool.query(
      "UPDATE account_auth_challenges SET user_id=$2,approved=true WHERE id=$1 AND user_id IS NULL",
      [r.id, me.id],
    );
    return { ok: true };
  });
  app.post(
    "/auth/account-handoff/claim",
    { config: { rateLimit: { max: 20, timeWindow: "1 minute" } } },
    async (req, reply) => {
      const b = req.body || {},
        r = (
          await pool.query(
            `SELECT * FROM account_auth_challenges WHERE id=$1 AND kind='handoff' AND expires_at>now()`,
            [uuid(b.id)],
          )
        ).rows[0];
      if (!r || !equal(r.secret_hash, hash(b.secret)))
        fail(403, "invalid_handoff");
      if (!r.approved) fail(409, "approval_pending");
      if (!deps.issueSession) fail(503, "session_issuer_unavailable");
      if (
        !(
          await pool.query(
            "DELETE FROM account_auth_challenges WHERE id=$1 RETURNING id",
            [r.id],
          )
        ).rows.length
      )
        fail(403, "invalid_handoff");
      const u = (
        await pool.query(
          "SELECT * FROM users WHERE id=$1 AND banned=false AND login_disabled_at IS NULL",
          [r.user_id],
        )
      ).rows[0];
      if (!u) fail(403, "account_disabled");
      await audit(
        pool,
        u.id,
        { ip: req.ip, device_name: "QR handoff" },
        "handoff",
      );
      await deps.sendToUser?.(u.id, {
        type: "account.new_login",
        device_name: "QR handoff",
      });
      return deps.issueSession(u, req, reply);
    },
  );
  const boundDevice = async (me, req, device) => {
    if (!deps.getSessionDevice) fail(503, "device_binding_unavailable");
    if ((await deps.getSessionDevice(me, req)) !== device)
      fail(403, "secret_device_mismatch");
  };
  const chat = async (me, id, device, req) => {
    text(device, 100);
    await boundDevice(me, req, device);
    const c = (
      await pool.query(
        "SELECT * FROM account_secret_chats WHERE id=$1 AND closed_at IS NULL",
        [uuid(id)],
      )
    ).rows[0];
    if (
      !c ||
      !(
        (c.initiator === me.id && c.initiator_device === device) ||
        (c.recipient === me.id && c.recipient_device === device)
      )
    )
      fail(403, "secret_device_mismatch");
    return c;
  };
  app.get(
    "/me/account/secret-chats",
    async (req) =>
      (
        await pool.query(
          "SELECT * FROM account_secret_chats WHERE (initiator=$1 OR recipient=$1) AND closed_at IS NULL",
          [(await requireAuth(req)).id],
        )
      ).rows,
  );
  app.post("/me/account/secret-chats", async (req) => {
    const me = await requireAuth(req),
      b = req.body || {},
      peer = uuid(b.peer_id);
    if (peer === me.id) fail(400, "self_chat");
    if (!(await canWrite(pool, me.id, peer))) fail(403, "privacy_write_denied");
    const a = text(b.device_id, 100),
      d = text(b.peer_device_id, 100),
      ttl = Number(b.ttl ?? 3600);
    await boundDevice(me, req, a);
    if (!Number.isInteger(ttl) || ttl < 30 || ttl > 86400)
      fail(400, "invalid_ttl");
    for (const [u, device] of [
      [me.id, a],
      [peer, d],
    ])
      if (
        !(
          await pool.query(
            "SELECT 1 FROM user_devices WHERE user_id=$1 AND device_id=$2",
            [u, device],
          )
        ).rows.length
      )
        fail(400, "unknown_device");
    const r = (
      await pool.query(
        "INSERT INTO account_secret_chats(initiator,recipient,initiator_device,recipient_device,ttl)VALUES($1,$2,$3,$4,$5)RETURNING *",
        [me.id, peer, a, d, ttl],
      )
    ).rows[0];
    await deps.sendToUser?.(peer, { type: "account.secret_chat", chat: r });
    return r;
  });
  app.post("/me/account/secret-chats/:id/messages", async (req) => {
    const me = await requireAuth(req),
      b = req.body || {};
    if (
      b.body !== undefined ||
      b.text !== undefined ||
      typeof b.ciphertext !== "string" ||
      b.ciphertext.length < 24 ||
      b.ciphertext.length > 65536 ||
      !/^[A-Za-z0-9+/=_-]+$/.test(b.ciphertext)
    )
      fail(400, "ciphertext_required");
    const c = await chat(me, req.params.id, b.device_id, req),
      peer = c.initiator === me.id ? c.recipient : c.initiator;
    if (!(await canWrite(pool, me.id, peer))) fail(403, "privacy_write_denied");
    const n = text(b.nonce, 128);
    return transaction(pool, async (client) => {
      await client.query(
        "SELECT id FROM account_secret_chats WHERE id=$1 FOR UPDATE",
        [c.id],
      );
      await client.query(
        "DELETE FROM account_secret_mailbox WHERE expires_at<=now()",
      );
      if (
        Number(
          (
            await client.query(
              "SELECT count(*) AS n FROM account_secret_mailbox WHERE chat_id=$1",
              [c.id],
            )
          ).rows[0].n,
        ) >= 100
      )
        fail(429, "mailbox_full");
      const old = (
        await client.query(
          "SELECT id FROM account_secret_mailbox WHERE chat_id=$1 AND sender_id=$2 AND nonce=$3",
          [c.id, me.id, n],
        )
      ).rows[0];
      if (old) return old;
      const r = (
        await client.query(
          `INSERT INTO account_secret_mailbox(chat_id,sender_id,nonce,ciphertext,expires_at)VALUES($1,$2,$3,$4,now()+($5::int*interval '1 second'))RETURNING id,expires_at`,
          [c.id, me.id, n, b.ciphertext, c.ttl],
        )
      ).rows[0];
      return r;
    });
  });
  app.get("/me/account/secret-chats/:id/messages", async (req) => {
    const me = await requireAuth(req);
    const c = await chat(me, req.params.id, req.query.device_id, req);
    await clean();
    return (
      await pool.query(
        "SELECT id,sender_id,ciphertext,created_at,expires_at FROM account_secret_mailbox WHERE chat_id=$1 AND sender_id<>$2 AND expires_at>now() ORDER BY created_at LIMIT 100",
        [c.id, me.id],
      )
    ).rows;
  });
  app.post("/me/account/secret-chats/:id/ack", async (req) => {
    const me = await requireAuth(req),
      b = req.body || {},
      c = await chat(me, req.params.id, b.device_id, req);
    if (!Array.isArray(b.ids) || b.ids.length > 100) fail(400, "invalid_ids");
    await pool.query(
      "DELETE FROM account_secret_mailbox WHERE chat_id=$1 AND sender_id<>$2 AND id=ANY($3::uuid[])",
      [c.id, me.id, b.ids.map(uuid)],
    );
    return { ok: true };
  });
  app.delete("/me/account/secret-chats/:id", async (req) => {
    const me = await requireAuth(req),
      c = await chat(me, req.params.id, req.body?.device_id, req);
    await pool.query("DELETE FROM account_secret_chats WHERE id=$1", [c.id]);
    return { ok: true };
  });
  app.post(
    "/me/account/delete",
    { config: { rateLimit: { max: 5, timeWindow: "1 minute" } } },
    async (req) => {
      const me = await requireAuth(req);
      await sensitive(me, req.body || {});
      if (!deps.purgeAccountData || !deps.revokeUserSessions)
        fail(503, "account_cleanup_unavailable");
      await transaction(pool, async (c) => {
        await c.query("SELECT id FROM users WHERE id=$1 FOR UPDATE", [me.id]);
        await deps.purgeAccountData(c, me.id);
        await c.query("DELETE FROM sessions WHERE user_id=$1", [me.id]);
        await c.query(
          "UPDATE messages SET text=NULL,encrypted_content=NULL,meta=$2,media_id=NULL WHERE sender_id=$1",
          [me.id, {}],
        );
        for (const table of [
          "account_security",
          "account_privacy",
          "account_login_audit",
          "account_trusted_devices",
          "account_auth_challenges",
        ])
          await c.query(`DELETE FROM ${table} WHERE user_id=$1`, [me.id]);
        await c.query(
          "DELETE FROM account_contacts WHERE user_id=$1 OR contact_id=$1",
          [me.id],
        );
        await c.query(
          "DELETE FROM user_blocks WHERE blocker_id=$1 OR blocked_id=$1",
          [me.id],
        );
        await c.query(
          "DELETE FROM account_secret_chats WHERE initiator=$1 OR recipient=$1",
          [me.id],
        );
        await c.query("DELETE FROM room_members WHERE user_id=$1", [me.id]);
        // Capture-edit triggers ran on the message tombstone; erase their output too.
        await deps.afterAccountTombstone?.(c, me.id);
        await c.query(
          `UPDATE users SET username=$2,password_hash=$3,display_name=NULL,avatar_id=NULL,profile_tagline=NULL,fcm_token=NULL,last_seen=NULL,banned=true,login_disabled_at=now() WHERE id=$1`,
          [me.id, "deleted_" + me.id.replaceAll("-", "").slice(0, 20), nonce()],
        );
      });
      await deps.revokeUserSessions(me.id);
      return { ok: true };
    },
  );
  const timer = setInterval(
    () => clean().catch((e) => app.log?.error(e)),
    30000,
  );
  timer.unref();
  app.addHook("onClose", async () => clearInterval(timer));
  return {
    authGate: (user, ctx = {}) => authGate(user, { ...deps, ...ctx }),
    verifyFactor: (userId, body = {}) =>
      factor(pool, userId, body, securityKey),
    canWrite: (a, b) => canWrite(pool, a, b),
    canReadReceipts: (a, b) => canReadReceipts(pool, a, b),
    filterProfile: (p, v) => filterProfile(pool, p, v),
    cleanup: clean,
  };
}
