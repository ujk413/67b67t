import {listConversationFolders,reorderConversationFolders} from './conversation-features.js';
export async function installOrganization(app, { pool, requireAuth, notifyRoomListUpdate }) {
  await pool.query(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS is_saved boolean NOT NULL DEFAULT false;
    CREATE UNIQUE INDEX IF NOT EXISTS rooms_saved_owner ON rooms(created_by) WHERE is_saved;
    CREATE TABLE IF NOT EXISTS chat_folders(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,title text NOT NULL,room_ids jsonb NOT NULL DEFAULT '[]',created_at timestamptz NOT NULL DEFAULT now());
    ALTER TABLE chat_folders ADD COLUMN IF NOT EXISTS position integer NOT NULL DEFAULT 0;
    ALTER TABLE chat_folders ADD COLUMN IF NOT EXISTS revision integer NOT NULL DEFAULT 0;
    ALTER TABLE chat_folders ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();`);
  app.get('/me/folders', async req => { const me = await requireAuth(req); return listConversationFolders(pool,me.id); });
  app.put('/me/folders/order',async req=>{const me=await requireAuth(req);const items=await reorderConversationFolders(pool,me.id,req.body?.items);await notifyRoomListUpdate(me.id);return items;});
  async function save(req, reply) {
    const me = await requireAuth(req), title = String(req.body?.title || '').trim();
    if (!Array.isArray(req.body?.room_ids)) return reply.code(400).send({error:'bad_folder'});
    const ids = [...new Set(req.body.room_ids)];
    if (!title || title.length > 40 || ids.length > 500 || ids.some(id => typeof id !== 'string' || !/^[a-f0-9-]{36}$/i.test(id))) return reply.code(400).send({ error: 'bad_folder' });
    const members = (await pool.query('SELECT room_id FROM room_members WHERE user_id=$1 AND room_id=ANY($2::uuid[])', [me.id, ids])).rows;
    if (members.length !== ids.length) return reply.code(403).send({ error: 'forbidden' });
    const rows = req.params.id
      ? (await pool.query('UPDATE chat_folders SET title=$3,room_ids=$4::jsonb,revision=revision+1,updated_at=now() WHERE id=$1 AND user_id=$2 RETURNING id,title,room_ids', [req.params.id, me.id, title, JSON.stringify(ids)])).rows
      : (await pool.query('INSERT INTO chat_folders(user_id,title,room_ids) VALUES($1,$2,$3::jsonb) RETURNING id,title,room_ids', [me.id, title, JSON.stringify(ids)])).rows;
    if (!rows.length) return reply.code(404).send({ error: 'not_found' });
    await notifyRoomListUpdate(me.id);
    return rows[0];
  }
  app.post('/me/folders', save); app.put('/me/folders/:id', save);
  app.delete('/me/folders/:id', async req => { const me = await requireAuth(req); await pool.query('DELETE FROM chat_folders WHERE id=$1 AND user_id=$2', [req.params.id, me.id]);await notifyRoomListUpdate(me.id); return { ok: true }; });
  app.post('/me/saved', async req => {
    const me = await requireAuth(req), client = await pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query("INSERT INTO rooms(kind,title,created_by,is_saved) VALUES('dm','Избранное',$1,true) ON CONFLICT(created_by) WHERE is_saved DO UPDATE SET title=EXCLUDED.title RETURNING id", [me.id]);
      await client.query("INSERT INTO room_members(room_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING", [rows[0].id, me.id]);
      await client.query('COMMIT'); await notifyRoomListUpdate(me.id).catch(()=>{}); return rows[0];
    } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
  });
}
