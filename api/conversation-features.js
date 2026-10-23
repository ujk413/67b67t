import {readFile} from 'node:fs/promises';
const uuid=/^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i;
const fail=(code,error,extra={})=>Object.assign(new Error(error),{statusCode:code,...extra});
const integer=(value,min,max)=>Number.isSafeInteger(value)&&value>=min&&value<=max;
const encrypted=value=>typeof value==='string'&&value.length>0&&Buffer.byteLength(value)<=262144;
const settingsColumns='id,kind,conversation_epoch,slow_mode_seconds,auto_delete_seconds,public_slug,public_title,public_description,e2e_enabled';
export function unpackConversationBody(payload){
 let parsed;try{parsed=JSON.parse(payload);}catch{return {encrypted_content:payload,meta:{}};}
 if(parsed?.v!==1||!parsed.body)return {encrypted_content:payload,meta:{}};
 const b=parsed.body;
 if(b.encrypted_content&&encrypted(b.encrypted_content))return {encrypted_content:b.encrypted_content,meta:b.meta?.e2e_group===true?{e2e_group:true,v:1}:{}};
 if(b.meta?.encrypted===true&&typeof b.text==='string'&&typeof b.meta.iv==='string')return {text:b.text,meta:{encrypted:true,e2ee_v:b.meta.e2ee_v,iv:b.meta.iv}};
 throw fail(400,'encrypted_body_required');
}
export function validateScheduledConversationBody(room,body){
 if(room.kind==='group'&&room.e2e_enabled&&(!body.encrypted_content||body.meta?.e2e_group!==true))throw fail(400,'e2e_required');
 if(body.meta?.e2e_group===true){
  let envelope;try{envelope=JSON.parse(body.encrypted_content);}catch{throw fail(400,'bad_encrypted_content');}
  const b64=(s,min,max)=>typeof s==='string'&&/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(s)&&Buffer.from(s,'base64').length>=min&&Buffer.from(s,'base64').length<=max;
  if(!envelope||Array.isArray(envelope)||envelope.v!==1||!integer(envelope.n,0,Number.MAX_SAFE_INTEGER)||!b64(envelope.kid,12,12)||!b64(envelope.ct,16,262144)||!b64(envelope.sig,64,64))throw fail(400,'bad_encrypted_content');
 }
 return body;
}
export const conversationMigration=()=>readFile(new URL('./conversation-features.sql',import.meta.url),'utf8');
export async function listConversationFolders(pool,userId){
 return (await pool.query(`SELECT f.id,f.title,f.room_ids,f.position,f.revision,f.updated_at,
 (SELECT count(*)::int FROM messages m JOIN room_members rm ON rm.room_id=m.room_id AND rm.user_id=f.user_id
 WHERE f.room_ids @> to_jsonb(ARRAY[m.room_id::text]) AND m.sender_id<>f.user_id
 AND NOT COALESCE(m.read_by,'[]'::jsonb) @> to_jsonb(ARRAY[f.user_id::text])
 AND NOT EXISTS(SELECT 1 FROM hidden_messages h WHERE h.message_id=m.id AND h.user_id=f.user_id)) AS unread_count
 FROM chat_folders f WHERE f.user_id=$1 ORDER BY f.position,f.created_at,f.id`,[userId])).rows;
}
export async function reorderConversationFolders(pool,userId,items){
 if(!Array.isArray(items)||items.length>100||items.some(x=>!uuid.test(x?.id)||!integer(x?.revision,0,2147483646))||new Set(items.map(x=>x.id)).size!==items.length)throw fail(400,'bad_folder_order');
 const c=await pool.connect();try{await c.query('BEGIN');const rows=(await c.query('SELECT id,revision FROM chat_folders WHERE user_id=$1 ORDER BY id FOR UPDATE',[userId])).rows;if(rows.length!==items.length||rows.some(row=>!items.some(x=>x.id===row.id&&x.revision===row.revision)))throw fail(409,'folder_conflict');for(let i=0;i<items.length;i++)await c.query('UPDATE chat_folders SET position=$3,revision=revision+1,updated_at=now() WHERE id=$1 AND user_id=$2',[items[i].id,userId,i]);await c.query('COMMIT');}catch(e){await c.query('ROLLBACK');throw e;}finally{c.release();}
 return listConversationFolders(pool,userId);
}
export async function installConversationFeatures(app,deps){
 const {pool,requireAuth,isRoomMember,getRoomRole,broadcastToRoom=()=>{},notifyRoomListUpdate=async()=>{}}=deps;
 await pool.query(await conversationMigration());
 async function member(req){const me=await requireAuth(req),r=String(req.params.id);if(!uuid.test(r))throw fail(400,'bad_room_id');if(!await isRoomMember(r,me.id))throw fail(404,'not_found');return {me,r};}
 async function manager(req){const ctx=await member(req);if(!['owner','admin'].includes(await getRoomRole(ctx.r,ctx.me.id)))throw fail(403,'forbidden');return ctx;}
 const visible=`NOT EXISTS(SELECT 1 FROM hidden_messages h WHERE h.message_id=m.id AND h.user_id=$2)`;
 app.get('/rooms/:id/pins',async req=>{const {me,r}=await member(req);return (await pool.query(`SELECT m.*,p.created_at AS pinned_at,p.pinned_by FROM room_pins p JOIN messages m ON m.id=p.message_id WHERE p.room_id=$1 AND ${visible} ORDER BY p.created_at DESC,m.id DESC`,[r,me.id])).rows;});
 async function pin(req,remove){const {me,r}=await member(req),mid=req.params.messageId;if(!uuid.test(mid))throw fail(400,'bad_message_id');const room=(await pool.query('SELECT kind FROM rooms WHERE id=$1',[r])).rows[0];if(room.kind!=='dm'&&!['owner','admin'].includes(await getRoomRole(r,me.id)))throw fail(403,'forbidden');if(!(await pool.query(`SELECT m.id FROM messages m WHERE m.id=$3 AND m.room_id=$1 AND ${visible}`,[r,me.id,mid])).rows.length)throw fail(404,'not_found');if(remove){await pool.query('DELETE FROM room_pins WHERE room_id=$1 AND message_id=$2',[r,mid]);await pool.query('UPDATE rooms SET pinned_message_id=(SELECT message_id FROM room_pins WHERE room_id=$1 ORDER BY created_at DESC,message_id DESC LIMIT 1) WHERE id=$1',[r]);}else{await pool.query('INSERT INTO room_pins(room_id,message_id,pinned_by) VALUES($1,$2,$3) ON CONFLICT DO NOTHING',[r,mid,me.id]);await pool.query('UPDATE rooms SET pinned_message_id=$2 WHERE id=$1',[r,mid]);}broadcastToRoom(r,{type:'room.pins',room_id:r});return {ok:true};}
 app.put('/rooms/:id/pins/:messageId',req=>pin(req,false));app.delete('/rooms/:id/pins/:messageId',req=>pin(req,true));
 app.get('/rooms/:id/draft',async req=>{const {me,r}=await member(req);return (await pool.query('SELECT revision,encrypted_content,updated_at FROM encrypted_drafts WHERE user_id=$1 AND room_id=$2',[me.id,r])).rows[0]||{revision:0,encrypted_content:null};});
 app.put('/rooms/:id/draft',async(req,reply)=>{const {me,r}=await member(req),b=req.body||{};if(!integer(b.revision,0,2147483646)||b.text!==undefined||(!encrypted(b.encrypted_content)&&b.encrypted_content!==null))throw fail(400,'encrypted_draft_required');await pool.query('INSERT INTO encrypted_drafts(user_id,room_id) VALUES($1,$2) ON CONFLICT DO NOTHING',[me.id,r]);const {rows}=await pool.query('UPDATE encrypted_drafts SET revision=revision+1,encrypted_content=$4,updated_at=now() WHERE user_id=$1 AND room_id=$2 AND revision=$3 RETURNING revision,encrypted_content,updated_at',[me.id,r,b.revision,b.encrypted_content]);if(rows[0]){await notifyRoomListUpdate(me.id);return rows[0];}const current=(await pool.query('SELECT revision,encrypted_content,updated_at FROM encrypted_drafts WHERE user_id=$1 AND room_id=$2',[me.id,r])).rows[0];return reply.code(409).send({error:'draft_conflict',current,incoming:{revision:b.revision,encrypted_content:b.encrypted_content}});});
 app.get('/rooms/:id/anchor',async req=>{const {me,r}=await member(req),date=new Date(req.query.date);if(!req.query.date||!Number.isFinite(+date))throw fail(400,'bad_date');const row=(await pool.query(`SELECT m.id,m.created_at FROM messages m WHERE m.room_id=$1 AND m.created_at>=$3 AND ${visible} ORDER BY m.created_at,m.id LIMIT 1`,[r,me.id,date.toISOString()])).rows[0];return {message_id:row?.id||null,created_at:row?.created_at||null};});
 app.get('/rooms/:id/messages/:messageId/history',async req=>{const {me,r}=await member(req);if(!uuid.test(req.params.messageId))throw fail(400,'bad_message_id');return (await pool.query(`SELECT h.id,h.encrypted_content,h.created_at,m.sender_id,m.room_id,m.id AS message_id FROM message_edit_history h JOIN messages m ON m.id=h.message_id WHERE m.room_id=$1 AND h.message_id=$3 AND ${visible} ORDER BY h.id DESC LIMIT 100`,[r,me.id,req.params.messageId])).rows;});
 app.post('/me/read-all',async req=>{const me=await requireAuth(req);const rows=(await pool.query(`UPDATE messages m SET read_by=COALESCE(m.read_by,'[]'::jsonb)||$2::jsonb WHERE m.sender_id<>$1 AND NOT COALESCE(m.read_by,'[]'::jsonb) @> $2::jsonb AND EXISTS(SELECT 1 FROM room_members rm WHERE rm.room_id=m.room_id AND rm.user_id=$1) AND NOT EXISTS(SELECT 1 FROM hidden_messages h WHERE h.message_id=m.id AND h.user_id=$1) RETURNING room_id,id`,[me.id,JSON.stringify([me.id])])).rows;await pool.query('UPDATE room_preferences SET marked_unread=false WHERE user_id=$1',[me.id]);for(const room of new Set(rows.map(row=>row.room_id)))broadcastToRoom(room,{type:'messages.read',room_id:room,user_id:me.id,message_ids:rows.filter(row=>row.room_id===room).map(row=>row.id)});await notifyRoomListUpdate(me.id);return {ok:true,count:rows.length};});
 app.get('/rooms/:id/conversation-settings',async req=>{const {r}=await member(req);return (await pool.query(`SELECT ${settingsColumns} FROM rooms WHERE id=$1`,[r])).rows[0];});
 app.put('/rooms/:id/conversation-settings',async req=>{const {r}=await manager(req),b=req.body||{};if(b.slow_mode_seconds!==undefined&&!integer(b.slow_mode_seconds,0,3600)||b.auto_delete_seconds!==undefined&&!([0,3600,86400,604800,2592000].includes(b.auto_delete_seconds)))throw fail(400,'bad_settings');if(b.public!==undefined&&typeof b.public!=='boolean')throw fail(400,'bad_settings');if(b.public===true&&(typeof b.public_title!=='string'||!b.public_title.trim()||b.public_title.trim().length>120))throw fail(400,'public_title_required');if(b.public===true&&(!/^[a-z][a-z0-9-]{3,39}$/.test(b.slug)||typeof b.description!=='string'||b.description.length>500))throw fail(400,'bad_public_profile');const client=await pool.connect();try{await client.query('BEGIN');const room=(await client.query('SELECT kind FROM rooms WHERE id=$1 FOR UPDATE',[r])).rows[0];if(room.kind==='dm')throw fail(400,'group_only');await client.query('UPDATE rooms SET slow_mode_seconds=COALESCE($2,slow_mode_seconds),auto_delete_seconds=COALESCE($3,auto_delete_seconds),public_slug=CASE WHEN $4::boolean IS NULL THEN public_slug WHEN $4 THEN $5 ELSE NULL END,public_description=CASE WHEN $4::boolean THEN $6 ELSE public_description END,public_title=CASE WHEN $4::boolean THEN $7 WHEN $4::boolean=false THEN NULL ELSE public_title END WHERE id=$1',[r,b.slow_mode_seconds??null,b.auto_delete_seconds??null,b.public??null,b.slug??null,b.description??null,b.public_title?.trim()??null]);await client.query('COMMIT');}catch(e){await client.query('ROLLBACK');if(e.code==='23505')throw fail(409,'slug_taken');throw e;}finally{client.release();}broadcastToRoom(r,{type:'room.settings',room_id:r});return {ok:true};});
 app.get('/rooms/:id/scheduled',async req=>{const {me,r}=await member(req);return (await pool.query("SELECT id,encrypted_content,send_at,kind,status,message_id,next_attempt_at,attempts,last_error FROM scheduled_messages WHERE room_id=$1 AND user_id=$2 ORDER BY send_at DESC LIMIT 200",[r,me.id])).rows;});
 app.post('/rooms/:id/scheduled',async req=>{const {me,r}=await member(req),b=req.body||{},date=new Date(b.send_at);if(!encrypted(b.encrypted_content)||b.text!==undefined||!Number.isFinite(+date)||+date<Date.now()+1000||+date>Date.now()+366*86400000||!['message','reminder'].includes(b.kind||'message'))throw fail(400,'encrypted_schedule_required');unpackConversationBody(b.encrypted_content);const client=await pool.connect();try{await client.query('BEGIN');const room=(await client.query('SELECT kind,e2e_enabled,conversation_epoch FROM rooms WHERE id=$1 FOR UPDATE',[r])).rows[0];const role=(await client.query('SELECT role FROM room_members WHERE room_id=$1 AND user_id=$2',[r,me.id])).rows[0]?.role;if(!role)throw fail(404,'not_found');validateScheduledConversationBody(room,unpackConversationBody(b.encrypted_content));if(room.kind==='channel'&&!['owner','admin'].includes(role)&&b.kind!=='reminder')throw fail(403,'channel_read_only');const row=(await client.query('INSERT INTO scheduled_messages(room_id,user_id,encrypted_content,send_at,kind,epoch) VALUES($1,$2,$3,$4,$5,$6) RETURNING id,status,send_at',[r,me.id,b.encrypted_content,date.toISOString(),b.kind||'message',room.conversation_epoch])).rows[0];await client.query('COMMIT');return row;}catch(e){await client.query('ROLLBACK');throw e;}finally{client.release();}});
 app.patch('/rooms/:id/scheduled/:scheduleId',async req=>{const {me,r}=await member(req),date=new Date(req.body?.send_at);if(!uuid.test(req.params.scheduleId)||!Number.isFinite(+date)||+date<Date.now()+1000||+date>Date.now()+366*86400000||Object.keys(req.body||{}).some(key=>key!=='send_at'))throw fail(400,'bad_schedule_time');const row=(await pool.query("UPDATE scheduled_messages SET send_at=$4,next_attempt_at=$4,last_error=NULL WHERE id=$1 AND room_id=$2 AND user_id=$3 AND status='pending' RETURNING id,status,send_at",[req.params.scheduleId,r,me.id,date.toISOString()])).rows[0];if(!row)throw fail(404,'not_found');return row;});
 app.delete('/rooms/:id/scheduled/:scheduleId',async req=>{const {me,r}=await member(req);if(!uuid.test(req.params.scheduleId))throw fail(400,'bad_schedule_id');await pool.query("UPDATE scheduled_messages SET status='cancelled' WHERE id=$1 AND room_id=$2 AND user_id=$3 AND status IN ('pending','due','needs_reencrypt')",[req.params.scheduleId,r,me.id]);return {ok:true};});
 app.post('/rooms/:id/members/batch',async req=>{const {me,r}=await manager(req),b=req.body||{};if(!['kick','admin','member'].includes(b.action)||!Array.isArray(b.user_ids)||b.user_ids.length<1||b.user_ids.length>100||b.user_ids.some(x=>typeof x!=='string'||!uuid.test(x)))throw fail(400,'bad_batch');const results=[];for(const target of new Set(b.user_ids)){const c=await pool.connect();try{await c.query('BEGIN');await c.query('SELECT id FROM rooms WHERE id=$1 FOR UPDATE',[r]);const rows=(await c.query('SELECT user_id,role FROM room_members WHERE room_id=$1 AND user_id=ANY($2::uuid[])',[r,[me.id,target]])).rows,actor=rows.find(x=>x.user_id===me.id)?.role,role=rows.find(x=>x.user_id===target)?.role;if(!role){await c.query('ROLLBACK');results.push({user_id:target,error:'not_found'});continue;}if(!['owner','admin'].includes(actor)||target===me.id||role==='owner'||(b.action!=='kick'&&actor!=='owner')||(role==='admin'&&actor!=='owner')){await c.query('ROLLBACK');results.push({user_id:target,error:'forbidden'});continue;}if(b.action==='kick'){if(typeof deps.evictRoomUser!=='function')throw fail(503,'room_eviction_unavailable');await deps.evictRoomUser(r,target);await c.query('DELETE FROM room_members WHERE room_id=$1 AND user_id=$2',[r,target]);await c.query('DELETE FROM group_sender_keys WHERE room_id=$1 AND to_user_id=$2',[r,target]);}else await c.query('UPDATE room_members SET role=$3 WHERE room_id=$1 AND user_id=$2',[r,target,b.action]);await c.query('COMMIT');results.push({user_id:target,ok:true});if(b.action==='kick')broadcastToRoom(r,{type:'room.members_changed',room_id:r,removed_user_id:target});await notifyRoomListUpdate(target);}catch(e){await c.query('ROLLBACK');throw e;}finally{c.release();}}return {results};});
 const publicSelect=`SELECT r.id,r.kind,r.public_title,r.public_title AS title,r.public_slug AS slug,r.public_description AS description,(SELECT count(*)::int FROM room_members rm WHERE rm.room_id=r.id) AS member_count FROM rooms r WHERE r.public_slug IS NOT NULL AND r.public_title IS NOT NULL AND r.kind IN ('group','channel')`;
 app.get('/communities',async req=>{await requireAuth(req);const q=String(req.query.q||'').slice(0,100);return (await pool.query(`${publicSelect} AND (r.public_title ILIKE $1 OR r.public_slug ILIKE $1) ORDER BY r.public_slug LIMIT 50`,['%'+q.replace(/[%_\\]/g,'\\$&')+'%'])).rows;});
 app.get('/communities/:slug',async req=>{await requireAuth(req);return (await pool.query(`${publicSelect} AND r.public_slug=$1`,[String(req.params.slug)])).rows[0]||(()=>{throw fail(404,'not_found');})();});
 app.post('/communities/:slug/join',async req=>{const me=await requireAuth(req),c=await pool.connect();try{await c.query('BEGIN');const room=(await c.query("SELECT id FROM rooms WHERE public_slug=$1 AND public_title IS NOT NULL AND kind IN ('group','channel') FOR UPDATE",[String(req.params.slug)])).rows[0];if(!room)throw fail(404,'not_found');const joined=(await c.query("INSERT INTO room_members(room_id,user_id,role) VALUES($1,$2,'member') ON CONFLICT DO NOTHING RETURNING user_id",[room.id,me.id])).rows.length;await c.query('COMMIT');if(joined)broadcastToRoom(room.id,{type:'room.members_changed',room_id:room.id,added_user_id:me.id});await notifyRoomListUpdate(me.id);return {ok:true,room_id:room.id};}catch(e){await c.query('ROLLBACK');throw e;}finally{c.release();}});
 // No autonomous interval: host owns lifecycle, observes errors, and calls this worker.
 return {processJobs:()=>processConversationJobs(deps)};
}
export async function processConversationJobs({pool,broadcastToRoom=()=>{},notifyRoomListUpdate=async()=>{},getMessage,canWrite,onReminder=async()=>{}}){
 const expired=(await pool.query('DELETE FROM messages WHERE expires_at<=now() RETURNING id,room_id')).rows;
 for(const row of expired)broadcastToRoom(row.room_id,{type:'message.delete',room_id:row.room_id,message_id:row.id});
 const errors=[];
 for(let i=0;i<100;i++){
  const c=await pool.connect();let job,delivery=null,committed=false,reminder=false;
  try{
   await c.query('BEGIN');
   job=(await c.query("SELECT * FROM scheduled_messages WHERE status='pending' AND send_at<=now() AND next_attempt_at<=now() ORDER BY send_at,id LIMIT 1 FOR UPDATE SKIP LOCKED")).rows[0];
   if(!job){await c.query('COMMIT');break;}
   const room=(await c.query('SELECT conversation_epoch,kind,e2e_enabled FROM rooms WHERE id=$1 FOR UPDATE',[job.room_id])).rows[0];
   const role=(await c.query('SELECT role FROM room_members WHERE room_id=$1 AND user_id=$2',[job.room_id,job.user_id])).rows[0]?.role;
   let blocked=room.kind==='dm'&&(await c.query('SELECT 1 FROM user_blocks b JOIN room_members peer ON peer.room_id=$1 AND peer.user_id<>$2 WHERE (b.blocker_id=$2 AND b.blocked_id=peer.user_id) OR (b.blocked_id=$2 AND b.blocker_id=peer.user_id) LIMIT 1',[job.room_id,job.user_id])).rows.length>0;
   if(room.kind==='dm'&&canWrite&&job.kind!=='reminder'){const peers=(await c.query('SELECT user_id FROM room_members WHERE room_id=$1 AND user_id<>$2',[job.room_id,job.user_id])).rows;for(const peer of peers)if(!await canWrite(job.user_id,peer.user_id))blocked=true;}
   let status=blocked?'cancelled':'needs_reencrypt',messageId=null,validationError=null,body;
   try{body=validateScheduledConversationBody(room,unpackConversationBody(job.encrypted_content));}catch(e){if(e.statusCode!==400)throw e;validationError=e.message;}
   if(role&&job.kind==='reminder'&&!validationError){status='due';reminder=true;}
   else if(!blocked&&role&&!validationError&&String(room.conversation_epoch)===String(job.epoch)&&(room.kind!=='channel'||['owner','admin'].includes(role))){
    await c.query('SAVEPOINT scheduled_publish');
    try{
     delivery=(await c.query("INSERT INTO messages(room_id,sender_id,type,text,encrypted_content,meta,client_message_id,is_post) VALUES($1,$2,'text',$3,$4,$5::jsonb,$6,$7) ON CONFLICT(sender_id,client_message_id) DO UPDATE SET client_message_id=EXCLUDED.client_message_id RETURNING *",[job.room_id,job.user_id,body.text||null,body.encrypted_content||null,JSON.stringify(body.meta),'scheduled_'+job.id,room.kind==='channel'])).rows[0];
     messageId=delivery.id;status='sent';
    }catch(e){
     if(e.code!=='P0001'||!['slow_mode','duplicate_spam'].includes(e.message))throw e;
     await c.query('ROLLBACK TO SAVEPOINT scheduled_publish');
     const backoff=Math.min(3600,Math.max(15,2**Math.min(job.attempts,10)*15));
     await c.query("UPDATE scheduled_messages SET attempts=attempts+1,next_attempt_at=now()+make_interval(secs=>$2),last_error=$3 WHERE id=$1",[job.id,backoff,e.message]);
     await c.query('COMMIT');committed=true;continue;
    }
   }
   await c.query('UPDATE scheduled_messages SET status=$2,message_id=$3,last_error=$4 WHERE id=$1',[job.id,status,messageId,blocked?'privacy_restricted':validationError]);
   await c.query('COMMIT');committed=true;
  }catch(e){
   await c.query('ROLLBACK');
   if(job)await c.query("UPDATE scheduled_messages SET attempts=attempts+1,next_attempt_at=now()+interval '1 minute',last_error='publication_failed' WHERE id=$1 AND status='pending'",[job.id]);
   errors.push(e);
   if(!job)break;
  }finally{c.release();}
  if(committed&&job){try{await notifyRoomListUpdate(job.user_id);if(reminder)await onReminder(job);if(delivery)broadcastToRoom(job.room_id,{type:'message.new',message:getMessage?await getMessage(delivery.id):{...delivery,reactions:{}}});}catch(e){errors.push(e);}}
 }
 if(errors.length)throw new AggregateError(errors,'conversation_jobs_failed');
 return {expired:expired.length};
}
