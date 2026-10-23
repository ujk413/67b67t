import {readFile} from 'node:fs/promises';
import {randomInt,randomUUID} from 'node:crypto';
import {PAYLOAD_PREFIX,parseMessagePayload} from './expressive-payloads.js';
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const fail=(error,statusCode=400)=>{throw Object.assign(new Error(error),{statusCode});};
const uuid=value=>typeof value==='string'&&UUID.test(value)?value:fail('bad_id');
const body=(value,allowed)=>{if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>!allowed.includes(k)))fail('bad_body');return value;};
const text=(value,max,empty=false)=>typeof value==='string'&&value.length<=max&&(empty||value.trim().length)&&!/[\u0000-\u0008]/.test(value)?value.trim():fail('bad_text');
const tags=value=>value===undefined?[]:Array.isArray(value)&&value.length<=20?[...new Set(value.map(v=>text(v,40)))]:fail('bad_tags');
function pagination(query={}){
 const limit=query.limit===undefined?40:Number(query.limit);if(!Number.isSafeInteger(limit)||limit<1||limit>100)fail('bad_limit');
 let cursor=null;if(query.cursor){if(typeof query.cursor!=='string'||query.cursor.length>100)fail('bad_cursor');try{cursor=Buffer.from(query.cursor,'base64url').toString('utf8');if(!UUID.test(cursor)||Buffer.from(cursor).toString('base64url')!==query.cursor)fail('bad_cursor');}catch{fail('bad_cursor');}}
 return{limit,cursor};
}
const page=(rows,limit)=>({items:rows.slice(0,limit),next_cursor:rows.length>limit?Buffer.from(rows[limit-1].id).toString('base64url'):null});
const mutableKinds=new Set(['contact','sticker','gif']);

/** No poll/location REST transport: these are encrypted ordinary text messages. */
export async function installExpressiveFeatures(app,{pool,requireAuth,isRoomMember,getRoomRole,broadcastToRoom,mapGeoJSON=null}){
 await pool.query(await readFile(new URL('./expressive-features.sql',import.meta.url),'utf8'));
 const auth=async req=>{const me=await requireAuth(req);uuid(me.id);return me;};
 const member=async(req,write=false)=>{
  const me=await auth(req),roomId=uuid(req.params.roomId);
  if(!await isRoomMember(roomId,me.id))fail('not_found',404);
  const room=(await pool.query('SELECT id,kind,e2e_enabled FROM rooms WHERE id=$1',[roomId])).rows[0];if(!room)fail('not_found',404);
  if(write&&room.kind==='channel'&&!['owner','admin'].includes(await getRoomRole(roomId,me.id)))fail('forbidden',403);
  return{me,room};
 };
 const writeConfig={config:{rateLimit:{max:30,timeWindow:'1 minute'}}};
 const ownedPack=async(id,user)=>{
  const pack=(await pool.query('SELECT * FROM expressive_packs WHERE id=$1 AND owner_id=$2 AND deleted_at IS NULL',[uuid(id),user])).rows[0];if(!pack)fail('not_found',404);return pack;
 };
 const availableItem=async(id,user)=>(await pool.query(`SELECT c.*,p.title AS pack_title,p.visibility,p.owner_id FROM expressive_catalog c JOIN expressive_packs p ON p.id=c.pack_id WHERE c.id=$1 AND c.deleted_at IS NULL AND p.deleted_at IS NULL AND (p.owner_id=$2 OR p.visibility='public')`,[uuid(id),user])).rows[0];
 app.get('/expressive/capabilities',async req=>{await auth(req);return{version:1,catalog:true,encrypted_typed_messages:true,ordinary_dice:true,offline_osm:!!mapGeoJSON};});
 app.get('/expressive/map-bundle',async(req,reply)=>{await auth(req);if(!mapGeoJSON)return reply.code(404).send({error:'offline_map_not_installed'});reply.header('Cache-Control','private, max-age=86400');return mapGeoJSON;});
 app.get('/expressive/packs',async req=>{
  const me=await auth(req),{limit,cursor}=pagination(req.query);
  const rows=(await pool.query(`SELECT id,title,tags,visibility,owner_id FROM expressive_packs WHERE deleted_at IS NULL AND (owner_id=$1 OR visibility='public') AND ($2::uuid IS NULL OR id>$2) ORDER BY id LIMIT $3`,[me.id,cursor,limit+1])).rows;return page(rows,limit);
 });
 app.get('/expressive/installed',async req=>{
  const me=await auth(req),{limit,cursor}=pagination(req.query);
  const rows=(await pool.query(`SELECT p.id,p.title,p.tags,p.visibility,p.owner_id FROM expressive_installed_packs i JOIN expressive_packs p ON p.id=i.pack_id WHERE i.user_id=$1 AND p.deleted_at IS NULL AND (p.owner_id=$1 OR p.visibility='public') AND ($2::uuid IS NULL OR p.id>$2) ORDER BY p.id LIMIT $3`,[me.id,cursor,limit+1])).rows;return page(rows,limit);
 });
 app.put('/expressive/installed/:id',writeConfig,async req=>{
  const me=await auth(req),id=uuid(req.params.id);body(req.body||{},[]);
  const result=await pool.query(`INSERT INTO expressive_installed_packs(user_id,pack_id) SELECT $1,id FROM expressive_packs WHERE id=$2 AND deleted_at IS NULL AND (owner_id=$1 OR visibility='public') ON CONFLICT(user_id,pack_id)DO UPDATE SET added_at=now() RETURNING pack_id`,[me.id,id]);
  if(!result.rows.length)fail('not_found',404);return{ok:true};
 });
 app.delete('/expressive/installed/:id',writeConfig,async req=>{const me=await auth(req);await pool.query('DELETE FROM expressive_installed_packs WHERE user_id=$1 AND pack_id=$2',[me.id,uuid(req.params.id)]);return{ok:true};});
 app.post('/expressive/packs',writeConfig,async req=>{
  const me=await auth(req),b=body(req.body,['title','tags','visibility']),visibility=b.visibility||'private';if(!['private','public'].includes(visibility))fail('bad_visibility');
  const count=(await pool.query('SELECT count(*)::int AS n FROM expressive_packs WHERE owner_id=$1 AND deleted_at IS NULL',[me.id])).rows[0].n;if(count>=100)fail('pack_limit',429);
  return(await pool.query('INSERT INTO expressive_packs(id,owner_id,title,tags,visibility) VALUES($1,$2,$3,$4,$5) RETURNING id,title,tags,visibility,owner_id',[randomUUID(),me.id,text(b.title,100),tags(b.tags),visibility])).rows[0];
 });
 app.patch('/expressive/packs/:id',writeConfig,async req=>{
  const me=await auth(req),pack=await ownedPack(req.params.id,me.id),b=body(req.body,['title','tags','visibility']);if(b.visibility!==undefined&&!['private','public'].includes(b.visibility))fail('bad_visibility');
  return(await pool.query('UPDATE expressive_packs SET title=$2,tags=$3,visibility=$4 WHERE id=$1 RETURNING id,title,tags,visibility,owner_id',[pack.id,b.title===undefined?pack.title:text(b.title,100),b.tags===undefined?pack.tags:tags(b.tags),b.visibility||pack.visibility])).rows[0];
 });
 app.delete('/expressive/packs/:id',writeConfig,async req=>{const me=await auth(req),pack=await ownedPack(req.params.id,me.id);await pool.query('UPDATE expressive_packs SET deleted_at=now() WHERE id=$1',[pack.id]);return{ok:true};});
 app.post('/expressive/packs/:id/items',writeConfig,async req=>{
  const me=await auth(req),pack=await ownedPack(req.params.id,me.id),b=body(req.body,['media_id','kind','label','tags']);if(!['sticker','gif'].includes(b.kind))fail('bad_kind');
  const media=(await pool.query('SELECT id,mime,size_bytes FROM media WHERE id=$1 AND owner_id=$2',[uuid(b.media_id),me.id])).rows[0];if(!media)fail('not_found',404);
  if(!['image/webp','image/png','image/jpeg','image/gif','video/mp4','video/webm'].includes(media.mime)||Number(media.size_bytes)>20*1024*1024)fail('bad_catalog_media');
  if(b.kind==='sticker'&&!['image/webp','image/png','image/jpeg'].includes(media.mime))fail('bad_sticker_media');
  if(b.kind==='gif'&&!['image/gif','video/mp4','video/webm','image/webp'].includes(media.mime))fail('bad_gif_media');
  const count=(await pool.query('SELECT count(*)::int AS n FROM expressive_catalog WHERE pack_id=$1 AND deleted_at IS NULL',[pack.id])).rows[0].n;if(count>=500)fail('pack_item_limit',429);
  return(await pool.query(`INSERT INTO expressive_catalog(id,pack_id,media_id,kind,label,tags) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(pack_id,media_id) DO UPDATE SET deleted_at=NULL,kind=excluded.kind,label=excluded.label,tags=excluded.tags RETURNING id,pack_id,media_id,kind,label,tags`,[randomUUID(),pack.id,media.id,b.kind,text(b.label||'',100,true),tags(b.tags)])).rows[0];
 });
 app.delete('/expressive/items/:id',writeConfig,async req=>{
  const me=await auth(req),item=await availableItem(req.params.id,me.id);if(!item||item.owner_id!==me.id)fail('not_found',404);
  await pool.query('UPDATE expressive_catalog SET deleted_at=now() WHERE id=$1',[item.id]);return{ok:true};
 });
 app.get('/expressive/catalog',async req=>{
  const me=await auth(req),{limit,cursor}=pagination(req.query),q=text(req.query.q||'',80,true),kind=req.query.kind||null,pack=req.query.pack_id?uuid(req.query.pack_id):null;
  if(kind&&!['sticker','gif'].includes(kind))fail('bad_kind');
  const rows=(await pool.query(`SELECT c.id,c.pack_id,c.media_id,c.kind,c.label,c.tags,p.title AS pack_title,m.mime
   FROM expressive_catalog c JOIN expressive_packs p ON p.id=c.pack_id JOIN media m ON m.id=c.media_id
   WHERE c.deleted_at IS NULL AND p.deleted_at IS NULL AND (p.owner_id=$1 OR p.visibility='public')
   AND ($2::text='' OR position(lower($2) in lower(c.label||' '||p.title||' '||array_to_string(c.tags,' ')||' '||array_to_string(p.tags,' ')))>0)
   AND ($3::text IS NULL OR c.kind=$3) AND ($4::uuid IS NULL OR c.pack_id=$4) AND ($5::uuid IS NULL OR c.id>$5)
   ORDER BY c.id LIMIT $6`,[me.id,q,kind,pack,cursor,limit+1])).rows;return page(rows,limit);
 });
 app.post('/expressive/recent/:id',writeConfig,async req=>{const me=await auth(req),item=await availableItem(req.params.id,me.id);if(!item)fail('not_found',404);
  await pool.query('INSERT INTO expressive_recent(user_id,item_id) VALUES($1,$2) ON CONFLICT(user_id,item_id) DO UPDATE SET used_at=now()',[me.id,item.id]);
  await pool.query('DELETE FROM expressive_recent WHERE user_id=$1 AND item_id NOT IN (SELECT item_id FROM expressive_recent WHERE user_id=$1 ORDER BY used_at DESC,item_id DESC LIMIT 100)',[me.id]);return{ok:true};
 });
 app.get('/expressive/recent',async req=>{const me=await auth(req),{limit}=pagination(req.query);return{items:(await pool.query(`SELECT c.id,c.pack_id,c.media_id,c.kind,c.label,c.tags,m.mime FROM expressive_recent r JOIN expressive_catalog c ON c.id=r.item_id JOIN expressive_packs p ON p.id=c.pack_id JOIN media m ON m.id=c.media_id WHERE r.user_id=$1 AND c.deleted_at IS NULL AND p.deleted_at IS NULL AND (p.owner_id=$1 OR p.visibility='public') ORDER BY r.used_at DESC,c.id DESC LIMIT $2`,[me.id,limit])).rows,next_cursor:null};});
 app.post('/expressive/rooms/:roomId/dice',writeConfig,async req=>{
  const{me,room}=await member(req,true);body(req.body||{},[]);if(room.e2e_enabled)fail('use_encrypted_device_dice',409);
  const count=(await pool.query("SELECT count(*)::int AS n FROM expressive_dice WHERE sender_id=$1 AND created_at>now()-interval '1 minute'",[me.id])).rows[0].n;if(count>=20)fail('dice_rate_limit',429);
  return(await pool.query('INSERT INTO expressive_dice(id,room_id,sender_id,value) VALUES($1,$2,$3,$4) RETURNING id,value,created_at',[randomUUID(),room.id,me.id,randomInt(1,7)])).rows[0];
 });
 app.get('/expressive/rooms/:roomId/dice/:id',async req=>{
  const{me,room}=await member(req);const rows=(await pool.query(`SELECT d.id,d.value,d.sender_id,d.message_id,d.created_at FROM expressive_dice d JOIN messages m ON m.id=d.message_id WHERE d.id=$1 AND d.room_id=$2 AND NOT EXISTS(SELECT 1 FROM hidden_messages h WHERE h.user_id=$3 AND h.message_id=m.id)`,[uuid(req.params.id),room.id,me.id])).rows;
  if(!rows[0])fail('not_found',404);return rows[0];
 });
 app.post('/expressive/rooms/:roomId/dice/:id/bind',writeConfig,async req=>{
  const{me,room}=await member(req,true),b=body(req.body,['message_id']),messageId=uuid(b.message_id),diceId=uuid(req.params.id);
  const message=(await pool.query('SELECT id FROM messages WHERE id=$1 AND room_id=$2 AND sender_id=$3',[messageId,room.id,me.id])).rows[0];if(!message)fail('not_found',404);
  const result=await pool.query('UPDATE expressive_dice SET message_id=$4 WHERE id=$1 AND room_id=$2 AND sender_id=$3 AND (message_id IS NULL OR message_id=$4) RETURNING id',[diceId,room.id,me.id,messageId]);
  if(!result.rows.length)fail('dice_already_used',409);
  broadcastToRoom(room.id,{type:'expressive.dice.bound',room_id:room.id,message_id:messageId});return{ok:true};
 });
 async function canReadCatalogMedia(mediaId,userId){return(await pool.query(`SELECT 1 FROM expressive_catalog c JOIN expressive_packs p ON p.id=c.pack_id WHERE c.media_id=$1 AND c.deleted_at IS NULL AND p.deleted_at IS NULL AND (p.owner_id=$2 OR p.visibility='public') LIMIT 1`,[uuid(mediaId),uuid(userId)])).rows.length>0;}
 async function validatePlaintext({roomId,userId,text:plaintext,encrypted=false}){
  if(encrypted||typeof plaintext!=='string'||!plaintext.startsWith(PAYLOAD_PREFIX))return null;
  const p=parseMessagePayload(plaintext);if(!p)fail('invalid_message_payload');
  if(!await isRoomMember(uuid(roomId),uuid(userId)))fail('not_found',404);
  if(p.kind==='dice'&&p.source==='server'){
   const d=(await pool.query('SELECT value FROM expressive_dice WHERE id=$1 AND room_id=$2 AND sender_id=$3',[p.id,roomId,userId])).rows[0];if(!d||d.value!==p.value)fail('invalid_dice');
  }
  return p;
 }
 /** Invoke INSIDE the existing send transaction after INSERT ... RETURNING.
  * Unique binding makes a certificate single-use even under concurrent sends. */
 async function bindDice({roomId,userId,messageId,text:plaintext,query=pool.query.bind(pool)}){
  const p=parseMessagePayload(plaintext);if(p?.kind!=='dice'||p.source!=='server')return;
  const result=await query(`UPDATE expressive_dice SET message_id=$4 WHERE id=$1 AND room_id=$2 AND sender_id=$3 AND value=$5 AND (message_id IS NULL OR message_id=$4) RETURNING id`,[p.id,roomId,userId,uuid(messageId),p.value]);if(!result.rows.length)fail('dice_already_used',409);
 }
 function canEditText(plaintext){const p=parseMessagePayload(plaintext);return!p||mutableKinds.has(p.kind);}
 return{canReadCatalogMedia,validatePlaintext,bindDice,canEditText};
}
