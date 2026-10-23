import {createHmac,randomUUID} from 'node:crypto';
import {readFileSync} from 'node:fs';

export const callFeaturesMigration=readFileSync(new URL('./call-features.sql',import.meta.url),'utf8');
const date=value=>value==null?null:new Date(value).toISOString();
const isUuid=value=>/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(String(value));

/** Native/server SDK independent LiveKit Twirp adapter, only for the configured self-host. */
export function createLiveKitService({url=process.env.LIVEKIT_URL,apiUrl=process.env.LIVEKIT_API_URL,key=process.env.LIVEKIT_API_KEY,secret=process.env.LIVEKIT_API_SECRET,enabled=process.env.LIVEKIT_ENABLE_GROUP_CALLS==='true',gateway,fetchImpl=fetch}={}) {
 // Keep the release capability disabled until the host explicitly accepts and
 // deploys its SFU revocation policy; token expiry alone cannot end active media.
 if(!enabled||!url||!apiUrl||!key||!secret||!gateway?.enabled)return null;
 const publicUrl=new URL(url),controlUrl=new URL(apiUrl);
 if(!['wss:','ws:'].includes(publicUrl.protocol)||!['https:','http:'].includes(controlUrl.protocol))throw new Error('invalid_livekit_url');
 const token=async({identity,room,ttl=90,admin=false,leaseId,name})=>{
  if(!admin&&!isUuid(leaseId))throw Error('invalid_call_lease');
  const now=Math.floor(Date.now()/1000),encode=value=>Buffer.from(JSON.stringify(value)).toString('base64url');
  const head=encode({alg:'HS256',typ:'JWT'}),body=encode({iss:key,sub:admin?identity:identity+':'+leaseId,...(name?{name}:{}),nbf:now-5,exp:now+ttl,video:admin?{roomCreate:true,roomAdmin:true,room}:{roomJoin:true,room,canPublish:true,canSubscribe:true,canPublishData:false,canUpdateOwnMetadata:false}});
  return `${head}.${body}.${createHmac('sha256',secret).update(`${head}.${body}`).digest('base64url')}`;
 };
 async function rpc(method,payload){
  const response=await fetchImpl(new URL('/twirp/livekit.RoomService/'+method,controlUrl),{method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer '+await token({identity:'67chat-api',room:payload.name||payload.room,ttl:30,admin:true})},body:JSON.stringify(payload),signal:AbortSignal.timeout(6000)});
  if(!response.ok){const error=await response.json().catch(()=>({}));if(method==='RemoveParticipant'&&error.code==='not_found')return {};throw new Error('livekit_unavailable');}return response.json();
 }
 return {url:publicUrl.toString().replace(/\/$/,''),token,createRoom:options=>rpc('CreateRoom',options),removeParticipant:(room,user,leaseId)=>{gateway.closeLease(user,leaseId);return rpc('RemoveParticipant',{room,identity:user+':'+leaseId});}};
}

export function installCallFeatures(app,{pool,requireAuth,sendToUser,sfu=null,broadcastToRoom,getMessage,notifyRoomListUpdateForRoom}) {
 let relay=null;
 const occupancy={
  async isBusy(user){return (await pool.query(`SELECT 1 FROM group_call_leases WHERE user_id=$1 AND expires_at>now()`,[user])).rows.length>0;},
  async acquire(user,room,sessionId){
   const existing=(await pool.query(`SELECT room_id,lease_id,expires_at<=now() AS expired FROM group_call_leases WHERE user_id=$1`,[user])).rows[0];
   if(existing){if(!existing.expired)throw Object.assign(new Error('busy'),{statusCode:409});await occupancy.end(user,existing.room_id,existing.lease_id);}
   const id=randomUUID(),{rows}=await pool.query(`INSERT INTO group_call_leases(user_id,room_id,expires_at,lease_id,session_id) VALUES($1,$2,now()+interval '120 seconds',$3,$4) ON CONFLICT(user_id) DO NOTHING RETURNING lease_id`,[user,room,id,sessionId||null]);if(!rows.length)throw Object.assign(new Error('busy'),{statusCode:409});return id;
  },
  async end(user,onlyRoom,leaseId){const params=[user,onlyRoom||null,leaseId||null];const {rows}=await pool.query(`UPDATE group_call_leases SET revoking_at=COALESCE(revoking_at,now()) WHERE user_id=$1 AND ($2::uuid IS NULL OR room_id=$2) AND ($3::uuid IS NULL OR lease_id=$3) RETURNING room_id,lease_id`,params);for(const row of rows)if(sfu)await sfu.removeParticipant('67chat-'+row.room_id,String(user),row.lease_id);await pool.query(`DELETE FROM group_call_leases WHERE user_id=$1 AND ($2::uuid IS NULL OR room_id=$2) AND ($3::uuid IS NULL OR lease_id=$3)`,params);},
  async endSessions(user,{onlySessionId,exceptSessionId}){const {rows}=await pool.query(`SELECT room_id,lease_id FROM group_call_leases WHERE user_id=$1 AND ($2::uuid IS NULL OR session_id=$2) AND ($3::uuid IS NULL OR session_id IS DISTINCT FROM $3)`,[user,onlySessionId||null,exceptSessionId||null]);for(const row of rows)await occupancy.end(user,row.room_id,row.lease_id);},
 };
 const coordinate=fn=>relay?relay.coordinate(fn):fn();
 const leaseCleanup=setInterval(()=>{void coordinate(async()=>{const {rows}=await pool.query(`SELECT user_id,room_id,lease_id FROM group_call_leases WHERE expires_at<now() OR revoking_at IS NOT NULL`);for(const row of rows)await occupancy.end(row.user_id,row.room_id,row.lease_id);}).catch(error=>app.log?.warn({error:error.message},'Call lease cleanup failed'));},15000);leaseCleanup.unref?.();app.addHook('onClose',async()=>clearInterval(leaseCleanup));
 const eventSql=`INSERT INTO messages(id,room_id,sender_id,type,text,meta,created_at)
   SELECT h.id,h.room_id,h.caller_id,'call',NULL,
    jsonb_build_object('call_id',h.id,'status',h.status,'reason',h.reason,'is_video',h.is_video,'duration_ms',h.duration_ms,'duration_sec',h.duration_ms/1000,'started_at',h.started_at,'answered_at',h.answered_at,'ended_at',h.ended_at,'caller_id',h.caller_id,'callee_id',h.callee_id),h.ended_at
   FROM ended h WHERE EXISTS(SELECT 1 FROM room_members m WHERE m.room_id=h.room_id AND m.user_id=h.caller_id)
   ON CONFLICT(id) DO NOTHING RETURNING *`;
 async function announce(rows){for(const row of rows){const {event,...call}=row;for(const user of [call.caller_id,call.callee_id])sendToUser(user,{type:'room.call',room_id:call.room_id,call});if(event&&broadcastToRoom){const message=getMessage?await getMessage(event.id):event;broadcastToRoom(call.room_id,{type:'message.new',message});}if(event)await notifyRoomListUpdateForRoom?.(call.room_id);}}
 const history={
  async start(call){await pool.query(`INSERT INTO call_history(id,room_id,caller_id,callee_id,is_video,started_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO NOTHING`,[call.id,call.room_id,call.from,call.to,!!call.video,date(call.started_at)]);},
  async answer(call){await pool.query(`UPDATE call_history SET status='active',answered_at=$2 WHERE id=$1 AND ended_at IS NULL`,[call.id,date(call.answered_at)]);},
  async save(call){
   const reason=['ended','declined','rejected','busy','timeout','shutdown','replaced','not_allowed','session_revoked','failed','cancelled','network_error'].includes(call.reason)?call.reason:'ended';
   const {rows}=await pool.query(`WITH ended AS (UPDATE call_history SET status=$2,reason=$3,ended_at=$4,answered_at=$5,duration_ms=$6 WHERE id=$1 AND ended_at IS NULL RETURNING *), event AS (${eventSql}) SELECT ended.*,to_jsonb(event) AS event FROM ended LEFT JOIN event ON ended.id=event.id`,[call.id,call.status,reason,date(call.ended_at),date(call.answered_at),call.duration_ms]);
   await announce(rows);
  },
  // On API restart no in-memory peer lease survives; preserve interrupted outcomes.
  async recover(){const {rows}=await pool.query(`WITH ended AS (UPDATE call_history SET status=CASE WHEN answered_at IS NULL THEN 'missed' ELSE 'interrupted' END,reason='server_restart',ended_at=now(),duration_ms=CASE WHEN answered_at IS NULL THEN 0 ELSE GREATEST(0,EXTRACT(EPOCH FROM (now()-answered_at))*1000)::bigint END WHERE ended_at IS NULL RETURNING *),event AS (${eventSql}) SELECT ended.*,to_jsonb(event) AS event FROM ended LEFT JOIN event ON ended.id=event.id`);await announce(rows);},
 };
 app.get('/calls/history',async(req,reply)=>{
  const me=await requireAuth(req),limit=Math.min(100,Math.max(1,Number(req.query?.limit)||40));
  const before=req.query?.before?new Date(req.query.before):null;if(before&&!Number.isFinite(before.getTime()))return reply.code(400).send({error:'invalid_cursor'});
  if(req.query?.before_id&&!isUuid(req.query.before_id))return reply.code(400).send({error:'invalid_cursor'});
  const room=String(req.query?.room_id||'');if(room&&!isUuid(room))return reply.code(400).send({error:'invalid_room'});
  const {rows}=await pool.query(`SELECT * FROM call_history WHERE (caller_id=$1 OR callee_id=$1) AND ($2::uuid IS NULL OR room_id=$2) AND ($3::timestamptz IS NULL OR (started_at,id)<($3,$4::uuid)) ORDER BY started_at DESC,id DESC LIMIT $5`,[me.id,room||null,before?.toISOString()||null,req.query?.before_id||'ffffffff-ffff-ffff-ffff-ffffffffffff',limit]);
  return {items:rows,next:rows.length===limit?{before:rows.at(-1).started_at,before_id:rows.at(-1).id}:null};
 });
 app.post('/rooms/:id/calls/join',{config:{rateLimit:{max:20,timeWindow:'1 minute'}}},async(req,reply)=>{
  const me=await requireAuth(req),room=String(req.params.id);if(!isUuid(room))return reply.code(400).send({error:'invalid_room'});
  const {rows}=await pool.query(`SELECT r.kind FROM rooms r JOIN room_members m ON m.room_id=r.id WHERE r.id=$1 AND m.user_id=$2`,[room,me.id]);
  if(!rows.length)return reply.code(403).send({error:'not_member'});
  if(rows[0].kind!=='group')return reply.code(400).send({error:'group_required'});
  if(!sfu)return reply.code(503).send({error:'group_calls_unavailable'});
  let leaseId;
  try{leaseId=relay?await relay.reserveGroup(me.id,()=>occupancy.acquire(me.id,room,me.sessionId)):await occupancy.acquire(me.id,room,me.sessionId);}catch(error){if(error.statusCode===409)return reply.code(409).send({error:'busy'});return reply.code(503).send({error:'group_calls_unavailable'});}
  try{
   const name='67chat-'+room;
   await sfu.createRoom({name,maxParticipants:6,emptyTimeout:60,departureTimeout:20});
   // Creation/network I/O may have outlived membership/session revocation.
   // Lock the current lease and membership until token signing commits; a
   // revocation that already completed must never produce a fresh grant.
   const db=typeof pool.connect==='function'?await pool.connect():pool;let token;
   try{
    await db.query('BEGIN');
    const valid=await db.query(`SELECT l.lease_id FROM group_call_leases l JOIN room_members m ON m.user_id=l.user_id AND m.room_id=l.room_id JOIN rooms r ON r.id=m.room_id WHERE l.user_id=$1 AND l.room_id=$2 AND l.lease_id=$3 AND l.expires_at>now() AND l.revoking_at IS NULL AND r.kind='group' FOR UPDATE OF l,m,r`,[me.id,room,leaseId]);
    if(!valid.rows.length)throw Object.assign(new Error('not_member'),{statusCode:403});
    await requireAuth(req);
    if(me.sessionId){const session=await db.query(`SELECT id FROM sessions WHERE id=$1 AND user_id=$2 FOR SHARE`,[me.sessionId,me.id]);if(!session.rows.length)throw Object.assign(new Error('session_revoked'),{statusCode:401});}
    token=await sfu.token({identity:String(me.id),room:name,ttl:90,leaseId,name:me.display_name||me.username||undefined});
    await db.query('COMMIT');
   }catch(error){await db.query('ROLLBACK');throw error;}finally{db.release?.();}
   return {token,url:sfu.url,room:name,lease_id:leaseId,max_participants:6,expires_in:90,heartbeat_interval:30};
  }catch(error){await coordinate(()=>occupancy.end(me.id,room,leaseId)).catch(()=>{});if([401,403].includes(error.statusCode))return reply.code(error.statusCode).send({error:error.message});app.log?.warn({error:error.message},'LiveKit join failed');return reply.code(503).send({error:'group_calls_unavailable'});}
 });
 app.post('/rooms/:id/calls/heartbeat',async(req,reply)=>{
  const me=await requireAuth(req),room=String(req.params.id),leaseId=req.body?.lease_id;
  if(!isUuid(me.sessionId))return reply.code(401).send({error:'session_required'});
  if(!isUuid(room)||!isUuid(leaseId))return reply.code(400).send({error:'invalid_call_lease'});
  const {rows}=await pool.query(`UPDATE group_call_leases SET expires_at=now()+interval '120 seconds' WHERE user_id=$1 AND room_id=$2 AND lease_id=$3 AND session_id=$4 AND revoking_at IS NULL AND expires_at>now() AND EXISTS(SELECT 1 FROM room_members m WHERE m.user_id=$1 AND m.room_id=$2) RETURNING user_id`,[me.id,room,leaseId,me.sessionId]);
  if(!rows.length)return reply.code(409).send({error:'call_lease_expired'});return {ok:true};
 });
 app.post('/rooms/:id/calls/leave',async(req,reply)=>{
  const me=await requireAuth(req),room=String(req.params.id),leaseId=req.body?.lease_id;
  if(!isUuid(me.sessionId))return reply.code(401).send({error:'session_required'});
  if(!isUuid(room)||!isUuid(leaseId))return reply.code(400).send({error:'invalid_call_lease'});
  try{
   const ended=await coordinate(async()=>{
    const {rows}=await pool.query(`SELECT 1 FROM group_call_leases WHERE user_id=$1 AND room_id=$2 AND lease_id=$3 AND session_id=$4 AND revoking_at IS NULL`,[me.id,room,leaseId,me.sessionId]);
    if(!rows.length)return false;await occupancy.end(me.id,room,leaseId);return true;
   });
   if(!ended)return reply.code(409).send({error:'call_lease_expired'});return {ok:true};
  }catch{return reply.code(503).send({error:'group_calls_unavailable'});}
 });
 return {history,setRelay(value){relay=value;relay.setExternalCalls(occupancy);},async revokeMembership(room,user){await coordinate(()=>occupancy.end(user,room));},capabilities:{call_history:true,call_waiting:true,group_calls:!!sfu,group_call_limit:6}};
}
