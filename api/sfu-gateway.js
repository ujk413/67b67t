import {createHmac,timingSafeEqual} from 'node:crypto';
import WebSocket from 'ws';
const UUID='[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
const identityPattern=new RegExp(`^(${UUID}):(${UUID})$`,'i'),roomPattern=new RegExp(`^67chat-(${UUID})$`,'i');
const paths=new Set(['/rtc','/rtc/v1','/rtc/validate','/rtc/v1/validate']);
const denied=()=>Object.assign(Error('sfu_access_denied'),{statusCode:401});
export function verifySfuGrant(token,{key,secret,now=Date.now()}){
 if(typeof token!=='string'||token.length>16384||!key||!secret)throw denied();
 const parts=token.split('.');if(parts.length!==3||parts.some(p=>!p||!/^[A-Za-z0-9_-]+$/.test(p)))throw denied();
 const expected=createHmac('sha256',secret).update(parts[0]+'.'+parts[1]).digest(),signature=Buffer.from(parts[2],'base64url');
 if(signature.length!==expected.length||!timingSafeEqual(signature,expected))throw denied();
 let header,claims;try{header=JSON.parse(Buffer.from(parts[0],'base64url'));claims=JSON.parse(Buffer.from(parts[1],'base64url'));}catch{throw denied();}
 const identity=identityPattern.exec(claims.sub),room=roomPattern.exec(claims.video?.room),seconds=Math.floor(now/1000);
 if(header.alg!=='HS256'||claims.iss!==key||!identity||!room||!Number.isFinite(claims.exp)||claims.exp<=seconds||!Number.isFinite(claims.nbf)||claims.nbf>seconds+5||claims.video.roomJoin!==true||claims.video.roomAdmin||claims.video.roomCreate)throw denied();
 return{userId:identity[1],leaseId:identity[2],roomId:room[1],identity:claims.sub};
}
/** Sole public signaling ingress. ICE ports may be public; raw LiveKit HTTP/WS may not. */
export function installSfuGateway(app,{pool,key,secret,upstream,enabled=false,connect=(url,token)=>new WebSocket(url,{headers:{Authorization:'Bearer '+token},maxPayload:1048576,followRedirects:false,handshakeTimeout:10000}),fetchImpl=fetch}={}){
 const connections=new Set();let origin=null;
 if(enabled){origin=new URL(upstream);if(!['http:','https:'].includes(origin.protocol)||origin.username||origin.password||origin.search||origin.hash||origin.pathname!=='/')throw Error('invalid_sfu_upstream');}
 const rawToken=req=>req.query?.access_token||String(req.headers.authorization||'').replace(/^Bearer /i,'');
 async function authorize(grant){
  const {rows}=await pool.query(`SELECT l.lease_id FROM group_call_leases l
   JOIN room_members m ON m.user_id=l.user_id AND m.room_id=l.room_id
   JOIN sessions s ON s.id=l.session_id AND s.user_id=l.user_id
   JOIN users u ON u.id=l.user_id JOIN rooms r ON r.id=l.room_id
   WHERE l.user_id=$1 AND l.room_id=$2 AND l.lease_id=$3 AND l.expires_at>now() AND l.revoking_at IS NULL
   AND s.expires_at>now() AND NOT COALESCE(u.banned,false) AND u.login_disabled_at IS NULL AND r.kind='group'`,[grant.userId,grant.roomId,grant.leaseId]);
  if(!rows.length)throw denied();return grant;
 }
 function parse(req){if(!enabled)throw Object.assign(Error('group_calls_unavailable'),{statusCode:503});return verifySfuGrant(rawToken(req),{key,secret});}
 function destination(req,websocket=false){
  const parsed=new URL(req.raw.url,'http://local'),relative=parsed.pathname.slice('/livekit'.length);
  if(!paths.has(relative))throw denied();const target=new URL(relative,origin);target.search=parsed.search;
  if(websocket)target.protocol=target.protocol==='https:'?'wss:':'ws:';return target;
 }
 function shutdown(entry){if(entry.closed)return;entry.closed=true;connections.delete(entry);try{entry.client.close(4003,'Call access ended');}catch{}try{entry.upstream?.close();}catch{}entry.queued=[];}
 async function http(req,reply){
  const grant=await authorize(parse(req)),target=destination(req);
  const response=await fetchImpl(target,{redirect:'manual',headers:{Authorization:'Bearer '+rawToken(req)},signal:AbortSignal.timeout(10000)});
  await authorize(grant);if(response.status>=300&&response.status<400)return reply.code(502).send({error:'sfu_redirect_forbidden'});
  const data=new Uint8Array(await response.arrayBuffer());if(data.byteLength>1048576)throw Object.assign(Error('sfu_response_too_large'),{statusCode:502});
  return reply.code(response.status).header('Cache-Control','no-store').type(response.headers.get('content-type')||'application/octet-stream').send(Buffer.from(data));
 }
 function websocket(client,req){
  const entry={client,upstream:null,closed:false,ready:false,queued:[],queuedBytes:0,incoming:[],incomingBytes:0,grant:null};connections.add(entry);
  // Attach synchronously: no early client frame can be lost or forwarded before authorization.
  client.on('message',(data,isBinary)=>{if(entry.closed)return;if(data.length>1048576||entry.queuedBytes+data.length>1048576){shutdown(entry);return;}if(entry.ready){if(entry.upstream.bufferedAmount>1048576){shutdown(entry);return;}entry.upstream.send(data,{binary:isBinary});}else{entry.queued.push([data,isBinary]);entry.queuedBytes+=data.length;}});
  client.on('close',()=>shutdown(entry));client.on('error',()=>shutdown(entry));
  void(async()=>{
   entry.grant=parse(req);await authorize(entry.grant);if(entry.closed)return;
   const peer=connect(destination(req,true),rawToken(req));entry.upstream=peer;
   peer.on('error',()=>shutdown(entry));peer.on('close',()=>shutdown(entry));
   peer.on('message',(data,isBinary)=>{if(entry.closed)return;if(!entry.ready){if(entry.incomingBytes+data.length>1048576){shutdown(entry);return;}entry.incoming.push([data,isBinary]);entry.incomingBytes+=data.length;return;}if(client.bufferedAmount>1048576){shutdown(entry);return;}client.send(data,{binary:isBinary});});
   peer.on('open',()=>void(async()=>{await authorize(entry.grant);if(entry.closed)return;entry.ready=true;for(const [data,binary]of entry.incoming)client.send(data,{binary});entry.incoming=[];entry.incomingBytes=0;for(const [data,binary]of entry.queued)peer.send(data,{binary});entry.queued=[];entry.queuedBytes=0;})().catch(()=>shutdown(entry)));
  })().catch(()=>shutdown(entry));
 }
 for(const path of paths)app.route({method:'GET',url:'/livekit'+path,logLevel:'silent',handler:http,...(!path.endsWith('/validate')?{wsHandler:websocket}:{})});
 const timer=setInterval(()=>{for(const entry of connections)if(entry.grant)void authorize(entry.grant).catch(()=>shutdown(entry));},10000);timer.unref?.();
 app.addHook('onClose',async()=>{clearInterval(timer);for(const entry of [...connections])shutdown(entry);});
 return{enabled,closeLease(userId,leaseId){for(const entry of [...connections])if(entry.grant?.userId===userId&&(!leaseId||entry.grant.leaseId===leaseId))shutdown(entry);}};
}
