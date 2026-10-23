import { randomUUID, timingSafeEqual } from 'node:crypto';

export function installCalls(app, {pool, requireAuth, sendToUser,sendToSession,sendToUserExceptSession,sendToUserExceptEndpoint, push, now=Date.now, history,canCall=async()=>true}) {
  if(typeof sendToSession!=='function'||typeof sendToUserExceptSession!=='function')throw new TypeError('Call signaling requires authenticated session delivery');
  const calls=new Map(), rooms=new Map(),earlyIce=new Map(),busy=new Map();
  let externalCalls=null;
  const isBusy=async user=>busy.has(String(user))||!!await externalCalls?.isBusy(String(user));
  let queue=Promise.resolve();
  const serial=fn=>{const next=queue.then(fn);queue=next.catch(error=>app.log?.error(error));return next;};
  const cleanup=setInterval(()=>{void serial(expire);},5000);cleanup.unref?.();
  app.addHook('onClose',async()=>clearInterval(cleanup));
  const publicCall=call=>({type:'call.offer',call_id:call.id,room_id:call.room_id,from_user_id:call.from,from_username:call.name,is_video:call.video,offer:call.offer,sdp:call.offer,expires_at:call.expires,candidates:call.candidates});
  const accepted=(call,endpoint,phase)=>{const event={type:'call.accepted',call_id:call.id,room_id:call.room_id,phase};if(endpoint.exact)sendToUserExceptEndpoint(call.to,endpoint.id,event);else sendToUserExceptSession(call.to,endpoint.session,event);};
  async function end(call,by,reason='ended') {
    if(!calls.has(call.id))return;
    // Save before releasing the lease: failed persistence must not silently lose history.
    const status=call.answered_at!=null?'completed':(['declined','rejected'].includes(reason)?'rejected':reason==='busy'?'busy':reason==='timeout'?'missed':reason==='shutdown'?'interrupted':'cancelled');
    await history?.save({...call,status,reason,ended_at:now(),duration_ms:call.answered_at==null?0:Math.max(0,now()-call.answered_at)});
    calls.delete(call.id);if(rooms.get(call.room_id)===call.id)rooms.delete(call.room_id);
    for(const user of [call.from,call.to])if(busy.get(user)===call.id)busy.delete(user);
    for(const user of [call.from,call.to]) {sendToUser(user,{type:'call.hangup',call_id:call.id,room_id:call.room_id,from_user_id:by,reason});void push(user,'','',{call_event:'cancel',call_id:call.id,room_id:call.room_id});}
  }
  async function expire(){for(const call of calls.values()){if([call.from_endpoint,call.to_endpoint].some(endpoint=>endpoint?.reconnect_until&&endpoint.reconnect_until<now()))await end(call,call.from,'network_error');else if(call.expires<now())await end(call,call.to,'timeout');}for(const [key,item] of earlyIce)if(item.at<now()-10000)earlyIce.delete(key);}
  app.get('/calls/:id',async(req,reply)=>{
    const me=await requireAuth(req),call=calls.get(String(req.params.id));
    if(!call || call.expires<now() || call.state!=='ringing' || call.to!==String(me.id))return reply.code(404).send({error:'call_expired'});
    return publicCall(call);
  });
  // A short-lived, single-call capability permits declining while the JS app is stopped.
  app.post('/calls/:id/decline',{config:{rateLimit:{max:30,timeWindow:'1 minute'}}},async(req,reply)=>{
    const declined=await serial(async()=>{const call=calls.get(String(req.params.id)),nonce=String(req.body?.nonce||'');
      if(!call || call.state!=='ringing' || call.expires<now() || Buffer.byteLength(nonce)!==Buffer.byteLength(call.nonce) || !timingSafeEqual(Buffer.from(nonce),Buffer.from(call.nonce)))return false;
      await end(call,call.to,'declined');return true;
    });
    return declined?{ok:true}:reply.code(404).send({error:'call_expired'});
  });
  app.addHook('onClose',async()=>{await serial(async()=>{for(const call of [...calls.values()])await end(call,call.from,'shutdown');});});
  function relay(msg,me,transport) {return serial(async()=>{
    await expire();
    const owner=String(me.sessionId||'');if(!owner)return;
    const endpoint={id:transport?.id||'session:'+owner,session:owner,exact:!!transport,send:transport?.send||((data)=>sendToSession(me.id,owner,data))};
    if(transport&&(typeof transport.id!=='string'||typeof transport.send!=='function'||typeof sendToUserExceptEndpoint!=='function'))return;
    let room=String(msg.room_id||msg.toRoom||msg.to_room_id||'');
    if(!room)return;
    const {rows}=await pool.query(`SELECT r.kind,rm.user_id FROM rooms r JOIN room_members rm ON rm.room_id=r.id WHERE r.id=$1`,[room]);
    const members=rows.map(r=>String(r.user_id));
    if(rows[0]?.kind!=='dm' || members.length!==2 || !members.includes(String(me.id)))return;
    const to=members.find(id=>id!==String(me.id));let call=calls.get(rooms.get(room));
    if(msg.type==='call.resume'){
      if(!call||msg.call_id!==call.id)return;const role=String(me.id)===call.from?'from':'to',selected=call[role+'_endpoint'],token=String(msg.resume_token||'');
      if(!selected||selected.session!==owner||Buffer.byteLength(token)!==Buffer.byteLength(call[role+'_resume'])||!timingSafeEqual(Buffer.from(token),Buffer.from(call[role+'_resume'])))return;
      if(!await canCall(String(me.id),to)){await end(call,String(me.id),'not_allowed');return;}
      call[role+'_endpoint']=endpoint;endpoint.send({type:'call.resumed',call_id:call.id,room_id:room,state:call.state});
      call[(role==='from'?'to':'from')+'_endpoint']?.send({type:'call.peer_reconnected',call_id:call.id,room_id:room});return;
    }
    // Sibling sessions may see a ringing offer, but never control a selected peer.
    if(call&&msg.type!=='call.offer'){
      const selected=String(me.id)===call.from?call.from_endpoint:call.to_endpoint;
      if(selected&&(selected.session!==owner||selected.id!==endpoint.id)){if(msg.type==='call.answer')endpoint.send({type:'call.accepted',call_id:call.id,room_id:room,phase:'answered'});return;}
    }
    if(msg.type!=='call.hangup' && !await canCall(String(me.id),to)){if(call)await end(call,String(me.id),'not_allowed');else endpoint.send({type:'call.hangup',room_id:room,reason:'not_allowed'});return;}
    const earlyKey=room+':'+me.id+':'+endpoint.id;
    if(!call && msg.type==='call.ice') {
      if(earlyIce.size>1000)earlyIce.delete(earlyIce.keys().next().value);
      const pending=earlyIce.get(earlyKey)||{at:now(),items:[]};if(pending.items.length<128)pending.items.push(msg.candidate||msg.ice);earlyIce.set(earlyKey,pending);return;
    }
    if(msg.type==='call.offer') {
      const offer=msg.offer||msg.sdp;
      if(!offer || typeof offer.sdp!=='string' || offer.sdp.length>100000)return;
      if(call && call.expires>now() || await isBusy(String(me.id))) {
        const attempt={id:randomUUID(),room_id:room,from:String(me.id),to,video:!!msg.is_video,started_at:now(),ended_at:now(),status:'busy',reason:'busy',duration_ms:0};
        await history?.start?.(attempt);await history?.save(attempt);endpoint.send({type:'call.created',call_id:attempt.id,room_id:room,status:'busy'});endpoint.send({type:'call.hangup',call_id:attempt.id,room_id:room,reason:'busy'});return;
      }
      call={id:randomUUID(),room_id:room,from:String(me.id),from_endpoint:endpoint,from_resume:randomUUID(),to,to_endpoint:null,to_resume:randomUUID(),name:String(me.username||'Абонент'),video:!!msg.is_video,offer,started_at:now(),expires:now()+45000,nonce:randomUUID(),state:await isBusy(to)?'waiting':'ringing',candidates:[]};
      await history?.start?.(call);
      const pending=earlyIce.get(earlyKey);if(pending && pending.at>now()-10000)call.candidates=pending.items;earlyIce.delete(earlyKey);
      calls.set(call.id,call);rooms.set(room,call.id);busy.set(call.from,call.id);if(call.state==='ringing')busy.set(to,call.id);
      endpoint.send({type:'call.created',call_id:call.id,room_id:room,resume_token:call.from_resume});
      if(call.state==='waiting'){sendToUser(to,{...publicCall(call),type:'call.waiting'});endpoint.send({type:'call.status',call_id:call.id,room_id:room,status:'waiting'});return;}
      sendToUser(to,publicCall(call));
      void push(to,'','',{call_event:'ring',call_id:call.id,room_id:room,caller:call.name,is_video:call.video,expires_at:call.expires,decline_nonce:call.nonce});return;
    }
    if(!call || call.expires<now() || (msg.call_id && msg.call_id!==call.id))return;
    if(msg.type==='call.waiting.reject'){if(String(me.id)===call.to && call.state==='waiting')await end(call,call.to,'busy');return;}
    if(msg.type==='call.waiting.accept'){
      if(String(me.id)!==call.to || call.state!=='waiting')return;
      const previous=calls.get(busy.get(call.to));if(previous)await end(previous,call.to,'replaced');
      await externalCalls?.end(call.to);
      call.state='ringing';call.to_endpoint=endpoint;busy.set(call.to,call.id);accepted(call,endpoint,'selected');endpoint.send({...publicCall(call),resume_token:call.to_resume});return;
    }
    if(msg.type==='call.hangup')return end(call,String(me.id),String(msg.reason||'ended'));
    if(msg.type==='call.answer') {if(String(me.id)!==call.to || call.state!=='ringing')return;call.to_endpoint=endpoint;call.state='active';call.nonce='';call.answered_at=now();call.expires=now()+24*3600000;await history?.answer?.(call);accepted(call,endpoint,'answered');endpoint.send({type:'call.answered',call_id:call.id,room_id:room,resume_token:call.to_resume});void push(call.to,'','',{call_event:'cancel',call_id:call.id,room_id:room});}
    if(!['call.answer','call.ice','call.renegotiate','call.renegotiate.answer'].includes(msg.type))return;
    if(msg.type.startsWith('call.renegotiate') && call.state!=='active')return;
    if(msg.type==='call.ice' && call.state!=='active' && String(me.id)===call.to){const pending=earlyIce.get(earlyKey)||{at:now(),items:[]};if(pending.items.length<128)pending.items.push(msg.candidate||msg.ice);earlyIce.set(earlyKey,pending);return;}
    if(msg.type==='call.ice' && call.state==='ringing' && String(me.id)===call.from && call.candidates.length<128)call.candidates.push(msg.candidate||msg.ice);
    const payload={...msg,room_id:room,call_id:call.id,from_user_id:me.id,from_username:me.username},recipientEndpoint=to===call.from?call.from_endpoint:call.to_endpoint;
    if(recipientEndpoint)recipientEndpoint.send(payload);else sendToUser(to,payload);
    if(msg.type==='call.answer'){const pending=earlyIce.get(earlyKey);if(pending?.at>now()-10000)for(const candidate of pending.items)call.from_endpoint.send({type:'call.ice',room_id:room,call_id:call.id,from_user_id:call.to,candidate});for(const key of earlyIce.keys())if(key.startsWith(room+':'+call.to+':'))earlyIce.delete(key);}
  });};
  relay.revokeUser=(user,{onlySessionId=null,exceptSessionId=null}={})=>serial(async()=>{
    if(onlySessionId||exceptSessionId)await externalCalls?.endSessions?.(String(user),{onlySessionId,exceptSessionId});else await externalCalls?.end(String(user));
    const matches=endpoint=>onlySessionId?endpoint?.session===onlySessionId:exceptSessionId?endpoint?.session!==exceptSessionId:true;
    for(const call of [...calls.values()]){const endpoint=call.from===String(user)?call.from_endpoint:call.to===String(user)?call.to_endpoint:undefined;if(endpoint!==undefined&&matches(endpoint))await end(call,String(user),'session_revoked');}
    for(const key of earlyIce.keys())if(key.includes(':'+user+':'))earlyIce.delete(key);
  });
  relay.revokeSession=(user,session)=>relay.revokeUser(user,{onlySessionId:session});
  relay.endpointClosed=id=>serial(async()=>{for(const call of calls.values())for(const role of ['from','to'])if(call[role+'_endpoint']?.id===id)call[role+'_endpoint'].reconnect_until=now()+30000;});
  relay.isBusy=isBusy;
  relay.setExternalCalls=value=>{externalCalls=value;};
  relay.coordinate=fn=>serial(fn);
  // Serialize SFU identity acquisition with P2P offers/answers, including other devices.
  relay.reserveGroup=(user,acquire)=>serial(async()=>{await expire();if(busy.has(String(user)))throw Object.assign(new Error('busy'),{statusCode:409});return acquire();});
  return relay;
}
