import { randomUUID, timingSafeEqual } from 'node:crypto';

export function installCalls(app, {pool, requireAuth, sendToUser, push, now=Date.now}) {
  const calls=new Map(), rooms=new Map(),earlyIce=new Map();
  const cleanup=setInterval(()=>{for(const [id,call] of calls)if(call.expires<now()) {calls.delete(id);if(rooms.get(call.room_id)===id)rooms.delete(call.room_id);}},15000);cleanup.unref?.();
  app.addHook('onClose',async()=>clearInterval(cleanup));
  const publicCall=call=>({type:'call.offer',call_id:call.id,room_id:call.room_id,from_user_id:call.from,from_username:call.name,is_video:call.video,offer:call.offer,sdp:call.offer,expires_at:call.expires,candidates:call.candidates});
  async function end(call,by,reason='ended') {
    calls.delete(call.id);if(rooms.get(call.room_id)===call.id)rooms.delete(call.room_id);
    for(const user of [call.from,call.to]) {sendToUser(user,{type:'call.hangup',call_id:call.id,room_id:call.room_id,from_user_id:by,reason});void push(user,'','',{call_event:'cancel',call_id:call.id,room_id:call.room_id});}
  }
  app.get('/calls/:id',async(req,reply)=>{
    const me=await requireAuth(req),call=calls.get(String(req.params.id));
    if(!call || call.expires<now() || call.state!=='ringing' || call.to!==String(me.id))return reply.code(404).send({error:'call_expired'});
    return publicCall(call);
  });
  // A short-lived, single-call capability permits declining while the JS app is stopped.
  app.post('/calls/:id/decline',{config:{rateLimit:{max:30,timeWindow:'1 minute'}}},async(req,reply)=>{
    const call=calls.get(String(req.params.id)),nonce=String(req.body?.nonce||'');
    if(!call || call.expires<now() || nonce.length!==call.nonce.length || !timingSafeEqual(Buffer.from(nonce),Buffer.from(call.nonce)))return reply.code(404).send({error:'call_expired'});
    await end(call,call.to,'declined');return {ok:true};
  });
  return async function relay(msg,me) {
    let room=String(msg.room_id||msg.toRoom||msg.to_room_id||'');
    if(!room)return;
    const {rows}=await pool.query(`SELECT r.kind,rm.user_id FROM rooms r JOIN room_members rm ON rm.room_id=r.id WHERE r.id=$1`,[room]);
    const members=rows.map(r=>String(r.user_id));
    if(rows[0]?.kind!=='dm' || members.length!==2 || !members.includes(String(me.id)))return;
    const to=members.find(id=>id!==String(me.id));let call=calls.get(rooms.get(room));
    const earlyKey=room+':'+me.id;
    if(!call && msg.type==='call.ice') {
      if(earlyIce.size>1000)earlyIce.delete(earlyIce.keys().next().value);
      const pending=earlyIce.get(earlyKey)||{at:now(),items:[]};if(pending.items.length<128)pending.items.push(msg.candidate||msg.ice);earlyIce.set(earlyKey,pending);return;
    }
    if(msg.type==='call.offer') {
      const offer=msg.offer||msg.sdp;
      if(!offer || typeof offer.sdp!=='string' || offer.sdp.length>100000)return;
      if(call && call.expires>now()) {sendToUser(me.id,{type:'call.hangup',room_id:room,reason:'busy'});return;}
      call={id:randomUUID(),room_id:room,from:String(me.id),to,name:String(me.username||'Абонент'),video:!!msg.is_video,offer,expires:now()+45000,nonce:randomUUID(),state:'ringing',candidates:[]};
      const pending=earlyIce.get(earlyKey);if(pending && pending.at>now()-10000)call.candidates=pending.items;earlyIce.delete(earlyKey);
      calls.set(call.id,call);rooms.set(room,call.id);
      sendToUser(me.id,{type:'call.created',call_id:call.id,room_id:room});sendToUser(to,publicCall(call));
      void push(to,'','',{call_event:'ring',call_id:call.id,room_id:room,caller:call.name,is_video:call.video,expires_at:call.expires,decline_nonce:call.nonce});return;
    }
    if(!call || call.expires<now() || (msg.call_id && msg.call_id!==call.id))return;
    if(msg.type==='call.hangup')return end(call,String(me.id),String(msg.reason||'ended'));
    if(msg.type==='call.answer') {if(String(me.id)!==call.to || call.state!=='ringing')return;call.state='active';call.expires=now()+24*3600000;void push(call.to,'','',{call_event:'cancel',call_id:call.id,room_id:room});}
    if(msg.type==='call.ice' && call.state==='ringing' && String(me.id)===call.from && call.candidates.length<128)call.candidates.push(msg.candidate||msg.ice);
    sendToUser(to,{...msg,room_id:room,call_id:call.id,from_user_id:me.id,from_username:me.username});
  };
}
