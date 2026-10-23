import {userError} from './errors.js';
import {locale as uiLocale} from "./i18n.js";
import {t as uiText} from "./i18n.js";
// All media/signaling stays on the configured self-host; the SDK is bundled locally.
export class BrowserGroupCall {
 constructor({request,loadSDK=()=>import('/vendor/livekit-client.esm.mjs'),onState=()=>{},onTrack=()=>{},onTrackRemoved=()=>{}}){Object.assign(this,{request,loadSDK,onState,onTrack,onTrackRemoved});this.room=null;this.roomId=null;this.leaseId=null;this.heartbeat=null;this.state='disconnected';this.generation=0;}
 update(state){this.state=state;this.onState(state);}
 detach(){clearInterval(this.heartbeat);this.heartbeat=null;const old={room:this.room,roomId:this.roomId,leaseId:this.leaseId};this.room=null;this.roomId=null;this.leaseId=null;return old;}
 async release({room,roomId,leaseId}){try{if(room)await room.disconnect();}catch{}finally{if(roomId)await this.request(`/rooms/${roomId}/calls/leave`,{method:'POST',body:{lease_id:leaseId}}).catch(()=>{});}}
 async join(roomId){
  const generation=++this.generation;await this.release(this.detach());if(generation!==this.generation)return;
  this.update('connecting');let grant=null,room=null;
  const current=()=>generation===this.generation&&this.room===room;
  try{
   grant=await this.request(`/rooms/${roomId}/calls/join`,{method:'POST',body:{}});
   if(generation!==this.generation){await this.release({roomId,leaseId:grant.lease_id});return;}
   this.roomId=roomId;this.leaseId=grant.lease_id;
   const SDK=await this.loadSDK();if(generation!==this.generation)return;
   room=new SDK.Room({adaptiveStream:true,dynacast:true,audioCaptureDefaults:{echoCancellation:true,noiseSuppression:true,autoGainControl:true}});this.room=room;
   room.on(SDK.RoomEvent.Disconnected,()=>{if(current())void this.close();}).on(SDK.RoomEvent.Reconnecting,()=>{if(current())this.update('reconnecting');}).on(SDK.RoomEvent.Reconnected,()=>{if(current())this.update('connected');});
   room.on(SDK.RoomEvent.TrackSubscribed,(track,publication,participant)=>{if(current())this.onTrack(track,participant);});
   room.on(SDK.RoomEvent.TrackUnsubscribed,track=>{if(current())this.onTrackRemoved(track);});
   await room.connect(grant.url,grant.token);
   if(!current()){await this.release({room,roomId,leaseId:grant.lease_id});return;}
   await room.localParticipant.setMicrophoneEnabled(true);
   if(!current()){await this.release({room,roomId,leaseId:grant.lease_id});return;}
   this.update('connected');
   this.heartbeat=setInterval(()=>{if(!current())return;void this.request(`/rooms/${roomId}/calls/heartbeat`,{method:'POST',body:{lease_id:grant.lease_id}}).catch(()=>{if(current())return this.close();});},30000);
  }catch(error){
   // A previous connect/capture may finish after another call has replaced it.
   // Its failure may release only its own room and lease, never current state.
   if(generation===this.generation){this.detach();this.update('disconnected');}
   await this.release({room,roomId:grant?roomId:null,leaseId:grant?.lease_id});throw error;
  }
 }
 async setMicrophone(enabled){if(!this.room)throw Error('Сначала подключитесь к звонку');await this.room.localParticipant.setMicrophoneEnabled(enabled);}
 async setCamera(enabled){if(!this.room)throw Error('Сначала подключитесь к звонку');await this.room.localParticipant.setCameraEnabled(enabled);}
 async setScreenShare(enabled){if(!this.room)throw Error('Сначала подключитесь к звонку');await this.room.localParticipant.setScreenShareEnabled(enabled);}
 async close(){++this.generation;const old=this.detach();this.update('disconnected');await this.release(old);}
}

/** Add media recovery/screenshare to the existing P2P instance without a new call UI. */
export function enhancePeerCall(peer,{sendOffer,onError=()=>{},isInitiator=true}={}){
 let disposed=false,restarting=false,attempts=0,timer=null,screen=null;const pc=peer.peerConnection;
 async function restart(){if(disposed||!isInitiator||restarting||!pc||pc.signalingState!=='stable')return;if(++attempts>3){peer.hangup();onError(Error('Не удалось восстановить звонок'));return;}restarting=true;try{const offer=await pc.createOffer({iceRestart:true});if(disposed)return;await pc.setLocalDescription(offer);await sendOffer(offer);}catch(e){onError(e);}finally{restarting=false;}}
 const state=()=>{if(pc.connectionState==='connected'){attempts=0;clearTimeout(timer);}else if(['failed','disconnected'].includes(pc.connectionState)){clearTimeout(timer);timer=setTimeout(restart,pc.connectionState==='failed'?0:2500);}};
 pc?.addEventListener('connectionstatechange',state);globalThis.addEventListener?.('online',restart);
 async function stopScreenShare(){const stream=screen;screen=null;if(!stream)return;try{const sender=pc?.getSenders().find(s=>s.track?.kind==='video');if(sender)await sender.replaceTrack(peer.localStream?.getVideoTracks()[0]||null);}finally{for(const track of stream.getTracks()){track.onended=null;track.stop();}}}
 return {restart,async startScreenShare(){if(disposed||screen)return;if(!navigator.mediaDevices.getDisplayMedia)throw Error('Демонстрация экрана не поддерживается браузером');const stream=await navigator.mediaDevices.getDisplayMedia({video:true,audio:false});if(disposed){stream.getTracks().forEach(t=>t.stop());return;}const sender=pc?.getSenders().find(s=>s.track?.kind==='video'),track=stream.getVideoTracks()[0];if(!sender||!track){stream.getTracks().forEach(t=>t.stop());throw Error('Демонстрация экрана доступна в видеозвонке');}try{await sender.replaceTrack(track);screen=stream;track.onended=()=>{void stopScreenShare().catch(onError);};}catch(e){stream.getTracks().forEach(t=>t.stop());throw e;}},stopScreenShare,async dispose(){disposed=true;clearTimeout(timer);pc?.removeEventListener('connectionstatechange',state);globalThis.removeEventListener?.('online',restart);await stopScreenShare();}};
}

const el=(tag,text)=>{const node=document.createElement(tag);if(text!==undefined)node.textContent=text;return node;};
const labels={ringing:'Вызов',active:'Идёт разговор',completed:'Завершён',missed:'Пропущен',busy:'Занято',rejected:'Отклонён',cancelled:'Отменён',interrupted:'Прерван'};
export async function openCallFeature(mode,c){
 const dialog=el('dialog'),content=el('section'),status=el('p'),media=el('div');
 dialog.style.cssText='width:min(720px,95vw);max-height:90vh;overflow:auto;border:1px solid var(--border,GrayText);border-radius:16px;background:var(--panel,Canvas);color:var(--text,CanvasText);padding:20px';status.setAttribute('role','status');dialog.append(el('h2',uiText("Звонки")),status,content,media);document.body.append(dialog);dialog.showModal();
 const tracks=new Map(),call=new BrowserGroupCall({request:c.request,onState:state=>{status.textContent={connecting:uiText("Подключение…"),connected:uiText("Подключено"),reconnecting:uiText("Восстановление соединения…"),disconnected:uiText("Соединение завершено")}[state];},onTrack:(track,participant)=>{const node=track.attach();node.autoplay=true;node.controls=true;node.style.cssText='width:100%;max-height:280px';if(track.kind==='video')node.playsInline=true;const wrap=el('div');wrap.append(el('p',participant.name||participant.identity),node);media.append(wrap);tracks.set(track,wrap);node.play?.().catch(e=>{status.textContent=uiText("Нажмите воспроизведение для звука");});},onTrackRemoved:track=>{track.detach();tracks.get(track)?.remove();tracks.delete(track);}});
 let closed=false;dialog.addEventListener('close',()=>{closed=true;void call.close();for(const track of tracks.keys())track.detach();dialog.remove();},{once:true});
 const run=fn=>async()=>{status.textContent='';try{await fn();}catch(e){status.textContent=e.message==='group_calls_unavailable'?uiText("Сервер групповых звонков недоступен"):userError(e);c.report?.(e);}};
 const button=(text,fn)=>{const node=el('button',text);node.type='button';node.addEventListener('click',run(fn));content.append(node);return node;};
 button(uiText("Закрыть"),async()=>dialog.close());
 if(c.room?.kind==='group'){
  button(uiText("Войти в групповой звонок · до 6 участников"),async()=>{c.pauseMedia?.();await call.join(c.room.id);if(closed)await call.close();});
  let mic=true,camera=false,screen=false;
  const micButton=button(uiText("Выключить микрофон"),async()=>{await call.setMicrophone(!mic);mic=!mic;micButton.textContent=mic?uiText("Выключить микрофон"):uiText("Включить микрофон");});
  const camButton=button(uiText("Включить камеру"),async()=>{await call.setCamera(!camera);camera=!camera;camButton.textContent=camera?uiText("Выключить камеру"):uiText("Включить камеру");});
  const screenButton=button(uiText("Демонстрация экрана"),async()=>{await call.setScreenShare(!screen);screen=!screen;screenButton.textContent=screen?uiText("Остановить демонстрацию"):uiText("Демонстрация экрана");});
  button(uiText("Выйти из звонка"),()=>call.close());content.append(el('p',uiText("Медиа защищены при передаче; собственный сервер обрабатывает потоки.")));
 }
 const list=el('div');content.append(list);let next=null;
 async function history(more=false){const q=new URLSearchParams();if(c.room?.id)q.set('room_id',c.room.id);if(more&&next){q.set('before',next.before);q.set('before_id',next.before_id);}const page=await c.request('/calls/history?'+q,{method:'GET'});if(closed)return;if(!more)list.replaceChildren();next=page.next;for(const item of page.items){const row=el('p',`${item.caller_id===c.me.id?'↗':'↙'} ${uiText(labels[item.status]||item.status)} · ${new Date(item.started_at).toLocaleString(uiLocale())}${Number(item.duration_ms)>0?' · '+Math.floor(item.duration_ms/1000)+(" "+uiText("с")+""):''}`),open=el('button',uiText("Открыть чат"));open.addEventListener('click',run(async()=>{dialog.close();await c.openRoom(item.room_id);}));row.append(open);list.append(row);}if(!page.items.length&&!more)list.append(el('p',uiText("История звонков пуста")));}
 button(uiText("Обновить историю"),()=>history());button(uiText("Ещё"),async()=>{if(next)await history(true);});await run(()=>history())();
 return {dialog,close:()=>dialog.close()};
}
