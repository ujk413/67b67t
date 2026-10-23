import {html as escapeUi} from './ui.js';
import {t as uiText} from "./i18n.js";
import {html} from './ui.js';
import {mediaDetails} from './media-details.js';
const playing=new Set(),rates=new Map(),players=new Map();let current=null,generation=0;
export function stopVoicePlayers(){++generation;for(const [audio,node]of players){audio.pause();if(!node.isConnected)players.delete(audio);}current=null;playing.clear();}
export function renderVoicePlayer(message,url='') {
 const waveform=mediaDetails(message._mediaDetails).waveform || Array(30).fill(0.1),peak=Math.max(0.001,...waveform);
 const bars=waveform.map(value=>`<i style="height:${Math.max(3,Math.round(Math.sqrt(value/peak)*26))}px"></i>`).join('');
 const listened=(message.listened_by || []).some(id=>id!==message.sender_id);
 return `<div class="voice-note" data-voice-id="${html(message.id)}"><button type="button" class="voice-toggle" aria-label="${playing.has(message.id)?uiText("Приостановить"):uiText("Воспроизвести")} голосовое"><img src="/icons/${playing.has(message.id)?'pause':'play'}@2x.png" alt="" width="24" height="24"></button><button type="button" class="voice-wave" aria-label="${escapeUi(uiText("Перемотка голосового"))}">${bars}</button><button type="button" class="voice-speed" aria-label="${escapeUi(uiText("Скорость голосового"))}">${rates.get(message.id) || 1}×</button><audio preload="none" ${url?`src="${html(url)}"`:''}></audio><small class="voice-status">${listened?uiText("Прослушано"):''}</small></div>`;
}
export function bindVoicePlayers(box,{findMessage,source,listened,report}) {
 for(const [audio,node]of players)if(!node.isConnected){audio.pause();players.delete(audio);if(current===audio){current=null;++generation;}}
 for(const node of box.querySelectorAll('[data-voice-id]')) {
  if(node.dataset.bound)continue;node.dataset.bound='yes';
  const id=node.dataset.voiceId,audio=node.querySelector('audio'),toggle=node.querySelector('.voice-toggle'),wave=node.querySelector('.voice-wave'),speed=node.querySelector('.voice-speed'),status=node.querySelector('.voice-status');let busy=false,reported=false,lease=0;players.set(audio,node);
  const label=()=>{const img=toggle.querySelector('img');if(img)img.src='/icons/'+(audio.paused?'play':'pause')+'@2x.png';toggle.setAttribute('aria-label',audio.paused?uiText("Воспроизвести голосовое"):uiText("Приостановить голосовое"));};
  toggle.onclick=async()=>{
   if(busy)return;busy=true;
   try {if(!audio.paused){audio.pause();return;}
    stopVoicePlayers();lease=generation;
    if(!audio.getAttribute('src')){const message=findMessage(id);if(!message)return;const resolved=await source(message);if(!node.isConnected||lease!==generation)return;audio.src=resolved.url;}
    if(lease!==generation||!node.isConnected)return;
    audio.playbackRate=rates.get(id) || 1;if(audio.ended)audio.currentTime=0;current=audio;await audio.play();if(lease!==generation||!node.isConnected)audio.pause();
   }catch(error){if(lease===generation&&node.isConnected)report(error);}finally{busy=false;label();}
  };
  audio.onplay=()=>{if(lease!==generation||!node.isConnected){audio.pause();return;}playing.add(id);label();};audio.onpause=()=>{playing.delete(id);label();};audio.onended=()=>{playing.delete(id);label();};
  audio.ontimeupdate=()=>{
   const progress=audio.duration>0?audio.currentTime/audio.duration:0;
   [...wave.children].forEach((bar,index)=>bar.classList.toggle('played',index/wave.children.length<=progress));
   if(!audio.paused && audio.currentTime>=0.25 && !reported){reported=true;void listened(findMessage(id)).catch(error=>report(error));}
  };
  wave.onclick=event=>{if(audio.duration>0){const rect=wave.getBoundingClientRect();audio.currentTime=Math.max(0,Math.min(audio.duration,(event.clientX-rect.left)/rect.width*audio.duration));}};
  speed.onclick=()=>{const value=rates.get(id) || 1,next=value===1?1.5:value===1.5?2:1;rates.set(id,next);audio.playbackRate=next;speed.textContent=next+'×';};
 }
}
