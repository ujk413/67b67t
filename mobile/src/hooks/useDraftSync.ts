import {useEffect,useRef,useState} from 'react';
import {Alert,AppState} from 'react-native';
import {DraftSync,draftValue,type DraftValue} from '../../../web/draft-sync.js';
import {request} from '../api';
import type {AccountRepository,Draft} from '../data/repository';
import type {Message} from '../types';

type Props={repository:AccountRepository|null;ready:boolean;baseUrl:string;token:string|null;userId:string|null;roomId:string|null;text:string;reply:Message|null;encode:(room:string,text:string)=>Promise<Record<string,unknown>>;decode:(message:Message)=>Promise<Message>;apply:(room:string,draft:Draft)=>void};
export function useDraftSync(p:Props){
 const latest=useRef(p),sync=useRef<DraftSync|null>(null);latest.current=p;
 const [previews,setPreviews]=useState<Record<string,string>>({});
 useEffect(()=>{
  const {repository:repo,token,userId,baseUrl}=p;if(!repo||!token||!userId||!p.ready)return;
  let alive=true;setPreviews({});const prompted=new Set<string>();
  const preview=async()=>{const values=await repo.drafts();if(alive)setPreviews(Object.fromEntries(Object.entries(values).filter(([,draft])=>draft.text||draft.reply).map(([room,draft])=>[room,draft.text||'Ответ на сообщение'])));};
  const call=<T,>(path:string,method='GET',body?:unknown)=>request<T>(baseUrl,path,{token,method,body});
  const key=(room:string)=>'draft-sync:'+room;
  const apply=async(room:string,value:DraftValue,expected?:DraftValue)=>{if(!alive)return false;let changed=false;await repo.update('draft:'+room,{text:''},current=>{if(!alive||expected&&JSON.stringify(draftValue(current))!==JSON.stringify(expected))return current;changed=true;return value;});if(changed&&alive)latest.current.apply(room,value as Draft);return changed;};
  const engine=new DraftSync({
   state:room=>repo.get(key(room),null),local:room=>repo.get('draft:'+room,{text:''}),
   fetch:room=>call('/rooms/'+room+'/draft'),
   decode:async(room,row)=>{const envelope=JSON.parse(row.encrypted_content),body=envelope.v===1?envelope.body:null;if(!body)throw Error('Неизвестный формат черновика');const decoded=await latest.current.decode({...body,id:`draft-${room}-${row.revision}`,room_id:room,sender_id:userId,type:'text',created_at:row.updated_at} as Message);if(!decoded._decrypted)throw Error('Не удалось расшифровать черновик');try{const value=JSON.parse(decoded.text||'');return value.format==='67chat-draft-v1'?value.draft:{text:decoded.text};}catch{return{text:decoded.text};}},
   encode:async(room,value)=>{const body=await latest.current.encode(room,JSON.stringify({format:'67chat-draft-v1',draft:value}));if(!body.encrypted_content && !(body.meta as {encrypted?:boolean})?.encrypted)throw Error('Черновик должен быть зашифрован');return JSON.stringify({v:1,body});},
   put:(room,body)=>call('/rooms/'+room+'/draft','PUT',body),apply,saveState:(room,state)=>repo.set(key(room),state),
   conflict:async(room,conflict)=>{
    await repo.set('draft-conflict:'+room,conflict);const mark=room+':'+conflict.revision;if(prompted.has(mark)||!alive)return;prompted.add(mark);
    Alert.alert('Два черновика',`На этом устройстве: ${conflict.local.text.slice(0,160)}\n\nНа другом: ${conflict.remote.text.slice(0,160)}`,[{text:'Решить позже',style:'cancel'},{text:'Оставить мой',onPress:()=>{if(alive)void repo.set(key(room),{revision:conflict.revision,value:conflict.remote}).then(()=>engine.sync(room)).catch(()=>{});}},{text:'Взять другой',onPress:()=>{if(!alive)return;void repo.get('draft:'+room,{text:''}).then(async current=>{if(JSON.stringify(draftValue(current))!==JSON.stringify(conflict.local))return;await apply(room,conflict.remote);await repo.set(key(room),{revision:conflict.revision,value:conflict.remote});}).catch(()=>{});}}]);
   },
  });sync.current=engine;
  const run=()=>{const room=latest.current.roomId;if(alive&&room)void engine.sync(room).catch(()=>{}).finally(()=>{if(alive)void preview().catch(()=>{});});else if(alive)void preview().catch(()=>{});};run();const timer=setInterval(run,15000),sub=AppState.addEventListener('change',state=>{if(state==='active')run();});
  return()=>{alive=false;engine.stop();if(sync.current===engine)sync.current=null;clearInterval(timer);sub.remove();};
 },[p.repository,p.ready,p.token,p.userId,p.baseUrl]);
 useEffect(()=>{const room=p.roomId;if(!room||!p.ready)return;setPreviews(old=>{const value={...old};if(p.text||p.reply)value[room]=p.text||'Ответ на сообщение';else delete value[room];return value;});const timer=setTimeout(()=>{void sync.current?.sync(room).catch(()=>{});},900);return()=>clearTimeout(timer);},[p.roomId,p.text,p.reply,p.ready]);
 return previews;
}
