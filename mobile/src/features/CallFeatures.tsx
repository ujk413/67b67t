import {userError} from '../errors';
import {locale as uiLocale} from "../../../web/i18n.js";
import {t as uiText} from "../../../web/i18n.js";
import React,{useEffect,useRef,useState} from 'react';
import {ScrollView,Text,Pressable,ActivityIndicator,View} from 'react-native';
import {request} from '../api';
import {useTheme} from '../ThemeProvider';
import {assertCallsAllowed} from '../accountRegistry';
import {GroupCall,type GroupCallCredentials} from '../components/GroupCall';
import type {FeatureContext} from './types';
type Call={id:string;room_id:string;caller_id:string;callee_id:string;started_at:string;status:string;reason:string;duration_ms:number;is_video:boolean};
type Page={items:Call[];next:{before:string;before_id:string}|null};
const statuses:Record<string,string>={ringing:'Вызов',active:'Идёт разговор',completed:'Завершён',missed:'Пропущен',busy:'Занято',rejected:'Отклонён',cancelled:'Отменён',interrupted:'Прерван'};
export function CallFeatures({mode,context:c}:{mode:string;context:FeatureContext}){
 const {colors}=useTheme(),[page,setPage]=useState<Page>({items:[],next:null}),[busy,setBusy]=useState(false),[error,setError]=useState(''),[credentials,setCredentials]=useState<GroupCallCredentials|null>(null);
 const mounted=useRef(true);
 useEffect(()=>{mounted.current=true;return()=>{mounted.current=false;};},[]);
 const run=async(fn:()=>Promise<unknown>)=>{setBusy(true);setError('');try{await fn();}catch(e){setError(userError(e));}finally{setBusy(false);}};
 const load=async(more=false)=>{const q=new URLSearchParams();if(c.room?.id)q.set('room_id',c.room.id);if(more&&page.next){q.set('before',page.next.before);q.set('before_id',page.next.before_id);}const result=await request<Page>(c.baseUrl,'/calls/history?'+q,{token:c.token});setPage(old=>({...result,items:more?[...old.items,...result.items]:result.items}));};
 useEffect(()=>{void run(()=>load());},[c.room?.id]);
 const leave=()=>{setCredentials(null);if(c.room?.id&&credentials)void request(c.baseUrl,`/rooms/${c.room.id}/calls/leave`,{token:c.token,method:'POST',body:{lease_id:credentials.lease_id}}).catch(()=>{});void run(()=>load());};
 useEffect(()=>()=>{if(credentials&&c.room?.id)void request(c.baseUrl,`/rooms/${c.room.id}/calls/leave`,{token:c.token,method:'POST',body:{lease_id:credentials.lease_id}}).catch(()=>{});},[credentials,c.room?.id,c.token]);
 if(credentials)return <GroupCall credentials={credentials} onLeave={leave} onHeartbeat={()=>request(c.baseUrl,`/rooms/${c.room!.id}/calls/heartbeat`,{token:c.token,method:'POST',body:{lease_id:credentials.lease_id}})}/>;
 const button=(text:string,fn:()=>Promise<unknown>)=><Pressable accessibilityRole="button" disabled={busy} onPress={()=>void run(fn)} style={{padding:13,marginVertical:5,borderRadius:9,backgroundColor:colors.windowBackgroundGray}}><Text style={{color:colors.text.blue}}>{text}</Text></Pressable>;
 return <ScrollView style={{flex:1,backgroundColor:colors.windowBackground}} contentContainerStyle={{padding:16,paddingBottom:60}}><Text accessibilityRole="header" style={{fontSize:23,color:colors.text.primary}}>{uiText("Звонки")}</Text>{busy&&<ActivityIndicator/>}{error?<Text accessibilityRole="alert" style={{color:'#d83d4f'}}>{error==='group_calls_unavailable'?uiText("Сервер групповых звонков недоступен"):error}</Text>:null}
 {c.room?.kind==='group'&&<>{button(uiText("Войти в групповой звонок · до 6 участников"),async()=>{await assertCallsAllowed();const room=c.room!.id,grant=await request<GroupCallCredentials>(c.baseUrl,`/rooms/${room}/calls/join`,{token:c.token,method:'POST',body:{}});if(!mounted.current){await request(c.baseUrl,`/rooms/${room}/calls/leave`,{token:c.token,method:'POST',body:{lease_id:grant.lease_id}}).catch(()=>{});return;}setCredentials(grant);})}<Text style={{color:colors.text.secondary}}>{uiText("Камера и демонстрация экрана включаются после подключения.")}</Text></>}
 {button(uiText("Обновить историю"),()=>load())}{!page.items.length&&!busy&&<Text style={{color:colors.text.secondary}}>{uiText("История звонков пуста")}</Text>}{page.items.map(call=>{const room=c.rooms.find(r=>r.id===call.room_id);return <View key={call.id} style={{paddingVertical:12,borderBottomWidth:1,borderColor:colors.divider}}><Pressable accessibilityRole="button" onPress={()=>void c.onOpen(call.room_id)}><Text style={{color:colors.text.primary,fontWeight:'600'}}>{call.caller_id===c.me.id?'↗':'↙'} {room?.title||uiText("Личный звонок")} · {call.is_video?uiText("Видео"):uiText("Аудио")}</Text><Text style={{color:colors.text.secondary}}>{uiText(statuses[call.status]||call.status)} · {new Date(call.started_at).toLocaleString(uiLocale())}{call.duration_ms>0?` · ${Math.floor(call.duration_ms/60000)}:${String(Math.floor(call.duration_ms/1000)%60).padStart(2,'0')}`:''}</Text></Pressable></View>;})}{page.next&&button(uiText("Ещё"),()=>load(true))}{button(uiText("Закрыть"),async()=>c.onClose())}</ScrollView>;
}
