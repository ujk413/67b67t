import {userError} from '../errors';
import {locale as uiLocale} from "../../../web/i18n.js";
import {t as uiText} from "../../../web/i18n.js";
import {useEffect,useState,useRef} from 'react';
import {ActivityIndicator,Pressable,Text,View} from 'react-native';
import QRCode from 'react-native-qrcode-svg';
import {useTheme} from '../ThemeProvider';
import {request} from '../api';
import type {AuthResponse} from '../types';
import {settleMobileLogin} from './loginLifecycle';
type Handoff={id:string;secret:string;qr:string|Record<string,unknown>;expires_at:string};
export function QrLogin({baseUrl,onSuccess,onCancel}:{baseUrl:string;onSuccess:(session:AuthResponse)=>Promise<void>;onCancel:()=>void}){
 const {colors}=useTheme(),[handoff,setHandoff]=useState<Handoff|null>(null),[error,setError]=useState(''),[attempt,setAttempt]=useState(0);
 const generation=useRef(0);
 useEffect(()=>{let alive=true,timer:ReturnType<typeof setTimeout>;const owner=++generation.current,current=()=>alive&&generation.current===owner;setHandoff(null);setError('');
  const call=<T,>(path:string,body:unknown)=>request<T>(baseUrl,path,{method:'POST',bearerSession:true,body});
  const begin=async()=>{const pending=await call<Handoff>('/auth/account-handoff',{});if(!current())return;setHandoff(pending);
   const poll=async()=>{if(!current())return;if(Date.now()>=Date.parse(pending.expires_at)){setError(uiText("QR-код истёк. Создайте новый."));return;}try{const result=await call<AuthResponse>('/auth/account-handoff/claim',{id:pending.id,secret:pending.secret});await settleMobileLogin(baseUrl,result,current,onSuccess);}catch(e){if(!current())return;const status=(e as {status?:number}).status;if(status===409||!status||status>=500){if(status!==409)setError(uiText("Нет соединения. Ожидаем восстановления…"));timer=setTimeout(()=>void poll(),5000);}else setError(uiText("Запрос входа истёк или был отклонён. Создайте новый QR-код."));}};timer=setTimeout(()=>void poll(),2000);
  };void begin().catch(e=>{if(current())setError(userError(e));});return()=>{alive=false;clearTimeout(timer);};
 },[baseUrl,attempt]);
 const qr=handoff?typeof handoff.qr==='string'?handoff.qr:JSON.stringify(handoff.qr):'';
 return <View style={{flex:1,backgroundColor:colors.windowBackground,justifyContent:'center',alignItems:'center',padding:24,gap:18}}><Text style={{fontSize:23,color:colors.text.primary}}>{uiText("Вход по QR")}</Text><Text style={{color:colors.text.secondary}}>{uiText("На авторизованном устройстве откройте «Настройки → Вход по QR» и подтвердите этот запрос.")}</Text>{qr?<QRCode value={qr} size={230} backgroundColor="#fff" color="#000"/>:<ActivityIndicator/>}<Text selectable style={{color:colors.text.secondary}}>{handoff?uiText("Действует до {0}",new Date(handoff.expires_at).toLocaleTimeString(uiLocale())):''}</Text>{!!error&&<Text accessibilityLiveRegion="polite" style={{color:colors.misc.error}}>{error}</Text>}<Pressable accessibilityRole="button" onPress={()=>{generation.current++;setAttempt(n=>n+1);}} style={{padding:14}}><Text style={{color:colors.text.blue}}>{uiText("Новый QR-код")}</Text></Pressable><Pressable accessibilityRole="button" onPress={()=>{generation.current++;onCancel();}} style={{padding:14}}><Text style={{color:colors.text.blue}}>{uiText("Вернуться ко входу")}</Text></Pressable></View>;
}
