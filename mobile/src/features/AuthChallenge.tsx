import {userError} from '../errors';
import {t as uiText} from "../../../web/i18n.js";
import {useState,useRef,useEffect} from 'react';
import {ActivityIndicator,Pressable,Text,TextInput,View} from 'react-native';
import {useTheme} from '../ThemeProvider';
import {request} from '../api';
import type {AuthResponse} from '../types';
import {settleMobileLogin} from './loginLifecycle';
export type AccountChallenge={id:string;secret:string;kind:'factor'|'device';expires_at:string};
export function AuthChallenge({baseUrl,challenge,onSuccess,onCancel}:{baseUrl:string;challenge:AccountChallenge;onSuccess:(session:AuthResponse)=>Promise<void>;onCancel:()=>void}){
 const {colors}=useTheme(),[code,setCode]=useState(''),[backup,setBackup]=useState(false),[busy,setBusy]=useState(false),[error,setError]=useState('');
 const generation=useRef(0),mounted=useRef(true);useEffect(()=>{mounted.current=true;generation.current++;return()=>{mounted.current=false;generation.current++;};},[baseUrl,challenge.id]);
 const cancel=()=>{generation.current++;onCancel();};
 const submit=async()=>{if(busy)return;const owner=generation.current,current=()=>mounted.current&&generation.current===owner;setBusy(true);setError('');try{if(Date.now()>=Date.parse(challenge.expires_at))throw Error('Запрос истёк. Вернитесь ко входу и повторите.');const result=await request<AuthResponse>(baseUrl,'/auth/account-challenge/complete',{method:'POST',bearerSession:true,body:{id:challenge.id,secret:challenge.secret,...(backup?{backup_code:code}:{code})}});if(current())setCode('');await settleMobileLogin(baseUrl,result,current,onSuccess);}catch(e){if(current())setError((e as Error).message==='device_approval_required'?uiText("Подтвердите новое устройство в настройках безопасности на уже авторизованном устройстве, затем нажмите «Проверить подтверждение»."):(e as Error).message==='bad_factor'?uiText("Неверный или уже использованный код."):userError(e));}finally{if(current())setBusy(false);}};
 const button=(text:string,action:()=>void)=><Pressable disabled={busy} accessibilityRole="button" onPress={action} style={{padding:16,minHeight:48}}><Text style={{color:colors.text.blue}}>{text}</Text></Pressable>;
 return <View style={{flex:1,backgroundColor:colors.windowBackground,justifyContent:'center',padding:24,gap:12}}><Text accessibilityRole="header" style={{fontSize:24,color:colors.text.primary}}>{uiText("Подтверждение входа")}</Text><Text style={{color:colors.text.secondary}}>{uiText("Введите код из приложения аутентификатора или резервный код. Если включено подтверждение устройств, подтвердите запрос в настройках на другом устройстве.")}</Text><TextInput accessibilityLabel={backup?uiText("Резервный код"):uiText("Одноразовый код")} value={code} onChangeText={setCode} autoCapitalize="none" autoCorrect={false} keyboardType={backup?'default':'number-pad'} secureTextEntry={backup} style={{color:colors.text.primary,backgroundColor:colors.windowBackgroundGray,padding:14,borderRadius:12}}/>{button(backup?uiText("Использовать код аутентификатора"):uiText("Использовать резервный код"),()=>{setCode('');setBackup(!backup);})}{busy?<ActivityIndicator/>:button(uiText("Проверить подтверждение"),()=>void submit())}{!!error&&<Text accessibilityLiveRegion="polite" style={{color:colors.misc.error}}>{error}</Text>}{button(uiText("Вернуться ко входу"),cancel)}</View>;
}
