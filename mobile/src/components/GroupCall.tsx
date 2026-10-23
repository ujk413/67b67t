import {t as uiText} from "../../../web/i18n.js";
import {userError} from '../errors';
import '../features/livekitBootstrap';
import React,{useEffect,useState} from 'react';
import {View,Text,Pressable,ScrollView,ActivityIndicator} from 'react-native';
import {AudioSession,LiveKitRoom,VideoTrack,useTracks,useRoomContext,useLocalParticipant,useParticipants,useConnectionState,isTrackReference} from '@livekit/react-native';
import {Track,ConnectionState} from 'livekit-client';
import {useTheme} from '../ThemeProvider';
import {assertCallsAllowed} from '../accountRegistry';
import {beginCall,endCall} from '../audioSession';

export type GroupCallCredentials={url:string;token:string;room:string;max_participants:number;lease_id:string};

function Controls({onLeave}:{onLeave:()=>void}){
 const {colors}=useTheme(),room=useRoomContext(),participants=useParticipants(),state=useConnectionState(),{localParticipant,isMicrophoneEnabled,isCameraEnabled,isScreenShareEnabled}=useLocalParticipant();
 const tracks=useTracks([Track.Source.Camera,Track.Source.ScreenShare],{onlySubscribed:true});
 const [error,setError]=useState(''),[busy,setBusy]=useState(false);
 const action=async(fn:()=>Promise<unknown>)=>{setError('');setBusy(true);try{await assertCallsAllowed();await fn();}catch(e){setError(userError(e));}finally{setBusy(false);}};
 const button=(text:string,fn:()=>Promise<unknown>,danger=false)=><Pressable accessibilityRole="button" disabled={busy} onPress={()=>void action(fn)} style={{padding:13,borderRadius:10,margin:4,backgroundColor:colors.windowBackgroundGray}}><Text style={{color:danger?'#d83d4f':colors.text.blue}}>{text}</Text></Pressable>;
 return <ScrollView style={{flex:1,backgroundColor:colors.windowBackground}} contentContainerStyle={{padding:12}}><Text accessibilityRole="header" style={{fontSize:23,color:colors.text.primary}}>{uiText("Групповой звонок ·")}{participants.length}/6</Text><Text accessibilityLiveRegion="polite" style={{color:colors.text.secondary}}>{state===ConnectionState.Connected?uiText("Подключено"):state===ConnectionState.Reconnecting?uiText("Восстановление соединения…"):state===ConnectionState.Disconnected?uiText("Соединение завершено"):uiText("Подключение…")}</Text><Text style={{color:colors.text.secondary}}>{uiText("Собственный сервер звонков. Медиа защищены при передаче; сервер обрабатывает потоки.")}</Text>
 {participants.map(participant=><Text key={participant.identity} style={{color:colors.text.primary,padding:6}}>{participant.isSpeaking?'● ':''}{participant.name||participant.identity}{participant.isLocal?(" "+uiText("· Вы")+""):''}</Text>)}
 {tracks.filter(isTrackReference).map(track=><View key={`${track.participant.identity}:${track.source}`} style={{marginVertical:6}}><VideoTrack trackRef={track} objectFit="contain" style={{height:210,width:'100%'}}/><Text style={{color:colors.text.secondary}}>{track.participant.name||track.participant.identity} · {track.source===Track.Source.ScreenShare?uiText("Экран"):uiText("Камера")}</Text></View>)}
 {error?<Text accessibilityRole="alert" style={{color:'#d83d4f'}}>{error}</Text>:null}{busy&&<ActivityIndicator/>}
 {button(isMicrophoneEnabled?uiText("Выключить микрофон"):uiText("Включить микрофон"),()=>localParticipant.setMicrophoneEnabled(!isMicrophoneEnabled,{echoCancellation:true,noiseSuppression:true,autoGainControl:true}))}
 {button(isCameraEnabled?uiText("Выключить камеру"):uiText("Включить камеру"),()=>localParticipant.setCameraEnabled(!isCameraEnabled))}
 {button(isScreenShareEnabled?uiText("Остановить демонстрацию"):uiText("Демонстрация экрана"),()=>localParticipant.setScreenShareEnabled(!isScreenShareEnabled))}
 {button(uiText("Завершить"),async()=>{await room.disconnect();await AudioSession.stopAudioSession();onLeave();},true)}</ScrollView>;
}

export function GroupCall({credentials,onLeave,onHeartbeat}:{credentials:GroupCallCredentials;onLeave:()=>void;onHeartbeat:()=>Promise<unknown>}){
 const {colors}=useTheme(),[ready,setReady]=useState(false),[error,setError]=useState('');
 useEffect(()=>{let cancelled=false,owner:number|undefined;void (async()=>{await assertCallsAllowed();if(cancelled)return;owner=await beginCall();if(cancelled){await endCall(owner);return;}await AudioSession.startAudioSession();if(!cancelled)setReady(true);else{await AudioSession.stopAudioSession();await endCall(owner);}})().catch(e=>{if(!cancelled)setError(userError(e));if(owner!==undefined)void endCall(owner);});return()=>{cancelled=true;void AudioSession.stopAudioSession().then(()=>owner===undefined?undefined:endCall(owner));};},[]);
 useEffect(()=>{const timer=setInterval(()=>{void onHeartbeat().catch(()=>onLeave());},30000);return()=>clearInterval(timer);},[onHeartbeat,onLeave]);
 if(error)return <View style={{padding:16}}><Text style={{color:colors.text.primary}}>{error}</Text><Pressable onPress={onLeave}><Text style={{color:colors.text.blue}}>{uiText("Закрыть")}</Text></Pressable></View>;
 if(!ready)return <ActivityIndicator/>;
 return <LiveKitRoom serverUrl={credentials.url} token={credentials.token} connect audio={{echoCancellation:true,noiseSuppression:true,autoGainControl:true}} video={false} options={{adaptiveStream:true,dynacast:true}} onError={e=>setError(userError(e))} onDisconnected={onLeave}><Controls onLeave={onLeave}/></LiveKitRoom>;
}
