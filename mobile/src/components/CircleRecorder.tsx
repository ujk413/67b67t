import {t as uiText} from "../../../web/i18n.js";
import {useEffect,useRef,useState} from 'react';
import {ActivityIndicator,AppState,Linking,PanResponder,Pressable,Text,View,useWindowDimensions} from 'react-native';
import {CameraView,useCameraPermissions,useMicrophonePermissions} from 'expo-camera';
import {Video} from './Video';
import {Icon} from './Icon';
import {useTheme} from '../ThemeProvider';
import {ensurePlaybackMode,ensureRecordingMode} from '../audioSession';
import {userError} from '../errors';
import * as Files from 'expo-file-system/legacy';
import {onMediaCallStarted} from '../playbackOwnership';
import {runMediaTask} from '../mediaLifecycle';
import {recordingGesture} from '../../../web/media-details.js';

export function CircleRecorder({onRecorded,onCancel,userId}:{onRecorded:(uri:string)=>Promise<void>;onCancel:()=>void;userId?:string}) {
  const {colors}=useTheme(),{width}=useWindowDimensions();
  const [permission,request]=useCameraPermissions(),[microphone,requestMic]=useMicrophonePermissions();
  const [facing,setFacing]=useState<'front'|'back'>('front'),[ready,setReady]=useState(false),[recording,setRecording]=useState(false),[locked,setLocked]=useState(false),[preparing,setPreparing]=useState(true),[seconds,setSeconds]=useState(0),[clip,setClip]=useState(''),[error,setError]=useState(''),[saving,setSaving]=useState(false);
  const camera=useRef<CameraView>(null),live=useRef(true),held=useRef(false),lock=useRef(false),cancelled=useRef(false),working=useRef(false),clipRef=useRef(clip),sending=useRef(false);clipRef.current=clip;
  const removeClip=()=>{const previous=clipRef.current;clipRef.current='';setClip('');if(previous)void Files.deleteAsync(previous,{idempotent:true}).catch(()=>{});};
  // Cancellation is terminal for this recording; releasing the finger afterwards
  // must never turn a discarded recording back into a preview.
  const stop=(discard=false)=>{cancelled.current ||= discard;held.current=false;try{camera.current?.stopRecording();}catch{}};
  const begin=async(fixed=false)=>{
    if(working.current || !ready || !permission?.granted || !microphone?.granted)return;
    held.current=true;lock.current=fixed;cancelled.current=false;working.current=true;setLocked(fixed);setRecording(true);setSeconds(0);setError('');
    try{
      await runMediaTask(userId || 'shared',async task=>{
        const remove=task.onCancel(()=>{actions.current.stop(true);});let result:{uri:string}|undefined;
        try{
          await ensureRecordingMode();task.check();
          if(cancelled.current || (!held.current && !lock.current) || !live.current)return;
          result=await camera.current!.recordAsync({maxDuration:60});task.check();
          if(result && live.current && !cancelled.current){setClip(result.uri);result=undefined;}
        }finally{remove();if(result)await Files.deleteAsync(result.uri,{idempotent:true}).catch(()=>{});}
      });
    }catch(value){if(live.current && !cancelled.current)setError(userError(value));}
    finally{working.current=false;lock.current=false;if(live.current){setRecording(false);setLocked(false);}void ensurePlaybackMode();}
  };
  const actions=useRef({begin,stop});actions.current={begin,stop};
  const gesture=useRef(PanResponder.create({
    onStartShouldSetPanResponder:()=>true,
    onPanResponderGrant:()=>void actions.current.begin(),
    onPanResponderMove:(_,event)=>{if(cancelled.current || !working.current)return;const action=recordingGesture({dx:event.dx,dy:event.dy,locked:lock.current});if(action==='cancel')actions.current.stop(true);else if(action==='lock'){lock.current=true;setLocked(true);}},
    onPanResponderRelease:()=>{held.current=false;if(!lock.current)actions.current.stop(false);},
    onPanResponderTerminationRequest:()=>false,
    onPanResponderTerminate:()=>{if(!lock.current)actions.current.stop(true);},
  })).current;
  useEffect(()=>{live.current=true;const interrupt=onMediaCallStarted(()=>{actions.current.stop(true);});const sub=AppState.addEventListener('change',value=>{if(value!=='active' && working.current)actions.current.stop(true);});
    void (async()=>{await request();await requestMic();})().catch(value=>{if(live.current)setError(userError(value));}).finally(()=>{if(live.current)setPreparing(false);});
    return()=>{live.current=false;cancelled.current=true;held.current=false;actions.current.stop(true);sub.remove();interrupt();if(clipRef.current && !sending.current)void Files.deleteAsync(clipRef.current,{idempotent:true}).catch(()=>{});void ensurePlaybackMode();};
  },[]);
  useEffect(()=>{if(!recording)return;const timer=setInterval(()=>setSeconds(value=>Math.min(60,value+1)),1000);return()=>clearInterval(timer);},[recording]);
  const size=Math.min(320,width-40),allowed=permission?.granted && microphone?.granted;
  const button=(title:string,action:()=>void,disabled=false)=><Pressable accessibilityRole="button" accessibilityLabel={title} onPress={action} disabled={disabled} style={{padding:14,minHeight:48,opacity:disabled?0.4:1}}><Text style={{color:'#fff'}}>{title}</Text></Pressable>;
  return <View style={{flex:1,backgroundColor:'#000',justifyContent:'center',alignItems:'center',gap:14,paddingVertical:24}}>
    <View style={{flexDirection:'row',width:'100%',justifyContent:'space-between'}}>{button(uiText("Отмена"),()=>{actions.current.stop(true);onCancel();},saving)}{!clip && button(uiText("Сменить камеру"),()=>{setReady(false);setFacing(value=>value==='front'?'back':'front');},recording || !allowed)}</View>
    {clip?<View style={{width:size,height:size,borderRadius:size/2,overflow:'hidden'}}><Video source={{uri:clip}} style={{width:size,height:size}} useNativeControls /></View>:allowed?<View style={{width:size,height:size,borderRadius:size/2,overflow:'hidden'}}><CameraView key={facing} ref={camera} mode="video" facing={facing} videoQuality="480p" videoBitrate={2000000} onCameraReady={()=>setReady(true)} onMountError={()=>{setReady(false);setError(uiText("Не удалось открыть камеру. Закройте другое приложение с камерой и повторите."));}} style={{flex:1,transform:facing==='front'?[{scaleX:-1}]:undefined}}/></View>:<Text style={{color:'#fff',padding:20}}>{uiText("Разрешите камеру и микрофон для записи кружка.")}</Text>}
    {preparing && <ActivityIndicator color="#fff"/>}
    {!!error && <Text accessibilityLiveRegion="polite" style={{color:colors.misc.error,paddingHorizontal:20}}>{error}</Text>}
    {clip?<><Text style={{color:'#fff'}}>{uiText("Просмотрите кружок перед отправкой")}</Text><View style={{flexDirection:'row'}}>{button(uiText("Перезаписать"),removeClip,saving)}{button(uiText("Удалить"),()=>{removeClip();setError('');},saving)}{button(saving?uiText("Сохраняем…"):uiText("Отправить"),()=>{setSaving(true);sending.current=true;const selected=clip;let success=false;void onRecorded(selected).then(()=>{success=true;}).catch(value=>{if(live.current)setError(userError(value));}).finally(()=>{sending.current=false;if(success || !live.current)void Files.deleteAsync(selected,{idempotent:true}).catch(()=>{});if(live.current)setSaving(false);});},saving)}</View></>:<>
      <Text style={{color:'#fff'}}>{recording?uiText("{0} с / 60 с · {1}",seconds,locked?uiText("Запись зафиксирована"):uiText("Влево — отмена, вверх — фиксация")):uiText("Удерживайте кнопку для записи")}</Text>
      <View {...gesture.panHandlers} accessible accessibilityRole="button" accessibilityLabel={recording?uiText("Запись кружка"):uiText("Удерживать для записи кружка")} accessibilityActions={[{name:'activate',label:uiText("Записать без удержания")}]} onAccessibilityAction={()=>void begin(true)} style={{width:80,height:80,borderRadius:40,backgroundColor:recording?colors.misc.error:colors.text.blue,alignItems:'center',justifyContent:'center',opacity:ready?1:0.4}}><Icon name="video" color="#fff" size={32}/></View>
      {recording && locked?button(uiText("Остановить и посмотреть"),()=>stop(false)):!recording && button(uiText("Записать без удержания"),()=>void begin(true),!ready)}
    </>}
    {!allowed && !preparing && button(uiText("Настройки разрешений"),()=>void Linking.openSettings())}
  </View>;
}
