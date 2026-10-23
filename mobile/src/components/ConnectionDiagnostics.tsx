import {t as uiText} from "../../../web/i18n.js";
import {useEffect,useState} from 'react';
import {ActivityIndicator,Pressable,Text,View} from 'react-native';
import {RTCPeerConnection} from '@livekit/react-native-webrtc';
import {diagnoseConnection,type DiagnosticResult} from '../../../web/diagnostics.js';
import {request} from '../api';
import {useTheme} from '../ThemeProvider';
export function ConnectionDiagnostics({baseUrl,token}:{baseUrl:string;token:string}) {
  const {colors}=useTheme();const [results,setResults]=useState<DiagnosticResult[]>([]),[running,setRunning]=useState(false),[attempt,setAttempt]=useState(0);
  useEffect(()=>{let active=true;setRunning(true);setResults([]);
    void diagnoseConnection({request:path=>request(baseUrl,path,{token}),wsUrl:baseUrl.replace(/^http/,'ws')+'/ws',token,Peer:RTCPeerConnection})
      .then(value=>{if(active)setResults(value);}).finally(()=>{if(active)setRunning(false);});
    return()=>{active=false;};
  },[baseUrl,token,attempt]);
  return <View style={{padding:16,gap:16}}>
    <Text style={{color:colors.text.secondary}}>{uiText("Проверяем сервер, авторизацию соединения и получение маршрута звонка. Содержимое переписок не отправляется.")}</Text>
    {running && <ActivityIndicator accessibilityLabel={uiText("Проверка соединения")} color={colors.text.blue}/>}
    {results.map(result=><View key={result.id} accessibilityLiveRegion="polite"><Text style={{color:result.ok?colors.text.blue:colors.misc.error,fontWeight:'600'}}>{result.title}: {result.ok?uiText("Работает"):uiText("Нужна проверка")}</Text>{!!result.action && <Text style={{color:colors.text.secondary,marginTop:6}}>{result.action}</Text>}</View>)}
    <Pressable disabled={running} accessibilityRole="button" onPress={()=>setAttempt(n=>n+1)} style={{minHeight:48,justifyContent:'center'}}><Text style={{color:colors.text.blue}}>{running?uiText("Проверяем…"):uiText("Повторить проверку")}</Text></Pressable>
  </View>;
}
