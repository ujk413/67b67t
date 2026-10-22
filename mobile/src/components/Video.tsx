import {useEffect,useState} from 'react';
import {StyleSheet,Text,View,type StyleProp,type ViewStyle} from 'react-native';
import {useVideoPlayer,VideoView,type VideoSource} from 'expo-video';
import {useTheme} from '../ThemeProvider';
export const ResizeMode={CONTAIN:'contain',COVER:'cover'} as const;
// Each view owns its native player. Expo releases it when a preview or message unmounts.
export function Video({source,style,resizeMode='contain',useNativeControls=false,isLooping=false,shouldPlay=false}:{source:VideoSource;style?:StyleProp<ViewStyle>;resizeMode?:'contain'|'cover';useNativeControls?:boolean;isLooping?:boolean;shouldPlay?:boolean}) {
  const player=useVideoPlayer(null),[error,setError]=useState(''),{colors}=useTheme();
  const key=JSON.stringify(source);
  useEffect(()=>{
    let live=true;setError('');
    void player.replaceAsync(source).then(()=>{if(live && shouldPlay)player.play();}).catch(()=>{if(live)setError('Не удалось открыть видео');});
    return ()=>{live=false;};
  },[player,key]);
  useEffect(()=>{player.loop=isLooping;if(shouldPlay)player.play();else player.pause();},[player,isLooping,shouldPlay]);
  useEffect(()=>{const sub=player.addListener('statusChange',event=>{if(event.status==='error')setError('Не удалось воспроизвести видео');});return()=>sub.remove();},[player]);
  return <View style={[style,{overflow:'hidden'}]}><VideoView player={player} nativeControls={useNativeControls} contentFit={resizeMode} surfaceType="textureView" style={StyleSheet.absoluteFill} />{!!error && <Text accessibilityLiveRegion="polite" style={{color:colors.misc.error,padding:8}}>{error}</Text>}</View>;
}
