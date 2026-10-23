import {t as uiText} from "../../../web/i18n.js";
import {useEffect,useRef,useState} from 'react';
import {StyleSheet,Text,View,type StyleProp,type ViewStyle} from 'react-native';
import {useVideoPlayer,VideoView,type VideoSource} from 'expo-video';
import {useTheme} from '../ThemeProvider';
import {ownVideoPlayer,type PlaybackOwner} from '../playbackOwnership';
export const ResizeMode={CONTAIN:'contain',COVER:'cover'} as const;
// Each view owns its native player. Expo releases it when a preview or message unmounts.
export function Video({source,style,resizeMode='contain',useNativeControls=false,isLooping=false,shouldPlay=false}:{source:VideoSource;style?:StyleProp<ViewStyle>;resizeMode?:'contain'|'cover';useNativeControls?:boolean;isLooping?:boolean;shouldPlay?:boolean}) {
  const player=useVideoPlayer(null),[error,setError]=useState(''),{colors}=useTheme();
  const key=JSON.stringify(source),owner=useRef<PlaybackOwner|null>(null),queue=useRef(Promise.resolve());
  useEffect(()=>{const lease=ownVideoPlayer(player);owner.current=lease;return()=>{owner.current=null;lease.dispose();};},[player]);
  useEffect(()=>{
    let live=true;setError('');
    queue.current=queue.current.catch(()=>{}).then(async()=>{if(!live)return;await player.replaceAsync(source);if(live && shouldPlay && await owner.current?.claim())player.play();}).catch(()=>{if(live)setError(uiText("Не удалось открыть видео"));});
    return ()=>{live=false;player.pause();owner.current?.release();};
  },[player,key]);
  useEffect(()=>{player.loop=isLooping;if(shouldPlay)void owner.current?.claim().then(allowed=>{if(allowed)player.play();});else player.pause();},[player,isLooping,shouldPlay]);
  useEffect(()=>{const sub=player.addListener('statusChange',event=>{if(event.status==='error')setError(uiText("Не удалось воспроизвести видео"));});return()=>sub.remove();},[player]);
  return <View style={[style,{overflow:'hidden'}]}><VideoView player={player} nativeControls={useNativeControls} contentFit={resizeMode} surfaceType="textureView" allowsPictureInPicture style={StyleSheet.absoluteFill} />{!!error && <Text accessibilityLiveRegion="polite" style={{color:colors.misc.error,padding:8}}>{error}</Text>}</View>;
}
