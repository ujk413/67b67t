import React from 'react';
import {StyleSheet} from 'react-native';
import Svg,{Circle,Defs,Path,Pattern,Rect} from 'react-native-svg';
import type {WallpaperMode} from '../ThemeProvider';

export function WallpaperPattern({mode,dark}:{mode:WallpaperMode;dark:boolean}){
 if(mode==='plain')return null;
 const ink=dark?'#FFFFFF':'#24476C';
 return <Svg pointerEvents="none" style={StyleSheet.absoluteFillObject} width="100%" height="100%">
  <Defs><Pattern id="quality-wallpaper" patternUnits="userSpaceOnUse" width={mode==='dots'?20:32} height={mode==='dots'?20:32}>
   {mode==='dots'?<Circle cx="10" cy="10" r="1.2" fill={ink} opacity={0.15}/>:<Path d="M0 0L32 32M32 0L0 32" stroke={ink} strokeWidth="1" opacity={0.08}/>}
  </Pattern></Defs>
  <Rect width="100%" height="100%" fill="url(#quality-wallpaper)"/>
 </Svg>;
}
