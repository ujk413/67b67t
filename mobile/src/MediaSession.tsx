import { createContext, useContext, useEffect, useRef, useState } from "react";
import type {Message} from './types';
import {resolveMediaSource} from './decryptedMedia';
export const MediaSession = createContext<Record<string, string>>({});
export const useMediaHeaders = () => useContext(MediaSession);
export const MediaDecoder=createContext<{baseUrl:string;userId:string;decode:(message:Message)=>Promise<Message>;onListened?:(message:Message)=>void}|null>(null);
export function useProtectedMedia(message:Message,enabled:boolean) {
  const session=useContext(MediaDecoder),headers=useMediaHeaders(),latest=useRef(session);latest.current=session;
  const [uri,setUri]=useState('');
  useEffect(()=>{
    let live=true;setUri('');const owner=latest.current;
    if(enabled && owner && message.meta?.media_encryption)void resolveMediaSource(message,owner.baseUrl,owner.userId,headers,owner.decode).then(result=>{if(live)setUri(result.uri);}).catch(()=>{});
    return ()=>{live=false;};
  },[message.media_id,session?.userId,session?.baseUrl,enabled]);
  return uri;
}
