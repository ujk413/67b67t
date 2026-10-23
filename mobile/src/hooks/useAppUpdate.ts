import { useCallback, useEffect, useRef, useState } from "react";
import { AppState, Platform } from "react-native";
import { addNetworkStateListener } from 'expo-network';
import {checkForUpdate,discardDownload,downloadRelease,downloadedSize,verifyRelease,installRelease,openInstallPermissionSettings,type AppRelease} from "../updates";

type ReleaseTarget = {release: AppRelease; url: string};
export type UpdateState =
  | {phase: "idle"}
  | (ReleaseTarget & {phase: "available"})
  | (ReleaseTarget & {phase: "downloading"; progress: number})
  | (ReleaseTarget & {phase: "ready" | "installing"; fileUri: string})
  | (ReleaseTarget & {phase: "error"; fileUri?: string; message: string; needsPermission: boolean});
const CHECK_THROTTLE_MS = 60 * 60 * 1000;

export function useAppUpdate(baseUrl: string, enabled: boolean) {
  const [state,setState] = useState<UpdateState>({phase:"idle"});
  const [dismissed,setDismissed] = useState<number|null>(null);
  const [checkStatus,setCheckStatus] = useState("idle");
  const lastCheck=useRef(0),checking=useRef(false),busy=useRef(false);
  const generation=useRef(0),abort=useRef<AbortController|null>(null);
  const resume=useRef<()=>void>(()=>{});
  const check=useCallback(async(force=false)=>{
    if(!enabled || Platform.OS!=="android" || checking.current) return;
    if(!force && Date.now()-lastCheck.current<CHECK_THROTTLE_MS)return;
    checking.current=true;setCheckStatus("checking");
    try {
      const result=await checkForUpdate(baseUrl);
      setCheckStatus(result.status);
      // Only successful checks consume the interval; retry after reconnect.
      if(result.status==="unavailable")return;
      lastCheck.current=Date.now();
      if(result.status==="available") {
        if(force)setDismissed(null);
        setState(prev=>prev.phase==="idle" || prev.phase==="available" ? {phase:"available",release:result.release,url:result.url}:prev);
      }
    } finally {checking.current=false;}
  },[baseUrl,enabled]);
  useEffect(()=>{
    void check();
    const sub=AppState.addEventListener("change",next=>{if(next==="active")void check();});
    return()=>{sub.remove();abort.current?.abort();generation.current++;};
  },[check]);
  const install=useCallback(async(target:ReleaseTarget,fileUri:string,mine:number)=>{
    if(generation.current!==mine)return;
    setState({...target,phase:"installing",fileUri});
    try {
      await installRelease(fileUri,abort.current?.signal);
      // Returning from the installer, including Cancel, leaves an actionable UI.
      // A completed upgrade normally restarts the process.
      if(generation.current===mine)setState({...target,phase:"ready",fileUri});
    } catch(error) {
      if(generation.current===mine){
        const message=error instanceof Error?error.message:String(error);
        setState({...target,phase:"error",fileUri,message,needsPermission:/permission|unknown|denied/i.test(message)});
      }
    }
  },[]);
  const perform=useCallback(async(target:ReleaseTarget,existing?:string)=>{
    if(busy.current)return;
    busy.current=true;const mine=++generation.current;
    const controller=new AbortController();abort.current=controller;
    let fileUri=existing;
    try {
      if(fileUri && await downloadedSize(fileUri)<=0)fileUri=undefined;
      if(generation.current!==mine)return;
      if(!fileUri){
        setState({...target,phase:"downloading",progress:0});
        fileUri=await downloadRelease(target.url,progress=>{
          if(generation.current===mine)setState({...target,phase:"downloading",progress});
        },controller.signal,target.release);
      }
      if(generation.current!==mine)return;
      const actual=await downloadedSize(fileUri);
      if(generation.current!==mine || controller.signal.aborted)return;
      if(target.release.size>0 && actual!==target.release.size){
        await discardDownload(fileUri);fileUri=undefined;throw Error("incomplete_download");
      }
      try {await verifyRelease(fileUri,target.release,controller.signal);}
      catch(error){
        if(error instanceof Error && error.message==='checksum_mismatch'){await discardDownload(fileUri);fileUri=undefined;}
        throw error;
      }
      if(generation.current!==mine || controller.signal.aborted)return;
      await install(target,fileUri,mine);
    } catch(error) {
      if(generation.current===mine){
        const message=error instanceof Error?error.message:String(error);
        setState({...target,phase:"error",fileUri,message,needsPermission:false});
      }
    } finally {busy.current=false;abort.current=null;}
  },[install]);
  const start=useCallback(async()=>{
    if(state.phase==="available")await perform(state);
  },[state,perform]);
  const retryInstall=useCallback(async()=>{
    if(state.phase==="ready" || state.phase==="error")await perform(state,state.fileUri);
  },[state,perform]);
  const dismiss=useCallback(()=>{
    if(state.phase==="idle")return;
    setDismissed(state.release.versionCode);
    generation.current++;abort.current?.abort();
    setState({phase:"idle"});
  },[state]);
  const openPermissionSettings=useCallback(async()=>{
    try {await openInstallPermissionSettings();}catch{}
  },[]);
  resume.current=()=>{
    if(state.phase==='error' && !state.needsPermission && /network|timeout|offline|incomplete_download|download_http_5|connect|resolve.*host|end of stream/i.test(state.message))void perform(state,state.fileUri);
  };
  useEffect(()=>{
    const network=addNetworkStateListener(value=>{if(value.isConnected && AppState.currentState==='active')resume.current();});
    const foreground=AppState.addEventListener('change',value=>{if(value==='active')resume.current();});
    return()=>{network.remove();foreground.remove();};
  },[baseUrl,enabled]);
  return {state,checkStatus,visible:state.phase!=="idle" && (state.phase!=="available" || dismissed!==state.release.versionCode),start,dismiss,retryInstall,openPermissionSettings,recheck:()=>check(true)};
}
