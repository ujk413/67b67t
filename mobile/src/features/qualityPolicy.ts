import * as Network from 'expo-network';
import {storage} from '../storage';
import {shouldAutoDownload,type QualityDownloadPolicy} from '../../../web/quality-policy.js';
import {useEffect,useSyncExternalStore} from 'react';

type MediaType='image'|'video'|'voice'|'file';
const policies=new Map<string,QualityDownloadPolicy>(),loading=new Set<string>(),listeners=new Set<()=>void>();
let network:'wifi'|'cellular'|'unknown'='unknown',networkWatcher:{remove():void}|null=null;
const networkKind=(state:Network.NetworkState)=>state.type===Network.NetworkStateType.WIFI?'wifi':state.type===Network.NetworkStateType.CELLULAR?'cellular':'unknown';
const publish=()=>{for(const listener of listeners)listener();};
const subscribe=(listener:()=>void)=>{
 listeners.add(listener);
 if(!networkWatcher){networkWatcher=Network.addNetworkStateListener(state=>{network=networkKind(state);publish();});void Network.getNetworkStateAsync().then(state=>{network=networkKind(state);publish();}).catch(()=>{});}
 return()=>{listeners.delete(listener);if(!listeners.size){networkWatcher?.remove();networkWatcher=null;network='unknown';}};
};
export function setAccountDownloadPolicy(userId:string,policy:QualityDownloadPolicy){policies.set(userId,policy);publish();return storage.set(qualityStorageKey(userId,'download'),JSON.stringify(policy));}
export function useMediaAutoDownload(type:MediaType,userId:string|undefined){
 useEffect(()=>{if(!userId||policies.has(userId)||loading.has(userId))return;loading.add(userId);void storage.get(qualityStorageKey(userId,'download')).then(value=>{if(value){try{policies.set(userId,JSON.parse(value));publish();}catch{}}}).finally(()=>loading.delete(userId));},[userId]);
 return useSyncExternalStore(subscribe,()=>{const policy=userId?policies.get(userId):null;return !!policy&&shouldAutoDownload(policy,type,network);},()=>false);
}

export const qualityStorageKey=(userId:string,field:'download'|'quiet'|'cache')=>`quality.${field}:${userId}`;
export async function canAutoDownloadMedia(type:MediaType,userId:string){
 const saved=await storage.get(qualityStorageKey(userId,'download'));if(!saved)return false;
 let policy:QualityDownloadPolicy;try{policy=JSON.parse(saved);}catch{return false;}
 const state=await Network.getNetworkStateAsync();
 const network=state.type===Network.NetworkStateType.WIFI?'wifi':state.type===Network.NetworkStateType.CELLULAR?'cellular':'unknown';
 return shouldAutoDownload(policy,type,network);
}
