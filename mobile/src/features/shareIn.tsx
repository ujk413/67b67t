import {userError} from '../errors';
import {t as uiText} from "../../../web/i18n.js";
import React,{useEffect,useRef,useState} from 'react';
import {AppState,FlatList,Pressable,Text,View} from 'react-native';
import {requireOptionalNativeModule} from 'expo';
import {useTheme} from '../ThemeProvider';
import type {FeatureContext} from './types';
import {deliverShare,discardShareDelivery,type ShareDelivery,type ShareJournal} from '../../../web/share-delivery.js';

type PendingShare={id:string;text:string|null;uris:string[];mime:string|null;rejectedFiles?:number;delivery?:ShareDelivery|null;error?:string|null;notice?:string|null;pendingCount?:number};
type Bridge={getPendingShare():PendingShare|null;confirmPendingShare:(id:string,account:string,room:string)=>PendingShare;updatePendingShare:(id:string,account:string,room:string,next:number)=>void;discardPendingShare:(id:string)=>void;removeAccountShares:(account:string)=>void;setUnreadCount?:(count:number)=>void;addListener?:(name:string,callback:()=>void)=>{remove():void}};
const bridge=requireOptionalNativeModule<Bridge>('ExpressiveIntegration');
export function updateUnreadWidget(count:number){bridge?.setUnreadCount?.(Math.max(0,Math.min(99999,Math.round(count))));}
export function removeIncomingAccountShares(account:string){bridge?.removeAccountShares(account);}
const shareError=(code?:string|null)=>code==='share_queue_full'?uiText('Очередь входящих файлов заполнена. Завершите или отмените предыдущую отправку и поделитесь ещё раз.'):code?uiText('Входящий файл недоступен. Отмените отправку и выберите файл заново в приложении-источнике.'):'';

/** Preview is restored from the encrypted native journal. Every send starts with a tap. */
export function ShareInConfirm({context:c,onShareFile}:{context:FeatureContext;onShareFile?:(roomId:string,uri:string,mime:string,queueId:string)=>Promise<void>}){
 const {colors}=useTheme(),[share,setShare]=useState<PendingShare|null>(null),[roomId,setRoomId]=useState<string|null>(null),[busy,setBusy]=useState(false),[status,setStatus]=useState(''),[choosingAccount,setChoosingAccount]=useState(false);
 const alive=useRef(true),latest=useRef(c);latest.current=c;
 const read=()=>{const next=bridge?.getPendingShare()||null;setShare(next);setRoomId(previous=>next?.delivery?.roomId||previous);};
 useEffect(()=>{alive.current=true;read();const sub=AppState.addEventListener('change',state=>{if(state==='active')read();}),received=bridge?.addListener?.('shareReceived',read);return()=>{alive.current=false;sub.remove();received?.remove();};},[]);
 if(!share)return null;
 const discard=()=>{if(busy)return;try{discardShareDelivery(share.id);bridge?.discardPendingShare(share.id);setRoomId(null);setStatus('');if(share.id==='unavailable')setShare(null);else read();}catch(error){setStatus(userError(error));}};
 const wrongAccount=!!share.delivery&&share.delivery.recipientId!==c.me.id;
 const confirm=async()=>{
  if(!roomId||busy||wrongAccount||share.error||share.rejectedFiles)return;
  setBusy(true);setStatus('');const account=c.me.id,token=c.token,base=c.baseUrl;
  const check=()=>{if(!alive.current||latest.current.me.id!==account||latest.current.token!==token||latest.current.baseUrl!==base)throw Error('Сеанс изменился');};
  try{
   check();if(!bridge?.confirmPendingShare)throw Error('Обновите приложение для защищённого входящего шаринга');
   if(share.uris.length&&!onShareFile)throw Error('Вложения из Android Share недоступны в этой сборке');
   const confirmed=bridge.confirmPendingShare(share.id,account,roomId);setShare(confirmed);
   const journal:ShareJournal={read:()=>confirmed.delivery||null,save:(_id,state)=>{check();bridge.updatePendingShare(confirmed.id,state.recipientId,state.roomId,state.next);confirmed.delivery=state;}};
   await deliverShare({...confirmed,roomId,recipientId:account},async(item,queueId)=>{check();if(item.kind==='text')await c.onSend(roomId,item.value.slice(0,10000),{__queue_id:queueId});else await onShareFile!(roomId,item.value,confirmed.mime&&/^[\w.+-]+\/[\w.+-]+$/.test(confirmed.mime)?confirmed.mime:'application/octet-stream',queueId);},journal);
   check();bridge.discardPendingShare(confirmed.id);discardShareDelivery(confirmed.id);setRoomId(null);read();await c.onOpen(roomId);
  }catch(error){if(alive.current){setStatus(userError(error));read();}}finally{if(alive.current)setBusy(false);}
 };
 const accountButton=<Pressable disabled={busy} onPress={()=>{setChoosingAccount(true);c.onMode('account-accounts');}} style={{padding:14}}><Text style={{color:colors.text.blue}}>{uiText('Выбрать аккаунт')}</Text></Pressable>;
 if(choosingAccount)return <View style={{position:'absolute',bottom:8,left:16,right:16,backgroundColor:colors.windowBackground}}><Pressable onPress={()=>setChoosingAccount(false)} style={{padding:14}}><Text style={{color:colors.text.blue}}>{uiText('Вернуться к входящей отправке')}</Text></Pressable></View>;
 return <View style={{position:'absolute',top:0,bottom:0,left:0,right:0,backgroundColor:colors.windowBackground,padding:18}}><FlatList
  data={wrongAccount?[]:c.rooms.filter(room=>!share.delivery||room.id===share.delivery.roomId)} keyExtractor={room=>room.id} initialNumToRender={12} windowSize={5}
  ListHeaderComponent={<><Text accessibilityRole="header" style={{fontSize:22,color:colors.text.primary}}>{uiText('Поделиться через 67chat')}</Text><Text style={{color:colors.text.secondary,marginVertical:8}}>{uiText('Аккаунт: @')}{c.me.username}{uiText('. Выберите чат и подтвердите отправку.')}</Text>{accountButton}{!!share.text&&<Text numberOfLines={5} style={{color:colors.text.primary,marginVertical:8}}>{share.text}</Text>}{!!share.pendingCount&&share.pendingCount>1&&<Text style={{color:colors.text.secondary}}>{uiText('В очереди входящих отправок: {0}',share.pendingCount)}</Text>}{!!share.delivery&&<Text style={{color:colors.text.secondary}}>{uiText('Продолжение подтверждённой отправки: {0}/{1}. Получатель сохранён.',share.delivery.next,share.delivery.itemCount)}</Text>}{wrongAccount&&<Text style={{color:colors.text.secondary}}>{uiText('Для продолжения выберите исходный аккаунт. Эта отправка уже подтверждена для другого аккаунта.')}</Text>}{!!share.rejectedFiles&&<Text style={{color:colors.text.secondary}}>{uiText('Источник файла отклонён или недоступен. Отмените отправку и выберите файлы заново в приложении-источнике.')}</Text>}{!!(share.error||share.notice)&&<Text style={{color:colors.text.secondary}}>{shareError(share.error||share.notice)}</Text>}{share.uris.length>0&&<Text style={{color:colors.text.secondary}}>{share.uris.length} {uiText('вложений')}</Text>}</>}
  renderItem={({item:room})=><Pressable disabled={busy||!!share.delivery} onPress={()=>setRoomId(room.id)} accessibilityRole="radio" accessibilityState={{selected:roomId===room.id}} style={{padding:13,borderBottomWidth:1,borderBottomColor:colors.divider}}><Text style={{color:colors.text.primary}}>{roomId===room.id?'◉':'○'} {room.title||room.other_display_name||room.other_username||uiText('Чат')}</Text></Pressable>}
  ListFooterComponent={<>{!!status&&<Text style={{color:colors.text.secondary}}>{status}</Text>}<Pressable disabled={busy||!roomId||wrongAccount||!!share.error||!!share.rejectedFiles||!share.text?.trim()&&!share.uris.length} onPress={()=>void confirm()} style={{padding:14}}><Text style={{color:colors.text.blue}}>{share.delivery?uiText('Продолжить отправку исходному получателю'):uiText('Отправить в выбранный чат')}</Text></Pressable><Pressable disabled={busy} onPress={discard} style={{padding:14}}><Text style={{color:colors.text.blue}}>{uiText('Отмена')}</Text></Pressable></>}/></View>;
}
