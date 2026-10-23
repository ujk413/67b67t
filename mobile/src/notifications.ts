import * as Notifications from "expo-notifications";
import {t as uiText} from '../../web/i18n.js';
import Constants from "expo-constants";
import { AppState, Platform } from "react-native";
import { addNetworkStateListener } from "expo-network";
import { storage } from "./storage";
import { newId } from "./data/repository";
import { mobileApi } from "./mobileApi";
import { requestWithTimeout } from "./requestTimeout";
import { requireOptionalNativeModule } from 'expo';
import {currentTransport,accountRegistry,pushInstallationCapability,installPushTransportHooks} from './accountRegistry';
const replyNative=requireOptionalNativeModule<{consumeQuickReply():{roomId:string;recipientId:string;text:string}|null;consumeMessageOpen?():{roomId:string;recipientId:string}|null;addListener(name:string,callback:()=>void):{remove():void}}> ('RoomKey');
const isQuietHours=(quiet:any,at=new Date())=>{if(!quiet?.enabled||!/^([01]\d|2[0-3]):[0-5]\d$/.test(quiet.start)||!/^([01]\d|2[0-3]):[0-5]\d$/.test(quiet.end)||quiet.start===quiet.end)return false;try{const parts=new Intl.DateTimeFormat('en-GB',{timeZone:quiet.time_zone,hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(at);const value=Number(parts.find(x=>x.type==='hour')?.value)*60+Number(parts.find(x=>x.type==='minute')?.value),minutes=(s:string)=>Number(s.slice(0,2))*60+Number(s.slice(3)),start=minutes(quiet.start),end=minutes(quiet.end);return start<end?value>=start&&value<end:value>=start||value<end;}catch{return false;}};

export type PushStatus = { status: "unconfigured" | "permission" | "connecting" | "enabled" | "error"; message: string };
class PushNotifications {
  private callback: ((data: any) => void) | null = null;
  private quickReply:((value:{roomId:string;recipientId:string;text:string})=>Promise<void>)|null=null;
  private pendingReply:{roomId:string;recipientId:string;text:string}|null=null;
  private token: string | null = null;
  private id: string | null = null;
  private dispose: (() => void) | null = null;
  private activate: ((prompt?: boolean) => Promise<void>) | null = null;
  private refreshRegistration: (() => Promise<void>) | null = null;
  private generation = 0;
  private transportPaused=false;
  private reconnect:(()=>Promise<void>)|null=null;
  private writes: Promise<unknown> = Promise.resolve();
  private opened = new Set<string>();
  private listeners = new Set<(state: PushStatus) => void>();
  private state: PushStatus = { status: "connecting", message: "Подключение уведомлений…" };
  get configured() { return !this.transportPaused&&currentTransport().mode==='direct'&&Platform.OS === "android" && !!Constants.expoConfig?.extra?.fcmConfigured; }
  get status() { return this.state; }
  subscribe(listener: (state: PushStatus) => void) {
    this.listeners.add(listener); listener(this.state);
    return () => { this.listeners.delete(listener); };
  }
  private report(status: PushStatus["status"], message: string) {
    this.state = { status, message };
    for (const listener of this.listeners) listener(this.state);
  }
  async initialize(prompt = true): Promise<string | null> {
    if(this.transportPaused||currentTransport().mode!=='direct'){this.report('unconfigured','Push через Google недоступен в режиме прокси / Tor');return null;}
    if (!this.configured) return null;
    if(typeof Notifications.setNotificationCategoryAsync==='function')await Notifications.setNotificationCategoryAsync('67chat.quick-reply',[{identifier:'67chat.reply',buttonTitle:uiText('Ответить'),textInput:{submitButtonTitle:uiText('Отправить'),placeholder:uiText('Сообщение')},options:{opensAppToForeground:true}}]);
    await Notifications.setNotificationChannelAsync("messages", {
      name: uiText("Сообщения"), importance: Notifications.AndroidImportance.HIGH,
      sound: "default", enableVibrate: true, showBadge: true,
      vibrationPattern: [0, 250, 150, 250],
    });
    let permission = await Notifications.getPermissionsAsync();
    if (permission.status !== "granted" && permission.canAskAgain !== false && prompt)
      permission = await Notifications.requestPermissionsAsync();
    if (permission.status !== "granted") return null;
    const value = await requestWithTimeout(() => Notifications.getDevicePushTokenAsync(), 20000);
    if (typeof value.data !== "string" || !value.data) throw new Error("push_token_missing");
    return value.data;
  }
  async installation() {
    this.id ||= await storage.get("push.installation");
    if (!this.id) { this.id = newId(); await storage.set("push.installation", this.id); }
    return this.id;
  }
  async connect(base: string, auth: string, activeRoomId: () => string | null, userId?:string) {
    this.reconnect=async()=>{await this.connect(base,auth,activeRoomId,userId);};
    this.dispose?.();
    const generation = ++this.generation;
    if (!this.configured) {
      this.report("unconfigured", currentTransport().mode!=='direct'?'Push через Google недоступен в режиме прокси / Tor':"Установите обновление приложения для уведомлений");
      return () => {};
    }
    let stopped = false, foreground = AppState.currentState === "active";
    let initializing: Promise<void> | null = null;
    const current = () => !this.transportPaused && !stopped && generation === this.generation;
    const api = mobileApi(base, auth);
    const register = () => {
      const task = this.writes.catch(() => {}).then(async () => {
        if (!current() || !this.token || currentTransport().mode!=='direct') return;
        const capability=await pushInstallationCapability(base),id=capability.id;
        if (!current()) return;
        const targets=[{baseUrl:base,token:auth,active:true}];
        for(const account of await accountRegistry.list()){
          const saved=await accountRegistry.token(account.id);if(saved&&!targets.some(row=>row.token===saved&&row.baseUrl===account.baseUrl))targets.push({baseUrl:account.baseUrl,token:saved,active:false});
        }
        for(const target of targets){
          if(!current()||currentTransport().mode!=='direct')return;
          await pushInstallationCapability(target.baseUrl);
          const targetApi=mobileApi(target.baseUrl,target.token);
          try{
            const state=await targetApi.pushInstallation(id,capability.secret);
            if(!current()||currentTransport().mode!=='direct'||!this.token)return;
            await targetApi.registerPush(id,this.token,target.active&&foreground?activeRoomId():null,{installation_secret:capability.secret,installation_epoch:state.epoch,resume_push:true});
          }catch(error){if(!target.active&&(error as {status?:number}).status===401)continue;throw error;}
        }
        if (current()) this.report("enabled", "Включены · звук и вибрация");
      });
      this.writes = task.catch(() => {
        if (current()) this.report("error", "Не удалось подключить уведомления. Повторите при подключении к сети.");
      });
      return task;
    };
    const activate = (prompt = false) => {
      if (initializing) return initializing;
      initializing = (async () => {
        try {
          const token = await this.initialize(prompt);
          if (!current()) return;
          this.token = token;
          if (!token) {
            this.report("permission", "Разрешите уведомления в настройках телефона");
            await api.unregisterPush(await this.installation());
            return;
          }
          await register();
        } catch (error) {
          if (current()) this.report("error", "Не удалось подключить уведомления. Повторите при подключении к сети.");
          throw error;
        }
      })().finally(() => { initializing = null; });
      return initializing;
    };
    this.activate = activate;
    this.refreshRegistration = register;
    this.report("connecting", "Подключение уведомлений…");
    Notifications.setNotificationHandler({
      handleNotification: async notification => {
        const data=notification.request.content.data,recipient=typeof data?.recipient_id==='string'?data.recipient_id:typeof data?.recipientId==='string'?data.recipientId:null;
        const active = current() && foreground && (!recipient||recipient===userId) && !!activeRoomId() &&
          notification.request.content.data?.room_id === activeRoomId();
        let quiet=false;try{const owner=recipient||userId,saved=owner?await storage.get(`quality.quiet:${owner}`):null;quiet=!!saved&&isQuietHours(JSON.parse(saved));}catch{}
        if(notification.request.content.data?.call_event)quiet=false;
        return { shouldPlaySound: !active&&!quiet, shouldSetBadge: true, shouldShowBanner: !active&&!quiet, shouldShowList: !active&&!quiet };
      },
    });
    const open = (response: Notifications.NotificationResponse) => {
      if (!current()) return;
      const key = response.notification.request.identifier;
      if (this.opened.has(key)) return;
      if (this.opened.size >= 128) this.opened.delete(this.opened.values().next().value!);
      this.opened.add(key);
      if(response.actionIdentifier==='67chat.reply'&&response.userText){
        const data=response.notification.request.content.data;
        if(typeof data?.room_id==='string'&&typeof data?.recipient_id==='string'&&/^[a-f0-9-]{36}$/i.test(data.room_id)&&/^[a-f0-9-]{36}$/i.test(data.recipient_id)){
          this.pendingReply={roomId:data.room_id,recipientId:data.recipient_id,text:response.userText.slice(0,5000)};
          this.flushQuickReply();
        }
        void Notifications.clearLastNotificationResponseAsync().catch(()=>{});
        return;
      }
      this.callback?.(response.notification.request.content.data);
      void Notifications.clearLastNotificationResponseAsync().catch(() => {});
    };
    const responses = Notifications.addNotificationResponseReceivedListener(open);
    const tokens = Notifications.addPushTokenListener(value => {
      if (!current() || typeof value.data !== "string") return;
      this.token = value.data;
      void register().catch(() => {});
    });
    void activate(true).catch(() => {});
    const appState = AppState.addEventListener("change", state => {
      foreground = state === "active";
      if (foreground) { void activate(false).catch(() => {});this.consumeNativeReply();this.consumeNativeOpen(); }
      else void register().catch(() => {});
    });
    const replyListener=replyNative?.addListener('quickReply',()=>this.consumeNativeReply());
    const openListener=replyNative?.addListener('messageOpen',()=>this.consumeNativeOpen());
    this.consumeNativeReply();
    this.consumeNativeOpen();
    const network = addNetworkStateListener(state => {
      if (state.isConnected && foreground && current()) void activate(false).catch(() => {});
    });
    const timer = setInterval(() => { if (foreground && current()) void activate(false).catch(() => {}); }, 30000);
    const dispose = () => {
      stopped = true; responses.remove(); tokens.remove(); appState.remove(); network.remove(); replyListener?.remove(); openListener?.remove();clearInterval(timer);
      if (generation === this.generation) { this.activate = null; this.refreshRegistration = null; }
    };
    this.dispose = dispose;
    void Notifications.getLastNotificationResponseAsync().then(initial => { if (initial) open(initial); }).catch(() => {});
    return dispose;
  }
  async refresh() { await this.refreshRegistration?.(); }
  async retry() { await this.activate?.(true); }
  async test(base: string, auth: string) {
    await this.retry();
    if (this.state.status !== "enabled") throw new Error(this.state.message);
    const result = await mobileApi(base, auth).testPush();
    if (!result.sent) throw new Error("Проверочное уведомление не отправлено. Повторите подключение.");
  }
  async disconnect(base: string, auth: string,strict=false,retain=false) {
    const priorToken=this.token;this.reconnect=null;
    this.dispose?.(); this.dispose = null; ++this.generation;
    this.activate = null; this.refreshRegistration = null; this.token = null;
    this.quickReply=null;this.pendingReply=null;
    await this.writes.catch(() => {});
    if(retain){if(priorToken&&!this.transportPaused&&currentTransport().mode==='direct'){const cap=await pushInstallationCapability(base),api=mobileApi(base,auth),state=await api.pushInstallation(cap.id,cap.secret);if(!this.transportPaused&&currentTransport().mode==='direct')await api.registerPush(cap.id,priorToken,null,{installation_secret:cap.secret,installation_epoch:state.epoch,resume_push:true});}}
    else if(strict)await mobileApi(base,auth).unregisterPush(await this.installation());
    else await mobileApi(base, auth).unregisterPush(await this.installation()).catch(() => {});
    await Notifications.setBadgeCountAsync(0).catch(() => {});
  }
  async pauseForTransport(){
    this.transportPaused=true;
    this.dispose?.();this.dispose=null;++this.generation;
    this.activate=null;this.refreshRegistration=null;this.token=null;
    await this.writes.catch(()=>{});
  }
  async resumeAfterTransportFailure(){this.transportPaused=false;await this.reconnect?.();}
  getFCMToken() { return this.token; }
  private flushQuickReply(){const item=this.pendingReply,handler=this.quickReply,generation=this.generation;if(!item||!handler)return;this.pendingReply=null;void handler(item).catch(()=>{if(this.generation===generation&&this.quickReply===handler&&!this.pendingReply)this.pendingReply=item;});}
  private consumeNativeOpen(){try{const item=replyNative?.consumeMessageOpen?.();if(item&&/^[a-f0-9-]{36}$/i.test(item.roomId)&&/^[a-f0-9-]{36}$/i.test(item.recipientId))this.callback?.({room_id:item.roomId,recipient_id:item.recipientId});}catch{}}
  private consumeNativeReply(){try{const item=replyNative?.consumeQuickReply();if(item&&/^[a-f0-9-]{36}$/i.test(item.roomId)&&/^[a-f0-9-]{36}$/i.test(item.recipientId)&&item.text.trim()){this.pendingReply={...item,text:item.text.slice(0,5000)};this.flushQuickReply();}}catch{}}
  onQuickReply(handler:((value:{roomId:string;recipientId:string;text:string})=>Promise<void>)|null){this.quickReply=handler;this.flushQuickReply();}
  onMessage(callback: (data: any) => void) { this.callback = callback; }
  handleMessage(data: any) { this.callback?.(data); }
  async showLocalNotification() {
    await Notifications.scheduleNotificationAsync({
      content: { title: "67chat", body: "Новое сообщение", sound: "default" },
      trigger: { channelId: "messages" },
    });
  }
  async setBadgeCount(count = 0) { if (this.configured) await Notifications.setBadgeCountAsync(count); }
  async clearBadge() { await this.setBadgeCount(0); }
}
export const pushNotifications = new PushNotifications();
installPushTransportHooks({pause:()=>pushNotifications.pauseForTransport(),resume:()=>pushNotifications.resumeAfterTransportFailure()});
