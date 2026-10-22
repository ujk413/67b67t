import { requireOptionalNativeModule } from 'expo';
const native=requireOptionalNativeModule<{
  pendingCallAsync():Promise<string|null>;
  clearCallAsync(id:string):Promise<void>;
  startCallServiceAsync(video:boolean):Promise<void>;
  stopCallServiceAsync():Promise<void>;
  speakerAsync(speaker:boolean):Promise<void>;
  addListener(name:string,callback:()=>void):{remove():void};
}>('RoomKey');
export const nativeCalls={
  pending:async()=>{const value=await native?.pendingCallAsync();return value ? JSON.parse(value):null;},
  clear:(id:string)=>native?.clearCallAsync(id),
  start:(video:boolean)=>native?.startCallServiceAsync(video),
  stop:()=>native?.stopCallServiceAsync(),
  speaker:(speaker:boolean)=>native?.speakerAsync(speaker),
  listen:(callback:()=>void)=>native?.addListener('callAction',callback),
};
