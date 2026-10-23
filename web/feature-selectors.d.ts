export type SelectorRow=Record<string,any>;
export class SecretPeerSelector {
 constructor(request:(path:string)=>Promise<SelectorRow[]>,current:()=>boolean);
 users:SelectorRow[];devices:SelectorRow[];peer:SelectorRow|null;device:SelectorRow|null;
 reset():void;guard(owner?:number):void;
 search(query:string):Promise<SelectorRow[]>;selectUser(id:string):Promise<SelectorRow[]>;selectDevice(id:string):void;
 payload(deviceId:string,ttl:number|string):{peer_id:string;peer_device_id:string;device_id:string;ttl:number};
}
export function peerLabel(user:SelectorRow|null|undefined):string;
export function deviceLabel(device:SelectorRow,index?:number):string;
export function secretChatLabels(chats:SelectorRow[],me:string,request:(path:string)=>Promise<any>,current:()=>boolean):Promise<SelectorRow[]>;
export function selectedFeatureMessage(context:{targetMessage?:SelectorRow|null;room?:SelectorRow|null},mode:string):SelectorRow|null;
