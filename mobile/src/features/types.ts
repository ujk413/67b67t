import type {Message,Room,User} from '../types';
import type {AccountRepository} from '../data/repository';
export type FeatureContext={
 baseUrl:string;token:string;me:User;room:Room|null;rooms:Room[];repository:AccountRepository|null;
 onOpen:(roomId:string,messageId?:string)=>Promise<void>;
 onSend:(roomId:string,text:string,body?:Record<string,unknown>)=>Promise<void>;
 onRefresh:()=>Promise<void>;onClose:()=>void;onMode:(mode:string)=>void;
 decode:(message:Message)=>Promise<Message>;
 buildText?:(roomId:string,text:string)=>Promise<Record<string,unknown>>;
 setDraft?:(roomId:string,text:string)=>Promise<void>|void;
 targetMessage?:Message|null;
 sendCatalogAsset?:(roomId:string,item:{media_id:string;kind:'sticker'|'gif';label?:string;mime?:string})=>Promise<void>;
 exportMedia?:(message:Message)=>Promise<{name:string;mime:string;base64:string}>;
 openContact?:(username:string)=>Promise<void>|void;
};
