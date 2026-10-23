type Row={id:string;room_id:string;[key:string]:any};
type Loader={<T extends Row>(options:{key:string;version:string;roomId:string;retry?:boolean;fetchPage:(room:string,before:string|null,limit:number)=>Promise<{messages:T[];has_older:boolean}>;decode:(message:T)=>Promise<T>;retain?:(message:T)=>boolean;onProgress?:(count:number)=>void}):{promise:Promise<T[]>;cancel:()=>void};removeMessages(prefix:string,ids:string[]):void;has(key:string):boolean;clear(prefix?:string):void;prune(prefix:string,rooms:string[]):void;reconcile<T extends Row>(key:string,changes:{upserts?:T[];removed?:string[]}):void;subscribe<T extends Row>(key:string,fn:(rows:T[]|null,error?:Error)=>void):()=>void};
export function createExpressiveHistoryLoader(options?:{maxMessages?:number;maxBytes?:number;maxRooms?:number}):Loader;
export const expressiveHistoryLoader:Loader;
export function expressiveHistoryPrefix(user:string,base:string,session:string):string;
export function expressiveHistoryKey(user:string,base:string,session:string,room:string):string;
