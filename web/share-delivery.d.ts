export type ShareJob={id:string;recipientId:string;roomId:string;text:string|null;uris:string[]};
export type ShareDelivery={confirmed:true;recipientId:string;roomId:string;next:number;itemCount:number};
export type ShareJournal={read:(id:string)=>ShareDelivery|null|Promise<ShareDelivery|null>;save:(id:string,state:ShareDelivery)=>void|Promise<void>};
export function confirmShareDelivery(job:ShareJob,store?:ShareJournal):Promise<ShareDelivery>;
export function deliverShare(job:ShareJob,send:(item:{kind:string;value:string},id:string)=>Promise<void>,store?:ShareJournal):Promise<void>;
export function discardShareDelivery(id:string):void;
