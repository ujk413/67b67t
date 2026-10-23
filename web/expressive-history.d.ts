import type {PayloadMessage} from './expressive-payloads.js';
export function fetchAllRoomMessages<T extends PayloadMessage>(roomId:string,fetchPage:(roomId:string,before:string|null,limit:number)=>Promise<{messages:T[];has_older:boolean}>,options?:{cancelled?:()=>boolean;maxPages?:number}):Promise<T[]>;
