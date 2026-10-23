import * as SecureStore from 'expo-secure-store';
import {request} from '../api';
import type {AuthResponse} from '../types';
const KEY='67chat.pending-auth-revocations.v1';
type Pending={baseUrl:string;userId:string;token:string};
let sequence:Promise<unknown>=Promise.resolve();
const serialized=<T>(work:()=>Promise<T>)=>{const result=sequence.then(work);sequence=result.catch(()=>{});return result;};
async function revoke(row:Pending){await request(row.baseUrl,'/auth/logout',{method:'POST',token:row.token,body:{}});}
export async function settleMobileLogin(baseUrl:string,session:AuthResponse,isCurrent:()=>boolean,accept:(session:AuthResponse)=>Promise<void>){
 if(!session.token||!session.user?.id)throw Error('Сервер не подтвердил вход');
 if(isCurrent()){await accept(session);return true;}
 const row={baseUrl,userId:session.user.id,token:session.token};
 // Keep revocation capability across a network outage or process death.
 await serialized(async()=>{const rows:Pending[]=JSON.parse(await SecureStore.getItemAsync(KEY)||'[]');if(!rows.some(p=>p.token===row.token))rows.push(row);await SecureStore.setItemAsync(KEY,JSON.stringify(rows),{keychainAccessible:SecureStore.WHEN_UNLOCKED_THIS_DEVICE_ONLY});});
 await retryDetachedLogins();return false;
}
export function retryDetachedLogins(){return serialized(async()=>{const rows:Pending[]=JSON.parse(await SecureStore.getItemAsync(KEY)||'[]'),remaining:Pending[]=[];for(const row of rows){try{await revoke(row);}catch(error){if((error as {status?:number}).status!==401)remaining.push(row);}}if(remaining.length)await SecureStore.setItemAsync(KEY,JSON.stringify(remaining));else await SecureStore.deleteItemAsync(KEY);});}
export function forgetDetachedLogins(userId:string){return serialized(async()=>{const rows:Pending[]=JSON.parse(await SecureStore.getItemAsync(KEY)||'[]'),keep=rows.filter(row=>row.userId!==userId);if(keep.length)await SecureStore.setItemAsync(KEY,JSON.stringify(keep));else await SecureStore.deleteItemAsync(KEY);});}
