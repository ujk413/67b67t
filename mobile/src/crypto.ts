import { requireOptionalNativeModule } from 'expo';
import { storage } from './storage';
import { request } from './api';

type Cipher = {type:number;body:string;sender_device_id?:string;recipient_device_id?:string};
const native = requireOptionalNativeModule<{
  signalBundleAsync(user:string,count:number):Promise<string>;
  signalHasSessionAsync(user:string,peer:string,device:string):Promise<boolean>;
  signalSessionAsync(user:string,peer:string,device:string,bundle:string):Promise<void>;
  signalEncryptAsync(user:string,peer:string,device:string,text:string):Promise<string>;
  signalDecryptAsync(user:string,peer:string,device:string,cipher:string):Promise<string>;
}>('RoomKey');
export async function getMobileDeviceId(userId:string) {
  const key = `mobile.e2eDeviceId.${userId}`, existing = await storage.get(key);
  if (existing && /^\d+$/.test(existing)) return existing;
  const bytes = crypto.getRandomValues(new Uint32Array(1));
  const generated = String((bytes[0] % 16380) + 2);
  await storage.set(key, generated); return generated;
}
export class E2EEncryption {
  private user = '';
  private options = {base:'',token:''};
  private sessions = new Map<string,Promise<void>>();
  configure(base:string,token:string) { this.options={base,token}; }
  async initialize(user:string) { if (!native) throw Error('Установите новую APK для шифрованных групп'); this.user=user; }
  async getPublicKeys(count=20) {
    if (!native || !this.user) throw Error('Ключи ещё загружаются');
    return JSON.parse(await native.signalBundleAsync(this.user,count));
  }
  async encrypt(peer:string,text:string,device?:string):Promise<Cipher> {
    if (!native || !this.user) throw Error('Ключи ещё загружаются');
    const user=this.user, {base,token}=this.options;
    let target=device;
    if (!target) { const devices=await request<{device_id:string}[]>(base,`/e2e/devices/${peer}`,{token}); target=devices[0]?.device_id; }
    if (!target) throw Error('У участника пока нет ключей шифрования. Нужно обновить его приложение.');
    const id=peer+'|'+target;
    if (!await native.signalHasSessionAsync(user,peer,target)) {
      let task=this.sessions.get(id);
      if (!task) {
        task=(async()=>{ const bundle=await request(base,`/e2e/keys/${peer}?device_id=${encodeURIComponent(target!)}`,{token});
          await native.signalSessionAsync(user,peer,target!,JSON.stringify(bundle)); })();
        this.sessions.set(id,task); task.finally(()=>this.sessions.delete(id)).catch(()=>{});
      }
      await task;
    }
    return {...JSON.parse(await native.signalEncryptAsync(user,peer,target,text)),sender_device_id:await getMobileDeviceId(user),recipient_device_id:target};
  }
  async decrypt(peer:string,cipher:Cipher):Promise<string> {
    if (!native || !this.user) throw Error('Ключи ещё загружаются');
    let device=cipher.sender_device_id;
    if (!device) { const {base,token}=this.options; const devices=await request<{device_id:string}[]>(base,`/e2e/devices/${peer}`,{token}); device=devices[0]?.device_id || '1'; }
    // A pre-key message creates the inbound session. Fetching a peer bundle here would consume an unrelated one-time key.
    return native.signalDecryptAsync(this.user,peer,device,JSON.stringify(cipher));
  }
}
export const e2e = new E2EEncryption();
