import { E2EEncryption, getMobileDeviceId } from './crypto';
import { request, api } from './api';
import type { AccountRepository } from './data/repository';
import type { Message } from './types';
import * as ratchet from './groupRatchet';

// Serialize ratchets per account. Concurrent history decoding must never consume the same chain twice.
export class GroupEncryption {
  private signal = new E2EEncryption();
  private sequence:Promise<unknown> = Promise.resolve();
  private readyTask:Promise<void>|null=null;
  constructor(private user:string,private base:string,private token:string,private repository:()=>AccountRepository|null) { this.signal.configure(base,token); }
  private get repo() { const repo=this.repository(); if (!repo || repo.userId!==this.user) throw Error('Хранилище ещё загружается'); return repo; }
  private call<T>(path:string,method='GET',body?:unknown) { return request<T>(this.base,path,{token:this.token,method,body}); }
  private run<T>(fn:()=>Promise<T>):Promise<T> { const task=this.sequence.then(fn); this.sequence=task.catch(()=>{}); return task; }
  private ready() {
    if (!this.readyTask) this.readyTask=(async()=>{ await this.signal.initialize(this.user); const device=await getMobileDeviceId(this.user);
      const count=await api.e2ePrekeyCount(this.base,this.token,device);
      if (count.unused<10) await api.uploadE2EKeys(this.base,this.token,device,await this.signal.getPublicKeys(40));
    })().catch(error=>{this.readyTask=null;throw error;});
    return this.readyTask;
  }
  private key(room:string,suffix:string) { return `group:${room}:${suffix}`; }
  async enabled(room:string) {
    try { const result=await api.roomE2e(this.base,this.token,room); await this.repo.set(`e2e:${room}`,result); return result.enabled; }
    catch(error) { const cached=await this.repo.get<{enabled:boolean}|null>(`e2e:${room}`,null); if (!cached) throw error; return cached.enabled; }
  }
  async inbox(room:string) { return this.run(()=>this.importKeys(room)); }
  private async importKeys(room:string) {
    await this.ready(); const device=await getMobileDeviceId(this.user);
    for (let page=0;page<20;page++) {
      const rows=await this.call<{id:string;from_user_id:string;payload:string}[]>(`/rooms/${room}/sender-keys?device_id=${device}`); let imported=0;
      for (const row of rows) {
        try {
          const cipher=JSON.parse(row.payload), pending=this.key(room,'distribution:'+row.id);
          // Persist a successfully decrypted Signal distribution before acknowledging the server.
          let distribution=await this.repo.get<any>(pending,null);
          if (!distribution) { distribution=JSON.parse(await this.signal.decrypt(row.from_user_id,cipher)); await this.repo.set(pending,distribution); }
          if (distribution.room_id!==room) continue;
          const initial=ratchet.receiverStateFromDistribution(distribution), key=this.key(room,`in:${row.from_user_id}:${initial.keyId}`);
          if (!await this.repo.get(key,null)) await this.repo.set(key,{initial,current:initial});
          await this.call(`/rooms/${room}/sender-keys/${row.id}?device_id=${device}`,'DELETE'); imported++;
        } catch(error) { console.warn('Ключ группы пока недоступен', error instanceof Error ? error.message : 'Ошибка'); }
      }
      if (rows.length<200 || !imported) break;
    }
  }
  encrypt(room:string,text:string) { return this.run(async()=>{
    await this.ready(); const members:string[]=[];
    for (let offset=0;;offset+=200) { const page=await this.call<{id:string}[]>(`/rooms/${room}/members?limit=200&offset=${offset}`);members.push(...page.map(m=>String(m.id)));if(page.length<200)break; }
    const myDevice=await getMobileDeviceId(this.user), targets:{user:string;device:string}[]=[];
    for (const user of members) { const devices=await this.call<{device_id:string}[]>(`/e2e/devices/${user}`);
      if (!devices.length) throw Error('Участнику нужно обновить приложение для шифрованной группы');
      targets.push(...devices.filter(d=>user!==this.user || d.device_id!==myDevice).map(d=>({user,device:d.device_id})));
    }
    const tokens=targets.map(t=>t.user+'|'+t.device);let own=await this.repo.get<ratchet.SenderState|null>(this.key(room,'own'),null);
    if (!own || own.n>=1000 || own.distributedTo.some(token=>!tokens.includes(token))) own=await ratchet.createSenderKey();
    const recipients=[];
    for (const target of targets.filter(t=>!own!.distributedTo.includes(t.user+'|'+t.device))) {
      recipients.push({user_id:target.user,device_id:target.device,payload:JSON.stringify(await this.signal.encrypt(target.user,JSON.stringify({...ratchet.distributionFor(own),room_id:room}),target.device))});
    }
    if(recipients.length) await this.call(`/rooms/${room}/sender-keys`,'POST',{key_id:own.keyId,recipients});
    own.distributedTo=tokens;
    await this.repo.set(this.key(room,`own:${own.keyId}`),own);
    const result=await ratchet.encryptWithState(own,room,this.user,text), encrypted_content=JSON.stringify(result.envelope);
    await this.repo.set(this.key(room,'own'),result.state);await this.repo.set(this.key(room,`own:${own.keyId}`),result.state);
    await this.repo.set(this.key(room,'plain:'+encrypted_content),text);
    return {type:'text',text:null,encrypted_content,meta:{e2e_group:true,v:1}};
  }); }
  decrypt(message:Message) { return this.run(async()=>{
    const room=message.room_id, content=message.encrypted_content!;
    const cached=await this.repo.get<string|null>(this.key(room,'plain:'+content),null);if(cached!==null)return cached;
    const envelope=JSON.parse(content);const key=this.key(room,`in:${message.sender_id}:${envelope.kid}`);
    let state=await this.repo.get<{initial:ratchet.ReceiverState;current:ratchet.ReceiverState}|null>(key,null);
    const own=message.sender_id===this.user ? await this.repo.get<ratchet.SenderState|null>(this.key(room,`own:${envelope.kid}`),null):null;
    if (!state && !own) {await this.importKeys(room);state=await this.repo.get(key,null);}
    if (!state && !own) throw Error('Ключ ещё не получен. Подключите устройство отправителя и повторите.');
    let result;
    if (own) result=await ratchet.decryptWithState(ratchet.ownReceiverState(own),room,message.sender_id,envelope);
    else {try {result=await ratchet.decryptWithState(state!.current,room,message.sender_id,envelope);} catch(error) {result=await ratchet.decryptWithState(state!.initial,room,message.sender_id,envelope);}
      if (result.state.n>=state!.current.n) await this.repo.set(key,{...state,current:result.state}); }
    await this.repo.set(this.key(room,'plain:'+content),result.plaintext);return result.plaintext;
  }); }
}
