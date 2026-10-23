// The transport receives only an encrypted envelope; comparison and conflict
// resolution happen locally. One coordinator belongs to one account.
export const draftValue = value => ({text: String(value?.text || ''), reply: value?.reply || null});
const same = (a,b) => JSON.stringify(draftValue(a)) === JSON.stringify(draftValue(b));
export class DraftSync {
  constructor(deps) { this.d=deps; this.running=new Map(); this.stopped=false; }
  stop(){this.stopped=true;}
  sync(room){
    if(this.stopped)return Promise.resolve();
    if(this.running.has(room))return this.running.get(room);
    const task=this.run(room).finally(()=>this.running.delete(room));this.running.set(room,task);return task;
  }
  async run(room){
    const d=this.d, state=await d.state(room), initial=draftValue(await d.local(room));
    const remote=await d.fetch(room);if(this.stopped)return;
    const remoteValue=remote.encrypted_content ? draftValue(await d.decode(room,remote)) : draftValue(null);
    if(this.stopped)return;
    const current=draftValue(await d.local(room));
    // Do not overwrite text typed while a request or decryption was in flight.
    if(!same(initial,current))return;
    const base=state?.value || draftValue(null),revision=state?.revision || 0;
    if(remote.revision!==revision && !same(current,base) && !same(current,remoteValue)){
      await d.conflict(room,{local:current,remote:remoteValue,revision:remote.revision});return;
    }
    if(same(current,base) || same(current,remoteValue)){
      if(!same(current,remoteValue) && await d.apply(room,remoteValue,current)===false)return;
      if(this.stopped)return;
      await d.saveState(room,{revision:remote.revision,value:remoteValue});return;
    }
    const encrypted_content=current.text || current.reply ? await d.encode(room,current) : null;
    if(this.stopped || !same(current,await d.local(room)))return;
    try{
      const saved=await d.put(room,{revision:remote.revision,encrypted_content});
      if(!this.stopped)await d.saveState(room,{revision:saved.revision,value:current});
    }catch(error){if(error.status!==409)throw error; /* next pass retrieves both versions */}
  }
}
