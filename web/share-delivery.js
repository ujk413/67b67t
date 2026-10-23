const deliveries=new Map(),inFlight=new Set();
const memory={read:id=>deliveries.get(id)||null,save:async(id,state)=>{deliveries.set(id,state);}};
const itemsFor=job=>[...(job.text?.trim()?[{kind:'text',value:job.text}]:[]),...job.uris.map(value=>({kind:'file',value}))];
function validate(job,state){
 if(!state?.confirmed)throw Error('share_confirmation_required');
 if(state.recipientId!==job.recipientId||state.roomId!==job.roomId)throw Error('share_target_changed');
 const count=itemsFor(job).length;
 if(state.itemCount!==count||!Number.isInteger(state.next)||state.next<0||state.next>count)throw Error('share_journal_invalid');
}
/** Only an explicit confirmation may create a sendable journal. Never retarget it. */
export async function confirmShareDelivery(job,store=memory){
 const existing=await store.read(job.id);if(existing){validate(job,existing);return existing;}
 const state={confirmed:true,recipientId:job.recipientId,roomId:job.roomId,next:0,itemCount:itemsFor(job).length};
 await store.save(job.id,state);return state;
}
/** Persist after each durable outbox registration. A crash replays only the same idempotency ID. */
export async function deliverShare(job,send,store=memory){
 if(inFlight.has(job.id))throw Error('share_delivery_busy');inFlight.add(job.id);
 try{
  let state=await store.read(job.id);validate(job,state);
  const items=itemsFor(job);
  for(let index=state.next;index<items.length;index++){
   await send(items[index],`${job.id}_${index}`);
   state={...state,next:index+1};await store.save(job.id,state);
  }
 }finally{inFlight.delete(job.id);}
}
export function discardShareDelivery(id){deliveries.delete(id);}
