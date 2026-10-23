import type { Message } from "../types";
const messageOrder=(a:Message,b:Message)=>a.created_at.localeCompare(b.created_at)||a.id.localeCompare(b.id);
const clientKey=(m:Message)=>JSON.stringify([m.sender_id,m.client_message_id]);
/** One pass through IDs and client acknowledgements, then chronological sort. */
export function mergeMessages(...groups: Message[][]) {
  const map=new Map<string,Message>(),clients=new Map<string,string>();
  for(const group of groups)for(const message of group){
    const old=map.get(message.id);if(old?.client_message_id&&clients.get(clientKey(old))===old.id)clients.delete(clientKey(old));
    if(message.client_message_id){const key=clientKey(message),prior=clients.get(key);if(prior&&prior!==message.id)map.delete(prior);clients.set(key,message.id);}
    map.set(message.id,message);
  }
  return [...map.values()].sort(messageOrder);
}
export type WindowDirection='incoming'|'older'|'newer';
export function mergeMessageWindow(previous:Message[],incoming:Message[],{direction='incoming',atBottom=true,hasNewer=false,limit=300}:{direction?:WindowDirection;atBottom?:boolean;hasNewer?:boolean;limit?:number}={}){
  const knownIds=new Set(previous.map(row=>row.id)),knownClients=new Set(previous.filter(row=>row.client_message_id).map(clientKey));
  const holdGap=direction==='incoming'&&hasNewer&&previous.length>0;
  const accepted=holdGap?incoming.filter(row=>knownIds.has(row.id)||!!row.client_message_id&&knownClients.has(clientKey(row))):incoming;
  const all=mergeMessages(previous,accepted);let start=0;
  if(all.length>limit){
    if(direction==='older')start=0;
    else if(direction==='newer'||atBottom||!previous.length)start=all.length-limit;
    else{
      const ids=new Set(previous.map(row=>row.id)),clients=new Set(previous.filter(row=>row.client_message_id).map(clientKey));
      start=Math.max(0,all.findIndex(row=>ids.has(row.id)||!!row.client_message_id&&clients.has(clientKey(row))));
      start=Math.min(start,all.length-limit);
    }
  }
  return{messages:all.slice(start,start+limit),hasOlder:start>0,hasNewer:holdGap||start+limit<all.length};
}
type Arrival={id:string;sender_id:string;created_at:string};
type WindowState={atBottom:boolean;hasOlder:boolean;hasNewer:boolean;unseen:Map<string,Arrival>;seenIncoming:Set<string>;unseenCapped:boolean};
const windows=new Map<string,WindowState>();
function windowFor(room:string){let value=windows.get(room);if(!value){value={atBottom:true,hasOlder:false,hasNewer:false,unseen:new Map(),seenIncoming:new Set(),unseenCapped:false};windows.set(room,value);while(windows.size>12)windows.delete(windows.keys().next().value!);}return value;}
export function messageWindowState(room:string){return windowFor(room);}
export function messageWindowUnseenCount(room:string,meId:string){return [...windowFor(room).unseen.values()].filter(row=>row.sender_id!==meId).length;}
/** Consume only arrivals whose chronological position the viewport has reached.
 * Passing no cursor is reserved for the contiguous latest edge, never a load-next tap. */
export function consumeMessageWindowUnseen(room:string,through?:Message){
 const state=windowFor(room);let changed=false;
 for(const [key,row] of state.unseen)if(!through||messageOrder(row as Message,through)<=0){state.unseen.delete(key);changed=true;}
 if(!through&&state.unseenCapped){state.unseenCapped=false;changed=true;}return changed;
}
export function setMessageViewport(room:string,atBottom:boolean){windowFor(room).atBottom=atBottom;}
export function clearMessageWindows(room?:string){if(room)windows.delete(room);else windows.clear();}
export function updateMessageWindowPage(room:string,page:{has_older?:boolean;has_newer?:boolean},direction:WindowDirection|'replace'='replace'){
 const state=windowFor(room);if(direction!=='newer'&&page.has_older!==undefined)state.hasOlder=page.has_older;if(direction!=='older'&&page.has_newer!==undefined)state.hasNewer=page.has_newer;
}
export function mergeDisplayWindow(previous:Message[],incoming:Message[],direction:WindowDirection='incoming'){
 const room=incoming[0]?.room_id||previous[0]?.room_id;if(!room)return [];
 const state=windowFor(room),wasGap=state.hasNewer;
 if(direction==='incoming'&&previous.length){
   const ids=new Set(previous.map(row=>row.id)),clients=new Set(previous.filter(row=>row.client_message_id).map(clientKey)),edge=previous.filter(row=>!row.delivery).at(-1)||previous.at(-1)!;
   for(const row of incoming){
     const key=row.client_message_id?clientKey(row):row.id;
     if(ids.has(row.id)||row.client_message_id&&clients.has(clientKey(row))||messageOrder(row,edge)<=0||state.seenIncoming.has(key))continue;
     state.seenIncoming.add(key);if(state.seenIncoming.size>20000)state.seenIncoming.delete(state.seenIncoming.values().next().value!);
     if(!state.atBottom||wasGap){state.unseen.set(key,{id:row.id,sender_id:row.sender_id,created_at:row.created_at});if(state.unseen.size>20000){state.unseen.delete(state.unseen.keys().next().value!);state.unseenCapped=true;}}
   }
 }
 const result=mergeMessageWindow(previous,incoming,{direction,atBottom:state.atBottom&&!wasGap,hasNewer:wasGap});
 state.hasOlder=direction==='older'?result.hasOlder:state.hasOlder||result.hasOlder;
 state.hasNewer=direction==='newer'?result.hasNewer:state.hasNewer||result.hasNewer;
 return result.messages;
}
export function mergeDisplayMessages(...groups:Message[][]){
 return mergeDisplayWindow(groups[0]||[],groups.slice(1).flat());
}
export function messageStatus(m: Message) {
  return m.delivery === "failed"
    ? "Ошибка"
    : m.delivery === "sending"
      ? "Отправляется…"
      : m.delivery === "queued"
        ? "В очереди"
        : m.read_by?.some((id) => id !== m.sender_id)
          ? "Прочитано"
          : "Отправлено";
}
