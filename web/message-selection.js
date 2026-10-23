export function selectLoadedRange(messages,pivot,direction){
 const ids=messages.filter(message=>!message.delivery).map(message=>message.id),index=ids.indexOf(pivot);
 if(direction==='all')return ids;
 if(index<0)return [];
 return direction==='before'?ids.slice(0,index+1):direction==='after'?ids.slice(index):[];
}
