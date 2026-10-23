/** Reads every authorized /history page with its stable (created_at,id) cursor. */
export async function fetchAllRoomMessages(roomId,fetchPage,{cancelled=()=>false,maxPages=10000}={}){
 const messages=[],seen=new Set();let before=null;
 for(let page=0;page<maxPages;page++){
  if(cancelled())throw Error('export_cancelled');
  const response=await fetchPage(roomId,before,100),rows=response?.messages;
  if(!Array.isArray(rows)||typeof response.has_older!=='boolean'||rows.length>100)throw Error('export_bad_page');
  for(const row of rows){if(row.room_id!==roomId||typeof row.id!=='string')throw Error('export_bad_page');if(!seen.has(row.id)){seen.add(row.id);messages.push(row);}}
  if(!response.has_older)return messages;
  const next=rows[0]?.id;
  if(!next||next===before)throw Error('export_pagination_stalled');
  before=next;
 }
 throw Error('export_too_large');
}
