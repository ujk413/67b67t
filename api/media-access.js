export async function canAccessMedia(pool,media,userId,{canReadCatalogMedia}={}){
 if(media.owner_id===userId)return true;
 const {rows}=await pool.query(`SELECT 1 WHERE
 EXISTS(SELECT 1 FROM users WHERE avatar_id=$1) OR
 EXISTS(SELECT 1 FROM rooms r WHERE r.avatar_id=$1 AND EXISTS(SELECT 1 FROM room_members rm WHERE rm.room_id=r.id AND rm.user_id=$2)) OR
 EXISTS(SELECT 1 FROM messages m JOIN room_members rm ON rm.room_id=m.room_id AND rm.user_id=$2
 WHERE m.media_id=$1 AND NOT EXISTS(SELECT 1 FROM hidden_messages h WHERE h.user_id=$2 AND h.message_id=m.id))`,[media.id,userId]);
 return rows.length>0 || !!(canReadCatalogMedia&&await canReadCatalogMedia(media.id,userId));
}
/** A single HTTP byte range, including suffix ranges; malformed ranges fail closed. */
export function mediaByteRange(header,size){
 if(!header)return null;
 const match=/^bytes=(\d*)-(\d*)$/.exec(String(header));if(!match||!Number.isSafeInteger(size)||size<=0||!match[1]&&!match[2])return false;
 let start,end;
 if(!match[1]){const suffix=Number(match[2]);if(!Number.isSafeInteger(suffix)||suffix<=0)return false;start=Math.max(0,size-suffix);end=size-1;}
 else {start=Number(match[1]);end=match[2]?Number(match[2]):size-1;if(!Number.isSafeInteger(start)||!Number.isSafeInteger(end)||start>=size||end<start)return false;end=Math.min(end,size-1);}
 return {start,end,length:end-start+1};
}
