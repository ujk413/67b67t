const TYPES=new Set(['image','video','voice','file']);
export function isQuietHours(quiet,at=new Date()){
 if(!quiet?.enabled||!/^([01]\d|2[0-3]):[0-5]\d$/.test(quiet.start)||!/^([01]\d|2[0-3]):[0-5]\d$/.test(quiet.end)||quiet.start===quiet.end)return false;
 let parts;try{parts=new Intl.DateTimeFormat('en-GB',{timeZone:quiet.time_zone,hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(at);}catch{return false;}
 const current=Number(parts.find(x=>x.type==='hour')?.value)*60+Number(parts.find(x=>x.type==='minute')?.value),minute=s=>Number(s.slice(0,2))*60+Number(s.slice(3)),start=minute(quiet.start),end=minute(quiet.end);
 return start<end?current>=start&&current<end:current>=start||current<end;
}
export function shouldAutoDownload(policy,type,network){
 if(!TYPES.has(type))return false;
 const link=network==='wifi'?'wifi':network==='cellular'||network==='mobile'?'mobile':null;
 return !!link&&policy?.[link]?.[type]===true;
}
export function crashPreview(input){
 const {platform,code,component,app_version,at}=input||{};
 if(!['web','android'].includes(platform)||!['render_failure','startup_failure','sync_failure','media_failure','call_failure','unknown'].includes(code)||!['app','chat','list','media','call','settings','database','network'].includes(component)||typeof app_version!=='string'||!/^[A-Za-z0-9._-]{1,32}$/.test(app_version)||typeof at!=='string'||!Number.isFinite(Date.parse(at)))throw Error('bad_crash_report');
 return{platform,code,component,app_version,at};
}
export async function clearWebMediaCache(){
 if(!globalThis.caches)return 0;
 let count=0;for(const name of await caches.keys()){if(!name.startsWith('67chat-media-'))continue;const cache=await caches.open(name);count+=(await cache.keys()).length;await caches.delete(name);}return count;
}
