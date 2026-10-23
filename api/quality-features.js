import {readFile} from 'node:fs/promises';
import {randomUUID} from 'node:crypto';

const DEFAULT_DOWNLOAD={wifi:{image:true,video:false,voice:true,file:false},mobile:{image:false,video:false,voice:false,file:false}};
const fail=(error,statusCode=400)=>{throw Object.assign(Error(error),{statusCode});};
const plain=x=>!!x&&typeof x==='object'&&!Array.isArray(x);
const exact=(x,required,optional=[])=>plain(x)&&required.every(k=>Object.hasOwn(x,k))&&Object.keys(x).every(k=>required.includes(k)||optional.includes(k));
const time=x=>typeof x==='string'&&/^([01]\d|2[0-3]):[0-5]\d$/.test(x);
const bool=x=>typeof x==='boolean';
const zones=new Map();
function zone(value){if(typeof value!=='string'||value.length>80||!/^[-A-Za-z0-9_+/]+$/.test(value))return false;if(zones.has(value))return zones.get(value);let valid=true;try{new Intl.DateTimeFormat('en-US',{timeZone:value}).format();}catch{valid=false;}zones.set(value,valid);return valid;}
const minute=x=>Number(x.slice(0,2))*60+Number(x.slice(3));
export function isQuietNow(quiet,at=new Date()){
 if(!quiet?.enabled||!time(quiet.start)||!time(quiet.end)||!zone(quiet.time_zone))return false;
 const parts=new Intl.DateTimeFormat('en-GB',{timeZone:quiet.time_zone,hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(at);
 const local=Number(parts.find(p=>p.type==='hour')?.value)*60+Number(parts.find(p=>p.type==='minute')?.value),start=minute(quiet.start),end=minute(quiet.end);
 return start<end?local>=start&&local<end:start>end&&(local>=start||local<end);
}
export async function readQualityPreference(pool,userId){let row;try{row=(await pool.query('SELECT * FROM quality_preferences WHERE user_id=$1',[userId])).rows[0];}catch(error){if(error.code!=='42P01')throw error;}return row?{language:row.language,quiet:{enabled:row.quiet_enabled,start:row.quiet_start,end:row.quiet_end,time_zone:row.time_zone},group_notifications:row.group_notifications,download:row.download_policy,cache_max_mb:row.cache_max_mb,wallpaper:row.wallpaper,accent:row.accent,crash_consent:row.crash_consent}:{language:'ru',quiet:{enabled:false,start:'22:00',end:'08:00',time_zone:'Europe/Moscow'},group_notifications:true,download:DEFAULT_DOWNLOAD,cache_max_mb:256,wallpaper:'plain',accent:'emerald',crash_consent:false};}
export async function userInQuietHours(pool,userId,at=new Date()){try{return isQuietNow((await readQualityPreference(pool,userId)).quiet,at);}catch(error){if(error.code==='42P01')return false;throw error;}}
function validatePreference(p){
 if(!exact(p,['language','quiet','group_notifications','download','cache_max_mb','wallpaper','accent','crash_consent']))fail('bad_quality_preference');
 if(!['ru','en'].includes(p.language)||!exact(p.quiet,['enabled','start','end','time_zone'])||!bool(p.quiet.enabled)||!time(p.quiet.start)||!time(p.quiet.end)||!zone(p.quiet.time_zone)||(p.quiet.enabled&&p.quiet.start===p.quiet.end))fail('bad_quiet_hours');
 if(!bool(p.group_notifications)||!bool(p.crash_consent)||!Number.isInteger(p.cache_max_mb)||p.cache_max_mb<64||p.cache_max_mb>2048||!['plain','dots','mesh'].includes(p.wallpaper)||!['emerald','blue','violet'].includes(p.accent))fail('bad_quality_preference');
 if(!exact(p.download,['wifi','mobile']))fail('bad_download_policy');for(const network of ['wifi','mobile']){const row=p.download[network];if(!exact(row,['image','video','voice','file'])||Object.values(row).some(v=>!bool(v)))fail('bad_download_policy');}
 return p;
}
const CODES=new Set(['render_failure','startup_failure','sync_failure','media_failure','call_failure','unknown']);
const COMPONENTS=new Set(['app','chat','list','media','call','settings','database','network']);
function validateCrash(x){if(!exact(x,['platform','code','component','app_version','at'])||!['android','web'].includes(x.platform)||!CODES.has(x.code)||!COMPONENTS.has(x.component)||typeof x.app_version!=='string'||!/^[A-Za-z0-9._-]{1,32}$/.test(x.app_version)||typeof x.at!=='string'||!Number.isFinite(Date.parse(x.at))||Math.abs(Date.now()-Date.parse(x.at))>7*86400000)fail('bad_crash_report');return x;}
export async function installQualityFeatures(app,{pool,requireAuth}){
 await pool.query(await readFile(new URL('./quality-features.sql',import.meta.url),'utf8'));
 const me=async req=>{const user=await requireAuth(req);if(!user?.id)fail('unauthorized',401);return user;};
 app.get('/quality/capabilities',async req=>{await me(req);return{version:1,account_preferences:true,quiet_hours:true,notification_grouping:true,download_policy:true,wallpapers:true,language:['ru','en'],crash_reports:true};});
 app.get('/me/quality',async req=>readQualityPreference(pool,(await me(req)).id));
 app.put('/me/quality',{config:{rateLimit:{max:20,timeWindow:'1 minute'}}},async req=>{const user=await me(req),p=validatePreference(req.body);await pool.query(`INSERT INTO quality_preferences(user_id,language,quiet_enabled,quiet_start,quiet_end,time_zone,group_notifications,download_policy,cache_max_mb,wallpaper,accent,crash_consent)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12)
 ON CONFLICT(user_id) DO UPDATE SET language=$2,quiet_enabled=$3,quiet_start=$4,quiet_end=$5,time_zone=$6,group_notifications=$7,download_policy=$8::jsonb,cache_max_mb=$9,wallpaper=$10,accent=$11,crash_consent=$12,updated_at=now()`,[user.id,p.language,p.quiet.enabled,p.quiet.start,p.quiet.end,p.quiet.time_zone,p.group_notifications,JSON.stringify(p.download),p.cache_max_mb,p.wallpaper,p.accent,p.crash_consent]);return readQualityPreference(pool,user.id);});
 app.post('/me/crash-reports',{config:{rateLimit:{max:5,timeWindow:'1 hour'}}},async req=>{const user=await me(req);if(!(await readQualityPreference(pool,user.id)).crash_consent)fail('crash_consent_required',403);const report=validateCrash(req.body);const count=(await pool.query("SELECT count(*)::int AS n FROM quality_crash_reports WHERE user_id=$1 AND created_at>now()-interval '1 day'",[user.id])).rows[0].n;if(count>=20)fail('crash_report_limit',429);await pool.query('INSERT INTO quality_crash_reports(id,user_id,platform,code,component,app_version,occurred_at) VALUES($1,$2,$3,$4,$5,$6,$7)',[randomUUID(),user.id,report.platform,report.code,report.component,report.app_version,report.at]);return{ok:true};});
 return{readQualityPreference,userInQuietHours};
}
