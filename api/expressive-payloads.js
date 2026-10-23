/** Typed messages are TEXT before encryption, never extra plaintext metadata. */
export const PAYLOAD_PREFIX='[67chat:v1]';
export const MAX_LIVE_LOCATION_MS=24*60*60*1000;
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const object=x=>!!x&&typeof x==='object'&&!Array.isArray(x)&&Object.getPrototypeOf(x)===Object.prototype;
const str=(x,n=300)=>typeof x==='string'&&x.length>0&&x.length<=n&&!/[\u0000-\u0008]/.test(x);
const keys=(x,required,optional=[])=>object(x)&&required.every(k=>Object.hasOwn(x,k))&&Object.keys(x).every(k=>required.includes(k)||optional.includes(k));
const id=x=>typeof x==='string'&&UUID.test(x), option=x=>typeof x==='string'&&/^[a-z0-9_-]{1,20}$/.test(x);
const time=x=>typeof x==='string'&&/^\d{4}-\d\d-\d\dT/.test(x)&&Number.isFinite(Date.parse(x));
const position=x=>typeof x.latitude==='number'&&Number.isFinite(x.latitude)&&Math.abs(x.latitude)<=90&&typeof x.longitude==='number'&&Number.isFinite(x.longitude)&&Math.abs(x.longitude)<=180;
const integer=(x,min,max)=>Number.isSafeInteger(x)&&x>=min&&x<=max;
export function validateMessagePayload(p){
 if(!object(p)||p.version!==1)return false;
 const base=['kind','version'];
 switch(p.kind){
  case 'sticker':case 'gif':return keys(p,[...base,'media_id'],['label','catalog_id','mime'])&&id(p.media_id)&&(p.catalog_id===undefined||id(p.catalog_id))&&(p.label===undefined||str(p.label,100))&&(p.mime===undefined||['image/webp','image/png','image/jpeg','image/gif','video/mp4','video/webm'].includes(p.mime));
  case 'contact':return keys(p,[...base,'username'],['display_name'])&&typeof p.username==='string'&&/^[\p{L}\p{N}_.-]{1,64}$/u.test(p.username)&&(p.display_name===undefined||str(p.display_name,100));
  case 'dice':return keys(p,[...base,'id','value','source'])&&id(p.id)&&integer(p.value,1,6)&&['server','device'].includes(p.source);
  case 'poll':return keys(p,[...base,'id','question','options','multiple','quiz','expires_at'],['answer_commitment'])&&id(p.id)&&str(p.question,300)&&Array.isArray(p.options)&&p.options.length>=2&&p.options.length<=10&&p.options.every(o=>keys(o,['id','text'])&&option(o.id)&&str(o.text,150))&&new Set(p.options.map(o=>o.id)).size===p.options.length&&typeof p.multiple==='boolean'&&typeof p.quiz==='boolean'&&(!p.quiz||!p.multiple)&&time(p.expires_at)&&(p.quiz?/^[a-f0-9]{64}$/.test(p.answer_commitment):p.answer_commitment===undefined);
  case 'poll_vote':return keys(p,[...base,'poll_id','choices','revision'])&&id(p.poll_id)&&Array.isArray(p.choices)&&p.choices.length<=10&&p.choices.every(option)&&new Set(p.choices).size===p.choices.length&&integer(p.revision,1,1000000);
  case 'poll_close':return keys(p,[...base,'poll_id'],['answer'])&&id(p.poll_id)&&(p.answer===undefined||(keys(p.answer,['option_id','salt'])&&option(p.answer.option_id)&&str(p.answer.salt,128)&&p.answer.salt.length>=32));
  case 'location':return keys(p,[...base,'id','latitude','longitude','live','expires_at'])&&id(p.id)&&position(p)&&typeof p.live==='boolean'&&time(p.expires_at);
  case 'location_update':return keys(p,[...base,'location_id','latitude','longitude','sequence'])&&id(p.location_id)&&position(p)&&integer(p.sequence,1,1000000);
  case 'location_stop':return keys(p,[...base,'location_id'])&&id(p.location_id);
  default:return false;
 }
}
export function parseMessagePayload(text){
 if(typeof text!=='string'||!text.startsWith(PAYLOAD_PREFIX)||text.length>5000)return null;
 try{const p=JSON.parse(text.slice(PAYLOAD_PREFIX.length));return validateMessagePayload(p)?p:null;}catch{return null;}
}
export function encodeMessagePayload(payload){
 if(!validateMessagePayload(payload))throw new Error('invalid_message_payload');
 const text=PAYLOAD_PREFIX+JSON.stringify(payload);if(text.length>5000)throw new Error('message_payload_too_large');return text;
}
export function secureDice(cryptoProvider=globalThis.crypto){
 if(!cryptoProvider?.getRandomValues)throw new Error('secure_random_unavailable');
 const b=new Uint8Array(1);do{cryptoProvider.getRandomValues(b);}while(b[0]>=252);
 return b[0]%6+1;
}
export function expressiveId(cryptoProvider=globalThis.crypto){
 const bytes=cryptoProvider.getRandomValues(new Uint8Array(16));bytes[6]=(bytes[6]&15)|64;bytes[8]=(bytes[8]&63)|128;
 const h=Array.from(bytes,x=>x.toString(16).padStart(2,'0')).join('');return`${h.slice(0,8)}-${h.slice(8,12)}-${h.slice(12,16)}-${h.slice(16,20)}-${h.slice(20)}`;
}
export async function quizCommitment(optionId,salt,cryptoProvider=globalThis.crypto){
 const digest=await cryptoProvider.subtle.digest('SHA-256',new TextEncoder().encode(`67chat:quiz:v1:${optionId}:${salt}`));return Array.from(new Uint8Array(digest),x=>x.toString(16).padStart(2,'0')).join('');
}
const ordered=messages=>[...messages].filter(m=>m&&Number.isFinite(Date.parse(m.created_at))).sort((a,b)=>Date.parse(a.created_at)-Date.parse(b.created_at)||String(a.id).localeCompare(String(b.id)));
const visible=m=>!m.hidden&&!m.deleted&&!m.deleted_at;
/** Input messages must have passed the normal authenticated receive/decrypt path. */
export async function reducePoll(origin,events,now=Date.now(),cryptoProvider=globalThis.crypto){
 const p=parseMessagePayload(origin.text);if(p?.kind!=='poll'||!visible(origin))return null;
 const created=Date.parse(origin.created_at),deadline=Date.parse(p.expires_at);
 if(!Number.isFinite(created)||deadline<=created||deadline-created>30*24*60*60*1000)return null;
 let closedAt=deadline,answer=null;const votes=new Map(),counts=Object.fromEntries(p.options.map(o=>[o.id,0]));
 for(const message of ordered(events)){
  if(!visible(message)||message.room_id!==origin.room_id)continue;
  const e=parseMessagePayload(message.text),at=Date.parse(message.created_at);
  if(!e||e.poll_id!==p.id||at<created||at>now||at>=closedAt)continue;
  if(e.kind==='poll_close'&&message.sender_id===origin.sender_id){
   closedAt=at;
   if(p.quiz&&e.answer&&p.options.some(o=>o.id===e.answer.option_id)&&await quizCommitment(e.answer.option_id,e.answer.salt,cryptoProvider)===p.answer_commitment)answer=e.answer.option_id;
  }else if(e.kind==='poll_vote'&&message.sender_id&&e.choices.every(c=>Object.hasOwn(counts,c))&&(p.multiple||e.choices.length<=1)){
   const previous=votes.get(message.sender_id);
   if(!previous||e.revision>previous.revision)votes.set(message.sender_id,e);
  }
 }
 for(const vote of votes.values())for(const choice of vote.choices)counts[choice]++;
 return{poll:p,closed:now>=closedAt,counts,voters:[...votes.values()].filter(v=>v.choices.length).length,answer,votes:Object.fromEntries(votes)};
}
export function reduceLocation(origin,events,now=Date.now()){
 const p=parseMessagePayload(origin.text);if(p?.kind!=='location'||!visible(origin))return null;
 const created=Date.parse(origin.created_at),deadline=Date.parse(p.expires_at);
 if(!Number.isFinite(created)||deadline<=created||deadline-created>MAX_LIVE_LOCATION_MS)return null;
 let latitude=p.latitude,longitude=p.longitude,sequence=0,stopped=false;
 for(const message of ordered(events)){
  if(!visible(message)||message.room_id!==origin.room_id||message.sender_id!==origin.sender_id)continue;
  const e=parseMessagePayload(message.text),at=Date.parse(message.created_at);
  if(!e||e.location_id!==p.id||at<created||at>now||at>=deadline||stopped||!p.live)continue;
  if(e.kind==='location_stop')stopped=true;
  else if(e.kind==='location_update'&&e.sequence>sequence){latitude=e.latitude;longitude=e.longitude;sequence=e.sequence;}
 }
 return{...p,latitude,longitude,sequence,active:p.live&&!stopped&&now<deadline,expired:now>=deadline};
}
export function isExpressiveControl(payload){return!!payload&&['poll_vote','poll_close','location_update','location_stop'].includes(payload.kind);}
export function expressiveSummary(payload){
 if(!payload)return'';
 const labels={sticker:'Стикер',gif:'GIF',contact:'Контакт',dice:'Кубик',poll:'Опрос',poll_vote:'Голос в опросе',poll_close:'Опрос закрыт',location:'Геолокация',location_update:'Обновление геолокации',location_stop:'Геолокация остановлена'};
 if(payload.kind==='poll')return`📊 ${payload.question}`;
 if(payload.kind==='contact')return`👤 ${payload.display_name||payload.username} (@${payload.username})`;
 if(payload.kind==='dice')return`🎲 ${payload.value}${payload.source==='device'?' · случайный результат устройства':''}`;
 return labels[payload.kind]||'';
}
export const EMOJI_CATALOG=[
 ['😀','улыбка радость smile happy'],['😂','смех слезы laugh tears'],['❤️','сердце любовь heart love'],['👍','палец хорошо да thumb yes ok'],['👎','нет плохо no bad'],['🎉','праздник поздравляю party'],['🔥','огонь fire'],['🙏','спасибо просьба thanks pray'],['😢','грусть плачу sad cry'],['🤔','думаю think'],['👋','привет пока hello bye wave'],['🎲','кубик игра dice game'],['🐈','кот кошка cat'],['🐕','собака dog'],['🌍','земля мир earth world'],['✅','готово done check'],['☕','кофе coffee'],['🚀','ракета rocket'],['💯','сто отлично hundred perfect'],['😎','круто очки cool glasses'],['🥰','люблю love cute'],['😴','сон sleep'],['🤝','рукопожатие handshake'],['💬','чат сообщение chat message'],['📍','место location pin'],['🌹','роза цветы rose flower'],['🎂','торт день рождения cake birthday'],['💪','сила muscle strong'],['👌','отлично ok'],['🤗','обнять hug']
].map(([emoji,terms])=>({emoji,terms}));
export const EMOJI_CATEGORIES=[{id:'all',label:'Все'},{id:'faces',label:'Лица'},{id:'gestures',label:'Жесты'},{id:'animals',label:'Природа'},{id:'objects',label:'Предметы'}];
const emojiCategory=emoji=>'😀😂😢🤔😎🥰😴🤗'.includes(emoji)?'faces':'👍👎🙏👋🤝💪👌'.includes(emoji)?'gestures':'🐈🐕🌍🌹'.includes(emoji)?'animals':'objects';
export function searchEmoji(query='',recent=[],category='all'){const q=query.trim().toLocaleLowerCase();const all=EMOJI_CATALOG.filter(e=>(category==='all'||emojiCategory(e.emoji)===category)&&(!q||e.emoji===q||e.terms.includes(q)));return [...all].sort((a,b)=>{const ai=recent.indexOf(a.emoji),bi=recent.indexOf(b.emoji);return(ai<0?999:ai)-(bi<0?999:bi);});}
const escapeHtml=text=>String(text??'').replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
export function isSecretRoom(room){return!!(room?.is_secret||room?.secret||room?.secret_chat||room?.kind==='secret'||room?.privacy_mode==='secret');}
/** Export uses an allowlist, including plaintext only after successful decryption. */
export async function buildChatExport({room,messages,decode,acknowledged=false,format='json',cancelled=()=>false,attachments=[]}){
 if(!acknowledged)throw new Error('export_warning_acknowledgment_required');
 if(!room||isSecretRoom(room))throw new Error('secret_chat_export_forbidden');
 if(!['json','html'].includes(format))throw new Error('invalid_export_format');
 const items=[];
 for(const original of ordered(messages)){
  if(cancelled())throw new Error('export_cancelled');
  if(!visible(original)||original.room_id!==room.id)continue;
  const m=await decode(original);if(cancelled())throw new Error('export_cancelled');
  if(!m||((original.encrypted_content||original.meta?.encrypted)&&!m._decrypted))throw new Error('export_decryption_failed');
  items.push({id:String(m.id),sender_id:String(m.sender_id),created_at:String(m.created_at),type:String(m.type),text:typeof m.text==='string'?m.text:''});
 }
 const title=String(room.title||'67chat'),exportedIds=new Set(items.map(item=>item.id)),files=[];let totalBytes=0;
 for(const attachment of attachments){
  if(cancelled())throw Error('export_cancelled');
  if(!exportedIds.has(attachment?.message_id))continue;
  if(typeof attachment.base64!=='string'||!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(attachment.base64))throw Error('export_invalid_attachment');
  totalBytes+=attachment.base64.length*3/4;if(totalBytes>20*1024*1024)throw Error('export_attachments_too_large');
  const mime=/^(image\/(png|jpeg|webp|gif)|video\/(mp4|webm)|audio\/(mp4|mpeg|ogg|webm)|application\/(pdf|zip|octet-stream))$/.test(attachment.mime)?attachment.mime:'application/octet-stream';
  files.push({message_id:attachment.message_id,name:String(attachment.name||'Вложение').slice(0,200),mime,base64:attachment.base64});
 }
 if(format==='html')return`<!doctype html><html lang="ru"><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'"><title>${escapeHtml(title)}</title><style>body{font:16px system-ui;max-width:800px;margin:auto;padding:24px}article{border-bottom:1px solid #888;padding:16px 0}p{white-space:pre-wrap;overflow-wrap:anywhere}</style><h1>${escapeHtml(title)}</h1><p>Расшифрованный экспорт. Храните файл в безопасном месте.</p>${items.map(m=>`<article><small>${escapeHtml(m.created_at)} · ${escapeHtml(m.sender_id)}</small><p>${escapeHtml(m.text)}</p>${files.filter(file=>file.message_id===m.id).map(file=>`<p><a download="${escapeHtml(file.name)}" href="data:${file.mime};base64,${file.base64}">${escapeHtml(file.name)}</a></p>`).join('')}</article>`).join('')}</html>`;
 return JSON.stringify({format:'67chat-export',version:1,room:{id:room.id,title},messages:items,...(files.length?{attachments:files}:{})},null,2);
}
