const uuid=/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
export function parse67Link(input){
 try{
  if(typeof input!=='string'||input.length>2048)return null;
  // Android accepts the historical digit-leading scheme; WHATWG URL does not.
  const native=/^67chat:\/\//i.test(input),url=new URL(native?input.replace(/^67chat:/i,'chat67:'):input);
  if(url.username||url.password||url.port)return null;
  if(!native && !(url.protocol==='https:' && url.hostname==='67chat.ru'))return null;
  const parts=(native?[url.hostname,...url.pathname.split('/')]:url.pathname.split('/')).filter(Boolean).map(decodeURIComponent);
  if(native&&parts.length===2&&parts[0]==='action'&&['compose','saved'].includes(parts[1]))return {type:'action',action:parts[1]};
  if(parts[0]==='chat' && uuid.test(parts[1]||'') && (parts.length===2 || parts.length===4&&parts[2]==='message'&&uuid.test(parts[3])))return {type:'chat',roomId:parts[1],...(parts[3]?{messageId:parts[3]}:{})};
  if(parts[0]==='profile'&&parts.length===2&&/^[A-Za-z0-9_]{3,30}$/.test(parts[1]))return {type:'profile',username:parts[1]};
  if(parts[0]==='invite'&&parts.length===2&&/^[A-Za-z0-9_-]{8,256}$/.test(parts[1]))return {type:'invite',token:parts[1]};
 }catch{}return null;
}
export function messageLink(roomId,messageId){if(!uuid.test(roomId)||!uuid.test(messageId))throw Error('Сообщение ещё не отправлено');return `https://67chat.ru/chat/${roomId}/message/${messageId}`;}
