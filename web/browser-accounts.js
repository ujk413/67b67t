// Only a non-secret account selector lives in per-tab storage. Sessions stay HttpOnly.
const KEY='67chat.active-browser-account.v1',UUID=/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;let generation=0,selectedMemory;
export function selectedBrowserAccount(){if(selectedMemory!==undefined)return selectedMemory;try{return sessionStorage.getItem(KEY)||null;}catch{return null;}}
export function selectBrowserAccount(id){if(id!==null&&id!=='none'&&!UUID.test(id))throw Error('Некорректный аккаунт');selectedMemory=id;generation++;try{if(id===null)sessionStorage.removeItem(KEY);else sessionStorage.setItem(KEY,id);}catch{}}
export function invalidateBrowserSession(){generation++;}
export function browserSessionGeneration(){return generation;}
export function browserAccountHeaders(){const id=selectedBrowserAccount();return id?{'X-Account-Id':id}:{};}
export function notificationTarget(data){
 if(!data||!UUID.test(data.roomId||''))return null;
 const recipient=data.recipientId||data.recipient_id;
 if(recipient&&!UUID.test(recipient))return null;
 return{roomId:data.roomId,...(UUID.test(data.messageId||'')?{messageId:data.messageId}:{}),...(recipient?{accountId:recipient}:{})};
}
export function initialBrowserAccount(url){try{const id=new URL(url).searchParams.get('account_id');return UUID.test(id||'')?id:null;}catch{return null;}}
export function accountMediaUrl(value){
 const account=selectedBrowserAccount();if(!account||account==='none'||!value)return value;
 const origin=globalThis.location?.origin||'https://67chat.local',url=new URL(value,origin);if(!['http:','https:'].includes(url.protocol))return value;
 url.searchParams.set('account_id',account);return String(value).startsWith('/')?url.pathname+url.search+url.hash:url.toString();
}
