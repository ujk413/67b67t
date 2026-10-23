import {createHash} from 'node:crypto';
const UUID=/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
const PREFIX='acct_';
export function accountCookieName(userId){if(!UUID.test(userId))throw Error('invalid_account');return PREFIX+String(userId).toLowerCase();}
export function browserCookies(req){
 if(req.cookies)return req.cookies;
 const result={};for(const item of String(req.headers?.cookie||'').split(';')){const equals=item.indexOf('=');if(equals<1)continue;try{result[item.slice(0,equals).trim()]=decodeURIComponent(item.slice(equals+1));}catch{}}
 return result;
}
export function readSessionToken(req){
 const auth=String(req.headers?.authorization||'');if(/^Bearer /i.test(auth))return auth.slice(7).trim();
 const selected=req.headers?.['x-account-id']??req.query?.account_id,cookies=browserCookies(req);
 if(selected!==undefined){if(!UUID.test(String(selected)))return'';return String(cookies[accountCookieName(selected)]||'');}
 return String(cookies.token||'');
}
export function selectedCookieAccount(req){if(/^Bearer /i.test(String(req.headers?.authorization||'')))return null;const id=req.headers?.['x-account-id']??req.query?.account_id;return id&&UUID.test(String(id))?String(id).toLowerCase():null;}
export function setBrowserSession(req,reply,token,days,user,cookieOptions){
 reply.setCookie('token',token,cookieOptions(req,days));
 if(user?.id&&String(req.headers?.['x-session-transport']||'').toLowerCase()!=='bearer')reply.setCookie(accountCookieName(user.id),token,cookieOptions(req,days));
}
export function clearBrowserSession(req,reply,userId,token){
 if(browserCookies(req)[accountCookieName(userId)]===token)reply.clearCookie(accountCookieName(userId),{path:'/'});
 if(browserCookies(req).token===token)reply.clearCookie('token',{path:'/'});
}
export function installBrowserAccounts(app,{pool,cookieOptions,ttlDays=30,revokeSession=async()=>{}}){
 const lookup=async token=>{
  if(typeof token!=='string'||!/^[a-f0-9]{64}$/i.test(token))return null;
  const {rows}=await pool.query(`SELECT s.id AS session_id,s.user_id AS id,u.username FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=$1 AND s.expires_at>now() AND NOT COALESCE(u.banned,false) AND u.login_disabled_at IS NULL`,[createHash('sha256').update(token).digest('hex')]);return rows[0]||null;
 };
 async function target(req){const id=String(req.body?.user_id||'');if(!UUID.test(id))throw Object.assign(Error('invalid_account'),{statusCode:400});const token=browserCookies(req)[accountCookieName(id)],user=await lookup(token);if(!user||user.id!==id.toLowerCase())throw Object.assign(Error('account_session_unavailable'),{statusCode:401});return{user,token};}
 const options={config:{rateLimit:{max:30,timeWindow:'1 minute'}}};
 app.get('/auth/accounts',options,async(req,reply)=>{
  const cookies=browserCookies(req),accounts=new Map();
  const names=Object.keys(cookies).filter(name=>name.startsWith(PREFIX)&&UUID.test(name.slice(PREFIX.length))).slice(0,12);
  for(const name of names){const user=await lookup(cookies[name]);if(user&&accountCookieName(user.id)===name)accounts.set(user.id,{id:user.id,userId:user.id,username:user.username});else reply.clearCookie(name,{path:'/'});}
  // Adopt an existing HttpOnly single-account session, without exposing its token.
  const legacy=await lookup(cookies.token);if(legacy&&!accounts.has(legacy.id)){reply.setCookie(accountCookieName(legacy.id),cookies.token,cookieOptions(req,ttlDays));accounts.set(legacy.id,{id:legacy.id,userId:legacy.id,username:legacy.username});}
  return{accounts:[...accounts.values()]};
 });
 app.post('/auth/accounts/select',options,async(req,reply)=>{const {user,token}=await target(req);setBrowserSession(req,reply,token,ttlDays,user,cookieOptions);return{user:{id:user.id,username:user.username}};});
 app.delete('/auth/accounts/:userId',options,async(req,reply)=>{req.body={user_id:req.params.userId};const {user,token}=await target(req);await pool.query('DELETE FROM sessions WHERE id=$1 AND user_id=$2',[user.session_id,user.id]);await revokeSession(user.id,user.session_id);clearBrowserSession(req,reply,user.id,token);return{ok:true};});
}
