import {test} from 'node:test';import assert from 'node:assert/strict';import {createRequire} from 'node:module';import {createHash} from 'node:crypto';
import {readSessionToken,accountCookieName,installBrowserAccounts,clearBrowserSession} from '../api/browser-accounts.js';
const require=createRequire(new URL('../api/package.json',import.meta.url)),Fastify=require('fastify'),cookie=require('@fastify/cookie'),{PGlite}=require('@electric-sql/pglite');
const a='00000000-0000-4000-8000-000000000001',b='00000000-0000-4000-8000-000000000002',ta='a'.repeat(64),tb='b'.repeat(64);
test('late Bearer logout cannot clear newer browser cookies of the same account',()=>{
 const cleared=[],reply={clearCookie:name=>cleared.push(name)};
 clearBrowserSession({cookies:{token:tb,[accountCookieName(a)]:tb}},reply,a,ta);
 assert.deepEqual(cleared,[]);
 clearBrowserSession({cookies:{token:ta,[accountCookieName(a)]:ta}},reply,a,ta);
 assert.deepEqual(cleared,[accountCookieName(a),'token']);
});
test('per-tab account selection never silently falls back to a different cookie; Bearer retains priority',()=>{
 const cookies={token:tb,[accountCookieName(a)]:ta,[accountCookieName(b)]:tb};
 assert.equal(readSessionToken({headers:{'x-account-id':a},cookies}),ta);assert.equal(readSessionToken({headers:{},cookies,query:{account_id:b}}),tb);assert.equal(readSessionToken({headers:{'x-account-id':'none'},cookies}),'');assert.equal(readSessionToken({headers:{'x-account-id':'00000000-0000-4000-8000-000000000003'},cookies}),'');assert.equal(readSessionToken({headers:{authorization:'Bearer native','x-account-id':a},cookies}),'native');
});
test('browser accounts stay in HttpOnly cookies, select only owned sessions and remove only that account session',async t=>{
 const db=new PGlite();t.after(()=>db.close());await db.exec('CREATE TABLE users(id uuid,username text,banned boolean DEFAULT false,login_disabled_at timestamptz);CREATE TABLE sessions(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),user_id uuid,token_hash text,expires_at timestamptz)');
 for(const [id,token,name]of[[a,ta,'alice'],[b,tb,'bob']]){await db.query('INSERT INTO users(id,username)VALUES($1,$2)',[id,name]);await db.query("INSERT INTO sessions(user_id,token_hash,expires_at)VALUES($1,$2,now()+interval '1 day')",[id,createHash('sha256').update(token).digest('hex')]);}
 const app=Fastify();await app.register(cookie);const revoked=[];installBrowserAccounts(app,{pool:{query:(q,p)=>db.query(q,p)},cookieOptions:()=>({httpOnly:true,sameSite:'lax',path:'/'}),revokeSession:async(...args)=>revoked.push(args)});t.after(()=>app.close());await app.ready();
 const cookies=`token=${tb}; ${accountCookieName(a)}=${ta}; ${accountCookieName(b)}=${tb}`;
 const list=await app.inject({url:'/auth/accounts',headers:{cookie:cookies}});assert.equal(list.statusCode,200);assert.deepEqual(list.json().accounts.map(x=>x.username),['alice','bob']);assert.ok(!list.body.includes(ta)&&!list.body.includes(tb));
 const selected=await app.inject({method:'POST',url:'/auth/accounts/select',headers:{cookie:cookies},payload:{user_id:a}});assert.equal(selected.statusCode,200);assert.equal(selected.json().user.id,a);assert.ok(selected.cookies.every(c=>c.httpOnly));assert.equal(selected.cookies.find(c=>c.name===accountCookieName(a)).value,ta);
 assert.equal((await app.inject({method:'POST',url:'/auth/accounts/select',headers:{cookie:`token=${ta}; ${accountCookieName(b)}=${ta}`},payload:{user_id:b}})).statusCode,401);
 await app.inject({method:'DELETE',url:'/auth/accounts/'+a,headers:{cookie:cookies}});assert.equal(revoked[0][0],a);assert.equal((await db.query('SELECT user_id FROM sessions')).rows[0].user_id,b);
 assert.equal((await app.inject({method:'POST',url:'/auth/accounts/select',headers:{cookie:cookies},payload:{user_id:a}})).statusCode,401);
});
