import {createHash,timingSafeEqual} from 'node:crypto';
const fail=(statusCode,message)=>{throw Object.assign(Error(message),{statusCode});};
const installationId=value=>{if(typeof value!=='string'||! /^[A-Za-z0-9_-]{8,100}$/.test(value))fail(400,'invalid_installation');return value;};
const secretHash=value=>{if(typeof value!=='string'||! /^[a-f0-9]{64}$/.test(value))fail(400,'invalid_installation_secret');return createHash('sha256').update(value).digest('hex');};
const verify=(row,secret)=>{const hash=secretHash(secret);if(!timingSafeEqual(Buffer.from(row.secret_hash,'hex'),Buffer.from(hash,'hex')))fail(403,'invalid_installation_secret');};
async function transaction(pool,id,run){
 const client=await pool.connect();
 try{await client.query('BEGIN');await client.query('SELECT pg_advisory_xact_lock(hashtext($1))',['push-installation:'+id]);const result=await run(client);await client.query('COMMIT');return result;}
 catch(error){await client.query('ROLLBACK');throw error;}finally{client.release();}
}
/** Registration and the device-wide revocation fence share one database lock.
 * A delayed request cannot recreate a row after the policy was paused. */
export async function withPushRegistration(pool,me,installation,body,write){
 const id=installationId(installation);
 return transaction(pool,id,async client=>{
  const row=(await client.query('SELECT * FROM account_push_installations WHERE installation_id=$1 FOR UPDATE',[id])).rows[0];
  if(body?.installation_secret!==undefined){
   if(!Number.isSafeInteger(body.installation_epoch)||body.installation_epoch<1)fail(400,'invalid_installation_epoch');
   if(!row)fail(409,'installation_claim_required');verify(row,body.installation_secret);
   if(body.installation_epoch!==row.epoch)fail(409,'stale_installation_epoch');
   if(row.paused){if(body.resume_push!==true)fail(409,'push_installation_paused');await client.query('UPDATE account_push_installations SET paused=false WHERE installation_id=$1',[id]);}
  }else if(row?.paused)fail(409,'push_installation_paused');
  return write(client);
 });
}
/** Recheck the current registration inside checkAndSend. Holding the same lock
 * through the bounded provider request makes disable wait for an in-flight send. */
export async function withPushDelivery(pool,installation,checkAndSend){
 const id=installationId(installation);
 return transaction(pool,id,async client=>{
  const row=(await client.query('SELECT paused FROM account_push_installations WHERE installation_id=$1',[id])).rows[0];
  return row?.paused?false:checkAndSend(client);
 });
}
export function installPushPolicy(app,{pool,requireAuth}){
 app.post('/me/account/push-installation',{config:{rateLimit:{max:30,timeWindow:'1 minute'}}},async req=>{
  const me=await requireAuth(req),id=installationId(req.body?.installation_id),hash=secretHash(req.body?.secret);
  return transaction(pool,id,async client=>{
   let row=(await client.query('SELECT * FROM account_push_installations WHERE installation_id=$1 FOR UPDATE',[id])).rows[0];
   if(row)verify(row,req.body.secret);
   else{
    // An installed legacy client must prove ownership before claiming its old id.
    const exists=(await client.query("SELECT to_regclass('public.push_devices') AS present")).rows[0]?.present;
    const owners=exists?(await client.query('SELECT user_id FROM push_devices WHERE installation_id=$1',[id])).rows:[];
    if(owners.length&&!owners.some(owner=>owner.user_id===me.id))fail(403,'installation_owned');
    row=(await client.query('INSERT INTO account_push_installations(installation_id,secret_hash) VALUES($1,$2) RETURNING epoch,paused',[id,hash])).rows[0];
   }
   return{epoch:row.epoch,paused:row.paused};
  });
 });
 app.post('/auth/account-push-disable',{config:{rateLimit:{max:30,timeWindow:'1 minute'}}},async req=>{
  const id=installationId(req.body?.installation_id);secretHash(req.body?.secret);
  return transaction(pool,id,async client=>{
   let row=(await client.query('SELECT * FROM account_push_installations WHERE installation_id=$1 FOR UPDATE',[id])).rows[0];
   if(!row){
    const exists=(await client.query("SELECT to_regclass('public.push_devices') AS present")).rows[0]?.present;
    if(exists&&(await client.query('SELECT 1 FROM push_devices WHERE installation_id=$1 LIMIT 1',[id])).rows.length)fail(409,'installation_claim_required');
    row=(await client.query('INSERT INTO account_push_installations(installation_id,secret_hash,paused) VALUES($1,$2,true) RETURNING *',[id,secretHash(req.body.secret)])).rows[0];
   }
   verify(row,req.body.secret);
   const updated=(await client.query('UPDATE account_push_installations SET paused=true,epoch=epoch+1 WHERE installation_id=$1 RETURNING epoch',[id])).rows[0];
   if((await client.query("SELECT to_regclass('public.push_devices') AS present")).rows[0]?.present)await client.query('DELETE FROM push_devices WHERE installation_id=$1',[id]);
   return{ok:true,epoch:updated.epoch};
  });
 });
}
