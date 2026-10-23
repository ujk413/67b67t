import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
const require=createRequire(new URL('../api/package.json',import.meta.url)),{PGlite}=require('@electric-sql/pglite');
const read=path=>readFileSync(new URL('../'+path,import.meta.url),'utf8').replace(/\r\n/g,'\n');
test('fresh and existing schemas include the same feature migrations and remain idempotent',async t=>{
 const schema=read('db/init.sql'),migration=read('db/migrations/20261023_remaining_features.sql');
 for(const name of ['mobile-schema','account-features','account-integration','account-push-policy','conversation-features','expressive-features','call-features','quality-features']){
  const body=read('api/'+name+'.sql').trim();assert.ok(schema.includes(body),`${name} fresh schema is synchronized`);assert.ok(migration.includes(body),`${name} manual migration is synchronized`);
 }
 const db=new PGlite();t.after(()=>db.close());
 // PGlite has gen_random_uuid built in; citext/pgcrypto extension packaging is
 // deployment-specific. All table, FK, index and trigger SQL runs unchanged.
 await db.exec(schema.replace(/^CREATE EXTENSION[^;]+;/gm,'').replace(/\bcitext\b/g,'text'));
 await db.exec(migration);await db.exec(migration);
 const tables=(await db.query("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")).rows.map(row=>row.table_name);
 for(const table of ['encrypted_drafts','message_edit_history','scheduled_messages','account_session_devices','account_device_proofs','expressive_catalog','call_history','quality_preferences'])assert.ok(tables.includes(table),table);
});
