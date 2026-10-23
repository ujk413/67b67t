import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {canAccessMedia,mediaByteRange} from '../api/media-access.js';
const require=createRequire(new URL('../api/package.json',import.meta.url)),{PGlite}=require('@electric-sql/pglite');
test('media and thumbnails require a visible message, owner, assigned avatar or accessible catalog asset',async t=>{
 const db=new PGlite();t.after(()=>db.close());await db.exec("CREATE TABLE users(id text,avatar_id text);CREATE TABLE rooms(id text,avatar_id text);CREATE TABLE messages(id text,room_id text,media_id text);CREATE TABLE room_members(room_id text,user_id text);CREATE TABLE hidden_messages(message_id text,user_id text);INSERT INTO rooms VALUES('room',NULL);INSERT INTO messages VALUES('msg','room','media');INSERT INTO room_members VALUES('room','peer')");const media={id:'media',owner_id:'owner'},pool={query:(q,p)=>db.query(q,p)};
 assert.equal(await canAccessMedia(pool,media,'owner'),true);assert.equal(await canAccessMedia(pool,media,'peer'),true);assert.equal(await canAccessMedia(pool,media,'outsider'),false);
 await db.exec("INSERT INTO hidden_messages VALUES('msg','peer')");assert.equal(await canAccessMedia(pool,media,'peer'),false);
 assert.equal(await canAccessMedia(pool,media,'outsider',{canReadCatalogMedia:async(id,user)=>id==='media'&&user==='outsider'}),true);
 await db.exec("INSERT INTO users VALUES('profile','avatar')");assert.equal(await canAccessMedia(pool,{id:'avatar',owner_id:'owner'},'peer'),true);
 assert.equal(await canAccessMedia(pool,{id:'unassigned-avatar',owner_id:'owner',kind:'avatar'},'peer'),false);
});
test('media ranges support seek/suffix and reject malformed or out-of-file requests',()=>{
 assert.deepEqual(mediaByteRange('bytes=0-9',100),{start:0,end:9,length:10});assert.deepEqual(mediaByteRange('bytes=90-',100),{start:90,end:99,length:10});assert.deepEqual(mediaByteRange('bytes=-10',100),{start:90,end:99,length:10});assert.deepEqual(mediaByteRange('bytes=90-999',100),{start:90,end:99,length:10});
 for(const value of ['bytes=-','bytes=100-','bytes=5-4','bytes=0-1,3-4','garbage','bytes=-0','bytes=99999999999999999999-'])assert.equal(mediaByteRange(value,100),false,value);
});
