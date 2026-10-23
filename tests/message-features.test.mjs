import test from 'node:test';import assert from 'node:assert/strict';import {createRequire} from 'node:module';import {readFile} from 'node:fs/promises';
import {createMessageInserter} from '../api/message-features.js';
const require=createRequire(import.meta.url),{PGlite}=require('../api/node_modules/@electric-sql/pglite');
test('failed certificate binding rolls back the inserted message and releases the client',async()=>{
 const db=new PGlite();await db.exec('CREATE TABLE messages(id text PRIMARY KEY,text text)');let released=0;
 const pool={connect:async()=>({query:(s,p)=>db.query(s,p),release:()=>released++})};
 const features={validatePlaintext:async()=>{},bindDice:async()=>{throw Error('dice_already_used');}};
 const insert=createMessageInserter(pool,()=>features);
 await assert.rejects(insert('INSERT INTO messages VALUES($1,$2) RETURNING id',['one','content'],{}),/dice_already_used/);
 assert.equal((await db.query('SELECT count(*)::int AS n FROM messages')).rows[0].n,0);assert.equal(released,1);
 features.bindDice=async()=>{};await insert('INSERT INTO messages VALUES($1,$2) RETURNING id',['two','content'],{});assert.equal((await db.query('SELECT count(*)::int AS n FROM messages')).rows[0].n,1);await db.close();
});
test('API image carries the identical shared payload parser without a sibling web directory',async()=>{
 const [api,web]=await Promise.all(['../api/expressive-payloads.js','../web/expressive-payloads.js'].map(path=>readFile(new URL(path,import.meta.url),'utf8')));assert.equal(api.replace(/\r\n/g,'\n'),web.replace(/\r\n/g,'\n'));
});
