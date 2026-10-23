import {test, before, after} from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import {createHash} from 'node:crypto';

const dir=fs.mkdtempSync(path.join(os.tmpdir(),'67chat-apk-test-'));
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const payload=Buffer.alloc(350000,71), other=Buffer.alloc(350000,72);
let server, origin, seen=[], cut=false, current=payload, ignoreRange=false;
const run=(command,args)=>new Promise((resolve,reject)=>{
  const child=spawn(command,args,{windowsHide:true});let stdout='',stderr='';
  child.stdout.on('data',x=>stdout+=x);child.stderr.on('data',x=>stderr+=x);child.on('error',reject);
  child.on('close',code=>resolve({code,stdout,stderr}));
});
const javaBin=name=>process.env.JAVA_HOME?path.join(process.env.JAVA_HOME,'bin',name+(process.platform==='win32'?'.exe':'')):name;
before(async()=>{
  fs.writeFileSync(path.join(dir,'ApkHarness.java'),`import java.io.File;import ru.b67t.roomkey.ApkDownload;
public class ApkHarness {public static void main(String[] a)throws Exception{System.out.print(ApkDownload.download(a[0],new File(a[1]),a[2],Long.parseLong(a[3]),a[4],()->false,(n,t)->{}));}}`);
  const compile=await run(javaBin('javac'),['-d',dir,new URL('../mobile/modules/room-key/android/src/main/java/ru/b67t/roomkey/ApkDownload.java',import.meta.url).pathname.replace(/^\/([A-Z]:)/,'$1'),path.join(dir,'ApkHarness.java')]);
  assert.equal(compile.code,0,compile.stderr);
  server=http.createServer((req,res)=>{
    seen.push(req.headers.range||'');const offset=ignoreRange?0:Number(req.headers.range?.match(/bytes=(\d+)-/)?.[1]||0);
    res.writeHead(offset?206:200,{'Content-Length':current.length-offset,...(offset?{'Content-Range':`bytes ${offset}-${current.length-1}/${current.length}`}:{})});
    if(cut){res.write(current.subarray(offset,offset+65536));setTimeout(()=>res.destroy(),30);}else res.end(current.subarray(offset));
  });
  await new Promise(r=>server.listen(0,'127.0.0.1',r));origin=`http://127.0.0.1:${server.address().port}/apk`;
});
after(async()=>{if(server)await new Promise(r=>server.close(r));assert.ok(path.basename(dir).startsWith('67chat-apk-test-'));fs.rmSync(dir,{recursive:true,force:true});});
const download=(file,identity,digest=hash(current))=>run(javaBin('java'),['-cp',dir,'ApkHarness',origin,file,identity,String(current.length),digest]);

test('a new process resumes a truncated APK and verifies the whole SHA-256',async()=>{
  current=payload;cut=true;seen=[];const file=path.join(dir,'resume.apk');
  const first=await download(file,'build-17');assert.notEqual(first.code,0);const offset=fs.statSync(file).size;assert.ok(offset>0&&offset<payload.length);
  cut=false;const second=await download(file,'build-17');assert.equal(second.code,0,second.stderr);assert.equal(seen.at(-1),`bytes=${offset}-`);assert.deepEqual(fs.readFileSync(file),payload);
});
test('a changed release discards the old partial APK even when its size is equal',async()=>{
  current=payload;cut=true;const file=path.join(dir,'changed.apk');await download(file,'old');
  current=other;cut=false;seen=[];const result=await download(file,'new');assert.equal(result.code,0,result.stderr);assert.equal(seen[0],'');assert.deepEqual(fs.readFileSync(file),other);
});
test('a server ignoring Range replaces the partial file instead of appending',async()=>{
  current=payload;cut=true;const file=path.join(dir,'range.apk');await download(file,'same');
  cut=false;ignoreRange=true;try{const result=await download(file,'same');assert.equal(result.code,0,result.stderr);assert.deepEqual(fs.readFileSync(file),payload);}finally{ignoreRange=false;}
});
test('a hash mismatch never yields an installable file and clears poisoned state',async()=>{
  current=other;const file=path.join(dir,'corrupt.apk');const result=await download(file,'bad',hash(payload));assert.notEqual(result.code,0);assert.match(result.stderr,/checksum_mismatch/);assert.equal(fs.existsSync(file),false);
});
