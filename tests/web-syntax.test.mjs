import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {spawnSync} from 'node:child_process';
import vm from 'node:vm';
test('every browser module parses explicitly as ESM before release',()=>{
 for(const file of fs.readdirSync(new URL('../web/',import.meta.url)).filter(file=>file.endsWith('.js'))){const result=spawnSync(process.execPath,['--input-type=module','--check'],{input:fs.readFileSync(new URL('../web/'+file,import.meta.url))});assert.equal(result.status,0,file+': '+result.stderr);}
});
test('JSON headers are sent only with a JSON body, including DELETE and multipart requests',async()=>{
 const source=fs.readFileSync(new URL('../web/app.js',import.meta.url),'utf8'),start=source.indexOf('async function api('),end=source.indexOf('// Login/register/logout',start),requests=[];
 const scope={API:'/api',AbortController,FormData,setTimeout,clearTimeout,isAuthEntryPath:()=>false,handleUnauthorized(){},fetch:async(url,options)=>{requests.push(options);return{ok:true,status:200,json:async()=>({ok:true})};}};
 vm.runInNewContext(source.slice(start,end)+'\nthis.call=api;',scope);
 await scope.call('/message',{method:'DELETE'});assert.equal(requests.at(-1).headers['Content-Type'],undefined);
 await scope.call('/upload',{method:'POST',body:new FormData()});assert.equal(requests.at(-1).headers['Content-Type'],undefined);
 await scope.call('/message',{method:'POST',body:'{}'});assert.equal(requests.at(-1).headers['Content-Type'],'application/json');assert.equal(requests.at(-1).credentials,'include');
});
