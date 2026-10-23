import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdirSync,writeFileSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
test('native share policy accepts external providers and refuses application private URI sources',()=>{
 const repository=fileURLToPath(new URL('../',import.meta.url)),root=path.join(repository,'builds/share-policy-test');mkdirSync(root,{recursive:true});
 writeFileSync(path.join(root,'SharePolicyTest.java'),`import ru.b67t.expressive.IncomingSharePolicy; public class SharePolicyTest { public static void main(String[] args) { String own="ru.b67t.messenger.mobile"; String[] denied={"file:///data/user/0/"+own+"/shared_prefs/SecureStore.xml","content://"+own+".FileSystemFileProvider/private/secret","https://tracker.invalid/file","content://bad@provider/asset"}; for(String uri:denied) if(IncomingSharePolicy.allows(uri,own))throw new AssertionError(uri); if(!IncomingSharePolicy.allows("content://com.android.providers.media.documents/document/image%3A123",own))throw new AssertionError("legitimate share refused"); } }`);
 const javaHome=process.env.JAVA_HOME||(process.platform==='win32'?'C:/Program Files/Eclipse Adoptium/jdk-17.0.18.8-hotspot':null),executable=name=>javaHome?path.join(javaHome,'bin',name+(process.platform==='win32'?'.exe':'')):name;
 const compiled=spawnSync(executable('javac'),['-d',root,path.join(repository,'mobile/modules/expressive-integration/android/src/main/java/ru/b67t/expressive/IncomingSharePolicy.java'),path.join(root,'SharePolicyTest.java')],{encoding:'utf8'});assert.equal(compiled.status,0,compiled.stderr);
 const run=spawnSync(executable('java'),['-cp',root,'SharePolicyTest'],{encoding:'utf8'});assert.equal(run.status,0,run.stderr);
});
