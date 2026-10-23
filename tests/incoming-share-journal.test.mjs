import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdirSync,writeFileSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
test('native encrypted source journal survives restart with recipient, pending preview, and file lifecycle',()=>{
 const repository=fileURLToPath(new URL('../',import.meta.url)),root=path.join(repository,'builds/share-journal-test');mkdirSync(root,{recursive:true});
 writeFileSync(path.join(root,'ShareJournalTest.java'),`import ru.b67t.expressive.IncomingShareJournal;import java.io.*;import java.nio.file.*;import java.util.*;import javax.crypto.*;import javax.crypto.spec.*;
 public class ShareJournalTest {
 static void check(boolean b){if(!b)throw new AssertionError();}static void fails(Run r,String expected)throws Exception{try{r.run();throw new AssertionError(expected);}catch(IOException e){check(e.getMessage().contains(expected));}}interface Run{void run()throws Exception;}
 public static void main(String[] args)throws Exception{
 File root=Files.createTempDirectory("share-journal").toFile();SecretKey key=new SecretKeySpec(new byte[32],"AES");IncomingShareJournal.KeyProvider keys=()->key;
 IncomingShareJournal j=new IncomingShareJournal(root,keys);String id=UUID.randomUUID().toString();j.capture(id,"fingerprint","private incoming text","text/plain",Arrays.asList("content://external/a"),0,u->new ByteArrayInputStream("file-content".getBytes()),true);
 check(!new String(Files.readAllBytes(new File(root,"journal.bin").toPath())).contains("private incoming text"));
 IncomingShareJournal restored=new IncomingShareJournal(root,keys);check(!restored.peek().confirmed);check(restored.peek().text.equals("private incoming text"));check(restored.fileUris(restored.peek()).size()==1);
 fails(()->restored.advance(id,"alice","room",1),"confirmation_required");restored.confirm(id,"alice","room");restored.advance(id,"alice","room",1);
 IncomingShareJournal after=new IncomingShareJournal(root,keys);check(after.peek().next==1);check(after.peek().accountId.equals("alice"));fails(()->after.confirm(id,"bob","room"),"target_changed");fails(()->after.advance(id,"alice","else",2),"target_changed");
 String second=UUID.randomUUID().toString();after.capture(second,"second","new incoming",null,Collections.emptyList(),0,u->null,true);check(after.size()==2);check(after.peek().id.equals(id));
 File attachment=new File(new java.net.URI(after.fileUris(after.peek()).get(0)));check(attachment.delete());fails(()->after.validateFiles(after.peek()),"source_unavailable");
 after.removeAccount("alice");check(after.size()==1);check(!new File(root,id).exists());check(after.peek().id.equals(second));after.discard(second);check(after.size()==0);
 check(after.capture(second,"second","new incoming",null,Collections.emptyList(),0,u->null,false)==null);
 String interrupted=UUID.randomUUID().toString();after.capture(interrupted,"interrupted","preview before file copy",null,Arrays.asList("pending"),0,u->{IncomingShareJournal crash=new IncomingShareJournal(root,keys);check(crash.peek()!=null);check(crash.peek().id.equals(interrupted));check(crash.peek().text.equals("preview before file copy"));throw new IOException("process died");},true);check(after.peek().rejectedFiles==1);after.discard(interrupted);
 String rejected=UUID.randomUUID().toString();after.capture(rejected,"reject",null,null,Arrays.asList("revoked"),0,u->{throw new IOException("revoked");},true);check(after.peek().rejectedFiles==1);fails(()->after.confirm(rejected,"alice","room"),"source_unavailable");
 try{new IncomingShareJournal(root,()->new SecretKeySpec(new byte[32]{{this[0]=1;}},"AES"));throw new AssertionError("wrong key accepted");}catch(javax.crypto.AEADBadTagException expected){}
 }} `.replace('new byte[32]{{this[0]=1;}}','new byte[]{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}'));
 const javaHome=process.env.JAVA_HOME||(process.platform==='win32'?'C:/Program Files/Eclipse Adoptium/jdk-17.0.18.8-hotspot':null),executable=name=>javaHome?path.join(javaHome,'bin',name+(process.platform==='win32'?'.exe':'')):name;
 const compiled=spawnSync(executable('javac'),['-d',root,path.join(repository,'mobile/modules/expressive-integration/android/src/main/java/ru/b67t/expressive/IncomingShareJournal.java'),path.join(root,'ShareJournalTest.java')],{encoding:'utf8'});assert.equal(compiled.status,0,compiled.stderr);
 const run=spawnSync(executable('java'),['-cp',root,'ShareJournalTest'],{encoding:'utf8'});assert.equal(run.status,0,run.stderr);
});
