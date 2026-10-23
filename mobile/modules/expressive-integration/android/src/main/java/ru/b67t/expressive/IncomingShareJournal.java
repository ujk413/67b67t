package ru.b67t.expressive;

import java.io.*;
import java.security.SecureRandom;
import java.util.*;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

/** Small encrypted source + confirmation journal. The only plaintext on disk is private file data.
 * A manifest commit is fsynced before a confirmed job may enter the normal durable outbox. */
public final class IncomingShareJournal {
  public interface KeyProvider { SecretKey get() throws Exception; }
  public interface Source { InputStream open(String uri) throws Exception; }
  public static final int MAX_FILES=10, MAX_JOBS=4;
  public static final long MAX_FILE_BYTES=100L*1024*1024-16, MAX_JOB_BYTES=250L*1024*1024;
  public static final class Share {
    public String id,text,mime,fingerprint,accountId,roomId;
    public long createdAt; public int rejectedFiles,next; public boolean confirmed;
    public final List<String> files=new ArrayList<>();
    Share copy(){Share s=new Share();s.id=id;s.text=text;s.mime=mime;s.fingerprint=fingerprint;s.accountId=accountId;s.roomId=roomId;s.createdAt=createdAt;s.rejectedFiles=rejectedFiles;s.next=next;s.confirmed=confirmed;s.files.addAll(files);return s;}
    public int itemCount(){return (text!=null&&!text.trim().isEmpty()?1:0)+files.size();}
  }
  private final File root,manifest,backup;
  private final KeyProvider keys;
  private List<Share> jobs=new ArrayList<>();
  private String lastFingerprint="";
  public IncomingShareJournal(File root,KeyProvider keys) throws Exception {
    this.root=root;this.keys=keys;root.mkdirs();manifest=new File(root,"journal.bin");backup=new File(root,"journal.bak");
    if(!manifest.exists()&&backup.exists()&&!backup.renameTo(manifest))throw new IOException("share_journal_recovery_failed");
    if(manifest.exists())read();
    cleanup();
  }
  public synchronized Share peek(){return jobs.isEmpty()?null:jobs.get(0).copy();}
  public synchronized int size(){return jobs.size();}
  public Share capture(String id,String fingerprint,String text,String mime,List<String> uris,int rejected,Source source,boolean fresh) throws Exception {
    Share share=new Share();share.id=UUID.fromString(id).toString();share.fingerprint=fingerprint;share.text=trim(text,10000);share.mime=trim(mime,100);share.createdAt=System.currentTimeMillis();
    // Persist the source preview BEFORE opening a provider. A killed import is restored
    // with rejectedFiles > 0, so an incomplete attachment set can never be confirmed.
    share.rejectedFiles=rejected+uris.size();
    synchronized(this){
      for(Share existing:jobs)if(existing.id.equals(id)||(!fresh&&existing.fingerprint.equals(fingerprint)))return existing.copy();
      if(!fresh&&Arrays.asList(lastFingerprint.split("\\|",-1)).contains(fingerprint))return null;
      if(jobs.size()>=MAX_JOBS)throw new IOException("share_queue_full");
      List<Share> next=copyJobs();next.add(share);commit(next,fingerprint);
    }
    File directory=new File(root,share.id);if(!directory.mkdirs()&&!directory.isDirectory())throw new IOException("share_copy_unavailable");
    long total=0;
    for(int i=0;i<Math.min(uris.size(),MAX_FILES);i++){
      synchronized(this){find(jobs,id);}
      File target=new File(directory,i+".shared");boolean copied=false;
      try(InputStream in=source.open(uris.get(i));FileOutputStream out=new FileOutputStream(target)){
        if(in==null)throw new IOException("share_source_unavailable");
        byte[] buffer=new byte[65536];long length=0;int count;
        while((count=in.read(buffer))!=-1){length+=count;if(length>MAX_FILE_BYTES||total+length>MAX_JOB_BYTES)throw new IOException("share_file_too_large");out.write(buffer,0,count);}
        out.getFD().sync();total+=length;copied=true;
      }catch(Exception error){target.delete();}
      if(copied){
        synchronized(this){
          try{List<Share> next=copyJobs();Share current=find(next,id);current.files.add(target.getName());current.rejectedFiles--;commit(next,lastFingerprint);}
          catch(Exception error){target.delete();throw error;}
        }
      }
    }
    synchronized(this){return find(jobs,id).copy();}
  }
  public synchronized Share confirm(String id,String account,String room) throws Exception {
    List<Share> next=copyJobs();Share s=find(next,id);
    if(s.confirmed&&(!Objects.equals(s.accountId,account)||!Objects.equals(s.roomId,room)))throw new IOException("share_target_changed");
    if(account==null||account.isEmpty()||room==null||room.isEmpty())throw new IOException("share_target_required");
    validateFiles(s);if(s.rejectedFiles>0)throw new IOException("share_source_unavailable");
    s.confirmed=true;s.accountId=account;s.roomId=room;commit(next,lastFingerprint);return s.copy();
  }
  public synchronized void advance(String id,String account,String room,int nextIndex) throws Exception {
    List<Share> next=copyJobs();Share s=find(next,id);
    if(!s.confirmed)throw new IOException("share_confirmation_required");
    if(!Objects.equals(s.accountId,account)||!Objects.equals(s.roomId,room))throw new IOException("share_target_changed");
    if(nextIndex<s.next||nextIndex>s.itemCount()||nextIndex>s.next+1)throw new IOException("share_journal_invalid");
    s.next=nextIndex;commit(next,lastFingerprint);
  }
  public synchronized void discard(String id) throws Exception {
    List<Share> next=copyJobs();Share s=find(next,id);next.remove(s);commit(next,lastFingerprint);deleteTree(new File(root,s.id));
  }
  public synchronized void removeAccount(String account) throws Exception {
    List<Share> next=copyJobs(),removed=new ArrayList<>();for(Share s:jobs)if(account.equals(s.accountId))removed.add(s);
    next.removeIf(s->account.equals(s.accountId));commit(next,lastFingerprint);for(Share s:removed)deleteTree(new File(root,s.id));
  }
  public synchronized List<String> fileUris(Share share) throws Exception {
    validateFiles(share);List<String> result=new ArrayList<>();for(String name:share.files)result.add(new File(new File(root,share.id),name).toURI().toString());return result;
  }
  public synchronized void validateFiles(Share share) throws Exception {
    for(String name:share.files){File file=new File(new File(root,share.id),name);if(!file.isFile()||!file.canRead())throw new IOException("share_source_unavailable");}
  }
  private List<Share> copyJobs(){List<Share> next=new ArrayList<>();for(Share s:jobs)next.add(s.copy());return next;}
  private static Share find(List<Share> jobs,String id)throws IOException{for(Share s:jobs)if(s.id.equals(id))return s;throw new IOException("share_not_found");}
  private static String trim(String s,int max){return s==null?null:s.substring(0,Math.min(s.length(),max));}
  private static void string(DataOutputStream out,String s)throws IOException{out.writeBoolean(s!=null);if(s!=null)out.writeUTF(s);}
  private static String string(DataInputStream in)throws IOException{return in.readBoolean()?in.readUTF():null;}
  private void commit(List<Share> next,String fingerprint)throws Exception{
    String history=lastFingerprint;
    if(!Objects.equals(fingerprint,lastFingerprint)){
      List<String> recent=new ArrayList<>(Arrays.asList(lastFingerprint.split("\\|",-1)));recent.remove("");recent.remove(fingerprint);recent.add(0,fingerprint);if(recent.size()>32)recent=recent.subList(0,32);history=String.join("|",recent);
    }
    ByteArrayOutputStream bytes=new ByteArrayOutputStream();try(DataOutputStream out=new DataOutputStream(bytes)){
      out.writeInt(1);string(out,history);out.writeInt(next.size());
      for(Share s:next){string(out,s.id);string(out,s.text);string(out,s.mime);string(out,s.fingerprint);string(out,s.accountId);string(out,s.roomId);out.writeLong(s.createdAt);out.writeInt(s.rejectedFiles);out.writeInt(s.next);out.writeBoolean(s.confirmed);out.writeInt(s.files.size());for(String name:s.files)string(out,name);}
    }
    Cipher cipher=Cipher.getInstance("AES/GCM/NoPadding");cipher.init(Cipher.ENCRYPT_MODE,keys.get());
    File temp=new File(root,"journal.tmp");try(FileOutputStream file=new FileOutputStream(temp);DataOutputStream out=new DataOutputStream(file)){
      byte[] iv=cipher.getIV();out.writeInt(iv.length);out.write(iv);out.write(cipher.doFinal(bytes.toByteArray()));out.flush();file.getFD().sync();
    }
    if(backup.exists()&&!backup.delete())throw new IOException("share_journal_write_failed");
    if(manifest.exists()&&!manifest.renameTo(backup))throw new IOException("share_journal_write_failed");
    if(!temp.renameTo(manifest)){backup.renameTo(manifest);throw new IOException("share_journal_write_failed");}
    backup.delete();jobs=next;lastFingerprint=history;
  }
  private void read()throws Exception{
    byte[] data;try(FileInputStream input=new FileInputStream(manifest)){if(manifest.length()>512*1024)throw new IOException("share_journal_invalid");data=new byte[(int)manifest.length()];new DataInputStream(input).readFully(data);}
    DataInputStream file=new DataInputStream(new ByteArrayInputStream(data));int length=file.readInt();if(length!=12)throw new IOException("share_journal_invalid");byte[] iv=new byte[length];file.readFully(iv);byte[] encrypted=new byte[data.length-4-length];file.readFully(encrypted);
    Cipher cipher=Cipher.getInstance("AES/GCM/NoPadding");cipher.init(Cipher.DECRYPT_MODE,keys.get(),new GCMParameterSpec(128,iv));
    try(DataInputStream in=new DataInputStream(new ByteArrayInputStream(cipher.doFinal(encrypted)))){
      if(in.readInt()!=1)throw new IOException("share_journal_invalid");lastFingerprint=string(in);int count=in.readInt();if(count<0||count>MAX_JOBS)throw new IOException("share_journal_invalid");
      for(int i=0;i<count;i++){Share s=new Share();s.id=string(in);UUID.fromString(s.id);s.text=string(in);s.mime=string(in);s.fingerprint=string(in);s.accountId=string(in);s.roomId=string(in);s.createdAt=in.readLong();s.rejectedFiles=in.readInt();s.next=in.readInt();s.confirmed=in.readBoolean();int files=in.readInt();if(files<0||files>MAX_FILES)throw new IOException("share_journal_invalid");for(int f=0;f<files;f++){String name=string(in);if(!name.matches("[0-9]{1,2}\\.shared"))throw new IOException("share_journal_invalid");s.files.add(name);}if(s.next<0||s.next>s.itemCount())throw new IOException("share_journal_invalid");jobs.add(s);}
    }
  }
  private void cleanup()throws Exception{
    // Expired unconfirmed imports and interrupted pre-commit copies cannot accumulate indefinitely.
    List<Share> next=copyJobs();next.removeIf(s->!s.confirmed&&System.currentTimeMillis()-s.createdAt>7L*24*3600000);
    if(next.size()!=jobs.size())commit(next,lastFingerprint);
    Set<String> keep=new HashSet<>();for(Share s:jobs)keep.add(s.id);
    for(Share share:jobs){File[] copied=new File(root,share.id).listFiles();if(copied!=null)for(File file:copied)if(!share.files.contains(file.getName()))deleteTree(file);}
    File[] children=root.listFiles();if(children!=null)for(File child:children)if(child.isDirectory()&&!keep.contains(child.getName()))deleteTree(child);
  }
  private static void deleteTree(File file){File[] children=file.listFiles();if(children!=null)for(File child:children)deleteTree(child);file.delete();}
}
