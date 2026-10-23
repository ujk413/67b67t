package ru.b67t.roomkey;

import java.io.*;
import java.net.*;
import java.security.MessageDigest;
import java.util.Properties;
import java.util.function.BooleanSupplier;
import java.util.regex.*;

/** Durable byte-range download. No APK bytes cross the JavaScript bridge. */
public final class ApkDownload {
  public interface Progress { void update(long bytes, long total); }
  private static final Pattern RANGE = Pattern.compile("bytes (\\d+)-(\\d+)/(\\d+)");
  private static void check(BooleanSupplier cancelled) throws IOException {
    if (cancelled.getAsBoolean()) throw new IOException("download_cancelled");
  }
  public static String sha256(File file, BooleanSupplier cancelled) throws Exception {
    MessageDigest digest=MessageDigest.getInstance("SHA-256");
    try(InputStream input=new FileInputStream(file)) {
      byte[] bytes=new byte[65536];int n;
      while((n=input.read(bytes))!=-1) {check(cancelled);digest.update(bytes,0,n);}
    }
    StringBuilder result=new StringBuilder();for(byte b:digest.digest())result.append(String.format("%02x",b & 255));return result.toString();
  }
  public static synchronized String download(String url, File file, String identity, long size, String hash,
      BooleanSupplier cancelled, Progress progress) throws Exception {
    if(size<=0 || size>512L*1024*1024 || !hash.matches("[a-fA-F0-9]{64}"))throw new IOException("invalid_release");
    file.getParentFile().mkdirs();File metadata=new File(file.getPath()+".identity");
    Properties state=new Properties();
    if(metadata.isFile())try(InputStream in=new FileInputStream(metadata)){state.load(in);}catch(IOException ignored){}
    String key=url+"\n"+identity+"\n"+size+"\n"+hash.toLowerCase(java.util.Locale.ROOT);
    if(!key.equals(state.getProperty("release")) || file.length()>size) {
      try(RandomAccessFile output=new RandomAccessFile(file,"rw")){output.setLength(0);}
      state.clear();state.setProperty("release",key);
      File next=new File(metadata.getPath()+".tmp");
      try(FileOutputStream out=new FileOutputStream(next)){state.store(out,null);out.getFD().sync();}
      if(metadata.exists() && !metadata.delete())throw new IOException("download_state_failed");
      if(!next.renameTo(metadata))throw new IOException("download_state_failed");
    }
    check(cancelled);long offset=file.length();progress.update(offset,size);
    if(offset<size) {
      HttpURLConnection connection=(HttpURLConnection)new URL(url).openConnection();
      connection.setInstanceFollowRedirects(false);connection.setConnectTimeout(15000);connection.setReadTimeout(15000);
      connection.setRequestProperty("Accept-Encoding","identity");
      if(offset>0)connection.setRequestProperty("Range","bytes="+offset+"-");
      try {
        int status=connection.getResponseCode();
        if(status==200)offset=0;
        else if(status==206) {
          Matcher range=RANGE.matcher(String.valueOf(connection.getHeaderField("Content-Range")));
          if(!range.matches() || Long.parseLong(range.group(1))!=offset || Long.parseLong(range.group(3))!=size || Long.parseLong(range.group(2))!=size-1)throw new IOException("invalid_download_range");
        } else throw new IOException("download_http_"+status);
        check(cancelled);
        try(InputStream input=connection.getInputStream();RandomAccessFile output=new RandomAccessFile(file,"rw")) {
          if(offset==0)output.setLength(0);output.seek(offset);
          byte[] buffer=new byte[65536];int count;
          while((count=input.read(buffer))!=-1) {
            check(cancelled);
            if(offset+count>size){output.setLength(0);throw new IOException("incomplete_download");}
            output.write(buffer,0,count);offset+=count;progress.update(offset,size);
          }
          output.getFD().sync();
        }
      } finally {connection.disconnect();}
    }
    check(cancelled);
    if(file.length()!=size)throw new IOException("incomplete_download");
    if(!hash.equalsIgnoreCase(sha256(file,cancelled))) {file.delete();metadata.delete();throw new IOException("checksum_mismatch");}
    return file.toURI().toString();
  }
}
