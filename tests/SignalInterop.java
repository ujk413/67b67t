import java.nio.file.*;
import java.util.*;
import java.io.*;
import org.whispersystems.libsignal.*;
import org.whispersystems.libsignal.protocol.*;
import org.whispersystems.libsignal.state.*;
import org.whispersystems.libsignal.state.impl.*;
import org.whispersystems.libsignal.util.KeyHelper;

public class SignalInterop {
  static String enc(byte[] value){return Base64.getEncoder().encodeToString(value);}
  static byte[] dec(String value){return Base64.getDecoder().decode(value);}
  public static void main(String[] args) throws Exception {
    Path path=Paths.get(args[1]); Properties p=new Properties();
    if(args[0].equals("generate")) {
      IdentityKeyPair identity=KeyHelper.generateIdentityKeyPair(); PreKeyRecord pre=KeyHelper.generatePreKeys(1,1).get(0); SignedPreKeyRecord signed=KeyHelper.generateSignedPreKey(identity,1);
      p.setProperty("identity",enc(identity.serialize()));p.setProperty("pre",enc(pre.serialize()));p.setProperty("signed",enc(signed.serialize()));p.setProperty("registration","123");
      p.setProperty("identityPublic",enc(identity.getPublicKey().serialize()));p.setProperty("prePublic",enc(pre.getKeyPair().getPublicKey().serialize()));p.setProperty("signedPublic",enc(signed.getKeyPair().getPublicKey().serialize()));p.setProperty("signature",enc(signed.getSignature()));
    } else {
      try(InputStream in=Files.newInputStream(path)){p.load(in);}
      InMemorySignalProtocolStore store=new InMemorySignalProtocolStore(new IdentityKeyPair(dec(p.getProperty("identity"))),123);
      store.storePreKey(1,new PreKeyRecord(dec(p.getProperty("pre"))));store.storeSignedPreKey(1,new SignedPreKeyRecord(dec(p.getProperty("signed"))));
      SessionCipher cipher=new SessionCipher(store,new SignalProtocolAddress("alice",7));
      byte[] plain=cipher.decrypt(new PreKeySignalMessage(dec(args[2])));
      p.setProperty("received",enc(plain));
      CiphertextMessage reply=cipher.encrypt("Ответ Android: проверка 🔑".getBytes("UTF-8"));p.setProperty("reply",enc(reply.serialize()));p.setProperty("replyType",reply.getType()==CiphertextMessage.PREKEY_TYPE?"3":"1");
    }
    try(OutputStream out=Files.newOutputStream(path)){p.store(out,"Synthetic interoperability fixture");}
  }
}
