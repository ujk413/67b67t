package ru.b67t.roomkey

import org.json.JSONObject
import org.junit.Assert.*
import org.junit.Test
import org.whispersystems.libsignal.ecc.Curve
import org.whispersystems.libsignal.IdentityKeyPair
import org.whispersystems.libsignal.state.SessionRecord
import org.whispersystems.libsignal.util.KeyHelper
import java.util.Base64

class BackupBridgeTest {
  private val user="00000000-0000-4000-8000-000000000001"
  private fun b64(value:ByteArray)=Base64.getEncoder().encodeToString(value)
  private fun binary(value:ByteArray)=JSONObject().put("__b64",b64(value))
  private fun webArchive():JSONObject {
    val identity=KeyHelper.generateIdentityKeyPair();val pre=Curve.generateKeyPair();val signed=KeyHelper.generateSignedPreKey(identity,1)
    fun pair(pub:ByteArray,priv:ByteArray)=JSONObject().put("pubKey",binary(pub)).put("privKey",binary(priv))
    return JSONObject().put("account_id",user).put("identityKey",pair(identity.publicKey.serialize(),identity.privateKey.serialize())).put("registrationId",123)
      .put("preKeys",JSONObject().put("0",pair(pre.publicKey.serialize(),pre.privateKey.serialize())))
      .put("signedPreKeys",JSONObject().put("1",pair(signed.keyPair.publicKey.serialize(),signed.keyPair.privateKey.serialize())))
      .put("meta",JSONObject().put("signedPreKeySignature",b64(signed.signature)))
      .put("sessions",JSONObject().put("session-peer","foreign-js-ratchet"))
      .put("groupKeys",JSONObject().put("gk:$user:room:own",JSONObject().put("chain","sender-chain")))
      .put("mobile_key_state",JSONObject().put("e2e:room",JSONObject().put("key","room-key")))
  }
  @Test fun importsActualWebSignatureStringAndPreservesForeignAndNativeState(){
    val web=webArchive();val imported=BackupBridge.import(user,web.toString());assertTrue(imported.sessionsRebuilt)
    val identity=IdentityKeyPair(imported.records.getValue("identity"));assertEquals(web.getJSONObject("identityKey").getJSONObject("pubKey").getString("__b64"),b64(identity.publicKey.serialize()))
    val records=imported.records.toMutableMap();records["session:00000000-0000-4000-8000-000000000002:12"]=SessionRecord().serialize()
    val exported=JSONObject(BackupBridge.export(user,records));assertEquals("foreign-js-ratchet",exported.getJSONObject("sessions").getString("session-peer"));assertTrue(exported.getJSONObject("meta").get("signedPreKeySignature") is String)
    assertEquals("sender-chain",exported.getJSONObject("groupKeys").getJSONObject("gk:$user:room:own").getString("chain"))
    assertEquals("room-key",exported.getJSONObject("mobile_key_state").getJSONObject("e2e:room").getString("key"))
    val roundtrip=BackupBridge.import(user,exported.toString());assertFalse(roundtrip.sessionsRebuilt);assertArrayEquals(records.getValue("session:00000000-0000-4000-8000-000000000002:12"),roundtrip.records.getValue("session:00000000-0000-4000-8000-000000000002:12"))
  }
  @Test fun rejectsAccountMismatchAndMismatchedPrivateKeyBeforeReturningRecords(){
    val archive=webArchive();assertThrows(IllegalArgumentException::class.java){BackupBridge.import("00000000-0000-4000-8000-000000000009",archive.toString())}
    archive.getJSONObject("identityKey").put("privKey",binary(Curve.generateKeyPair().privateKey.serialize()))
    assertThrows(IllegalArgumentException::class.java){BackupBridge.import(user,archive.toString())}
  }
  @Test fun rejectsInvalidSignedPrekeySignature(){val archive=webArchive();archive.getJSONObject("meta").put("signedPreKeySignature",b64(ByteArray(64)));assertThrows(IllegalArgumentException::class.java){BackupBridge.import(user,archive.toString())}}
}
