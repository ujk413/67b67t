package ru.b67t.roomkey

import android.util.Base64
import org.json.JSONObject
import org.whispersystems.libsignal.IdentityKey
import org.whispersystems.libsignal.IdentityKeyPair
import org.whispersystems.libsignal.ecc.Curve
import org.whispersystems.libsignal.ecc.ECKeyPair
import org.whispersystems.libsignal.state.PreKeyRecord
import org.whispersystems.libsignal.state.SessionRecord
import org.whispersystems.libsignal.state.SignedPreKeyRecord
import java.security.SecureRandom

/** Converts only after every record validates. No persistence or key logging in this class. */
internal object BackupBridge {
  data class Imported(val records: Map<String, ByteArray>, val sessionsRebuilt: Boolean)
  private fun b64(b: ByteArray) = Base64.encodeToString(b, Base64.NO_WRAP)
  private fun binary(b: ByteArray) = JSONObject().put("__b64", b64(b))
  private fun bytes(o: JSONObject, name: String): ByteArray {
    val wrapped = o.getJSONObject(name)
    require(wrapped.length() == 1) { "Invalid binary field" }
    val value = wrapped.getString("__b64")
    require(value.length <= 1024 * 1024) { "Backup field too large" }
    return Base64.decode(value, Base64.NO_WRAP)
  }
  private fun signature(o:JSONObject,name:String):ByteArray {val value=o.get(name);return if(value is String)Base64.decode(value,Base64.NO_WRAP)else bytes(o,name)}
  private fun pair(publicBytes: ByteArray, privateBytes: ByteArray): ECKeyPair {
    require(publicBytes.size == 33 && privateBytes.size == 32) { "Invalid Curve25519 key" }
    val publicKey = Curve.decodePoint(publicBytes, 0)
    val privateKey = Curve.decodePrivatePoint(privateBytes)
    val challenge = ByteArray(32).also { SecureRandom().nextBytes(it) }
    val signature = Curve.calculateSignature(privateKey, challenge)
    require(Curve.verifySignature(publicKey, challenge, signature)) { "Private key mismatch" }
    return ECKeyPair(publicKey, privateKey)
  }
  fun publicBundle(records:Map<String,ByteArray>):String {
    val identity=IdentityKeyPair(requireNotNull(records["identity"]));val signed=SignedPreKeyRecord(requireNotNull(records["signed:1"]));val pre=org.json.JSONArray()
    for((name,value)in records)if(name.startsWith("pre:")){val key=PreKeyRecord(value);pre.put(JSONObject().put("keyId",key.id).put("publicKey",b64(key.keyPair.publicKey.serialize())))}
    return JSONObject().put("identityKey",b64(identity.publicKey.serialize())).put("registrationId",requireNotNull(records["registration"]).toString(Charsets.UTF_8).toInt()).put("signedPreKey",JSONObject().put("keyId",signed.id).put("publicKey",b64(signed.keyPair.publicKey.serialize())).put("signature",b64(signed.signature))).put("preKeys",pre).toString()
  }
  fun export(user: String, records: Map<String, ByteArray>): String {
    val identity = IdentityKeyPair(requireNotNull(records["identity"]))
    val keys = JSONObject().put("account_id", user)
      .put("identityKey", JSONObject().put("pubKey", binary(identity.publicKey.serialize())).put("privKey", binary(identity.privateKey.serialize())))
      .put("registrationId", requireNotNull(records["registration"]).toString(Charsets.UTF_8).toInt())
    val pre = JSONObject(); val signed = JSONObject(); val nativeSessions = JSONObject()
    for ((name, value) in records) when {
      name.startsWith("pre:") -> {
        val key = PreKeyRecord(value)
        pre.put(key.id.toString(), JSONObject().put("keyId", key.id).put("pubKey", binary(key.keyPair.publicKey.serialize())).put("privKey", binary(key.keyPair.privateKey.serialize())))
      }
      name.startsWith("signed:") -> {
        val key = SignedPreKeyRecord(value)
        signed.put(key.id.toString(), JSONObject().put("keyId", key.id).put("pubKey", binary(key.keyPair.publicKey.serialize())).put("privKey", binary(key.keyPair.privateKey.serialize())).put("signature", binary(key.signature)).put("timestamp", key.timestamp))
      }
      name.startsWith("session:") -> nativeSessions.put(name.substringAfter(':'), b64(value))
    }
    val foreign = records["backup:foreign"]?.toString(Charsets.UTF_8)?.let { JSONObject(it) } ?: JSONObject()
    keys.put("preKeys", pre).put("signedPreKeys", signed).put("sessions", foreign.optJSONObject("sessions") ?: JSONObject())
      .put("groupKeys", foreign.optJSONObject("groupKeys") ?: JSONObject()).put("peerIdentities", foreign.optJSONObject("peerIdentities") ?: JSONObject())
      .put("meta", JSONObject().put("nextPreKeyId", records["next"]?.toString(Charsets.UTF_8)?.toIntOrNull() ?: 1).apply { records["signed:1"]?.let { put("signedPreKeySignature", b64(SignedPreKeyRecord(it).signature)) } })
      .put("mobile_key_state", foreign.opt("mobile_key_state") ?: JSONObject.NULL)
      .put("native_sessions", JSONObject().put("v", 1).put("account_id", user).put("records", nativeSessions))
    return keys.toString()
  }
  fun import(user: String, json: String): Imported {
    require(json.toByteArray().size <= 24 * 1024 * 1024) { "Backup too large" }
    val keys = JSONObject(json)
    require(!keys.has("secretChats") && !keys.has("secret_chats")) { "Secret chats cannot be imported" }
    if (keys.has("account_id")) require(keys.getString("account_id") == user) { "USER_MISMATCH" }
    val identityJson = keys.getJSONObject("identityKey")
    val identityPair = pair(bytes(identityJson, "pubKey"), bytes(identityJson, "privKey"))
    val identity = IdentityKeyPair(IdentityKey(identityPair.publicKey), identityPair.privateKey)
    val registration = keys.getInt("registrationId")
    require(registration in 1..16383) { "Invalid registration id" }
    val result = linkedMapOf("identity" to identity.serialize(), "registration" to registration.toString().toByteArray())
    val pre = keys.getJSONObject("preKeys"); require(pre.length() <= 10000)
    var next = 1
    for (name in pre.keys()) {
      val id = name.toInt(); require(id in 0..0xFFFFFF)
      val p = pre.getJSONObject(name); val keyPair = pair(bytes(p, "pubKey"), bytes(p, "privKey"))
      result["pre:$id"] = PreKeyRecord(id, keyPair).serialize(); next = maxOf(next, id + 1)
    }
    val signed = keys.getJSONObject("signedPreKeys"); require(signed.length() <= 100)
    for (name in signed.keys()) {
      val id = name.toInt(); require(id in 1..0xFFFFFF)
      val p = signed.getJSONObject(name); val keyPair = pair(bytes(p, "pubKey"), bytes(p, "privKey")); val signature = if (p.has("signature")) signature(p, "signature") else signature(keys.getJSONObject("meta"), "signedPreKeySignature")
      require(signature.size == 64 && Curve.verifySignature(identity.publicKey.publicKey, keyPair.publicKey.serialize(), signature)) { "Signed key signature mismatch" }
      result["signed:$id"] = SignedPreKeyRecord(id, p.optLong("timestamp", System.currentTimeMillis()), keyPair, signature).serialize()
    }
    require(keys.getJSONObject("sessions").length() <= 10000)
    result["next"] = next.toString().toByteArray()
    result["backup:foreign"] = JSONObject().put("sessions", keys.getJSONObject("sessions"))
      .put("groupKeys", keys.optJSONObject("groupKeys") ?: JSONObject()).put("peerIdentities", keys.optJSONObject("peerIdentities") ?: JSONObject()).put("mobile_key_state", keys.opt("mobile_key_state") ?: JSONObject.NULL).toString().toByteArray()
    val native = keys.optJSONObject("native_sessions")
    var rebuilt = true
    if (native != null) {
      require(native.getInt("v") == 1 && native.getString("account_id") == user) { "USER_MISMATCH" }
      val sessions = native.getJSONObject("records"); require(sessions.length() <= 10000)
      for (address in sessions.keys()) {
        require(Regex("[a-fA-F0-9-]{36}:[0-9]{1,10}").matches(address)) { "Invalid session address" }
        val record = SessionRecord(Base64.decode(sessions.getString(address), Base64.NO_WRAP))
        result["session:$address"] = record.serialize()
      }
      rebuilt = false
    }
    return Imported(result, rebuilt)
  }
}
