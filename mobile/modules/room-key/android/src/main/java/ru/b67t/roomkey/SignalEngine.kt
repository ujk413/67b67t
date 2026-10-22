package ru.b67t.roomkey

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import org.json.JSONArray
import org.json.JSONObject
import org.whispersystems.libsignal.IdentityKey
import org.whispersystems.libsignal.IdentityKeyPair
import org.whispersystems.libsignal.SessionBuilder
import org.whispersystems.libsignal.SessionCipher
import org.whispersystems.libsignal.SignalProtocolAddress
import org.whispersystems.libsignal.ecc.Curve
import org.whispersystems.libsignal.protocol.CiphertextMessage
import org.whispersystems.libsignal.protocol.PreKeySignalMessage
import org.whispersystems.libsignal.protocol.SignalMessage
import org.whispersystems.libsignal.state.PreKeyBundle
import org.whispersystems.libsignal.state.PreKeyRecord
import org.whispersystems.libsignal.state.SessionRecord
import org.whispersystems.libsignal.state.SignedPreKeyRecord
import org.whispersystems.libsignal.state.impl.InMemorySignalProtocolStore
import org.whispersystems.libsignal.util.KeyHelper
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

private fun encode(bytes: ByteArray) = Base64.encodeToString(bytes, Base64.NO_WRAP)
private fun decode(value: String) = Base64.decode(value, Base64.NO_WRAP)

// Protocol-v3 interoperability with the existing web libsignal implementation.
// Long-term identity, sessions and one-time private keys remain encrypted by Android Keystore.
internal class SignalEngine(private val context: Context) {
  private val stores = mutableMapOf<String, PersistentSignalStore>()
  @Synchronized private fun store(user: String): PersistentSignalStore {
    require(Regex("[a-fA-F0-9-]{36}").matches(user))
    return stores.getOrPut(user) { PersistentSignalStore(KeyVault(context, user)) }
  }
  private fun address(user: String, device: String): SignalProtocolAddress {
    require(Regex("[a-fA-F0-9-]{36}").matches(user))
    val id = device.toLongOrNull() ?: throw IllegalArgumentException("Invalid device id")
    return SignalProtocolAddress(user, (id and 0x7fffffff).toInt().coerceAtLeast(1))
  }
  @Synchronized fun bundle(user: String, count: Int): String {
    val store = store(user)
    if (!store.containsSignedPreKey(1)) store.storeSignedPreKey(1, KeyHelper.generateSignedPreKey(store.identityKeyPair, 1))
    val signed = store.loadSignedPreKey(1)
    val first = (store.vault.get("next")?.toString(Charsets.UTF_8)?.toIntOrNull() ?: 1)
    val keys = KeyHelper.generatePreKeys(first, count.coerceIn(1, 100))
    val preKeys = JSONArray()
    for (key in keys) { store.storePreKey(key.id, key); preKeys.put(JSONObject().put("keyId", key.id).put("publicKey", encode(key.keyPair.publicKey.serialize()))) }
    store.vault.put("next", (first + keys.size).toString().toByteArray())
    return JSONObject().put("identityKey", encode(store.identityKeyPair.publicKey.serialize())).put("registrationId", store.localRegistrationId)
      .put("signedPreKey", JSONObject().put("keyId", 1).put("publicKey", encode(signed.keyPair.publicKey.serialize())).put("signature", encode(signed.signature))).put("preKeys", preKeys).toString()
  }
  @Synchronized fun hasSession(user: String, peer: String, device: String) = store(user).containsSession(address(peer, device))
  @Synchronized fun session(user: String, peer: String, device: String, json: String) {
    val bundle = JSONObject(json)
    val signed = bundle.getJSONObject("signed_pre_key")
    val pre = bundle.optJSONObject("pre_key")
    val record = PreKeyBundle(bundle.optInt("registration_id", 0), address(peer, device).deviceId,
      pre?.getInt("keyId") ?: 0, pre?.getString("publicKey")?.let { Curve.decodePoint(decode(it), 0) },
      signed.getInt("keyId"), Curve.decodePoint(decode(signed.getString("publicKey")), 0), decode(signed.getString("signature")), IdentityKey(decode(bundle.getString("identity_key")), 0))
    SessionBuilder(store(user), address(peer, device)).process(record)
  }
  @Synchronized fun encrypt(user: String, peer: String, device: String, text: String): String {
    val message = SessionCipher(store(user), address(peer, device)).encrypt(text.toByteArray(Charsets.UTF_8))
    return JSONObject().put("type", if (message.type == CiphertextMessage.PREKEY_TYPE) 3 else 1).put("body", encode(message.serialize())).toString()
  }
  @Synchronized fun decrypt(user: String, peer: String, device: String, json: String): String {
    val message = JSONObject(json)
    val cipher = SessionCipher(store(user), address(peer, device))
    val bytes = decode(message.getString("body"))
    require(bytes.size <= 256 * 1024) { "Signal message too large" }
    val plain = when (message.getInt("type")) { 3 -> cipher.decrypt(PreKeySignalMessage(bytes)); 1 -> cipher.decrypt(SignalMessage(bytes)); else -> throw IllegalArgumentException("Invalid Signal message") }
    return plain.toString(Charsets.UTF_8)
  }
}

private class KeyVault(context: Context, user: String) {
  private val preferences = context.getSharedPreferences("67chat-signal-$user", Context.MODE_PRIVATE)
  private val key: SecretKey by lazy {
    val alias = "67chat-signal-v1"
    val store = KeyStore.getInstance("AndroidKeyStore").apply { load(null) }
    (store.getKey(alias, null) as? SecretKey) ?: KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, "AndroidKeyStore").run {
      init(KeyGenParameterSpec.Builder(alias, KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT).setBlockModes(KeyProperties.BLOCK_MODE_GCM).setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE).build()); generateKey()
    }
  }
  fun keys(): Set<String> = preferences.all.keys
  fun get(name: String): ByteArray? {
    val value = preferences.getString(name, null) ?: return null
    val bytes = decode(value)
    val cipher = Cipher.getInstance("AES/GCM/NoPadding")
    cipher.init(Cipher.DECRYPT_MODE, key, GCMParameterSpec(128, bytes.copyOfRange(0, 12)))
    cipher.updateAAD(name.toByteArray()); return cipher.doFinal(bytes.copyOfRange(12, bytes.size))
  }
  fun put(name: String, value: ByteArray) {
    val cipher = Cipher.getInstance("AES/GCM/NoPadding"); cipher.init(Cipher.ENCRYPT_MODE, key); cipher.updateAAD(name.toByteArray())
    check(preferences.edit().putString(name, encode(cipher.iv + cipher.doFinal(value))).commit()) { "Cannot persist Signal state" }
  }
  fun remove(name: String) { check(preferences.edit().remove(name).commit()) }
}
private fun identity(vault: KeyVault): IdentityKeyPair { val existing = vault.get("identity"); if (existing != null) return IdentityKeyPair(existing); val pair = KeyHelper.generateIdentityKeyPair(); vault.put("identity", pair.serialize()); return pair }
private fun registration(vault: KeyVault): Int { val existing = vault.get("registration"); if (existing != null) return existing.toString(Charsets.UTF_8).toInt(); val value = KeyHelper.generateRegistrationId(false); vault.put("registration", value.toString().toByteArray()); return value }
private class PersistentSignalStore(val vault: KeyVault) : InMemorySignalProtocolStore(identity(vault), registration(vault)) {
  init {
    for (key in vault.keys()) {
      val value = vault.get(key) ?: continue
      when {
        key.startsWith("pre:") -> super.storePreKey(key.substringAfter(':').toInt(), PreKeyRecord(value))
        key.startsWith("signed:") -> super.storeSignedPreKey(key.substringAfter(':').toInt(), SignedPreKeyRecord(value))
        key.startsWith("session:") -> super.storeSession(parseAddress(key.substringAfter(':')), SessionRecord(value))
        key.startsWith("trust:") -> super.saveIdentity(parseAddress(key.substringAfter(':')), IdentityKey(value, 0))
      }
    }
  }
  private fun parseAddress(value: String) = SignalProtocolAddress(value.substringBeforeLast(':'), value.substringAfterLast(':').toInt())
  private fun addressKey(address: SignalProtocolAddress) = address.name + ":" + address.deviceId
  override fun storePreKey(id: Int, record: PreKeyRecord) { vault.put("pre:$id", record.serialize()); super.storePreKey(id, record) }
  override fun removePreKey(id: Int) { vault.remove("pre:$id"); super.removePreKey(id) }
  override fun storeSignedPreKey(id: Int, record: SignedPreKeyRecord) { vault.put("signed:$id", record.serialize()); super.storeSignedPreKey(id, record) }
  override fun removeSignedPreKey(id: Int) { vault.remove("signed:$id"); super.removeSignedPreKey(id) }
  override fun storeSession(address: SignalProtocolAddress, record: SessionRecord) { vault.put("session:" + addressKey(address), record.serialize()); super.storeSession(address, record) }
  override fun deleteSession(address: SignalProtocolAddress) { vault.remove("session:" + addressKey(address)); super.deleteSession(address) }
  override fun deleteAllSessions(name: String) { for (key in vault.keys().filter { it.startsWith("session:$name:") }) vault.remove(key); super.deleteAllSessions(name) }
  override fun saveIdentity(address: SignalProtocolAddress, identity: IdentityKey): Boolean { val changed = super.saveIdentity(address, identity); vault.put("trust:" + addressKey(address), identity.serialize()); return changed }
}
