package ru.b67t.roomkey

import android.util.Base64
import java.io.File
import java.net.URI
import android.content.Intent
import android.media.AudioManager
import android.media.AudioDeviceInfo
import android.os.Build
import javax.crypto.Cipher
import javax.crypto.CipherInputStream
import javax.crypto.CipherOutputStream
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec
import expo.modules.kotlin.modules.Module
import expo.modules.kotlin.modules.ModuleDefinition

class RoomKeyModule : Module() {
  private val apkCancelled = java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicBoolean>()
  private val signal by lazy { SignalEngine(requireNotNull(appContext.reactContext)) }
  private val transcoder by lazy { MediaTranscoder(requireNotNull(appContext.reactContext)) }
  override fun definition() = ModuleDefinition {
    Name("RoomKey")
    Events("callAction", "quickReply", "messageOpen", "apkProgress", "mediaProgress")
    Function("setUiLanguage") { language:String -> NativeUi.setLanguage(requireNotNull(appContext.reactContext),language) }
    AsyncFunction("setScreenProtectionAsync") { enabled:Boolean, promise:expo.modules.kotlin.Promise ->
      val activity=appContext.currentActivity
      if(activity==null)promise.reject("screen_unavailable","Экран приложения недоступен",null)
      else activity.runOnUiThread {try{if(enabled)activity.window.addFlags(android.view.WindowManager.LayoutParams.FLAG_SECURE) else activity.window.clearFlags(android.view.WindowManager.LayoutParams.FLAG_SECURE);promise.resolve(null)}catch(error:Throwable){promise.reject("screen_protection_failed",error.message,error)}}
    }
    Function("cancelVideoProcessing") { id:String -> transcoder.cancel(id) }
    AsyncFunction("transcodeVideoAsync") { uri:String, height:Int, id:String, promise:expo.modules.kotlin.Promise ->
      try {transcoder.start(uri,height,id,{progress->sendEvent("mediaProgress",mapOf("id" to id,"progress" to progress))},{result->promise.resolve(result)},{error->promise.reject("video_processing_failed",error.message,error)})}
      catch(error:Throwable){promise.reject("video_processing_failed",error.message,error)}
    }
    Function("cancelApkDownload") { id: String -> apkCancelled[id]?.set(true); Unit }
    AsyncFunction("downloadApkAsync") { url: String, identity: String, size: Double, hash: String, id: String ->
      require(url.startsWith("https://")) { "invalid_release_url" }
      val context = requireNotNull(appContext.reactContext)
      val cancelled = java.util.concurrent.atomic.AtomicBoolean(false)
      require(apkCancelled.putIfAbsent(id, cancelled) == null) { "download_busy" }
      try {
        ApkDownload.download(url, File(context.filesDir, "updates/67chat-update.apk"), identity,
          size.toLong(), hash, { cancelled.get() }, { bytes, total ->
            sendEvent("apkProgress", mapOf("id" to id, "bytes" to bytes.toDouble(), "total" to total.toDouble()))
          })
      } finally { apkCancelled.remove(id) }
    }
    AsyncFunction("apkSha256Async") { uri: String ->
      val context = requireNotNull(appContext.reactContext)
      val file = File(URI(uri)).canonicalFile
      require(file.path.startsWith(context.filesDir.canonicalPath + File.separator) || file.path.startsWith(context.cacheDir.canonicalPath + File.separator))
      ApkDownload.sha256(file, { false })
    }
    OnNewIntent { intent ->
      if(intent.hasExtra("67chat_call_id")) { appContext.currentActivity?.intent=intent;sendEvent("callAction",emptyMap<String,String>()) }
      if(intent.hasExtra("67chat_reply_room")) { appContext.currentActivity?.intent=intent;sendEvent(if(intent.action=="67chat.notification.open")"messageOpen" else "quickReply",emptyMap<String,String>()) }
    }
    Function("consumeQuickReply") {
      val intent=appContext.currentActivity?.intent
      val reply=QuickReplyNotifications.fromIntent(intent)
      if(reply!=null) { intent?.removeExtra("67chat_reply_room");intent?.removeExtra("67chat_reply_recipient") }
      reply
    }
    Function("consumeMessageOpen") {
      val intent=appContext.currentActivity?.intent
      val target=QuickReplyNotifications.openFromIntent(intent)
      if(target!=null) { intent?.removeExtra("67chat_reply_room");intent?.removeExtra("67chat_reply_recipient") }
      target
    }
    AsyncFunction("pendingCallAsync") {
      val context=requireNotNull(appContext.reactContext)
      val saved=CallNotifications.pending(context)?.let { org.json.JSONObject(it) }
      val intent=appContext.currentActivity?.intent
      val id=intent?.getStringExtra("67chat_call_id")
      if(saved!=null && (id==null || id==saved.optString("call_id"))) {
        saved.put("action",intent?.getStringExtra("67chat_call_action") ?: "open")
        intent?.removeExtra("67chat_call_id");intent?.removeExtra("67chat_call_action");saved.toString()
      } else null
    }
    AsyncFunction("clearCallAsync") { id: String -> CallNotifications.clear(requireNotNull(appContext.reactContext),id) }
    AsyncFunction("startCallServiceAsync") { video: Boolean ->
      AccountTransport.requireDirectCalls()
      val context=requireNotNull(appContext.reactContext)
      val intent=Intent(context,ActiveCallService::class.java).putExtra("video",video)
      if(Build.VERSION.SDK_INT>=26)context.startForegroundService(intent) else context.startService(intent)
      Unit
    }
    AsyncFunction("stopCallServiceAsync") { requireNotNull(appContext.reactContext).stopService(Intent(appContext.reactContext,ActiveCallService::class.java)) }
    AsyncFunction("speakerAsync") { speaker: Boolean ->
      val audio=requireNotNull(appContext.reactContext).getSystemService(AudioManager::class.java)
      if(Build.VERSION.SDK_INT>=31) {
        val type=if(speaker)AudioDeviceInfo.TYPE_BUILTIN_SPEAKER else AudioDeviceInfo.TYPE_BUILTIN_EARPIECE
        val device=audio.availableCommunicationDevices.firstOrNull { it.type==type }
        if(device!=null)audio.setCommunicationDevice(device)
      } else audio.isSpeakerphoneOn=speaker
    }
    AsyncFunction("audioOutputsAsync") {
      val context=requireNotNull(appContext.reactContext)
      val audio=context.getSystemService(AudioManager::class.java)
      if(Build.VERSION.SDK_INT>=31)audio.availableCommunicationDevices.map { device -> mapOf("id" to device.id,"label" to when(device.type){AudioDeviceInfo.TYPE_BUILTIN_SPEAKER->NativeUi.text(context,"Динамик","Speaker");AudioDeviceInfo.TYPE_BUILTIN_EARPIECE->NativeUi.text(context,"Телефон","Earpiece");AudioDeviceInfo.TYPE_BLUETOOTH_SCO,AudioDeviceInfo.TYPE_BLE_HEADSET->"Bluetooth";AudioDeviceInfo.TYPE_WIRED_HEADSET,AudioDeviceInfo.TYPE_WIRED_HEADPHONES,AudioDeviceInfo.TYPE_USB_HEADSET->NativeUi.text(context,"Наушники","Headphones");else->NativeUi.text(context,"Аудиоустройство ${device.id}","Audio device ${device.id}")}) }
      else listOf(mapOf("id" to -1,"label" to NativeUi.text(context,"Телефон / наушники","Earpiece / headphones")),mapOf("id" to -2,"label" to NativeUi.text(context,"Динамик","Speaker")))
    }
    AsyncFunction("routeAudioAsync") { id:Int ->
      val audio=requireNotNull(appContext.reactContext).getSystemService(AudioManager::class.java)
      if(Build.VERSION.SDK_INT>=31){val device=audio.availableCommunicationDevices.firstOrNull{it.id==id} ?: throw IllegalArgumentException("Аудиоустройство отключено");check(audio.setCommunicationDevice(device)){"Не удалось переключить звук"}}
      else{require(id==-1||id==-2);audio.isSpeakerphoneOn=id==-2}
    }
    AsyncFunction("signalBundleAsync") { user: String, count: Int -> signal.bundle(user, count) }
    AsyncFunction("signalDeviceProofAsync") { user:String,challenge:String -> signal.deviceProof(user,challenge) }
    AsyncFunction("signalClearAccountAsync") { user:String -> signal.clearAccount(user) }
    AsyncFunction("signalPrepareBackupImportAsync") { user:String,json:String -> signal.prepareBackupImport(user,json) }
    AsyncFunction("signalSignBackupProofAsync") { user:String,id:String,challenge:String -> signal.signBackupProof(user,id,challenge) }
    AsyncFunction("signalCommitBackupImportAsync") { user:String,id:String -> signal.commitBackupImport(user,id) }
    AsyncFunction("signalFinishBackupImportAsync") { user:String,id:String,commit:Boolean -> signal.finishBackupImport(user,id,commit) }
    AsyncFunction("signalRecoverBackupImportAsync") { user:String,commit:Boolean -> signal.recoverBackupImport(user,commit) }
    AsyncFunction("signalExportBackupKeysAsync") { user:String -> signal.exportBackupKeys(user) }
    AsyncFunction("signalImportBackupKeysAsync") { user:String, json:String -> signal.importBackupKeys(user,json) }
    AsyncFunction("signalHasSessionAsync") { user: String, peer: String, device: String -> signal.hasSession(user, peer, device) }
    AsyncFunction("signalSessionAsync") { user: String, peer: String, device: String, bundle: String -> signal.session(user, peer, device, bundle) }
    AsyncFunction("signalEncryptAsync") { user: String, peer: String, device: String, text: String -> signal.encrypt(user, peer, device, text) }
    AsyncFunction("signalDecryptAsync") { user: String, peer: String, device: String, ciphertext: String -> signal.decrypt(user, peer, device, ciphertext) }

    // Expo dispatches AsyncFunction on its native worker queue, away from JS/UI.
    AsyncFunction("deriveAsync") { password: String, saltBase64: String ->
      val salt = Base64.decode(saltBase64, Base64.NO_WRAP)
      val key = RoomKeyDerivation.derive(password, salt)
      try {
        Base64.encodeToString(key, Base64.NO_WRAP)
      } finally {
        key.fill(0)
        salt.fill(0)
      }
    }

    AsyncFunction("decryptFileAsync") { inputUri: String, outputUri: String, keyBase64: String, ivBase64: String ->
      val context = requireNotNull(appContext.reactContext)
      val input = File(URI(inputUri)).canonicalFile
      val output = File(URI(outputUri)).canonicalFile
      val root = context.cacheDir.canonicalPath + File.separator
      require(input.path.startsWith(root) && output.path.startsWith(root) && input != output) { "Invalid cache file" }
      val key = Base64.decode(keyBase64, Base64.NO_WRAP)
      val iv = Base64.decode(ivBase64, Base64.NO_WRAP)
      require(key.size == 32 && iv.size == 12) { "Invalid media key" }
      try {
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(key, "AES"), GCMParameterSpec(128, iv))
        output.parentFile?.mkdirs()
        CipherInputStream(input.inputStream(), cipher).use { source -> output.outputStream().use { sink -> source.copyTo(sink) } }
        output.toURI().toString()
      } catch (error: Throwable) {
        output.delete()
        throw error
      } finally { key.fill(0); iv.fill(0) }
    }
    AsyncFunction("encryptFileAsync") { inputUri: String, outputUri: String, keyBase64: String, ivBase64: String ->
      val context=requireNotNull(appContext.reactContext)
      val input=File(URI(inputUri)).canonicalFile
      val output=File(URI(outputUri)).canonicalFile
      require((input.path.startsWith(context.cacheDir.canonicalPath+File.separator) || input.path.startsWith(context.filesDir.canonicalPath+File.separator)) && output.path.startsWith(context.cacheDir.canonicalPath+File.separator) && input!=output)
      require(input.length()<=100L*1024*1024-16) { "Вложение больше 100 МБ" }
      val key=Base64.decode(keyBase64,Base64.NO_WRAP);val iv=Base64.decode(ivBase64,Base64.NO_WRAP)
      require(key.size==32 && iv.size==12)
      try {
        val cipher=Cipher.getInstance("AES/GCM/NoPadding");cipher.init(Cipher.ENCRYPT_MODE,SecretKeySpec(key,"AES"),GCMParameterSpec(128,iv))
        output.parentFile?.mkdirs()
        input.inputStream().use { source -> CipherOutputStream(output.outputStream(),cipher).use { sink -> source.copyTo(sink) } }
        output.toURI().toString()
      }catch(error:Throwable){output.delete();throw error}finally{key.fill(0);iv.fill(0)}
    }
  }
}
