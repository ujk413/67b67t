package ru.b67t.expressive

import android.content.Intent
import android.net.Uri
import android.appwidget.AppWidgetManager
import android.content.ComponentName
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import expo.modules.kotlin.modules.Module
import expo.modules.kotlin.modules.ModuleDefinition
import java.io.File
import java.security.KeyStore
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.Executors
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey

/** The Android grant is consumed into private storage immediately. Only the encrypted
 * journal, including explicit confirmation and each durable outbox index, survives restart. */
class ExpressiveIntegrationModule : Module() {
  private var journal: IncomingShareJournal? = null
  private var capturedIntent: Intent? = null
  @Volatile private var notice: String? = null
  private val imports = Executors.newSingleThreadExecutor()
  private val captureId = "ru.b67t.expressive.shareJournalId"
  @Synchronized private fun journal(): IncomingShareJournal {
    journal?.let { return it }
    val context=appContext.reactContext ?: appContext.currentActivity ?: throw IllegalStateException("share_storage_unavailable")
    val alias=context.packageName+".incoming-share.v1"
    val value=IncomingShareJournal(File(context.noBackupFilesDir,"incoming-shares")) {
      val store=KeyStore.getInstance("AndroidKeyStore").apply { load(null) }
      (store.getKey(alias,null) as? SecretKey) ?: KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES,"AndroidKeyStore").apply {
        init(KeyGenParameterSpec.Builder(alias,KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT)
          .setBlockModes(KeyProperties.BLOCK_MODE_GCM).setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
          .setRandomizedEncryptionRequired(true).build())
      }.generateKey()
    }
    journal=value;return value
  }
  private fun asMap(share: IncomingShareJournal.Share): Map<String,Any?> {
    val store=journal();var error:String?=null
    val uris=try{store.fileUris(share).map{Uri.fromFile(File(java.net.URI(it))).toString()}}catch(e:Exception){error="share_source_unavailable";emptyList<String>()}
    return mapOf("id" to share.id,"text" to share.text,"uris" to uris,"mime" to share.mime,
      "rejectedFiles" to share.rejectedFiles,"error" to error,"notice" to notice,"pendingCount" to store.size(),
      "delivery" to if(share.confirmed)mapOf("confirmed" to true,"recipientId" to share.accountId,"roomId" to share.roomId,"next" to share.next,"itemCount" to share.itemCount())else null)
  }
  private fun capture(intent: Intent?,fresh:Boolean=false) {
    if(intent?.action!=Intent.ACTION_SEND&&intent?.action!=Intent.ACTION_SEND_MULTIPLE)return
    if(intent===capturedIntent)return
    val context=appContext.reactContext ?: appContext.currentActivity ?: return
    capturedIntent=intent
    appContext.currentActivity?.intent=intent
    val id=intent.getStringExtra(captureId) ?: UUID.randomUUID().toString().also{intent.putExtra(captureId,it)}
    val uris=if(intent.action==Intent.ACTION_SEND_MULTIPLE)intent.getParcelableArrayListExtra<Uri>(Intent.EXTRA_STREAM)?.map{it.toString()} ?: emptyList()
      else listOfNotNull(intent.getParcelableExtra<Uri>(Intent.EXTRA_STREAM)?.toString())
    val text=intent.getCharSequenceExtra(Intent.EXTRA_TEXT)?.toString()?.take(10000)
    val mime=intent.type?.take(100)
    // Hash only lives inside the encrypted manifest; no external URI/content is logged.
    val fingerprint=MessageDigest.getInstance("SHA-256").digest((text.orEmpty()+"\u0000"+mime.orEmpty()+"\u0000"+uris.joinToString("\u0000")).toByteArray()).joinToString(""){"%02x".format(it)}
    val permitted=uris.filter{value->
      IncomingSharePolicy.allows(value,context.packageName)&&context.packageManager.resolveContentProvider(Uri.parse(value).authority.orEmpty(),0)?.packageName!=context.packageName
    }
    imports.execute {
      try{
        journal().capture(id,fingerprint,text,mime,permitted,uris.size-permitted.size,{uri->context.contentResolver.openInputStream(Uri.parse(uri))},fresh)
        notice=null
      }catch(e:Exception){notice=if(e.message=="share_queue_full")"share_queue_full" else "share_import_failed"}
      sendEvent("shareReceived",mapOf("pending" to true))
    }
  }
  override fun definition() = ModuleDefinition {
    Name("ExpressiveIntegration")
    Events("shareReceived")
    OnCreate { capture(appContext.currentActivity?.intent) }
    OnNewIntent { intent -> capture(intent,true) }
    Function("getPendingShare") {
      try{synchronized(this){journal().peek()?.let{asMap(it)}}}
      catch(e:Exception){mapOf("id" to "unavailable","text" to null,"uris" to emptyList<String>(),"error" to "share_storage_unavailable")}
    }
    Function("confirmPendingShare") { id:String,account:String,room:String -> synchronized(this){asMap(journal().confirm(id,account,room))} }
    Function("updatePendingShare") { id:String,account:String,room:String,next:Int -> synchronized(this){journal().advance(id,account,room,next)} }
    Function("discardPendingShare") { id:String -> synchronized(this){if(id!="unavailable")journal().discard(id);notice=null} }
    Function("removeAccountShares") { account:String -> synchronized(this){journal().removeAccount(account)} }
    Function("setUiLanguage") { language:String ->
      val context=appContext.reactContext ?: return@Function
      context.getSharedPreferences("67chat-ui",0).edit().putString("language",if(language=="en")"en" else "ru").apply()
      val manager=AppWidgetManager.getInstance(context)
      ChatWidget().onUpdate(context,manager,manager.getAppWidgetIds(ComponentName(context,ChatWidget::class.java)))
    }
    Function("setUnreadCount") { count: Int ->
      val context=appContext.reactContext ?: return@Function
      context.getSharedPreferences("67chat-widget",0).edit().putInt("unread",count.coerceIn(0,99999)).apply()
      val manager=AppWidgetManager.getInstance(context)
      ChatWidget().onUpdate(context,manager,manager.getAppWidgetIds(ComponentName(context,ChatWidget::class.java)))
    }
  }
}
