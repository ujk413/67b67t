package ru.b67t.roomkey

import android.app.Notification
import android.app.NotificationManager
import android.app.NotificationChannel
import android.app.PendingIntent
import android.app.RemoteInput
import android.content.Context
import android.content.Intent
import android.os.Build
import android.net.Uri

/** Native RemoteInput only hands text to the app; JS must validate account and
 * enqueue via the existing encrypted room outbox. No network send occurs here. */
object QuickReplyNotifications {
  private const val INPUT = "67chat.reply.text"
  private val uuid=Regex("^[a-fA-F0-9-]{36}$")

  fun show(context:Context,data:Map<String,String>) {
    val room=data["room_id"] ?: return
    val recipient=data["recipient_id"] ?: return
    if(!uuid.matches(room)||!uuid.matches(recipient))return
    if(Build.VERSION.SDK_INT>=26)context.getSystemService(NotificationManager::class.java).createNotificationChannel(NotificationChannel("messages",NativeUi.text(context,"Сообщения","Messages"),NotificationManager.IMPORTANCE_HIGH))
    val open=context.packageManager.getLaunchIntentForPackage(context.packageName) ?: return
    open.addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP)
    open.putExtra("67chat_reply_room",room)
    open.putExtra("67chat_reply_recipient",recipient)
    // Intent extras do not participate in PendingIntent equality. Bind both
    // account and action into data, so another account cannot replace them.
    open.action="67chat.notification.open"
    open.data=Uri.parse("67chat-notification://open/$recipient/$room")
    val pending=PendingIntent.getActivity(context,0,open,PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
    val reply=Intent(open).apply { action="67chat.notification.reply";this.data=Uri.parse("67chat-notification://reply/$recipient/$room") }
    val replyPending=PendingIntent.getActivity(context,0,reply,PendingIntent.FLAG_UPDATE_CURRENT or (if(Build.VERSION.SDK_INT>=31)PendingIntent.FLAG_MUTABLE else 0))
    val replyLabel=NativeUi.text(context,"Ответить","Reply")
    val input=RemoteInput.Builder(INPUT).setLabel(replyLabel).build()
    val action=Notification.Action.Builder(android.R.drawable.ic_menu_send,replyLabel,replyPending).addRemoteInput(input).build()
    val notification=Notification.Builder(context,"messages")
      .setSmallIcon(android.R.drawable.stat_notify_chat)
      .setContentTitle("67chat")
      .setContentText(NativeUi.text(context,"Новое сообщение","New message"))
      .setVisibility(Notification.VISIBILITY_PRIVATE)
      .setCategory(Notification.CATEGORY_MESSAGE)
      .setAutoCancel(true)
      .setContentIntent(pending)
      .addAction(action)
      .apply { if(data["group_notifications"]!="false")setGroup("$recipient:$room") }
      .build()
    val tag=recipient+":"+(if(data["group_notifications"]=="false")data["message_id"] ?: room else room)
    try{context.getSystemService(NotificationManager::class.java).notify(tag,0,notification)}catch(_:SecurityException){}
  }

  fun fromIntent(intent:Intent?):Map<String,String>? {
    if(intent?.action!="67chat.notification.reply")return null
    val room=intent?.getStringExtra("67chat_reply_room") ?: return null
    val recipient=intent.getStringExtra("67chat_reply_recipient") ?: return null
    val text=RemoteInput.getResultsFromIntent(intent)?.getCharSequence(INPUT)?.toString()?.take(5000) ?: return null
    if(!uuid.matches(room)||!uuid.matches(recipient)||text.isBlank())return null
    return mapOf("roomId" to room,"recipientId" to recipient,"text" to text)
  }

  fun openFromIntent(intent:Intent?):Map<String,String>? {
    if(intent?.action!="67chat.notification.open")return null
    val room=intent.getStringExtra("67chat_reply_room") ?: return null
    val recipient=intent.getStringExtra("67chat_reply_recipient") ?: return null
    if(!uuid.matches(room)||!uuid.matches(recipient))return null
    return mapOf("roomId" to room,"recipientId" to recipient)
  }
}
