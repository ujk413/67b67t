package ru.b67t.roomkey

import android.app.*
import android.content.*
import android.content.pm.ServiceInfo
import android.graphics.Color
import android.media.AudioAttributes
import android.media.AudioDeviceInfo
import android.media.AudioFocusRequest
import android.media.AudioManager
import android.media.RingtoneManager
import android.os.Build
import android.os.Bundle
import android.os.IBinder
import android.view.Gravity
import android.widget.*
import com.google.firebase.messaging.RemoteMessage
import expo.modules.notifications.service.ExpoFirebaseMessagingService
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.Executors

internal object CallNotifications {
  const val INCOMING = 6701
  private fun preferences(context: Context) = context.getSharedPreferences("67chat-calls",Context.MODE_PRIVATE)
  fun remember(context: Context, data: Map<String,String>) { preferences(context).edit().putString("pending",JSONObject(data).toString()).apply() }
  fun pending(context: Context): String? = preferences(context).getString("pending",null)
  fun clear(context: Context, id: String? = null) {
    val value=pending(context)
    if (id != null && value != null && JSONObject(value).optString("call_id") != id) return
    preferences(context).edit().remove("pending").apply()
    context.getSystemService(NotificationManager::class.java).cancel(INCOMING)
  }
  fun launch(context: Context, id: String, action: String) {
    val intent=context.packageManager.getLaunchIntentForPackage(context.packageName) ?: return
    intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP)
    intent.putExtra("67chat_call_id",id).putExtra("67chat_call_action",action);context.startActivity(intent)
  }
  fun channel(context: Context, incoming: Boolean): String {
    val id=if(incoming) "incoming-calls-v1" else "active-calls-v1"
    if(Build.VERSION.SDK_INT>=26) {
      val channel=NotificationChannel(id,if(incoming) NativeUi.text(context,"Входящие звонки","Incoming calls") else NativeUi.text(context,"Текущий звонок","Current call"),if(incoming) NotificationManager.IMPORTANCE_HIGH else NotificationManager.IMPORTANCE_LOW)
      if(incoming) { channel.setSound(RingtoneManager.getDefaultUri(RingtoneManager.TYPE_RINGTONE),AudioAttributes.Builder().setUsage(AudioAttributes.USAGE_NOTIFICATION_RINGTONE).build());channel.enableVibration(true) }
      context.getSystemService(NotificationManager::class.java).createNotificationChannel(channel)
    };return id
  }
  fun builder(context: Context, incoming: Boolean): Notification.Builder {
    val id=channel(context,incoming)
    return (if(Build.VERSION.SDK_INT>=26) Notification.Builder(context,id) else Notification.Builder(context))
      .setSmallIcon(context.applicationInfo.icon).setContentTitle("67chat").setCategory(Notification.CATEGORY_CALL).setVisibility(Notification.VISIBILITY_PRIVATE)
  }
  fun show(context: Context, data: Map<String,String>) {
    val id=data["call_id"] ?: return
    val expires=data["expires_at"]?.toLongOrNull() ?: return
    if(expires<=System.currentTimeMillis() || expires>System.currentTimeMillis()+60000) return
    remember(context,data)
    val flags=PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
    val screen=Intent(context,IncomingCallActivity::class.java).putExtra("call_id",id).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
    val open=PendingIntent.getActivity(context,id.hashCode(),screen,flags)
    val answerIntent=context.packageManager.getLaunchIntentForPackage(context.packageName)!!.addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP).putExtra("67chat_call_id",id).putExtra("67chat_call_action","answer")
    val answer=PendingIntent.getActivity(context,id.hashCode()+1,answerIntent,flags)
    val decline=PendingIntent.getBroadcast(context,id.hashCode(),Intent(context,DeclineCallReceiver::class.java).putExtra("call_id",id).putExtra("nonce",data["decline_nonce"]),flags)
    val caller=(data["caller"] ?: NativeUi.text(context,"Абонент","Caller")).take(100)
    val notification=builder(context,true).setContentText(if(data["is_video"]=="true") NativeUi.text(context,"Входящий видеозвонок","Incoming video call") else NativeUi.text(context,"Входящий звонок","Incoming call")).setContentIntent(open).setFullScreenIntent(open,true).setOngoing(true)
    if(Build.VERSION.SDK_INT>=26)notification.setTimeoutAfter((expires-System.currentTimeMillis()).coerceAtLeast(1))
    if(Build.VERSION.SDK_INT>=31) notification.setStyle(Notification.CallStyle.forIncomingCall(Person.Builder().setName(caller).build(),decline,answer))
    else notification.setContentTitle(caller).setPriority(Notification.PRIORITY_MAX).addAction(0,NativeUi.text(context,"Отклонить","Decline"),decline).addAction(0,NativeUi.text(context,"Ответить","Answer"),answer)
    try {context.getSystemService(NotificationManager::class.java).notify(INCOMING,notification.build())}catch(_:SecurityException){}
  }
}

class CallMessagingService : ExpoFirebaseMessagingService() {
  override fun onMessageReceived(message: RemoteMessage) {
    if(!AccountTransport.allowsPush())return
    when(message.data["call_event"]) {
      "ring" -> CallNotifications.show(this,message.data)
      "cancel" -> CallNotifications.clear(this,message.data["call_id"])
      else -> if (message.data["quality_message"] == "true") QuickReplyNotifications.show(this,message.data) else super.onMessageReceived(message)
    }
  }
}

class DeclineCallReceiver : BroadcastReceiver() {
  override fun onReceive(context: Context,intent: Intent) {
    val id=intent.getStringExtra("call_id") ?: return;val nonce=intent.getStringExtra("nonce") ?: return
    CallNotifications.clear(context,id); val pending=goAsync()
    Executors.newSingleThreadExecutor().apply { execute {
      try {
        if(!Regex("[a-fA-F0-9-]{36}").matches(id))return@execute
        val connection=URL("https://67chat.ru/api/calls/$id/decline").openConnection() as HttpURLConnection
        connection.requestMethod="POST";connection.connectTimeout=5000;connection.readTimeout=5000;connection.doOutput=true;connection.setRequestProperty("Content-Type","application/json")
        connection.outputStream.use { it.write(JSONObject().put("nonce",nonce).toString().toByteArray()) };connection.responseCode;connection.disconnect()
      }catch(_:Exception){}finally {pending.finish();shutdown()}
    }}
  }
}

class IncomingCallActivity : Activity() {
  override fun onCreate(saved: Bundle?) {
    super.onCreate(saved)
    val data=CallNotifications.pending(this)?.let {JSONObject(it)}
    if(data==null || data.optLong("expires_at")<=System.currentTimeMillis()){finish();return}
    val layout=LinearLayout(this).apply {orientation=LinearLayout.VERTICAL;gravity=Gravity.CENTER;setPadding(36,36,36,36);setBackgroundColor(Color.rgb(23,33,43))}
    layout.addView(TextView(this).apply {text=data.optString("caller",NativeUi.text(this@IncomingCallActivity,"Абонент","Caller"));textSize=30f;setTextColor(Color.WHITE);gravity=Gravity.CENTER})
    layout.addView(TextView(this).apply {text=if(data.optString("is_video")=="true") NativeUi.text(this@IncomingCallActivity,"Видеозвонок · 67chat","Video call · 67chat") else NativeUi.text(this@IncomingCallActivity,"Входящий звонок · 67chat","Incoming call · 67chat");textSize=18f;setTextColor(Color.LTGRAY);setPadding(0,20,0,70)})
    layout.addView(Button(this).apply {text=NativeUi.text(this@IncomingCallActivity,"Ответить","Answer");setOnClickListener {CallNotifications.launch(this@IncomingCallActivity,data.getString("call_id"),"answer");finish()}})
    layout.addView(Button(this).apply {text=NativeUi.text(this@IncomingCallActivity,"Отклонить","Decline");setOnClickListener {sendBroadcast(Intent(this@IncomingCallActivity,DeclineCallReceiver::class.java).putExtra("call_id",data.getString("call_id")).putExtra("nonce",data.optString("decline_nonce")));finish()}})
    setContentView(layout)
    android.os.Handler(mainLooper).postDelayed({finish()},(data.optLong("expires_at")-System.currentTimeMillis()).coerceAtLeast(1))
  }
}

class ActiveCallService : Service() {
  private lateinit var audio: AudioManager
  private var focusRequest: AudioFocusRequest? = null

  override fun onCreate() {
    super.onCreate()
    audio = getSystemService(AudioManager::class.java)
    // Communication mode + audio focus: music and podcasts pause instead of
    // playing into the call.
    if (Build.VERSION.SDK_INT >= 26) {
      focusRequest = AudioFocusRequest.Builder(AudioManager.AUDIOFOCUS_GAIN_TRANSIENT)
        .setAudioAttributes(
          AudioAttributes.Builder()
            .setUsage(AudioAttributes.USAGE_VOICE_COMMUNICATION)
            .setContentType(AudioAttributes.CONTENT_TYPE_SPEECH)
            .build()
        )
        .setAcceptsDelayedFocusGain(false)
        .build()
        .also { audio.requestAudioFocus(it) }
    } else {
      @Suppress("DEPRECATION")
      audio.requestAudioFocus(null, AudioManager.STREAM_VOICE_CALL, AudioManager.AUDIOFOCUS_GAIN_TRANSIENT)
    }
    audio.mode = AudioManager.MODE_IN_COMMUNICATION
  }

  override fun onBind(intent: Intent?): IBinder? = null

  override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
    val open = PendingIntent.getActivity(this, 6702, packageManager.getLaunchIntentForPackage(packageName), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
    val note = CallNotifications.builder(this, false).setContentText(NativeUi.text(this,"Звонок идёт · нажмите, чтобы вернуться","Call in progress · tap to return")).setOngoing(true).setContentIntent(open).build()
    if (Build.VERSION.SDK_INT >= 29) startForeground(6702, note, ServiceInfo.FOREGROUND_SERVICE_TYPE_MICROPHONE or if (intent?.getBooleanExtra("video", false) == true) ServiceInfo.FOREGROUND_SERVICE_TYPE_CAMERA else 0) else startForeground(6702, note)
    return START_NOT_STICKY
  }

  // Leaving MODE_IN_COMMUNICATION behind is what made everything sound "like a
  // call" after hanging up: the recorder and the WebRTC session both set it, and
  // restoring a snapshot taken earlier is not enough. Reset it explicitly and
  // hand audio focus back.
  override fun onDestroy() {
    if (Build.VERSION.SDK_INT >= 31) {
      audio.clearCommunicationDevice()
    } else {
      @Suppress("DEPRECATION")
      audio.isSpeakerphoneOn = false
    }
    audio.mode = AudioManager.MODE_NORMAL
    if (Build.VERSION.SDK_INT >= 26) {
      focusRequest?.let { audio.abandonAudioFocusRequest(it) }
      focusRequest = null
    } else {
      @Suppress("DEPRECATION")
      audio.abandonAudioFocus(null)
    }
    super.onDestroy()
  }
}
