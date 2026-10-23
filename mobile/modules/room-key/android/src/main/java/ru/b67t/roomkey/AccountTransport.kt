package ru.b67t.roomkey

import android.app.Activity
import android.app.Application
import android.content.Context
import android.content.Intent
import android.content.ComponentName
import android.content.pm.PackageManager
import android.os.Bundle
import android.os.Build
import android.os.Process
import com.facebook.react.modules.network.OkHttpClientFactory
import com.facebook.react.modules.network.OkHttpClientProvider
import okhttp3.Dns
import okhttp3.OkHttpClient
import org.json.JSONObject
import java.net.ProxySelector
import java.net.UnknownHostException
import java.net.InetAddress
import java.util.Collections
import java.util.WeakHashMap

/** Application.attachBaseContext installs this before content providers, RN,
 * Expo FileSystem/Video and APK URLConnections create their network clients. */
object AccountTransport {
  private val selector=SocksOnlyProxySelector(SocksPolicy("blocked"))
  private val clients=Collections.newSetFromMap(WeakHashMap<OkHttpClient,Boolean>())
  @Volatile private var installed=false
  private var restartProcess=false
  private fun preferences(context:Context)=context.getSharedPreferences("67chat-network-policy",Context.MODE_PRIVATE)
  private fun pushDelivery(context:Context,enabled:Boolean){
    val manager=context.packageManager
    for((name,receiver)in listOf("com.google.firebase.iid.FirebaseInstanceIdReceiver" to true,"ru.b67t.roomkey.CallMessagingService" to false)){
      val component=ComponentName(context.packageName,name)
      try{if(receiver)manager.getReceiverInfo(component,PackageManager.MATCH_DISABLED_COMPONENTS)else manager.getServiceInfo(component,PackageManager.MATCH_DISABLED_COMPONENTS)}catch(_:PackageManager.NameNotFoundException){continue}
      manager.setComponentEnabledSetting(component,if(enabled)PackageManager.COMPONENT_ENABLED_STATE_ENABLED else PackageManager.COMPONENT_ENABLED_STATE_DISABLED,PackageManager.DONT_KILL_APP)
    }
  }
  private fun parse(json:String):SocksPolicy {val o=JSONObject(json);val mode=o.getString("mode");require(mode in listOf("direct","proxy","tor"));return SocksPolicy(mode,o.optString("host",""),o.optInt("port",0)).also{if(it.isProtected)it.endpoint()}}
  @JvmStatic @Synchronized fun install(context:Context){
    if(installed)return
    restartProcess=if(Build.VERSION.SDK_INT>=28)Application.getProcessName().endsWith(":transport_restart")else{
      val manager=context.getSystemService(Context.ACTIVITY_SERVICE) as android.app.ActivityManager
      manager.runningAppProcesses?.any{it.pid==Process.myPid()&&it.processName.endsWith(":transport_restart")}==true
    }
    val saved=preferences(context).getString("policy",null)
    try{selector.update(if(saved==null)SocksPolicy("direct")else parse(saved))}catch(_:Exception){selector.update(SocksPolicy("blocked"))}
    ProxySelector.setDefault(selector)
    pushDelivery(context,!selector.policy.isProtected)
    OkHttpClientProvider.setOkHttpClientFactory(OkHttpClientFactory {
      val client=OkHttpClientProvider.createClientBuilder().proxySelector(selector).dns(object:Dns {override fun lookup(host:String):List<InetAddress>{
        if(selector.policy.isProtected)throw UnknownHostException("Local DNS is forbidden in protected mode")
        return Dns.SYSTEM.lookup(host)
      }}).build();synchronized(clients){clients.add(client)};client
    })
    // FCM's Google Play network is outside this application's SOCKS stack.
    // Protected sessions never request/register FCM tokens; disable auto-init
    // before FirebaseInitProvider can run, including after a process restart.
    if(selector.policy.isProtected)check(context.getSharedPreferences("com.google.firebase.messaging",Context.MODE_PRIVATE).edit().putBoolean("auto_init",false).commit())
    installed=true
  }
  fun capabilities():Map<String,Boolean>{
    check(installed&&ProxySelector.getDefault()===selector) { "Early native transport installation missing" }
    return mapOf("proxy" to true,"tor" to true,"failClosed" to true,"dnsThroughProxy" to true,"callsThroughProxy" to false,"globalNetworkLayer" to true,"processRestartRequired" to true)
  }
  fun policy(context:Context):String {capabilities();return JSONObject().put("mode",selector.policy.mode).put("host",selector.policy.host).put("port",selector.policy.port).toString()}
  fun validate(json:String){parse(json)}
  fun unregisterPush(policyJson:String,targetsJson:String,installation:String,secret:String){
    val targetPolicy=parse(policyJson);val rows=org.json.JSONArray(targetsJson);require(rows.length()<=32)
    val targets=(0 until rows.length()).map{val row=rows.getJSONObject(it);val base=row.getString("baseUrl");val token=row.getString("token");val uri=java.net.URI(base);require(uri.scheme=="https"&&uri.host!=null&&uri.userInfo==null&&uri.query==null&&uri.fragment==null&&token.length<=4096);PushTarget(base,token)}
    SocksPushPreflight.disable(targetPolicy,targets,installation,secret)
  }
  fun allowsPush():Boolean=installed&&!selector.policy.isProtected
  @Synchronized fun configure(context:Context,json:String){
    capabilities();val next=parse(json)
    pushDelivery(context,!next.isProtected)
    check(preferences(context).edit().putString("policy",JSONObject().put("mode",next.mode).put("host",next.host).put("port",next.port).toString()).commit()) { "Cannot persist transport policy" }
    // No new routes may start between persistence and complete process restart.
    // A JS reload cannot evict connections owned by Expo media/file clients.
    selector.update(SocksPolicy("blocked"))
    synchronized(clients){for(client in clients){client.dispatcher.cancelAll();client.connectionPool.evictAll()}}
  }
  @JvmStatic fun requireDirectCalls(){check(installed&&selector.policy.mode=="direct") { "Calls are disabled while SOCKS/Tor protects the connection" }}
  @JvmStatic fun isRestartProcess():Boolean=restartProcess
  fun restart(context:Context){
    val intent=Intent(context,TransportRestartActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TASK).putExtra("old_pid",Process.myPid())
    context.startActivity(intent)
  }
}

/** Separate lightweight process closes *all* old HTTP/WS/media sockets, then
 * starts the application with the persisted policy. No account keys are reset. */
class TransportRestartActivity:Activity(){
  override fun onCreate(state:Bundle?){super.onCreate(state);val old=intent.getIntExtra("old_pid",0);if(old>0&&old!=Process.myPid())Process.killProcess(old)
    val launch=packageManager.getLaunchIntentForPackage(packageName)?:throw IllegalStateException("Launcher missing")
    launch.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TASK);startActivity(launch);finish();Process.killProcess(Process.myPid())
  }
}
