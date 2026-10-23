package ru.b67t.roomkey

import okhttp3.Dns
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONObject
import java.net.InetAddress
import java.net.UnknownHostException
import java.util.concurrent.TimeUnit

internal data class PushTarget(val baseUrl:String,val token:String)
/** Removes server→Google registrations through the proposed route BEFORE that
 * route becomes the persisted application policy. A failure aborts the switch. */
internal object SocksPushPreflight {
  fun disable(policy:SocksPolicy,targets:List<PushTarget>,installation:String,secret:String){
    require(policy.isProtected);policy.endpoint();require(Regex("[A-Za-z0-9_-]{8,100}").matches(installation));require(Regex("[a-f0-9]{64}").matches(secret))
    val client=OkHttpClient.Builder().proxySelector(SocksOnlyProxySelector(policy)).dns(object:Dns{override fun lookup(host:String):List<InetAddress>{throw UnknownHostException("Local DNS forbidden")}}).connectTimeout(10,TimeUnit.SECONDS).readTimeout(10,TimeUnit.SECONDS).writeTimeout(10,TimeUnit.SECONDS).retryOnConnectionFailure(false).build()
    try{
      fun post(base:String,path:String,token:String=""):Pair<Int,JSONObject>{
        val body=JSONObject().put("installation_id",installation).put("secret",secret).toString().toRequestBody("application/json".toMediaType())
        val request=Request.Builder().url(base.trimEnd('/')+path).post(body);if(token.isNotEmpty())request.header("Authorization","Bearer "+token)
        return client.newCall(request.build()).execute().use{response->response.code to try{JSONObject(response.body?.string()?:"{}")}catch(_:Exception){JSONObject()}}
      }
      for((base,accounts) in targets.groupBy{it.baseUrl}){
        var result=post(base,"/auth/account-push-disable")
        if(result.first==409&&result.second.optString("error").let{it=="installation_claim_required"||result.second.optString("message")=="installation_claim_required"}){
          // Upgrade an existing installation through any still-valid owning account.
          var claimed=false
          for(account in accounts.filter{it.token.isNotEmpty()}){
            val claim=post(base,"/me/account/push-installation",account.token)
            if(claim.first==200){claimed=true;break}
            check(claim.first==401||claim.first==403){"Transport installation claim failed (${claim.first})"}
          }
          check(claimed){"Sign in before revoking legacy push registrations"}
          result=post(base,"/auth/account-push-disable")
        }
        check(result.first==200&&result.second.optBoolean("ok")){"Transport push revocation failed (${result.first})"}
      }
    }finally{client.dispatcher.cancelAll();client.connectionPool.evictAll();client.dispatcher.executorService.shutdown()}
  }
}
