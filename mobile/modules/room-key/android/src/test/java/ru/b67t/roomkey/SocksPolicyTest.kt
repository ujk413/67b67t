package ru.b67t.roomkey

import org.junit.Assert.*
import org.junit.Test
import java.net.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import okhttp3.*
import okhttp3.RequestBody.Companion.toRequestBody

class SocksPolicyTest {
  private class SocksFixture(private val target:Int,private val connections:Int=1):AutoCloseable {
    val server=ServerSocket(0,10,InetAddress.getLoopbackAddress());val destination=AtomicReference<String>();val ready=CountDownLatch(1)
    init{Thread {try{repeat(connections){server.accept().use { client->client.soTimeout=5000;val input=client.getInputStream();val out=client.getOutputStream();check(input.read()==5);val methods=input.read();input.readNBytes(methods);out.write(byteArrayOf(5,0));out.flush();check(input.read()==5&&input.read()==1&&input.read()==0);val type=input.read();val host=when(type){3->String(input.readNBytes(input.read()),Charsets.UTF_8);1->input.readNBytes(4).joinToString("."){(it.toInt() and 255).toString()};else->error("Expected domain SOCKS request, got $type")};input.readNBytes(2);destination.set(host);ready.countDown();Socket(Proxy.NO_PROXY).apply { connect(InetSocketAddress(InetAddress.getLoopbackAddress(),target)) }.use { remote->out.write(byteArrayOf(5,0,0,1,127,0,0,1,0,1));out.flush();val upload=Thread{try{input.copyTo(remote.getOutputStream())}catch(_:Exception){}}.apply{isDaemon=true;start()};remote.getInputStream().copyTo(out);out.flush();upload.interrupt()}}}}catch(_:Exception){ready.countDown()}}.apply{isDaemon=true;start()}}
    override fun close(){server.close()}
  }
  private class HttpFixture(val status:Int=200,val response:String="ok",val replies:List<Pair<Int,String>> = listOf(status to response)):AutoCloseable {
    val server=ServerSocket(0,10,InetAddress.getLoopbackAddress());val body=AtomicReference<ByteArray>();val request=AtomicReference<String>()
    val requests=java.util.Collections.synchronizedList(mutableListOf<String>())
    init{Thread {try{for((replyStatus,replyBody) in replies)server.accept().use { socket->socket.soTimeout=5000;val input=socket.getInputStream();val header=StringBuilder();while(!header.endsWith("\r\n\r\n")){header.append(input.read().also{check(it>=0)}.toChar())};request.set(header.toString());requests.add(header.toString());val size=Regex("(?i)Content-Length: (\\d+)").find(header)?.groupValues?.get(1)?.toInt()?:0;body.set(input.readNBytes(size));val out=socket.getOutputStream();out.write("HTTP/1.1 $replyStatus Reply\r\nContent-Length: ${replyBody.toByteArray().size}\r\nConnection: close\r\n\r\n$replyBody".toByteArray());out.flush()}}catch(_:Exception){}}.apply{isDaemon=true;start()}}
    override fun close(){server.close()}
  }
  @Test fun okhttpUsesRemoteDomainAndStreamsBinaryUploadThroughSocks(){
    HttpFixture().use{target->SocksFixture(target.server.localPort).use{socks->val selector=SocksOnlyProxySelector(SocksPolicy("tor","127.0.0.1",socks.server.localPort));val client=OkHttpClient.Builder().proxySelector(selector).dns(object:Dns{override fun lookup(host:String):List<InetAddress>{throw AssertionError("Destination DNS must not resolve locally")}}).build();val bytes=ByteArray(128*1024){(it%251).toByte()};client.newCall(Request.Builder().url("http://account-fixture.invalid:${target.server.localPort}/upload").post(bytes.toRequestBody()).build()).execute().use{assertEquals("ok",it.body!!.string())};assertTrue(socks.ready.await(1,TimeUnit.SECONDS));assertEquals("account-fixture.invalid",socks.destination.get());assertArrayEquals(bytes,target.body.get());client.connectionPool.evictAll()}}
  }
  @Test fun urlConnectionUsesRemoteDnsViaGlobalSelector(){
    val original=ProxySelector.getDefault();try{HttpFixture().use{target->SocksFixture(target.server.localPort).use{socks->ProxySelector.setDefault(SocksOnlyProxySelector(SocksPolicy("proxy","127.0.0.1",socks.server.localPort)));val c=URL("http://account-urlconnection.invalid:${target.server.localPort}/").openConnection() as HttpURLConnection;c.connectTimeout=1000;c.readTimeout=3000;assertEquals("ok",c.inputStream.use{String(it.readBytes())});c.disconnect();assertEquals("account-urlconnection.invalid",socks.destination.get())}}}finally{ProxySelector.setDefault(original)}
  }
  @Test fun unavailableSocksNeverAttemptsDirectTarget(){
    ServerSocket(0,1,InetAddress.getLoopbackAddress()).use{target->target.soTimeout=350;val deadPort=ServerSocket(0).use{it.localPort};val selector=SocksOnlyProxySelector(SocksPolicy("proxy","127.0.0.1",deadPort));val client=OkHttpClient.Builder().proxySelector(selector).connectTimeout(300,TimeUnit.MILLISECONDS).build();assertThrows(java.io.IOException::class.java){client.newCall(Request.Builder().url("http://127.0.0.1:${target.localPort}/").build()).execute()};assertThrows(SocketTimeoutException::class.java){target.accept()};assertEquals(Proxy.Type.SOCKS,selector.select(URI("https://example.invalid")).single().type());client.connectionPool.evictAll()}
  }
  @Test fun strictPushPreflightUsesTheNewSocksRouteAndRejectsServerFailures(){
    HttpFixture(200,"{\"ok\":true}").use{target->SocksFixture(target.server.localPort).use{socks->SocksPushPreflight.disable(SocksPolicy("tor","127.0.0.1",socks.server.localPort),listOf(PushTarget("http://push-gate.invalid:${target.server.localPort}","")),"fixture-installation","a".repeat(64));assertEquals("push-gate.invalid",socks.destination.get());assertTrue(target.request.get().contains("POST /auth/account-push-disable"));assertFalse(target.request.get().contains("Authorization:"));assertTrue(String(target.body.get()).contains("fixture-installation"));assertTrue(String(target.body.get()).contains("a".repeat(64)))}}
    HttpFixture(503,"{\"error\":\"unavailable\"}").use{target->SocksFixture(target.server.localPort).use{socks->assertThrows(IllegalStateException::class.java){SocksPushPreflight.disable(SocksPolicy("proxy","127.0.0.1",socks.server.localPort),listOf(PushTarget("http://push-gate-fail.invalid:${target.server.localPort}","fixture-token")),"fixture-installation","a".repeat(64))}}}
  }
  @Test fun invalidProxyAndTransitionStateFailClosed(){val selector=SocksOnlyProxySelector(SocksPolicy("blocked"));assertThrows(IllegalArgumentException::class.java){selector.select(URI("https://example.invalid"))};assertThrows(IllegalArgumentException::class.java){selector.update(SocksPolicy("proxy","proxy.example.org",9050))};assertThrows(IllegalArgumentException::class.java){selector.update(SocksPolicy("tor","8.8.8.8",9050))};selector.update(SocksPolicy("direct"));assertEquals(Proxy.NO_PROXY,selector.select(URI("https://example.invalid")).single())}
  @Test fun legacyInstallationClaimsOwningAccountThenRevokesAllAccountsWithoutBearer(){
    HttpFixture(replies=listOf(409 to "{\"message\":\"installation_claim_required\"}",200 to "{\"epoch\":1}",200 to "{\"ok\":true}")).use{target->SocksFixture(target.server.localPort,3).use{socks->
      SocksPushPreflight.disable(SocksPolicy("tor","127.0.0.1",socks.server.localPort),listOf(PushTarget("http://legacy-push.invalid:${target.server.localPort}","owner-token")),"legacy-installation","b".repeat(64))
      assertEquals(3,target.requests.size);assertTrue(target.requests[0].startsWith("POST /auth/account-push-disable"));assertTrue(target.requests[1].startsWith("POST /me/account/push-installation"));assertTrue(target.requests[1].contains("Authorization: Bearer owner-token"));assertFalse(target.requests[2].contains("Authorization:"));assertEquals("legacy-push.invalid",socks.destination.get())
    }}
  }
}
