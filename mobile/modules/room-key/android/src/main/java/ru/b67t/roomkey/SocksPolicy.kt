package ru.b67t.roomkey

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Proxy
import java.net.ProxySelector
import java.net.URI
import java.net.SocketAddress
import java.io.IOException
import java.util.concurrent.atomic.AtomicReference

/** One SOCKS route, no DIRECT candidate, numeric proxy endpoint, destination DNS
 * stays in the SOCKS CONNECT request. Installed before any network clients. */
internal data class SocksPolicy(val mode:String,val host:String="",val port:Int=0) {
  val isProtected:Boolean get()=mode!="direct"
  fun endpoint():InetSocketAddress {
    require(mode=="proxy"||mode=="tor") { "Network is paused for transport restart" }
    val address=when(host){"::1"->ByteArray(16).also{it[15]=1};else->{val parts=host.split('.');require(parts.size==4&&parts.all{it.toIntOrNull() in 0..255&&it.all(Char::isDigit)}) { "Proxy host must be a numeric IP address" };parts.map{it.toInt().toByte()}.toByteArray()}}
    require(port in 1..65535);if(mode=="tor")require(host=="127.0.0.1"||host=="::1") { "Tor requires a local SOCKS service, such as Orbot" }
    return InetSocketAddress(InetAddress.getByAddress(address),port)
  }
}
internal class SocksOnlyProxySelector(initial:SocksPolicy):ProxySelector(){
  private val state=AtomicReference(initial)
  val policy:SocksPolicy get()=state.get()
  fun update(value:SocksPolicy){if(value.mode!="direct"&&value.mode!="blocked")value.endpoint();state.set(value)}
  override fun select(uri:URI?):List<Proxy>{
    require(uri!=null);val value=policy
    if(value.mode=="direct")return listOf(Proxy.NO_PROXY)
    require(uri.scheme?.lowercase() in listOf("http","https","ws","wss","socket")) { "Transport protocol is not supported" }
    return listOf(Proxy(Proxy.Type.SOCKS,value.endpoint()))
  }
  // Deliberately never add a direct route after a proxy failure.
  override fun connectFailed(uri:URI?,address:SocketAddress?,failure:IOException?)=Unit
}
