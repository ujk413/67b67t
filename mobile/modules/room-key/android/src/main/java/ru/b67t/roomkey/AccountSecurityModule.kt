package ru.b67t.roomkey

import expo.modules.kotlin.modules.Module
import expo.modules.kotlin.modules.ModuleDefinition

class AccountSecurityModule:Module(){
  override fun definition()=ModuleDefinition {
    Name("AccountSecurity")
    AsyncFunction("capabilitiesAsync"){AccountTransport.capabilities()}
    AsyncFunction("getTransportPolicyAsync"){AccountTransport.policy(requireNotNull(appContext.reactContext))}
    AsyncFunction("configureTransportAsync"){json:String->AccountTransport.configure(requireNotNull(appContext.reactContext),json)}
    AsyncFunction("validateTransportAsync"){json:String->AccountTransport.validate(json)}
    AsyncFunction("unregisterPushForTransportAsync"){policy:String,targets:String,installation:String,secret:String->AccountTransport.unregisterPush(policy,targets,installation,secret)}
    AsyncFunction("restartTransportAsync"){promise:expo.modules.kotlin.Promise->val context=requireNotNull(appContext.reactContext);android.os.Handler(android.os.Looper.getMainLooper()).post{try{AccountTransport.restart(context);promise.resolve(null)}catch(error:Throwable){promise.reject("transport_restart_failed",error.message,error)}};Unit}
    Function("assertCallsAllowed"){AccountTransport.requireDirectCalls()}
  }
}
