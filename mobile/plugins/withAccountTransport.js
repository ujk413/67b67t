const {withMainApplication,withAndroidManifest}=require('@expo/config-plugins');
module.exports=function withAccountTransport(config){
 config=withMainApplication(config,config=>{
  if(config.modResults.language!=='kt')throw Error('Account transport requires Kotlin MainApplication');
  let s=config.modResults.contents;
  if(!s.includes('import ru.b67t.roomkey.AccountTransport'))s=s.replace('import android.app.Application','import ru.b67t.roomkey.AccountTransport\nimport android.app.Application');
  if(!s.includes('override fun attachBaseContext'))s=s.replace('  override fun onCreate() {','  override fun attachBaseContext(base: android.content.Context) {\n    super.attachBaseContext(base)\n    AccountTransport.install(this)\n  }\n\n  override fun onCreate() {');
  if(!s.includes('if (AccountTransport.isRestartProcess()) return'))s=s.replace('    super.onCreate()','    super.onCreate()\n    if (AccountTransport.isRestartProcess()) return');
  config.modResults.contents=s;return config;
 });
 return withAndroidManifest(config,config=>{const app=config.modResults.manifest.application[0];app.activity=app.activity||[];
  if(!app.activity.some(a=>a.$['android:name']==='ru.b67t.roomkey.TransportRestartActivity'))app.activity.push({$: {'android:name':'ru.b67t.roomkey.TransportRestartActivity','android:process':':transport_restart','android:exported':'false','android:excludeFromRecents':'true','android:theme':'@android:style/Theme.Translucent.NoTitleBar'}});
  app['meta-data']=app['meta-data']||[];for(const key of ['firebase_messaging_auto_init_enabled','firebase_messaging_notification_delegation_enabled','firebase_analytics_collection_enabled']){let row=app['meta-data'].find(m=>m.$['android:name']===key);if(!row){row={$:{'android:name':key}};app['meta-data'].push(row);}row.$['android:value']='false';}
  return config;
 });
};
