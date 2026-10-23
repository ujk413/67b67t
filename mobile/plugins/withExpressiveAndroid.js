const {withAndroidManifest}=require('@expo/config-plugins');

/** Share targets and text-free launcher shortcuts survive expo prebuild --clean. */
module.exports=function withExpressiveAndroid(config){return withAndroidManifest(config,config=>{
 const app=config.modResults.manifest.application?.[0];if(!app)return config;
 const activity=app.activity?.find(a=>a.$['android:name']==='.MainActivity')||app.activity?.[0];if(!activity)return config;
 const filters=activity['intent-filter']||[];
 if(!filters.some(f=>f.data?.some(data=>data.$['android:scheme']==='67chat'))){filters.push({action:[{$:{'android:name':'android.intent.action.VIEW'}}],category:[{$:{'android:name':'android.intent.category.DEFAULT'}},{$:{'android:name':'android.intent.category.BROWSABLE'}}],data:[{$:{'android:scheme':'67chat'}}]});}
 if(!filters.some(f=>f.data?.some(data=>data.$['android:host']==='67chat.ru'))){filters.push({$:{'android:autoVerify':'true'},action:[{$:{'android:name':'android.intent.action.VIEW'}}],category:[{$:{'android:name':'android.intent.category.DEFAULT'}},{$:{'android:name':'android.intent.category.BROWSABLE'}}],data:[{$:{'android:scheme':'https','android:host':'67chat.ru','android:pathPrefix':'/chat/'}},{$:{'android:scheme':'https','android:host':'67chat.ru','android:pathPrefix':'/profile/'}},{$:{'android:scheme':'https','android:host':'67chat.ru','android:pathPrefix':'/invite/'}}]});}
 activity['intent-filter']=filters;
 if(!filters.some(f=>f.action?.some(a=>a.$['android:name']==='android.intent.action.SEND'))){
  for(const action of ['android.intent.action.SEND','android.intent.action.SEND_MULTIPLE'])filters.push({action:[{$:{'android:name':action}}],category:[{$:{'android:name':'android.intent.category.DEFAULT'}}],data:[{$:{'android:mimeType':'*/*'}}]});
  activity['intent-filter']=filters;
 }
 activity['meta-data']=activity['meta-data']||[];
 if(!activity['meta-data'].some(m=>m.$['android:name']==='android.app.shortcuts'))activity['meta-data'].push({$:{'android:name':'android.app.shortcuts','android:resource':'@xml/expressive_shortcuts'}});
 return config;
});};
