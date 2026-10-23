const {withMainApplication}=require('@expo/config-plugins');
module.exports=config=>withMainApplication(config,config=>{
 const line='com.oney.WebRTCModule.WebRTCModuleOptions.getInstance().enableMediaProjectionService = true';
 if(!config.modResults.contents.includes(line)){
  if(config.modResults.language!=='kt'||!config.modResults.contents.includes('super.onCreate()'))throw Error('Не удалось включить службу демонстрации экрана в MainApplication');
  config.modResults.contents=config.modResults.contents.replace('super.onCreate()',`super.onCreate()\n    ${line}`);
 }return config;
});
