import {requireOptionalNativeModule} from 'expo';
type NativeUi={setUiLanguage?:(language:string)=>void};
export function setNativeUiLanguage(language:'ru'|'en'){
 for(const name of ['RoomKey','ExpressiveIntegration']){
  // An older installed binary may lack the bridge; app UI still switches.
  try{requireOptionalNativeModule<NativeUi>(name)?.setUiLanguage?.(language);}catch{}
 }
}
