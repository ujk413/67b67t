import {requireOptionalNativeModule} from 'expo';
import * as Files from 'expo-file-system/legacy';
import {createUploadTask} from './uploads';
import {newId} from './data/repository';
const native=requireOptionalNativeModule<{encryptFileAsync(input:string,output:string,key:string,iv:string):Promise<string>}>('RoomKey');
export type UploadInput={uri:string;name:string;mime:string};
export type ProtectedUploader=(roomId:string,file:UploadInput,onProgress:(n:number)=>void)=>{request:{abort():void};promise:Promise<{id:string;meta:Record<string,unknown>}>};
export function protectedUploader(base:string,token:string,encode:(room:string,text:string)=>Promise<any>):ProtectedUploader {
  return (room,file,progress)=>{
    let cancelled=false,task:ReturnType<typeof createUploadTask>|null=null;
    const output=`${Files.cacheDirectory}encrypted-uploads/${newId()}.bin`;
    const promise=(async()=>{
      if(!native)throw Error('Установите новую APK для отправки защищённых вложений');
      const bytes=(n:number)=>btoa(Array.from(crypto.getRandomValues(new Uint8Array(n)),b=>String.fromCharCode(b)).join(''));
      const key=bytes(32),iv=bytes(12),info=await Files.getInfoAsync(file.uri);
      if(!info.exists)throw Error('Файл больше недоступен');if(info.size>100*1024*1024-16)throw Error('Вложение больше 100 МБ');
      const key_message=await encode(room,JSON.stringify({key,iv}));
      await Files.makeDirectoryAsync(`${Files.cacheDirectory}encrypted-uploads/`,{intermediates:true});
      await native.encryptFileAsync(file.uri,output,key,iv);
      if(cancelled)throw Error('Загрузка отменена');
      task=createUploadTask(base,token,'file',{uri:output,name:'attachment.bin',mime:'application/octet-stream'},progress);
      const id=await task.promise;
      return {id,meta:{media_encryption:{v:2,key_message,name:file.name,mime:file.mime,size:info.size}}};
    })().finally(()=>Files.deleteAsync(output,{idempotent:true}).catch(()=>{}));
    return {request:{abort(){cancelled=true;task?.request.abort();}},promise};
  };
}
