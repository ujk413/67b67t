import {requireOptionalNativeModule} from 'expo';
import * as Files from 'expo-file-system/legacy';
import {createUploadTask} from './uploads';
import {newId} from './data/repository';
const native=requireOptionalNativeModule<{encryptFileAsync(input:string,output:string,key:string,iv:string):Promise<string>}>('RoomKey');
export type PreparedUpload={uri:string;meta:Record<string,unknown>};
export type UploadInput={uri:string;name:string;mime:string;prepared?:PreparedUpload};
export type ProtectedUploader=(roomId:string,file:UploadInput,onProgress:(n:number)=>void,checkpoint?:(prepared:PreparedUpload)=>Promise<void>)=>{request:{abort():void};promise:Promise<{id:string;meta:Record<string,unknown>}>};
export function protectedUploader(base:string,token:string,encode:(room:string,text:string)=>Promise<any>,owner?:string):ProtectedUploader {
  return (room,file,progress,checkpoint)=>{
    let cancelled=false,task:ReturnType<typeof createUploadTask>|null=null;
    const output=`${Files.cacheDirectory}encrypted-uploads/${newId()}.bin`;
    const promise=(async()=>{
      if(!native)throw Error('Установите новую APK для отправки защищённых вложений');
      let prepared=file.prepared;
      const info=await Files.getInfoAsync(file.uri);
      if(!info.exists)throw Error('Файл больше недоступен');if(info.size>100*1024*1024-16)throw Error('Вложение больше 100 МБ');
      if(!prepared || !(await Files.getInfoAsync(prepared.uri)).exists) {
        const bytes=(n:number)=>btoa(Array.from(crypto.getRandomValues(new Uint8Array(n)),b=>String.fromCharCode(b)).join(''));
        const key=bytes(32),iv=bytes(12),key_message=await encode(room,JSON.stringify({key,iv}));
        await Files.makeDirectoryAsync(`${Files.cacheDirectory}encrypted-uploads/`,{intermediates:true});
        await native.encryptFileAsync(file.uri,output,key,iv);
        prepared={uri:output,meta:{media_encryption:{v:2,key_message,name:file.name,mime:file.mime,size:info.size}}};
        if(checkpoint) {
          if(!owner || !/^[a-f0-9-]{36}$/i.test(owner))throw Error('Сеанс изменился');
          const directory=`${Files.documentDirectory}draft-media/${owner}/`;
          await Files.makeDirectoryAsync(directory,{intermediates:true});
          const durable=directory+newId()+'-encrypted.bin';await Files.copyAsync({from:output,to:durable});
          prepared={...prepared,uri:durable};
          if(cancelled){await Files.deleteAsync(durable,{idempotent:true});throw Error('Загрузка отменена');}
          try{await checkpoint(prepared);}catch(error){await Files.deleteAsync(durable,{idempotent:true});throw error;}
        }
      }
      if(cancelled)throw Error('Загрузка отменена');
      task=createUploadTask(base,token,'file',{uri:prepared.uri,name:'attachment.bin',mime:'application/octet-stream'},progress);
      const id=await task.promise;
      return {id,meta:prepared.meta};
    })().finally(()=>Files.deleteAsync(output,{idempotent:true}).catch(()=>{}));
    return {request:{abort(){cancelled=true;task?.request.abort();}},promise};
  };
}
