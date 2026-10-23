import {requireOptionalNativeModule} from 'expo';
const native=requireOptionalNativeModule<{
 transcodeVideoAsync(uri:string,height:number,id:string):Promise<{uri:string;width:number;height:number;duration_ms:number;orientation:1;rotation:0|90|180|270}>;
 cancelVideoProcessing(id:string):void;
 addListener(name:string,fn:(event:{id:string;progress:number})=>void):{remove():void};
}>('RoomKey');
export function processVideo(uri:string,height:480|720,onProgress:(value:number)=>void) {
 const id=`${Date.now()}-${Math.random()}`;
 const subscription=native?.addListener('mediaProgress',event=>{if(event.id===id)onProgress(event.progress);});
 return {abort:()=>native?.cancelVideoProcessing(id),promise:(async()=>{
   try{if(!native?.transcodeVideoAsync)throw Error('Для обработки видео установите новую APK 67chat');return await native.transcodeVideoAsync(uri,height,id);}
   finally{subscription?.remove();}
 })()};
}
