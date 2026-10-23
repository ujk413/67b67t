import {t as uiText} from "./i18n.js";
import {fitCrop,mediaDetails} from './media-details.js';
const abortError=()=>Object.assign(new Error('Обработка отменена'),{name:'AbortError'});
export function waitForMediaMetadata(item,url,{signal,timeoutMs=15000}={}){
 return new Promise((resolve,reject)=>{
  let timer;
  const cleanup=()=>{clearTimeout(timer);signal?.removeEventListener('abort',abort);item.onload=null;item.onloadedmetadata=null;item.onerror=null;};
  const finish=error=>{cleanup();error?reject(error):resolve();};
  const abort=()=>finish(abortError());
  if(signal?.aborted){reject(abortError());return;}
  item.onload=()=>finish();item.onloadedmetadata=()=>finish();item.onerror=()=>finish(Error('Не удалось прочитать параметры медиа'));
  signal?.addEventListener('abort',abort,{once:true});timer=setTimeout(()=>finish(Error('Превышено время чтения медиа')),timeoutMs);item.src=url;
 });
}
export function videoDimensions(width,height,quality){
  if(![480,720].includes(quality)||width<=0||height<=0)throw Error('Некорректное качество видео');
  const scale=Math.min(1,quality/Math.min(width,height));
  return {width:Math.max(2,Math.floor(width*scale/2)*2),height:Math.max(2,Math.floor(height*scale/2)*2)};
}
export async function inspectMedia(file,options={}){
 if(!/^(image|video)\//.test(file.type))return {};
 const url=URL.createObjectURL(file),item=document.createElement(file.type.startsWith('image/')?'img':'video');
 try{await waitForMediaMetadata(item,url,options);return mediaDetails({width:item.naturalWidth||item.videoWidth,height:item.naturalHeight||item.videoHeight,orientation:1,duration_ms:Number.isFinite(item.duration)?item.duration*1000:undefined});}
 finally{item.removeAttribute('src');item.load?.();URL.revokeObjectURL(url);}
}
export async function editPhoto(file,{signal}={}){
 if(signal?.aborted)throw abortError();
 const image=await createImageBitmap(file,{imageOrientation:'from-image'}),dialog=document.createElement('dialog');dialog.className='media-dialog';
 if(signal?.aborted){image.close();throw abortError();}
 const title=document.createElement('h2');title.textContent=uiText("Изменить фото");const canvas=document.createElement('canvas');canvas.style.cssText='max-width:100%;max-height:55vh;object-fit:contain';
 const controls=document.createElement('div');controls.className='recording-controls';let rotation=0,ratio=0,scale=1,settled=false;
 const draw=()=>{const portrait=rotation%180!==0,w=portrait?image.height:image.width,h=portrait?image.width:image.height,crop=fitCrop(w,h,ratio||w/h,scale);canvas.width=Math.round(crop.width);canvas.height=Math.round(crop.height);const ctx=canvas.getContext('2d');ctx.translate(w/2-crop.originX,h/2-crop.originY);ctx.rotate(rotation*Math.PI/180);ctx.drawImage(image,-image.width/2,-image.height/2);};
 const result=new Promise(resolve=>{
  const abort=()=>finish(null);
  const finish=value=>{if(settled)return;settled=true;signal?.removeEventListener('abort',abort);image.close();dialog.close();dialog.remove();resolve(value);};
  signal?.addEventListener('abort',abort,{once:true});
  const button=(label,fn)=>{const b=document.createElement('button');b.type='button';b.className='btn';b.textContent=label;b.onclick=fn;controls.append(b);return b;};
  button(uiText("Поворот"),()=>{rotation=(rotation+90)%360;draw();});
  for(const [label,value]of[[uiText("Без обрезки"),0],['1:1',1],['4:3',4/3],['16:9',16/9]])button(label,()=>{ratio=value;draw();});
  const zoom=document.createElement('input');zoom.type='range';zoom.min='.3';zoom.max='1';zoom.step='.01';zoom.value='1';zoom.setAttribute('aria-label',uiText("Размер области обрезки"));zoom.oninput=()=>{scale=Number(zoom.value);draw();};controls.append(zoom);
  button(uiText("Сброс"),()=>{rotation=0;ratio=0;scale=1;zoom.value='1';draw();});
  button(uiText("Отмена"),()=>finish(null));const apply=button(uiText("Применить к копии"),()=>{apply.disabled=true;canvas.toBlob(blob=>{if(!blob){apply.disabled=false;return;}finish(new File([blob],file.name.replace(/\.[^.]+$/,'')+'-edited.jpg',{type:'image/jpeg'}));},'image/jpeg',.92);});
  dialog.addEventListener('cancel',event=>{event.preventDefault();finish(null);});
 });dialog.append(title,canvas,controls);document.body.append(dialog);dialog.showModal();draw();return result;
}
// Canvas/MediaRecorder processing happens before encryptFile. Unsupported
// browsers fail explicitly; the original remains selectable and unchanged.
export async function transcodeVideo(file,quality,{signal,progress=()=>{}}={}){
 if(signal?.aborted)throw abortError();
 const video=document.createElement('video'),url=URL.createObjectURL(file);video.playsInline=true;video.preload='auto';let recorder,stream,audio,timer,settle;
 try{
  await waitForMediaMetadata(video,url,{signal});
  const size=videoDimensions(video.videoWidth,video.videoHeight,quality);
  if(size.width>=video.videoWidth && size.height>=video.videoHeight)return file;
  if(!globalThis.MediaRecorder || !HTMLCanvasElement.prototype.captureStream)throw Error('Этот браузер не поддерживает изменение качества. Выберите оригинал.');
  const mime=['video/mp4;codecs=avc1,mp4a.40.2','video/webm;codecs=vp8,opus','video/webm'].find(type=>MediaRecorder.isTypeSupported(type));if(!mime)throw Error('Нет подходящего видеокодека. Выберите оригинал.');
  const canvas=document.createElement('canvas');canvas.width=size.width;canvas.height=size.height;const ctx=canvas.getContext('2d');stream=canvas.captureStream(30);
  const AudioContext=globalThis.AudioContext||globalThis.webkitAudioContext;if(!AudioContext)throw Error('Обработка звука недоступна. Выберите оригинал.');
  audio=new AudioContext();const destination=audio.createMediaStreamDestination();audio.createMediaElementSource(video).connect(destination);for(const track of destination.stream.getAudioTracks())stream.addTrack(track);await audio.resume();
  const chunks=[];recorder=new MediaRecorder(stream,{mimeType:mime,videoBitsPerSecond:quality===720?2500000:1200000,audioBitsPerSecond:128000});
  const processed=new Promise((resolve,reject)=>{settle=reject;recorder.ondataavailable=event=>{if(event.data.size)chunks.push(event.data);};recorder.onerror=()=>reject(Error('Не удалось обработать видео'));recorder.onstop=()=>signal?.aborted?reject(abortError()):resolve(new File(chunks,file.name.replace(/\.[^.]+$/,'')+(mime.startsWith('video/mp4')?'.mp4':'.webm'),{type:mime.split(';')[0]}));video.onended=()=>{clearInterval(timer);if(recorder.state!=='inactive')recorder.stop();};});
  const abort=()=>{video.pause();if(recorder.state!=='inactive')recorder.stop();settle?.(abortError());};signal?.addEventListener('abort',abort,{once:true});
  try{if(signal?.aborted)throw abortError();recorder.start(250);await video.play();timer=setInterval(()=>{ctx.drawImage(video,0,0,size.width,size.height);progress(Number.isFinite(video.duration)?video.currentTime/video.duration:0);},33);const result=await processed;progress(1);return result;}
  finally{signal?.removeEventListener('abort',abort);}
 }finally{clearInterval(timer);video.pause();if(recorder&&recorder.state!=='inactive')recorder.stop();stream?.getTracks().forEach(track=>track.stop());await audio?.close().catch(()=>{});video.removeAttribute('src');video.load();URL.revokeObjectURL(url);}
}
