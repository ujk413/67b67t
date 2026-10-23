export function mediaDetails(input={}) {
  const result={};
  if(Number.isInteger(input.orientation)&&input.orientation>=1&&input.orientation<=8)result.orientation=input.orientation;
  if([0,90,180,270].includes(input.rotation))result.rotation=input.rotation;
  for(const [key,max] of [['width',32768],['height',32768],['duration_ms',24*3600000]]) {
    const value=Math.round(Number(input[key]));if(Number.isFinite(value)&&value>0&&value<=max)result[key]=value;
  }
  if(Array.isArray(input.waveform)) {
    const samples=input.waveform.slice(0,20000).map(n=>Number.isFinite(Number(n))?Math.max(0,Math.min(1,Number(n))):0),count=Math.min(40,samples.length);
    result.waveform=Array.from({length:count},(_,i)=>{
      const start=Math.floor(i*samples.length/count),end=Math.max(start+1,Math.floor((i+1)*samples.length/count));
      return Math.sqrt(samples.slice(start,end).reduce((sum,n)=>sum+n*n,0)/(end-start));
    });
  }
  if(['sticker','gif'].includes(input.expressive?.kind)){
    result.expressive={kind:input.expressive.kind};
    if(typeof input.expressive.label==='string')result.expressive.label=input.expressive.label.slice(0,100);
  }
  return result;
}
export function recordingGesture({dx,dy,locked}) {
  if(locked)return 'none';if(dx < -80)return 'cancel';if(dy < -60)return 'lock';return 'none';
}
export function fitCrop(width,height,ratio=1,scale=1) {
  const w=Math.max(1,Math.floor(Number(width)||1)),h=Math.max(1,Math.floor(Number(height)||1));
  const target=Number.isFinite(ratio)&&ratio>0?ratio:1;
  const factor=Math.max(0.1,Math.min(1,Number(scale)||1));
  const cropWidth=Math.max(1,Math.floor(Math.min(w,h*target)*factor)),cropHeight=Math.max(1,Math.floor(Math.min(h,w/target)*factor));
  return {originX:Math.floor((w-cropWidth)/2),originY:Math.floor((h-cropHeight)/2),width:cropWidth,height:cropHeight};
}
