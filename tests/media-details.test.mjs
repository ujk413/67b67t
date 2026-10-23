import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mediaDetails,recordingGesture,fitCrop} from '../web/media-details.js';
test('protected media metadata retains EXIF orientation without allowing arbitrary fields',()=>{for(const orientation of [1,3,6,8])assert.equal(mediaDetails({orientation}).orientation,orientation);for(const orientation of [0,9,1.5,'6'])assert.equal(mediaDetails({orientation}).orientation,undefined);});
test('a real sampled waveform is downsampled without inventing URI-derived amplitude',()=>{
 const details=mediaDetails({waveform:Array.from({length:100},(_,i)=>i<50?0.1:0.9),width:1080,height:1920,duration_ms:60000});
 assert.equal(details.waveform.length,40);assert.ok(details.waveform[0]<details.waveform.at(-1));assert.equal(details.width,1080);assert.equal(details.height,1920);
 assert.equal(mediaDetails({waveform:[NaN,Infinity,-2,5]}).waveform.length,4);assert.deepEqual(mediaDetails({waveform:[NaN,Infinity,-2,5]}).waveform,[0,0,0,1]);
});
test('encrypted media details preserve a bounded sticker kind and strip transport secrets',()=>{
 assert.deepEqual(mediaDetails({expressive:{kind:'sticker',label:'Кот',token:'secret',catalog_id:'private'}}),{expressive:{kind:'sticker',label:'Кот'}});
 assert.deepEqual(mediaDetails({expressive:{kind:'poll',label:'secret'}}),{});
});
test('recording cancels to the left, locks upwards and leaves a locked capture recording on release',()=>{
 assert.equal(recordingGesture({dx:-90,dy:-70,locked:false}),'cancel');assert.equal(recordingGesture({dx:0,dy:-70,locked:false}),'lock');assert.equal(recordingGesture({dx:-100,dy:0,locked:true}),'none');
});
test('portrait and landscape crop rectangles stay within the rotated image without upscaling',()=>{
 for(const [width,height] of [[1080,1920],[1920,1080],[1,1]])for(const ratio of [1,4/3,16/9]){
  const crop=fitCrop(width,height,ratio);assert.ok(crop.originX>=0 && crop.originY>=0);assert.ok(crop.originX+crop.width<=width);assert.ok(crop.originY+crop.height<=height);assert.ok(crop.width>=1 && crop.height>=1);
 }
});
