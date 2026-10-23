import {test} from 'node:test';import assert from 'node:assert/strict';
import {bindVoicePlayers,stopVoicePlayers} from '../web/voice-player.js';
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return{promise,resolve};};
function fixture(id){
 const audio={paused:true,ended:false,src:'',getAttribute(){return this.src;},play(){this.paused=false;this.onplay?.();return Promise.resolve();},pause(){this.paused=true;this.onpause?.();}},toggle={querySelector:()=>({}),setAttribute(){}},wave={children:[]},speed={},status={};
 const node={dataset:{voiceId:id},isConnected:true,querySelector:s=>({'audio':audio,'.voice-toggle':toggle,'.voice-wave':wave,'.voice-speed':speed,'.voice-status':status})[s]};return{node,audio,toggle};
}
test('a slow voice source cannot interrupt a newer playback or resume after account reset',async()=>{
 const slow=deferred(),a=fixture('a'),b=fixture('b'),errors=[];
 bindVoicePlayers({querySelectorAll:()=>[a.node,b.node]},{findMessage:id=>({id}),source:message=>message.id==='a'?slow.promise:Promise.resolve({url:'b-url'}),listened:async()=>{},report:e=>errors.push(e)});
 const old=a.toggle.onclick();await b.toggle.onclick();slow.resolve({url:'a-url'});await old;
 assert.equal(a.audio.paused,true);assert.equal(b.audio.paused,false);
 stopVoicePlayers();assert.equal(b.audio.paused,true);assert.deepEqual(errors,[]);
});
test('closing a chat while source loads prevents detached audio playback',async()=>{
 const slow=deferred(),a=fixture('a');
 bindVoicePlayers({querySelectorAll:()=>[a.node]},{findMessage:id=>({id}),source:()=>slow.promise,listened:async()=>{},report:e=>{throw e;}});
 const playing=a.toggle.onclick();stopVoicePlayers();slow.resolve({url:'private'});await playing;assert.equal(a.audio.paused,true);
});
