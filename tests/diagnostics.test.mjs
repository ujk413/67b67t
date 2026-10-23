import {test} from 'node:test';
import assert from 'node:assert/strict';
import {diagnoseConnection} from '../web/diagnostics.js';
class Socket {
  constructor(){queueMicrotask(()=>this.onmessage?.({data:JSON.stringify({type:'auth.required'})}));}
  send(payload){assert.equal(JSON.parse(payload).token,'test-token');queueMicrotask(()=>this.onmessage?.({data:JSON.stringify({type:'auth.ok'})}));}
  close(){this.closed=true;}
}
class Peer {
  static instances=[];
  constructor(config){assert.equal(config.iceTransportPolicy,'relay');Peer.instances.push(this);}
  createDataChannel(){}async createOffer(){return{type:'offer',sdp:'test'};}
  async setLocalDescription(){queueMicrotask(()=>this.onicecandidate?.({candidate:{candidate:'candidate:1 1 UDP 1 127.0.0.1 50000 typ relay'}}));}
  close(){this.closed=true;}
}
test('diagnostics verifies authenticated WS and an actual TURN relay candidate and releases resources',async()=>{
 const result=await diagnoseConnection({request:async path=>path==='/health'?{ok:true}:{iceServers:[{urls:'turn:example.test',username:'secret-user',credential:'secret'}]},wsUrl:'wss://example.test/api/ws',token:'test-token',Socket,Peer});
 assert.deepEqual(result.map(x=>x.ok),[true,true,true]);assert.equal(Peer.instances.at(-1).closed,true);assert.ok(!JSON.stringify(result).includes('secret'));
});
test('a failed API does not suppress independent WS and TURN results',async()=>{
 const result=await diagnoseConnection({request:async path=>{if(path==='/health')throw Error('private internal response');return {iceServers:[]};},wsUrl:'wss://example.test/ws',token:'test-token',Socket,Peer});
 assert.deepEqual(result.map(x=>x.ok),[false,true,false]);assert.ok(result[0].action);assert.ok(!JSON.stringify(result).includes('private internal response'));
});
