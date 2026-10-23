// Shared by web and Android. Results never include URLs, SDP or TURN credentials.
export async function diagnoseConnection({request,wsUrl,token,Socket=globalThis.WebSocket,Peer=globalThis.RTCPeerConnection,timeout=8000}) {
  const results=[];
  const step=async(id,title,action,run)=>{try{await run();results.push({id,title,ok:true,message:'Проверка пройдена',action:''});}catch{results.push({id,title,ok:false,message:'Проверка не пройдена',action});}};
  await step('api','Сервер API','Проверьте интернет и доступность сайта 67chat.',async()=>{const value=await request('/health');if(!value?.ok)throw Error('health');});
  await step('ws','Соединение WebSocket','Переподключитесь. Проверьте, не блокирует ли сеть постоянные соединения.',()=>new Promise((resolve,reject)=>{
    let socket,done=false;const timer=setTimeout(()=>finish(false),timeout);
    function finish(ok){if(done)return;done=true;clearTimeout(timer);if(socket){socket.onmessage=null;socket.onerror=null;socket.onclose=null;try{socket.close();}catch{}}ok?resolve():reject(Error('socket'));}
    try{socket=new Socket(wsUrl);socket.onmessage=event=>{try{const data=JSON.parse(event.data);if(data.type==='auth.ok')finish(true);else if(data.type==='auth.required' && token)socket.send(JSON.stringify({type:'auth',token}));else if(data.error || data.type==='auth.error')finish(false);}catch{finish(false);}};socket.onerror=()=>finish(false);socket.onclose=()=>finish(false);}catch{finish(false);}
  }));
  await step('turn','Маршрут звонка TURN','Попробуйте другую сеть. Администратору нужно проверить TURN и порты сервера.',async()=>{
    const {iceServers}=await request('/rtc/ice-servers');
    const servers=(iceServers || []).filter(server=>[server.urls].flat().some(url=>/^turns?:/.test(url)));
    if(!servers.length || !Peer)throw Error('turn_unavailable');
    await new Promise((resolve,reject)=>{
      let peer,done=false;const timer=setTimeout(()=>finish(false),timeout);
      function finish(ok){if(done)return;done=true;clearTimeout(timer);if(peer){peer.onicecandidate=null;try{peer.close();}catch{}}ok?resolve():reject(Error('turn'));}
      try{peer=new Peer({iceServers:servers,iceTransportPolicy:'relay'});peer.onicecandidate=event=>{
        if(event.candidate && /\btyp relay\b/.test(event.candidate.candidate || ''))finish(true);
        else if(!event.candidate)finish(false);
      };peer.createDataChannel('diagnostic');Promise.resolve(peer.createOffer()).then(offer=>{if(!done)return peer.setLocalDescription(offer);}).catch(()=>finish(false));}catch{finish(false);}
    });
  });
  return results;
}
