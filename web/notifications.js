// Browser Web Push uses its standard push service; no Firebase project is needed.
const isQuietHours=(quiet,at=new Date())=>{if(!quiet?.enabled||!/^([01]\d|2[0-3]):[0-5]\d$/.test(quiet.start)||!/^([01]\d|2[0-3]):[0-5]\d$/.test(quiet.end)||quiet.start===quiet.end)return false;try{const parts=new Intl.DateTimeFormat('en-GB',{timeZone:quiet.time_zone,hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(at);const current=Number(parts.find(x=>x.type==='hour')?.value)*60+Number(parts.find(x=>x.type==='minute')?.value),minutes=s=>Number(s.slice(0,2))*60+Number(s.slice(3)),start=minutes(quiet.start),end=minutes(quiet.end);return start<end?current>=start&&current<end:current>=start||current<end;}catch{return false;}};
export function createWebNotifications({ api, activeRoom, status, account = () => null }) {
  let registration, subscription, timer, stopped = true, quiet = null, owner = null, epoch = 0, pending = Promise.resolve();
  const supported = () => 'Notification' in window && 'serviceWorker' in navigator && 'PushManager' in window;
  const report = enabled => status({ enabled, permission: supported() ? Notification.permission : 'unsupported', registration });
  const disabledKey = id => 'web-push-disabled:' + (id || 'legacy');
  const request = (id,path,options={}) => api(path,{...options,headers:{...options.headers,...(id?{'X-Account-Id':id}:{})}});
  async function register() {
    registration ||= await navigator.serviceWorker.register('/sw.js');
    await navigator.serviceWorker.ready;
    return registration;
  }
  function writeHeartbeat(id,current,room) {
    // Serialize active/inactive writes so an old active heartbeat cannot overtake pause.
    const write = () => request(id,'/me/web-push',{method:'PUT',keepalive:true,body:JSON.stringify({subscription:current.toJSON(),active_room_id:room})});
    const result = pending.catch(()=>{}).then(write);pending=result.catch(()=>{});return result;
  }
  async function heartbeat(inactive = false) {
    if (stopped || !subscription) return;
    const id=owner,current=subscription,generation=epoch;
    try {const value=await request(id,'/me/quality');if(generation===epoch)quiet=value.quiet;} catch {}
    if(stopped || generation!==epoch || account()!==id)return;
    const result=await writeHeartbeat(id,current,inactive || document.hidden || !document.hasFocus() ? null : activeRoom());
    if (!result) throw new Error('Не удалось сохранить подписку на уведомления');
  }
  async function enable(prompt = true) {
    const generation=++epoch,id=account();owner=id;stopped=false;
    const current=()=>generation===epoch&&account()===id&&!stopped;
    try {
      if (!supported()) throw new Error('Уведомления недоступны. На iPhone добавьте сайт на главный экран и откройте оттуда.');
      if (prompt && Notification.permission === 'default') await Notification.requestPermission();
      if(!current())return;
      if (Notification.permission !== 'granted') throw new Error('Разрешите уведомления для 67chat в настройках браузера.');
      const config = await request(id,'/web-push/config');if(!current())return;
      if (!config?.enabled) throw new Error('Сервер уведомлений временно недоступен');
      const worker=await register();if(!current())return;
      let found=await worker.pushManager.getSubscription();if(!current())return;
      if (!found) {
        const raw=atob(config.public_key.replace(/-/g,'+').replace(/_/g,'/'));
        found=await worker.pushManager.subscribe({userVisibleOnly:true,applicationServerKey:Uint8Array.from(raw,char=>char.charCodeAt(0))});
      }
      if(!current())return;subscription=found;await heartbeat();if(!current())return;
      clearInterval(timer);timer=setInterval(()=>heartbeat().catch(()=>{}),25000);
      localStorage.removeItem(disabledKey(id));report(true);
    } catch(error) {
      if(generation===epoch){stopped=true;clearInterval(timer);report(false);}throw error;
    }
  }
  function pause() {
    const id=owner,current=subscription;++epoch;stopped=true;clearInterval(timer);subscription=null;report(false);
    return current?writeHeartbeat(id,current,null):Promise.resolve();
  }
  async function disable() {
    const id=owner,current=subscription;const draining=pause();await draining.catch(()=>{});
    if(current)await request(id,'/me/web-push',{method:'DELETE',body:JSON.stringify({endpoint:current.endpoint})});
    // The PushSubscription belongs to the browser, shared by all signed-in accounts.
    // Only remove this user's server association; never unsubscribe other accounts.
  }
  document.addEventListener('visibilitychange',()=>heartbeat().catch(()=>{}));
  window.addEventListener('focus',()=>heartbeat().catch(()=>{}));
  window.addEventListener('blur',()=>heartbeat().catch(()=>{}));
  window.addEventListener('pagehide',()=>heartbeat(true).catch(()=>{}));
  return {
    async start() {
      if(!supported()){report(false);return;}
      const generation=++epoch,id=account();await register();if(generation!==epoch||account()!==id)return;report(false);
      if(Notification.permission==='granted' && localStorage.getItem(disabledKey(account()))!=='yes')await enable(false);
    },
    enable,pause,
    async toggle() {
      if(subscription&&!stopped){localStorage.setItem(disabledKey(owner),'yes');await disable();}else await enable();
    },
    disable,
    async test() {
      if(!subscription)await enable();
      const result=await request(owner,'/me/web-push/test',{method:'POST',body:'{}'});
      if(!result?.sent)throw new Error('Проверочное уведомление не отправлено. Обновите подписку и повторите.');
    },
    subscribed:()=>!!subscription&&!stopped&&owner===account(),quietNow:()=>isQuietHours(quiet),
  };
}
