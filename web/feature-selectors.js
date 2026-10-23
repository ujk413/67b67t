import {t,locale} from './i18n.js';

// A selection is valid only in the search/device response that supplied it.
export class SecretPeerSelector {
 constructor(request,current){this.request=request;this.current=current;this.generation=0;this.reset();}
 reset(){this.generation++;this.users=[];this.devices=[];this.peer=null;this.device=null;}
 guard(owner=this.generation){if(!this.current()||owner!==this.generation)throw Error('Сеанс изменился');}
 async search(query){this.reset();this.guard();const q=String(query).trim().replace(/^@/,'');if(q.length<2)throw Error('Введите не менее двух символов имени');const owner=this.generation,rows=await this.request('/users/search?q='+encodeURIComponent(q));this.guard(owner);this.users=rows;return rows;}
 async selectUser(id){this.guard();const peer=this.users.find(u=>u.id===id);if(!peer)throw Error('Выберите собеседника');const owner=++this.generation;this.peer=peer;this.device=null;this.devices=[];let rows;try{rows=await this.request('/e2e/devices/'+encodeURIComponent(id));}catch(error){this.guard(owner);if(error?.status===404||/keys_not_found/.test(error?.message||''))return [];throw error;}this.guard(owner);this.devices=rows.filter(d=>typeof d.device_id==='string'||typeof d.device_id==='number');return this.devices;}
 selectDevice(id){this.guard();const device=this.devices.find(d=>String(d.device_id)===String(id));if(!device)throw Error('Выберите устройство собеседника');this.device=device;}
 payload(deviceId,ttl){this.guard();if(!this.peer)throw Error('Выберите собеседника');if(!this.device||!this.devices.includes(this.device))throw Error('Выберите устройство собеседника');return {peer_id:this.peer.id,peer_device_id:String(this.device.device_id),device_id:String(deviceId),ttl:Number(ttl||3600)};}
}
export function peerLabel(user){const name=user?.display_name,username=user?.username;return name?(username?`${name} · @${username}`:name):(username||t('Собеседник'));}
export function deviceLabel(device,index=0){return [device?.label||device?.device_name||t('Устройство {0}',index+1),device?.platform,device?.device_id?('…'+String(device.device_id).slice(-4)):null].filter(Boolean).join(' · ');}
export async function secretChatLabels(chats,me,request,current){const profiles=new Map();await Promise.all([...new Set(chats.map(c=>c.initiator===me?c.recipient:c.initiator))].map(async id=>{try{const profile=await request('/users/'+encodeURIComponent(id));if(current())profiles.set(id,profile);}catch{/* Deleted/inaccessible peer retains a neutral label. */}}));if(!current())throw Error('Сеанс изменился');return chats.map(c=>{const own=c.initiator===me,peer=own?c.recipient:c.initiator,device=own?c.recipient_device:c.initiator_device;return {...c,_label:[peerLabel(profiles.get(peer)),deviceLabel({device_id:device,label:c.peer_device_label,platform:c.peer_device_platform}),c.created_at?new Date(c.created_at).toLocaleDateString(locale()):null].filter(Boolean).join(' · ')};});}
export function selectedFeatureMessage(context,mode){const m=context.targetMessage;if(!m?.id||m.room_id!==context.room?.id||m.delivery)return null;if(mode==='comments'&&(context.room.kind!=='channel'||m.is_post!==true))return null;return m;}
