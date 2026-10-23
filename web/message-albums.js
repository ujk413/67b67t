export function groupMessageAlbums(messages) {
 const groups=new Map(),key=message=>`${message.room_id}:${message.sender_id}:${message.meta?.album_id}`;
 const image=message=>String(message.meta?.media_encryption?.mime || message.media_mime || '').startsWith('image/');
 for(const message of messages)if(image(message) && typeof message.meta?.album_id==='string'){
  const value=key(message);if(!groups.has(value))groups.set(value,[]);groups.get(value).push(message);
 }
 return messages.flatMap(message=>{
  const album=image(message)?groups.get(key(message)):null;
  if(!album || album.length<2)return [message];
  return album[0].id===message.id?[{...message,_album:album}]:[];
 });
}
