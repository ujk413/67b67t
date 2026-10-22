import type { Message } from "../types";
const image=(message:Message)=>String((message.meta?.media_encryption as {mime?:string}|undefined)?.mime || message.media_mime || '').startsWith('image/');
export function groupAlbums(messages: Message[]) {
  const albums = new Map<string, Message[]>();
  for (const message of messages) {
    if (
      typeof message.meta?.album_id === "string" &&
      image(message)
    ) {
      const key = `${message.room_id}:${message.sender_id}:${message.meta.album_id}`;
      const group = albums.get(key) || [];
      group.push(message);
      albums.set(key, group);
    }
  }
  const groupFor = (message: Message) =>
    image(message)
      ? albums.get(
          `${message.room_id}:${message.sender_id}:${message.meta?.album_id}`,
        )
      : undefined;
  return {
    display: messages.filter(
      (message) =>
        !groupFor(message) || groupFor(message)![0].id === message.id,
    ),
    groupFor,
  };
}
