import { useEffect, useRef, useState } from "react";
import { addNetworkStateListener } from "expo-network";
import { AppState } from "react-native";
import {
  AccountRepository,
  newId,
  type Draft,
  type Outgoing,
} from "../data/repository";
import { Outbox } from "../data/outbox";
import { mergeMessages } from "../data/messages";
import { api } from "../api";
import { mobileApi, type History } from "../mobileApi";
import { encryptRoomText } from "../roomCrypto";
import type { Message, Room, User } from "../types";

type Params = {
  me: User | null;
  token: string | null;
  baseUrl: string;
  activeRoomId: string | null;
  decode: (m: Message) => Promise<Message>;
  onMessages: React.Dispatch<React.SetStateAction<Message[]>>;
  onRooms: React.Dispatch<React.SetStateAction<Room[]>>;
  onError: (e: string) => void;
  onRevoke: () => void;
  encode?: (roomId:string,text:string) => Promise<Record<string,any>>;
  getSecret: (id: string) => Promise<string | null>;
};
export function useAccountData(params: Params) {
  const latest = useRef(params);
  const repository = useRef<AccountRepository | null>(null),
    outbox = useRef<Outbox | null>(null);
  latest.current = {...params,onMessages: update=>params.onMessages(previous=>{
    const messages=typeof update==='function'?update(previous):update;
    return messages.filter(message=>!repository.current?.isHidden(message.id));
  })};
  const generation = useRef(0),
    syncing = useRef(false);
  const [ready, setReady] = useState(false),
    [history, setHistory] = useState<History | null>(null);
  const [loading, setLoading] = useState(false),
    [loadingOlder, setLoadingOlder] = useState(false);
  const [reply, setReply] = useState<Message | null>(null);
  useEffect(() => {
    if (!params.me || !params.token) return;
    const repo = new AccountRepository(params.me.id);
    repository.current = repo;
    let active = true;
    let serverReady = false;
    const notify = (item: Outgoing, message?: Message) => {
      const p = latest.current;
      if (!active || p.token !== params.token || p.me?.id !== params.me?.id)
        return;
      if (p.activeRoomId === item.roomId)
        p.onMessages((prev) =>
          message
            ? mergeMessages(
                prev.filter((m) => m.client_message_id !== item.id),
                [message],
              )
            : mergeMessages(prev, [pendingMessage(item, p.me!.id)]),
        );
    };
    const queue = new Outbox(
      repo,
      async (item) => {
        const p = latest.current;
        if (
          !active ||
          !p.token ||
          p.token !== params.token ||
          p.me?.id !== params.me?.id
        )
          throw new Error("Сессия завершена");
        if (!serverReady) {
          try {
            const capabilities = await mobileApi(
              p.baseUrl,
              p.token,
            ).capabilities();
            if (!capabilities.idempotent_send || !capabilities.sync)
              throw Object.assign(
                new Error(
                  "Сначала обновите сервер Messenger67. Сообщение сохранено на устройстве.",
                ),
                { status: 409 },
              );
            serverReady = true;
          } catch (error) {
            if ((error as { status?: number }).status === 404)
              throw new Error(
                "Сначала обновите сервер Messenger67. Сообщение сохранено на устройстве; после обновления нажмите «Повторить».",
              );
            throw error;
          }
        }
        if (!item.body) {
          const encode = p.encode || (async (roomId:string,text:string) => { const secret=await p.getSecret(roomId); if(!secret)throw Error('Ключ переписки недоступен');return encryptRoomText(roomId,secret,text); });
          const encrypted = await encode(item.roomId,item.text);
          const preview = item.reply ? await encode(item.roomId,item.reply.text || 'Вложение') : null;
          item.body = {
            type: "text",
            ...encrypted,
            meta: {
              ...encrypted.meta,
              ...(preview ? { reply_preview: preview } : {}),
            },
            reply_to: item.reply
              ? {
                  id: item.reply.id,
                  sender_id: item.reply.sender_id,
                  sender_username: item.reply.sender_username,
                  text: "Ответ на сообщение",
                }
              : null,
          };
          await repo.putOutgoing(item);
        }
        if (item.body.type!=='text' && item.text && p.encode && !(item.body.meta as any)?.caption_message) {
          item.body={...item.body,text:null,meta:{...(item.body.meta as any),caption_message:await p.encode(item.roomId,item.text)}};
          await repo.putOutgoing(item);
        }
        if(!active || latest.current.token!==params.token)throw Error('Сеанс завершён');
        const sent = await api.sendMediaMessage(
          p.baseUrl,
          p.token,
          item.roomId,
          { ...item.body, client_message_id: item.id },
        );
        return { ...sent, text: item.text || sent.text, _decrypted: true };
      },
      notify,
    );
    outbox.current = queue;
    void (async () => {
      await repo.open();
      const cached = await repo.rooms();
      if (active && cached.length)
        latest.current.onRooms((prev) => (prev.length ? prev : cached));
      setReady(active);
      void sync();
      await queue.flush();
    })().catch((e) => {
      if (active) latest.current.onError(String(e.message || e));
    });
    const network = addNetworkStateListener((state) => {
      if (state.isConnected && active) {
        void sync();
        void queue.flush();
      }
    });
    const timer = setInterval(() => {
      if (active && AppState.currentState === "active") {
        void sync();
        void queue.flush();
      }
    }, 30000);
    return () => {
      active = false;
      generation.current++;
      network.remove();
      clearInterval(timer);
      queue.stop();
      setReady(false);
      repository.current = null;
      outbox.current = null;
      void repo.close();
    };
  }, [params.me?.id, params.token]);
  async function sync() {
    const repo = repository.current,
      p = latest.current;
    if (!repo || !p.token || syncing.current) return;
    syncing.current = true;
    try {
      let cursor = await repo.get("sync", "0"),
        more = true;
      while (more && repository.current === repo) {
        const result = await mobileApi(p.baseUrl, p.token).sync(cursor);
        if (repository.current !== repo) return;
        await repo.pruneRooms(result.room_ids);
        latest.current.onRooms((prev) =>
          prev.filter((room) => result.room_ids.includes(room.id)),
        );
        if (
          latest.current.activeRoomId &&
          !result.room_ids.includes(latest.current.activeRoomId)
        ) {
          latest.current.onMessages([]);
          latest.current.onRevoke();
        }
        let decodedCount = 0;
        for (const change of result.changes) {
          if (change.message) {
            const message = await latest.current.decode(change.message);
            if (repository.current !== repo) return;
            await repo.saveMessages([message]);
            if(!(await repo.visibleMessages([message])).length)continue;
            if (repository.current !== repo) return;
            if (latest.current.activeRoomId === message.room_id)
              latest.current.onMessages((prev) =>
                prev.some(m => m.id === message.id) || !prev.length || message.created_at >= (prev.filter(m => !m.delivery).at(-1)?.created_at || "") ? mergeMessages(prev, [message]) : prev,
              );
          } else {
            await repo.hideMessage(change.message_id);
            if (latest.current.activeRoomId === change.room_id)
              latest.current.onMessages((prev) =>
                prev.filter((m) => m.id !== change.message_id),
              );
          }
          if (++decodedCount % 20 === 0) await new Promise(resolve => setTimeout(resolve, 0));
        }
        cursor = result.cursor;
        await repo.set("sync", cursor);
        more = result.has_more;
      }
    } catch {
    } finally {
      syncing.current = false;
    }
  }
  async function open(room: Room) {
    const mine = ++generation.current,
      p = latest.current,
      repo = repository.current;
    setLoading(true);
    setHistory(null);
    p.onMessages([]);
    setReply(null);
    try {
      if (repo) {
        const cached = await repo.messages(room.id);
        const pending = (await repo.outgoing())
          .filter((x) => x.roomId === room.id)
          .map((x) => pendingMessage(x, p.me!.id));
        if (mine !== generation.current) return;
        p.onMessages(mergeMessages(cached, pending));
      }
      if (!p.token) return;
      let page = await mobileApi(p.baseUrl, p.token).history(room.id);
      const position = await repo?.get<string | null>(
        `position:${room.id}`,
        null,
      );
      const anchor = position || page.first_unread_id;
      if (anchor && !page.messages.some((m) => m.id === anchor))
        try {
          page = await mobileApi(p.baseUrl, p.token).history(room.id, {
            around: anchor,
          });
        } catch {}
      let decoded = await Promise.all(
        page.messages.map((m) => latest.current.decode(m)),
      );
      if (mine !== generation.current) return;
      await repo?.saveMessages(decoded);
      if(repo)decoded=await repo.visibleMessages(decoded);
      const outgoing = ((await repo?.outgoing()) || [])
        .filter((x) => x.roomId === room.id)
        .map((x) => pendingMessage(x, p.me!.id));
      if (room.kind === "group")
        try {
          await repo?.set(
            `e2e:${room.id}`,
            await api.roomE2e(p.baseUrl, p.token, room.id),
          );
        } catch {}
      if (mine !== generation.current) return;
      setHistory({ ...page, messages: decoded });
      p.onMessages(mergeMessages(decoded, outgoing));
    } catch (e) {
      if (mine === generation.current)
        p.onError(
          e instanceof Error ? e.message : "Не удалось открыть переписку",
        );
    } finally {
      if (mine === generation.current) setLoading(false);
    }
  }
  async function older(before: string) {
    const p = latest.current,
      repo = repository.current,
      id = p.activeRoomId;
    if (!id || !p.token || loadingOlder) return;
    setLoadingOlder(true);
    const mine = generation.current;
    try {
      const page = await mobileApi(p.baseUrl, p.token).history(id, { before });
      let decoded = await Promise.all(page.messages.map(p.decode));
      if (mine !== generation.current) return;
      await repo?.saveMessages(decoded);
      if(repo)decoded=await repo.visibleMessages(decoded);
      p.onMessages((prev) => mergeMessages(decoded, prev));
      setHistory((prev) =>
        prev ? { ...prev, has_older: page.has_older } : page,
      );
    } catch (e) {
      p.onError(String(e));
    } finally {
      setLoadingOlder(false);
    }
  }
  async function send(
    text: string,
    body?: Record<string, unknown>,
    targetRoomId?: string,
    replyOverride?: Message | null,
  ) {
    const p = latest.current,
      repo = repository.current;
    if (!repo || !p.me || !(targetRoomId || p.activeRoomId))
      throw new Error("Хранилище ещё загружается");
    const item: Outgoing = {
      createdAt: new Date().toISOString(),
      id: newId(),
      roomId: targetRoomId || p.activeRoomId!,
      text,
      reply: replyOverride === undefined ? reply : replyOverride,
      state: "queued",
      body,
    };
    await repo.putOutgoing(item);
    if (latest.current.activeRoomId === item.roomId) {
      p.onMessages((prev) =>
        mergeMessages(prev, [pendingMessage(item, p.me!.id)]),
      );
      setReply(null);
    }
    await repo.set(`draft:${item.roomId}`, { text: "" });
    void outbox.current?.flush();
  }
  async function retry(id: string) {
    await outbox.current?.retry(id);
  }
  async function cancel(id: string) {
    await outbox.current?.cancel(id);
    latest.current.onMessages((prev) =>
      prev.filter((m) => m.client_message_id !== id),
    );
  }
  async function jump(id: string) {
    const p = latest.current,
      roomId = p.activeRoomId;
    if (!roomId || !p.token) return;
    const page = await mobileApi(p.baseUrl, p.token).history(roomId, {
      around: id,
    });
    let decoded = await Promise.all(page.messages.map(p.decode));
    if (latest.current.activeRoomId !== roomId) return;
    await repository.current?.saveMessages(decoded);
    if(repository.current)decoded=await repository.current.visibleMessages(decoded);
    setHistory({ ...page, messages: decoded });
    p.onMessages(decoded);
    return id;
  }
  async function newer(after: string) {
    const p = latest.current,
      id = p.activeRoomId;
    if (!id || !p.token) return;
    const mine = generation.current;
    const page = await mobileApi(p.baseUrl, p.token).history(id, { after });
    let decoded = await Promise.all(page.messages.map(p.decode));
    if (mine !== generation.current) return;
    await repository.current?.saveMessages(decoded);
    if(repository.current)decoded=await repository.current.visibleMessages(decoded);
    p.onMessages((prev) => mergeMessages(prev, decoded));
    setHistory((prev) =>
      prev ? { ...prev, has_newer: page.has_newer } : page,
    );
  }
  return {
    repository,
    outbox,
    ready,
    history,
    loading,
    loadingOlder,
    reply,
    setReply,
    open,
    older,
    newer,
    send,
    retry,
    cancel,
    jump,
    sync,
  };
}
function pendingMessage(item: Outgoing, userId: string): Message {
  return {
    id: `local-${item.id}`,
    client_message_id: item.id,
    room_id: item.roomId,
    sender_id: userId,
    type: (item.body?.type || "text") as Message["type"],
    text: item.text,
    created_at: item.createdAt || new Date().toISOString(),
    reply_to: item.reply,
    delivery: item.state,
    delivery_error: item.error,
  };
}
