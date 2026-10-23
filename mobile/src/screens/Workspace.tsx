import {locale as uiLocale} from "../../../web/i18n.js";
import {t as uiText} from "../../../web/i18n.js";
import { FolderManager } from "../components/FolderManager";
import { mediaUsage, clearMediaCache, trimMediaCache } from "../mediaFiles";
import { storage } from "../storage";
import * as ImagePicker from "expo-image-picker";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import { useEffect, useRef, useState } from "react";
import {
  ActivityIndicator,
  Alert,
  FlatList,
  Image,
  Linking,
  Modal,
  Pressable,
  ScrollView,
  Share,
  Switch,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import * as Clipboard from "expo-clipboard";
import { useTheme, type ThemeMode } from "../ThemeProvider";
import { mobileApi, type RoomPreference } from "../mobileApi";
import type { AccountRepository } from "../data/repository";
import { newId } from "../data/repository";
import { api as legacyApi, request } from "../api";
import { encryptRoomText } from "../roomCrypto";
import { roomTitle } from "../format";
import type { Message, Room, User } from "../types";
import { pushNotifications } from "../notifications";
import { MediaTray } from "../components/MediaTray";
import { ConnectionDiagnostics } from '../components/ConnectionDiagnostics';
import {ConversationFeatures} from '../features/ConversationFeatures';
import {AccountFeatures} from '../features/AccountFeatures';
import {ExpressiveFeatures} from '../features/ExpressiveFeatures';
import {QualityFeatures} from '../features/QualityFeatures';
import {CallFeatures} from '../features/CallFeatures';
import type {FeatureContext} from '../features/types';
import {messageLink} from '../../../web/deep-links.js';

import type { ProtectedUploader } from "../protectedUpload";
type Props = {
  featureContext:FeatureContext;
  uploader: ProtectedUploader;
  lockEnabled: boolean;
  onChangeLock: () => Promise<void>;
  onCheckUpdates: () => Promise<void>;
  updateLabel: string;
  embedded?: boolean;
  onMode: (mode: string) => void;
  mode: string | null;
  onClose: () => void;
  baseUrl: string;
  token: string;
  user: User;
  rooms: Room[];
  room: Room | null;
  repository: AccountRepository | null;
  decode?: (message:Message)=>Promise<Message>;
  onOpenMedia: (message: Message) => void;
  onOpen: (room: Room, messageId?: string) => Promise<void>;
  onOpenSearch: (room: Room, ids: string[], index: number) => Promise<void>;
  onClearData: () => Promise<void>;
  onProfile: () => void;
  onRefresh: () => Promise<void>;
  onError: (error: string) => void;
  onLogout: () => void;
  onPickImage: () => void;
  onPickVideo: () => void;
  onPickFile: () => void;
  onPickVideoNote: () => void;
  forwarding: Message[];
  forwardOperation:string;
  onForwarded: () => void;
  enqueueForward: (room: Room, message: Message,id?:string) => Promise<void>;
  onSendMedia: (
    text: string,
    body: Record<string, unknown>,
    roomId: string,
  ) => Promise<void>;
};
export function Row({
  label,
  onPress,
  danger = false,
  detail,
}: {
  label: string;
  onPress?: () => void;
  danger?: boolean;
  detail?: string;
}) {
  const { colors } = useTheme();
  return (
    <Pressable
      onPress={onPress}
      accessibilityRole={onPress ? "button" : undefined}
      style={{
        minHeight: 54,
        padding: 16,
        borderBottomWidth: 0.5,
        borderColor: colors.divider,
        justifyContent: "center",
      }}
    >
      <Text
        style={{
          color: danger ? colors.misc.error : colors.text.primary,
          fontSize: 16,
        }}
      >
        {label}
      </Text>
      {detail && (
        <Text style={{ color: colors.text.secondary, marginTop: 4 }}>
          {detail}
        </Text>
      )}
    </Pressable>
  );
}
export function Workspace(p: Props) {
  const insets = useSafeAreaInsets();
  const theme = useTheme(),
    { colors } = theme;
  const api = mobileApi(p.baseUrl, p.token);
  const [query, setQuery] = useState(""),
    [title, setTitle] = useState(""),
    [users, setUsers] = useState<User[]>([]),
    [results, setResults] = useState<Message[]>([]);
  const [selected, setSelected] = useState<string[]>([]),
    [busy, setBusy] = useState(false),
    [error, setError] = useState("");
  const [members, setMembers] = useState<any[]>([]),
    [media, setMedia] = useState<Message[]>([]),
    [sessions, setSessions] = useState<any[]>([]),
    [preferences, setPreferences] = useState<RoomPreference[]>([]);
  const [mediaKind, setMediaKind] = useState("all"), [moreMedia, setMoreMedia] = useState(false);
  const [cacheBytes, setCacheBytes] = useState(0);
  const [cacheLimit, setCacheLimit] = useState(256);
  const [pushStatus, setPushStatus] = useState(pushNotifications.status);
  useEffect(() => pushNotifications.subscribe(setPushStatus), []);
  const [invite, setInvite] = useState(""),
    [searchProgress, setSearchProgress] = useState(""),
    [autoDownload, setAutoDownload] = useState(false);
  const [forwardComment,setForwardComment]=useState(''),[forwardResults,setForwardResults]=useState<Record<string,string>>({});
  const completedForwardRooms=useRef(new Set<string>());
  useEffect(()=>{completedForwardRooms.current.clear();setForwardResults({});},[p.forwardOperation]);
  const searchGeneration = useRef(0),
    cancelSearch = useRef(false);
  const run = async (fn: () => Promise<unknown>) => {
    setBusy(true);
    setError("");
    try {
      await fn();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };
  useEffect(() => {
    setQuery("");
    setResults([]);
    setUsers([]);
    setSelected([]);
    setError("");
    setTitle(p.room?.title || "");
    setInvite("");
    cancelSearch.current = true;
    return () => {
      cancelSearch.current = true;
    };
  }, [p.mode]);
  useEffect(() => {
    if (!p.mode) return;
    let active = true;
    const timer = setTimeout(() => {
      void (async () => {
        if (
          ["new", "group", "search", "search-room"].includes(p.mode!) &&
          query.trim().length >= 2
        ) {
          const mine = ++searchGeneration.current;
          try {
            const found =
              p.mode === "search-room" ? [] : await api.users(query);
            if (active && mine === searchGeneration.current) setUsers(found);
          } catch {}
          if (p.repository) {
            const found = await p.repository.search(
              query,
              p.mode === "search-room" ? p.room?.id : undefined,
            );
            if (active && mine === searchGeneration.current) setResults(found);
          }
        } else {
          setUsers([]);
          setResults([]);
        }
      })().catch((e) => {
        if (active) setError(String(e));
      });
    }, 300);
    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, [query, p.mode]);
  useEffect(() => {
    if (!p.mode) return;
    void run(async () => {
      if (p.mode === "settings") {
        const response = await api.sessions();
        setSessions(Array.isArray(response) ? response : response.sessions);
        setAutoDownload(theme.autoDownload);
        setCacheBytes(p.repository ? (await mediaUsage(p.repository)).total : 0);
        setCacheLimit(Number(await storage.get("media.cacheLimitMB")) || 256);
      }
      if (p.mode === "details" && p.room) {
        setMembers(await legacyApi.roomMembers(p.baseUrl, p.token, p.room.id));
        const items = await api.media(p.room.id, { kind: mediaKind });
        setMedia(items); setMoreMedia(items.length === 100);
      }
      if (p.mode?.startsWith("room:") || p.mode === "archive" || p.mode === 'hidden')
        setPreferences(await api.preferences());
    });
  }, [p.mode, p.room?.id, mediaKind]);
  if (!p.mode) return null;
  if (p.mode === "folders") return <FolderManager baseUrl={p.baseUrl} token={p.token} rooms={p.rooms} onChanged={p.onRefresh} onClose={p.onClose} />;
  const input = (
    value: string,
    change: (v: string) => void,
    placeholder: string,
  ) => (
    <TextInput
      value={value}
      editable={!busy}
      onChangeText={change}
      placeholder={placeholder}
      placeholderTextColor={colors.text.secondary}
      style={{
        margin: 12,
        padding: 14,
        borderRadius: 14,
        backgroundColor: colors.windowBackgroundGray,
        color: colors.text.primary,
        fontSize: 16,
      }}
    />
  );
  const openUser = async (user: User) => {
    const { id } = await api.createRoom("dm", [user.username]);
    await p.onOpen(await api.room(id));
  };
  const forward = async () => {
    for(const room of p.rooms.filter(room=>selected.includes(room.id))){
      const completionId=p.forwardOperation+':'+room.id;
      if(completedForwardRooms.current.has(completionId))continue;
      try{
        if(forwardComment.trim()){const body=await p.featureContext.buildText?.(room.id,forwardComment);if(!body)throw Error("Шифрование не готово");await p.featureContext.onSend(room.id,forwardComment,{...body,__queue_id:p.forwardOperation+'_'+room.id+'_comment'});}
        for(const message of p.forwarding)await p.enqueueForward(room,message,p.forwardOperation+'_'+room.id.replace(/-/g,'')+'_'+message.id.replace(/-/g,''));
        completedForwardRooms.current.add(completionId);
        setForwardResults(old=>({...old,[room.id]:uiText("В очереди отправки")}));
      }catch(e){setForwardResults(old=>({...old,[room.id]:(e as Error).message}));}
    }
  };
  const scan = async () => {
    cancelSearch.current = false;
    setBusy(true);
    try {
      let count = 0;
      const targets = p.mode === "search-room" && p.room ? [p.room] : p.rooms;
      for (const room of targets) {
        const checkpoint = await p.repository?.get<{
          before: string;
          complete: boolean;
        }>(`scan:${room.id}`, { before: "", complete: false });
        if (checkpoint?.complete) continue;
        let before = checkpoint?.before || "";
        while (!cancelSearch.current) {
          const page = await api.history(
            room.id,
            before ? { before, limit: 100 } : { limit: 100 },
          );
          if (!page.messages.length) break;
          const { decryptRoomText } = await import("../roomCrypto");
          let secret: string | null = null;
          const decoded = [];
          for (const message of page.messages) {
            if (p.decode) { const value=await p.decode(message);if(value._decrypted || !value.encrypted_content)decoded.push(value);continue; }
            if (
              message.meta?.encrypted &&
              message.meta.e2ee_v === 2 &&
              typeof message.meta.iv === "string" &&
              message.text
            ) {
              secret ||= (await legacyApi.roomKey(p.baseUrl, p.token, room.id))
                .key;
              try {
                decoded.push({
                  ...message,
                  text: await decryptRoomText(
                    room.id,
                    secret,
                    message.text,
                    message.meta.iv,
                  ),
                  _decrypted: true,
                });
              } catch {}
            } else if (!message.encrypted_content) decoded.push(message);
          }
          await p.repository?.saveMessages(decoded);
          count += decoded.length;
          setSearchProgress(uiText("Проверено сообщений: {0}",count));
          setResults(
            (await p.repository?.search(
              query,
              p.mode === "search-room" ? room.id : undefined,
            )) || [],
          );
          before = page.messages[0].id;
          await p.repository?.set(`scan:${room.id}`, {
            before,
            complete: !page.has_older,
          });
          if (!page.has_older) break;
        }
      }
      setSearchProgress(
        cancelSearch.current
          ? uiText("Поиск остановлен")
          : uiText("Поиск доступной истории завершён"),
      );
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };
  const canManage =
    !!p.room &&
    (p.room.created_by === p.user.id ||
      ["owner", "admin"].includes(
        members.find((m) => m.id === p.user.id)?.role,
      ));
  const roomPreference = p.mode.startsWith("room:")
    ? preferences.find((x) => x.room_id === p.mode!.slice(5))
    : undefined;
  const prefRoom = p.rooms.find((x) => x.id === p.mode?.slice(5));
  const changePreference = async (body: Partial<RoomPreference>) => {
    if (!prefRoom) return;
    await api.preference(prefRoom.id, body);
    await p.onRefresh();
    p.onClose();
  };
  const heading =
    (
      {
        join: uiText("Вступить в группу"),
        new: uiText("Новая переписка"),
        group: uiText("Новая группа"),
        search: uiText("Поиск"),
        "search-room": uiText("Поиск в переписке"),
        details: uiText("Информация"),
        settings: uiText("Настройки"),
        attachments: uiText("Вложение"),
        circle: uiText("Видеокружок"),
        forward: uiText("Переслать"),
        archive: uiText("Архив"),
      } as Record<string, string>
    )[p.mode] || uiText("Действия с чатом");
  const feature = p.mode.startsWith('conversation:')
    ? <ConversationFeatures mode={p.mode.slice(13)} context={p.featureContext}/>
    : p.mode.startsWith('account-')
    ? <AccountFeatures mode={p.mode} context={p.featureContext}/>
    : p.mode.startsWith('expressive:')
    ? <ExpressiveFeatures mode={p.mode.slice(11)} context={p.featureContext}/>
    : p.mode==='quality'
    ? <QualityFeatures context={p.featureContext}/>
    : null;
  const content = feature || (p.mode.startsWith('calls:')?<CallFeatures mode={p.mode.slice(6)} context={p.featureContext}/>:null) || (
    <View
      style={{
        flex: 1,
        backgroundColor: colors.windowBackground,
        paddingTop: p.embedded ? 0 : insets.top,
        paddingBottom: p.embedded ? 0 : insets.bottom,
      }}
    >
      <View style={{ flexDirection: "row", alignItems: "center" }}>
        <Row label={uiText("Назад")} onPress={p.onClose} />
        <Text
          accessibilityRole="header"
          style={{
            fontSize: 20,
            fontWeight: "700",
            color: colors.text.primary,
          }}
        >
          {heading}
        </Text>
      </View>
      {!!error && (
        <Text
          accessibilityLiveRegion="polite"
          style={{ padding: 12, color: colors.misc.error }}
        >
          {error}
        </Text>
      )}
      {busy && <ActivityIndicator color={colors.text.blue} />}
      {p.mode!=="attachments" && p.mode!=="circle" && <ScrollView keyboardShouldPersistTaps="handled">
        {p.mode === "join" && (
          <>
            {input(query, setQuery, uiText("Токен приглашения"))}
            <Row
              label={uiText("Вступить")}
              onPress={() =>
                void run(async () => {
                  const { id } = await api.joinInvite(query.trim());
                  await p.onOpen(await api.room(id));
                })
              }
            />
          </>
        )}
        {["new", "group", "search", "search-room"].includes(p.mode) && (
          <>
            {p.mode === "new" && (
              <>
                <Row label={uiText("Создать группу")} onPress={() => p.onMode("group")} />
                <Row label={uiText("Создать канал")} onPress={() => p.onMode('conversation:channels')}/>
                <Row label={uiText("Публичные сообщества")} onPress={() => p.onMode('conversation:communities')}/>
                <Row
                  label={uiText("Вступить по приглашению")}
                  onPress={() => p.onMode("join")}
                />
              </>
            )}
            {p.mode === "group" && input(title, setTitle, uiText("Название группы"))}
            {input(query, setQuery, uiText("Имя пользователя или текст сообщения"))}
            {query.length > 0 && query.length < 2 && (
              <Text style={{ padding: 12, color: colors.text.secondary }}>
                {uiText("Введите хотя бы два символа")}</Text>
            )}
            {!!users.length && (
              <Text style={{ padding: 12, color: colors.text.secondary }}>
                {uiText("Люди")}</Text>
            )}
            {users.map((user) => (
              <Row
                key={user.id}
                label={`${selected.includes(user.username) ? "✓ " : ""}${user.display_name || user.username}`}
                detail={`@${user.username}`}
                onPress={() => {
                  if (p.mode === "group")
                    setSelected((prev) =>
                      prev.includes(user.username)
                        ? prev.filter((x) => x !== user.username)
                        : [...prev, user.username],
                    );
                  else void run(() => openUser(user));
                }}
              />
            ))}
            {p.mode === "group" && (
              <Row
                label={uiText("Создать группу · {0} участников",selected.length)}
                onPress={() =>
                  void run(async () => {
                    if (!title.trim() || !selected.length)
                      throw new Error("Укажите название и участников");
                    const { id } = await api.createRoom(
                      "group",
                      selected,
                      title.trim(),
                    );
                    await p.onOpen(await api.room(id));
                  })
                }
              />
            )}
            {(p.mode === "search" || p.mode === "search-room") && (
              <>
                <Text style={{ padding: 12, color: colors.text.secondary }}>
                  {uiText("Сообщения из загруженной истории. Для полной проверки\r\n                  запустите поиск дальше.")}</Text>
                {p.mode === "search" && (
                  <Text style={{ padding: 12, color: colors.text.secondary }}>
                    {uiText("Чаты")}</Text>
                )}
                {p.rooms
                  .filter(
                    (r) =>
                      p.mode === "search" &&
                      roomTitle(r, p.user.id)
                        .toLowerCase()
                        .includes(query.toLowerCase()),
                  )
                  .map((room) => (
                    <Row
                      key={room.id}
                      label={roomTitle(room, p.user.id)}
                      onPress={() => void p.onOpen(room)}
                    />
                  ))}
                <Text style={{ padding: 12, color: colors.text.secondary }}>
                  {uiText("Сообщения ·")}{results.length}
                  {results.length === 100 ? "+" : ""}
                </Text>
                {query.length >= 2 && !results.length && (
                  <Text style={{ padding: 12, color: colors.text.secondary }}>
                    {uiText("В загруженной истории совпадений нет")}</Text>
                )}
                {results.map((message, index) => (
                  <Row
                    key={message.id}
                    label={message.text || uiText("Вложение")}
                    detail={roomTitle(
                      p.rooms.find((r) => r.id === message.room_id) || {
                        id: message.room_id,
                        kind: "group",
                      },
                      p.user.id,
                    )}
                    onPress={() =>
                      void run(async () =>
                        p.mode === "search-room"
                          ? p.onOpenSearch(
                              p.room!,
                              results.map((m) => m.id),
                              index,
                            )
                          : p.onOpen(
                              p.rooms.find((r) => r.id === message.room_id) ||
                                (await api.room(message.room_id)),
                              message.id,
                            ),
                      )
                    }
                  />
                ))}
                {!!query && (
                  <Row
                    label={
                      busy ? uiText("Остановить поиск") : uiText("Искать дальше в истории")
                    }
                    onPress={() => {
                      if (busy) cancelSearch.current = true;
                      else void scan();
                    }}
                  />
                )}
                <Text style={{ padding: 12, color: colors.text.secondary }}>
                  {searchProgress}
                </Text>
              </>
            )}
          </>
        )}
        {p.mode === "forward" && <>
          {input(forwardComment,setForwardComment,uiText("Комментарий (необязательно)"))}
          {p.rooms.map((room) => (
            <Row
              key={room.id}
              label={(selected.includes(room.id)?'✓ ':'')+roomTitle(room, p.user.id)}
              detail={forwardResults[room.id]}
              onPress={() => setSelected(old=>old.includes(room.id)?old.filter(id=>id!==room.id):[...old,room.id])}
            />
          ))}
          <Row label={uiText("Переслать · {0} чатов",selected.length)} onPress={()=>void run(forward)}/>
          <Row label={uiText("Готово")} onPress={()=>{p.onForwarded();p.onClose();}}/>
        </>}
        {p.mode.startsWith("room:") && (
          <>
            <Row
              label={roomPreference?.pinned ? uiText("Открепить") : uiText("Закрепить")}
              onPress={() =>
                void run(() =>
                  changePreference({ pinned: !roomPreference?.pinned }),
                )
              }
            />
            <Row
              label={
                roomPreference?.muted
                  ? uiText("Включить уведомления")
                  : uiText("Отключить уведомления")
              }
              onPress={() =>
                void run(() =>
                  changePreference({ muted: !roomPreference?.muted }),
                )
              }
            />
            <Row
              label={roomPreference?.archived ? uiText("Вернуть из архива") : uiText("В архив")}
              onPress={() =>
                void run(() =>
                  changePreference({ archived: !roomPreference?.archived }),
                )
              }
            />
            <Row
              label={uiText("Отметить непрочитанным")}
              onPress={() =>
                void run(() => changePreference({ marked_unread: true }))
              }
            />
            <Row
              label={uiText("Удалить из моего списка")}
              danger
              onPress={() =>
                Alert.alert(
                  uiText("Удалить чат из списка?"),
                  uiText("Переписка у собеседника сохранится"),
                  [
                    { text: uiText("Отмена") },
                    {
                      text: uiText("Удалить"),
                      style: "destructive",
                      onPress: () =>
                        void run(() => changePreference({ hidden: true })),
                    },
                  ],
                )
              }
            />
          </>
        )}
        {p.mode === "archive" &&
          p.rooms
            .filter(
              (r) => preferences.find((x) => x.room_id === r.id)?.archived,
            )
            .map((room) => (
              <Row
                key={room.id}
                label={roomTitle(room, p.user.id)}
                onPress={() => void p.onOpen(room)}
              />
            ))}
        {p.mode==='hidden' && p.rooms.filter(room=>preferences.some(pref=>pref.room_id===room.id&&pref.hidden)).map(room=><Row key={room.id} label={roomTitle(room,p.user.id)} detail={uiText("Нажмите, чтобы восстановить в списке чатов")} onPress={()=>void run(async()=>{await api.preference(room.id,{hidden:false});setPreferences(prev=>prev.map(pref=>pref.room_id===room.id?{...pref,hidden:false}:pref));await p.onRefresh();})}/>)}
        {p.mode === "details" && p.room && (
          <>
            <Row label={uiText("Закрепления, дата и отложенные сообщения")} onPress={()=>p.onMode('conversation:conversation')}/>
            <Row label={p.room.kind==='group'?uiText("Групповой звонок и история"):uiText("История звонков")} onPress={()=>p.onMode('calls:history')}/>
            <Row label={uiText("Экспорт переписки")} onPress={()=>p.onMode('expressive:export')}/>
            {p.room.kind!=='dm' && <><Row label={uiText("Управление участниками")} onPress={()=>p.onMode('conversation:members')}/><Row label={uiText("Шифрование и настройки сообщества")} onPress={()=>p.onMode('conversation:settings')}/></>}
            <Row
              label={roomTitle(p.room, p.user.id)}
              detail={
                p.room.kind === "dm"
                  ? `@${p.room.other_username || ""}`
                  : uiText("Участников: {0}",members.length)
              }
            />
            {p.room.kind === "dm" && p.room.other_user_id && (
              <Row
                label={uiText("Заблокировать собеседника")}
                danger
                onPress={() =>
                  Alert.alert(
                    uiText("Заблокировать?"),
                    uiText("Личные сообщения будут недоступны"),
                    [
                      { text: uiText("Отмена") },
                      {
                        text: uiText("Заблокировать"),
                        onPress: () =>
                          void run(() =>
                            api.block(p.room!.other_user_id!, true),
                          ),
                      },
                    ],
                  )
                }
              />
            )}
            {p.room.kind === "dm" && p.room.other_user_id && (
              <Row
                label={uiText("Разблокировать собеседника")}
                onPress={() =>
                  void run(() => api.block(p.room!.other_user_id!, false))
                }
              />
            )}
            {members.map((member) => (
              <Row
                key={member.id}
                label={member.display_name || member.username}
                detail={
                  (
                    {
                      owner: uiText("Владелец"),
                      admin: uiText("Администратор"),
                      member: uiText("Участник"),
                    } as Record<string, string>
                  )[member.role] || uiText("Участник")
                }
                onPress={() => {
                  if (!canManage || member.id === p.room!.created_by) return;
                  Alert.alert(member.username, uiText("Управление участником"), [
                    { text: uiText("Отмена") },
                    {
                      text:
                        member.role === "admin"
                          ? uiText("Снять права администратора")
                          : uiText("Назначить администратором"),
                      onPress: () =>
                        void run(async () => {
                          await api.role(
                            p.room!.id,
                            member.id,
                            member.role === "admin" ? "member" : "admin",
                          );
                          setMembers(
                            await legacyApi.roomMembers(
                              p.baseUrl,
                              p.token,
                              p.room!.id,
                            ),
                          );
                        }),
                    },
                    {
                      text: uiText("Удалить"),
                      style: "destructive",
                      onPress: () =>
                        void run(async () => {
                          await api.kick(p.room!.id, member.id);
                          setMembers((prev) =>
                            prev.filter((x) => x.id !== member.id),
                          );
                        }),
                    },
                  ]);
                }}
              />
            ))}
            {p.room.kind === "group" && canManage && (
              <>
                <Row
                  label={uiText("Изменить аватар группы")}
                  onPress={() =>
                    void run(async () => {
                      if (
                        !(
                          await ImagePicker.requestMediaLibraryPermissionsAsync()
                        ).granted
                      )
                        throw new Error("Разрешите доступ к фото в настройках");
                      const result = await ImagePicker.launchImageLibraryAsync({
                        mediaTypes: ["images"],
                        allowsEditing: true,
                        quality: 0.9,
                      });
                      if (result.canceled) return;
                      const form = new FormData();
                      form.append("file", {
                        uri: result.assets[0].uri,
                        name: "avatar.jpg",
                        type: result.assets[0].mimeType || "image/jpeg",
                      } as never);
                      const response = await fetch(
                        `${p.baseUrl}/rooms/${p.room!.id}/avatar`,
                        {
                          method: "POST",
                          headers: { Authorization: `Bearer ${p.token}` },
                          body: form,
                        },
                      );
                      if (!response.ok)
                        throw new Error("Не удалось обновить аватар");
                      await p.onRefresh();
                    })
                  }
                />
                {input(title, setTitle, uiText("Название группы"))}
                <Row
                  label={uiText("Сохранить название")}
                  onPress={() =>
                    void run(async () => {
                      await api.updateRoom(p.room!.id, title);
                      await p.onRefresh();
                    })
                  }
                />
                {input(query, setQuery, uiText("Username нового участника"))}
                <Row
                  label={uiText("Добавить участника")}
                  onPress={() =>
                    void run(async () => {
                      await api.member(p.room!.id, query);
                      setMembers(
                        await legacyApi.roomMembers(
                          p.baseUrl,
                          p.token,
                          p.room!.id,
                        ),
                      );
                    })
                  }
                />
                <Row
                  label={uiText("Создать приглашение")}
                  onPress={() =>
                    void run(async () => {
                      const result = await api.invite(p.room!.id);
                      setInvite(result.token);
                      await Share.share({
                        message: uiText("Приглашение в Messenger67: {0}",result.token),
                      });
                    })
                  }
                />
                {!!invite && (
                  <Row
                    label={uiText("Отозвать приглашение")}
                    onPress={() =>
                      void run(async () => {
                        await api.revokeInvite(p.room!.id, invite);
                        setInvite("");
                      })
                    }
                  />
                )}
              </>
            )}
            <Text style={{ padding: 16, color: colors.text.secondary }}>
              {uiText("Общие медиа и файлы")}</Text>
            <ScrollView horizontal contentContainerStyle={{ paddingHorizontal: 12 }}>
              {Object.entries({ all: uiText("Все"), image: uiText("Фото"), video: uiText("Видео"), file: uiText("Файлы") }).map(([key, title]) => <Pressable key={key} onPress={() => setMediaKind(key)} accessibilityRole="button" accessibilityState={{ selected: mediaKind === key }} style={{ padding: 12, minHeight: 48 }}><Text style={{ color: mediaKind === key ? colors.text.blue : colors.text.secondary }}>{title}</Text></Pressable>)}
              <Pressable onPress={() => p.onMode("search-room")} style={{ padding: 12, minHeight: 48 }}><Text style={{ color: colors.text.blue }}>{uiText("Ссылки · поиск")}</Text></Pressable>
            </ScrollView>
            {media.map((message) => (
              <Row
                key={message.id}
                label={message.text || message.media_mime || uiText("Вложение")}
                detail={new Date(message.created_at).toLocaleDateString(
                  uiLocale(),
                )}
                onPress={() => p.onOpenMedia(message)}
              />
            ))}
            {moreMedia && <Row label={busy ? uiText("Загрузка…") : uiText("Загрузить ещё вложения")} onPress={() => { if (busy || !p.room) return; void run(async () => { const next = await api.media(p.room!.id, { kind: mediaKind, before: media.at(-1)!.id }); setMedia(prev => [...prev, ...next]); setMoreMedia(next.length === 100); }); }} />}
          </>
        )}
        {p.mode === 'diagnostics' && <ConnectionDiagnostics baseUrl={p.baseUrl} token={p.token}/>}
        {p.mode === "settings" && (
          <>
            <Row label={uiText("Профиль")} onPress={p.onProfile} />
            <Row label={uiText("История звонков")} onPress={()=>p.onMode('calls:history')}/>
            {Object.entries({contacts:uiText("Контакты"),privacy:uiText("Приватность"),blocked:uiText("Заблокированные"),backup:uiText("Защищённая копия ключей"),security:uiText("Защита и история входов"),aliases:uiText("Публичные имена"),accounts:uiText("Аккаунты"),qr:uiText("Вход по QR"),transport:uiText("Прокси и Tor"),secret:uiText("Секретные чаты"),delete:uiText("Удаление аккаунта")}).map(([mode,label])=><Row key={mode} label={label} onPress={()=>p.onMode('account-'+mode)}/>)}
            <Row label={uiText("Прочитать все чаты")} onPress={()=>void run(async()=>{await request(p.baseUrl,'/me/read-all',{token:p.token,method:'POST',body:{}});await p.onRefresh();})}/>
            <Row label={uiText("Скрытые чаты")} onPress={()=>p.onMode('hidden')}/>
            <Row label={uiText("Проверить соединение и звонки")} onPress={()=>p.onMode('diagnostics')}/>
            <Row label={uiText("Качество и оформление · Quality")} onPress={()=>p.onMode('quality')}/>
            <Row label={uiText("Проверить обновления")} detail={p.updateLabel} onPress={() => void run(p.onCheckUpdates)} />
            <Row label={uiText("Блокировка приложения")} detail={p.lockEnabled ? uiText("Включена · отпечаток, лицо или код устройства") : uiText("Выключена")} onPress={() => void run(p.onChangeLock)} />
            <Row label={uiText("Папки чатов")} onPress={() => p.onMode("folders")} />
            <Row label={uiText("Избранное")} onPress={() => void run(async () => { const room = await api.saved(); await p.onRefresh(); await p.onOpen(await api.room(room.id)); })} />
            <Row
              label={uiText("Уведомления")}
              detail={uiText(pushStatus.message)}
              onPress={() => void Linking.openSettings()}
            />
            {pushNotifications.configured && (
              <>
                {pushStatus.status !== "enabled" && (
                  <Row label={uiText("Подключить уведомления")} onPress={() => void run(() => pushNotifications.retry())} />
                )}
                <Row label={uiText("Проверить уведомления")} onPress={() => void run(async () => {
                  await pushNotifications.test(p.baseUrl, p.token);
                  Alert.alert("67chat", uiText("Проверочное уведомление отправлено"));
                })} />
              </>
            )}
            {(["light", "dark", "system"] as ThemeMode[]).map((mode) => (
              <Row
                key={mode}
                label={`${theme.mode === mode ? "✓ " : ""}${{ light: uiText("Светлая тема"), dark: uiText("Тёмная тема"), system: uiText("Как в системе") }[mode]}`}
                onPress={() => theme.setMode(mode)}
              />
            ))}
            <Row
              label={uiText("Размер текста: {0}",theme.fontSize)}
              onPress={() =>
                theme.setFontSize(
                  theme.fontSize >= 24 ? 14 : theme.fontSize + 2,
                )
              }
            />
            <View
              style={{
                flexDirection: "row",
                justifyContent: "space-between",
                padding: 16,
              }}
            >
              <Text style={{ color: colors.text.primary }}>
                {uiText("Автозагрузка медиа")}</Text>
              <Switch
                value={autoDownload}
                onValueChange={(value) => {
                  setAutoDownload(value);
                  theme.setAutoDownload(value);
                }}
              />
            </View>
            <Row
              label={uiText("Локальное хранилище: {0} МБ",(cacheBytes / 1048576).toFixed(1))}
            />
            <Row label={uiText("Лимит кеша медиа: {0} МБ",cacheLimit)} onPress={() => void run(async () => { const values = [64,128,256,512,1024], next = values[(values.indexOf(cacheLimit) + 1) % values.length]; await storage.set("media.cacheLimitMB", String(next)); setCacheLimit(next); await trimMediaCache(); if (p.repository) setCacheBytes((await mediaUsage(p.repository)).total); })} />
            <Row label={uiText("Очистить кеш медиа")} detail={uiText("Черновики, записи и очередь сохранятся")} onPress={() => Alert.alert(uiText("Очистить кеш медиа?"), uiText("Загруженные файлы можно будет открыть заново"), [{ text: uiText("Отмена") }, { text: uiText("Очистить"), onPress: () => void run(async () => { await clearMediaCache(); if (p.repository) setCacheBytes((await mediaUsage(p.repository)).total); }) }])} />
            <Row
              label={uiText("Очистить кеш истории")}
              detail={uiText("Черновики и очередь отправки сохранятся")}
              onPress={() =>
                Alert.alert(
                  uiText("Очистить кеш?"),
                  uiText("Историю можно будет загрузить заново"),
                  [
                    { text: uiText("Отмена") },
                    {
                      text: uiText("Очистить"),
                      onPress: () => void run(() => p.repository!.clearCache()),
                    },
                  ],
                )
              }
            />
            <Text style={{ padding: 16, color: colors.text.secondary }}>
              {uiText("Активные устройства")}</Text>
            {sessions.map((session) => (
              <Row
                key={session.id}
                label={String(session.deviceName || session.deviceType || uiText("Устройство")).replace(/^Unknown on /,'').replace(/^Unknown(?: Device)?$/,uiText("Неизвестное устройство"))}
                detail={
                  session.isCurrent ? uiText("Это устройство") : session.lastActivity ? new Date(session.lastActivity).toLocaleString(uiLocale()) : uiText("Нет данных о последней активности")
                }
                onPress={() => {
                  if (session.isCurrent) return;
                  Alert.alert(
                    uiText("Завершить сеанс?"),
                    uiText("Устройству потребуется войти заново"),
                    [
                      { text: uiText("Отмена") },
                      {
                        text: uiText("Завершить"),
                        onPress: () =>
                          void run(async () => {
                            await api.terminate(session.id);
                            setSessions((prev) =>
                              prev.filter((x) => x.id !== session.id),
                            );
                          }),
                      },
                    ],
                  );
                }}
              />
            ))}
            <Row
              label={uiText("Удалить локальные данные")}
              danger
              detail={uiText("Включая черновики и неотправленные сообщения")}
              onPress={() =>
                Alert.alert(
                  uiText("Удалить локальные данные?"),
                  uiText("Черновики и очередь будут потеряны. Потребуется войти заново."),
                  [
                    { text: uiText("Отмена") },
                    {
                      text: uiText("Удалить"),
                      style: "destructive",
                      onPress: () => void run(p.onClearData),
                    },
                  ],
                )
              }
            />
            <Row
              label={uiText("Выйти")}
              danger
              onPress={() => {
                p.onClose();
                p.onLogout();
              }}
            />
          </>
        )}
      </ScrollView>}
      {(p.mode === "attachments" || p.mode === "circle") && (
        <>{p.mode==='attachments' && <ScrollView horizontal style={{flexGrow:0}}>{Object.entries({emoji:uiText("Эмодзи"),stickers:uiText("Стикеры и GIF"),poll:uiText("Опрос"),dice:uiText("Кубик"),contact:uiText("Контакт"),location:uiText("Геолокация")}).map(([mode,title])=><Pressable key={mode} accessibilityRole="button" onPress={()=>p.onMode('expressive:'+mode)} style={{padding:14,minHeight:48}}><Text style={{color:colors.text.blue}}>{title}</Text></Pressable>)}</ScrollView>}<MediaTray
          uploader={p.uploader}
          key={p.user.id + ":" + p.room?.id}
          initialCircle={p.mode === "circle"}
          baseUrl={p.baseUrl}
          token={p.token}
          repository={p.repository}
          roomId={p.room?.id || ""}
          onSent={p.onClose}
          onSend={p.onSendMedia}
        /></>
      )}
    </View>
  );
  return p.embedded ? (
    content
  ) : (
    <Modal visible animationType="slide" onRequestClose={p.onClose}>
      {content}
    </Modal>
  );
}

export function MessageActions({
  messages,
  meId,
  onSelectMore,
  onClose,
  onReply,
  onReact,
  onEdit,
  onForward,
  onDelete,
  onPin,
  onHide,
  onHistory,
  onComments,
  onError,
}: {
  messages: Message[];
  meId: string;
  onSelectMore: () => void;
  onClose: () => void;
  onReply: (m: Message) => void;
  onReact: (m: Message) => void;
  onEdit: (m: Message) => void;
  onForward: (items: Message[]) => void;
  onDelete: (items: Message[]) => Promise<void>;
  onPin?: (message: Message) => Promise<void>;
  onHide: (items: Message[]) => Promise<void>;
  onHistory?:(message:Message)=>void;
  onComments?:(message:Message)=>void;
  onError: (error: string) => void;
}) {
  const { colors, fontSize } = useTheme();
  const insets = useSafeAreaInsets();
  const [selectingText, setSelectingText] = useState(false);
  useEffect(() => setSelectingText(false), [messages[0]?.id]);
  if (!messages.length) return null;
  const first = messages[0];
  const action = (fn: () => void) => {
    onClose();
    fn();
  };
  if (selectingText)
    return (
      <Modal transparent visible animationType="fade" onRequestClose={onClose}>
        <View
          style={{
            flex: 1,
            justifyContent: "flex-end",
            backgroundColor: colors.misc.backdrop,
          }}
        >
          <Pressable style={StyleSheet.absoluteFill} onPress={onClose} />
          <View
            style={{
              backgroundColor: colors.windowBackground,
              borderTopLeftRadius: 24,
              borderTopRightRadius: 24,
              padding: 20,
              paddingBottom: insets.bottom + 20,
              maxHeight: "80%",
            }}
          >
            <View
              style={{
                flexDirection: "row",
                alignItems: "center",
                justifyContent: "space-between",
              }}
            >
              <Text
                style={{
                  color: colors.text.primary,
                  fontSize: 18,
                  fontWeight: "600",
                }}
              >
                {uiText("Текст сообщения")}</Text>
              <Pressable
                onPress={onClose}
                style={{
                  minHeight: 48,
                  minWidth: 64,
                  alignItems: "center",
                  justifyContent: "center",
                }}
              >
                <Text style={{ color: colors.text.blue }}>{uiText("Готово")}</Text>
              </Pressable>
            </View>
            <ScrollView>
              <Text
                selectable
                style={{
                  fontSize,
                  lineHeight: fontSize * 1.4,
                  color: colors.text.primary,
                  paddingVertical: 12,
                }}
              >
                {first.text}
              </Text>
            </ScrollView>
          </View>
        </View>
      </Modal>
    );
  return (
    <Modal transparent visible animationType="fade" onRequestClose={onClose}>
      <Pressable
        onPress={onClose}
        style={{
          flex: 1,
          backgroundColor: "#0007",
          justifyContent: "flex-end",
        }}
      >
        <View
          style={{
            backgroundColor: colors.windowBackground,
            borderTopLeftRadius: 24,
            borderTopRightRadius: 24,
            maxHeight: "85%",
          }}
        >
          <ScrollView
            style={{ flexGrow: 0 }}
            contentContainerStyle={{ paddingBottom: insets.bottom + 16 }}
          >
            {messages.length === 1 && (
              <>
                <Row
                  label={uiText("Ответить")}
                  onPress={() => action(() => onReply(first))}
                />
                <Row
                  label={uiText("Реакция")}
                  onPress={() => action(() => onReact(first))}
                />
              </>
            )}
            <Row label={uiText("Выделить несколько")} onPress={onSelectMore} />
            {messages.length===1&&!first.delivery&&<><Row label={uiText("Копировать ссылку")} onPress={()=>action(()=>{void Clipboard.setStringAsync(messageLink(first.room_id,first.id)).catch(e=>onError(String(e)));})}/>{onHistory&&<Row label={uiText("История правок")} onPress={()=>action(()=>onHistory(first))}/>}{first.is_post&&onComments&&<Row label={uiText('Комментарии')} onPress={()=>action(()=>onComments(first))}/>}</>}
            {messages.length === 1 && !first.delivery && onPin && <Row label={uiText("Закрепить сообщение")} onPress={() => action(() => { void onPin(first).catch(e => onError(String(e))); })} />}
            {messages.every(m => !m.delivery) && <Row label={uiText("Удалить у себя")} danger onPress={() => Alert.alert(uiText("Удалить только у себя?"), uiText("У собеседника сообщения останутся"), [{text:uiText("Отмена")},{text:uiText("Удалить"),style:"destructive",onPress:()=>action(()=>{void onHide(messages).catch(e=>onError(String(e)));})}])} />}
            <Row
              label={uiText("Копировать")}
              onPress={() =>
                action(() => {
                  void Clipboard.setStringAsync(
                    messages.map((m) => m.text || "").join("\n"),
                  );
                })
              }
            />
            {messages.length === 1 && first.text && (
              <Row
                label={uiText("Выделить текст")}
                onPress={() => setSelectingText(true)}
              />
            )}
            <Row
              label={uiText("Переслать")}
              onPress={() => action(() => onForward(messages))}
            />
            {messages.length === 1 &&
              first.sender_id === meId &&
              first.type === "text" &&
              !first.delivery &&
              !first.meta?.e2e_group && (
                <Row
                  label={uiText("Изменить")}
                  onPress={() => action(() => onEdit(first))}
                />
              )}
            {messages.every((m) => m.sender_id === meId && !m.delivery) && (
              <Row
                label={uiText("Удалить у всех")}
                danger
                onPress={() =>
                  Alert.alert(
                    uiText("Удалить сообщения?"),
                    uiText("Это действие нельзя отменить"),
                    [
                      { text: uiText("Отмена") },
                      {
                        text: uiText("Удалить"),
                        style: "destructive",
                        onPress: () => {
                          onClose();
                          void onDelete(messages).catch((e) =>
                            onError(String(e)),
                          );
                        },
                      },
                    ],
                  )
                }
              />
            )}
          </ScrollView>
        </View>
      </Pressable>
    </Modal>
  );
}
