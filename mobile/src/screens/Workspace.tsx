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
import { api as legacyApi } from "../api";
import { encryptRoomText } from "../roomCrypto";
import { roomTitle } from "../format";
import type { Message, Room, User } from "../types";
import { pushNotifications } from "../notifications";
import { MediaTray } from "../components/MediaTray";

import type { ProtectedUploader } from "../protectedUpload";
type Props = {
  uploader: ProtectedUploader;
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
  onForwarded: () => void;
  enqueueForward: (room: Room, message: Message) => Promise<void>;
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
      if (p.mode?.startsWith("room:") || p.mode === "archive")
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
  const forward = async (room: Room) => {
    for (const message of p.forwarding) await p.enqueueForward(room, message);
    p.onForwarded();
    await p.onOpen(room);
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
          setSearchProgress(`Проверено сообщений: ${count}`);
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
          ? "Поиск остановлен"
          : "Поиск доступной истории завершён",
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
        join: "Вступить в группу",
        new: "Новая переписка",
        group: "Новая группа",
        search: "Поиск",
        "search-room": "Поиск в переписке",
        details: "Информация",
        settings: "Настройки",
        attachments: "Вложение",
        circle: "Видеокружок",
        forward: "Переслать",
        archive: "Архив",
      } as Record<string, string>
    )[p.mode] || "Действия с чатом";
  const content = (
    <View
      style={{
        flex: 1,
        backgroundColor: colors.windowBackground,
        paddingTop: p.embedded ? 0 : insets.top,
        paddingBottom: p.embedded ? 0 : insets.bottom,
      }}
    >
      <View style={{ flexDirection: "row", alignItems: "center" }}>
        <Row label="Назад" onPress={p.onClose} />
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
            {input(query, setQuery, "Токен приглашения")}
            <Row
              label="Вступить"
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
                <Row label="Создать группу" onPress={() => p.onMode("group")} />
                <Row
                  label="Вступить по приглашению"
                  onPress={() => p.onMode("join")}
                />
              </>
            )}
            {p.mode === "group" && input(title, setTitle, "Название группы")}
            {input(query, setQuery, "Имя пользователя или текст сообщения")}
            {query.length > 0 && query.length < 2 && (
              <Text style={{ padding: 12, color: colors.text.secondary }}>
                Введите хотя бы два символа
              </Text>
            )}
            {!!users.length && (
              <Text style={{ padding: 12, color: colors.text.secondary }}>
                Люди
              </Text>
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
                label={`Создать группу · ${selected.length} участников`}
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
                  Сообщения из загруженной истории. Для полной проверки
                  запустите поиск дальше.
                </Text>
                {p.mode === "search" && (
                  <Text style={{ padding: 12, color: colors.text.secondary }}>
                    Чаты
                  </Text>
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
                  Сообщения · {results.length}
                  {results.length === 100 ? "+" : ""}
                </Text>
                {query.length >= 2 && !results.length && (
                  <Text style={{ padding: 12, color: colors.text.secondary }}>
                    В загруженной истории совпадений нет
                  </Text>
                )}
                {results.map((message, index) => (
                  <Row
                    key={message.id}
                    label={message.text || "Вложение"}
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
                      busy ? "Остановить поиск" : "Искать дальше в истории"
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
        {p.mode === "forward" &&
          p.rooms.map((room) => (
            <Row
              key={room.id}
              label={roomTitle(room, p.user.id)}
              onPress={() => void run(() => forward(room))}
            />
          ))}
        {p.mode.startsWith("room:") && (
          <>
            <Row
              label={roomPreference?.pinned ? "Открепить" : "Закрепить"}
              onPress={() =>
                void run(() =>
                  changePreference({ pinned: !roomPreference?.pinned }),
                )
              }
            />
            <Row
              label={
                roomPreference?.muted
                  ? "Включить уведомления"
                  : "Отключить уведомления"
              }
              onPress={() =>
                void run(() =>
                  changePreference({ muted: !roomPreference?.muted }),
                )
              }
            />
            <Row
              label={roomPreference?.archived ? "Вернуть из архива" : "В архив"}
              onPress={() =>
                void run(() =>
                  changePreference({ archived: !roomPreference?.archived }),
                )
              }
            />
            <Row
              label="Отметить непрочитанным"
              onPress={() =>
                void run(() => changePreference({ marked_unread: true }))
              }
            />
            <Row
              label="Удалить из моего списка"
              danger
              onPress={() =>
                Alert.alert(
                  "Удалить чат из списка?",
                  "Переписка у собеседника сохранится",
                  [
                    { text: "Отмена" },
                    {
                      text: "Удалить",
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
        {p.mode === "details" && p.room && (
          <>
            <Row
              label={roomTitle(p.room, p.user.id)}
              detail={
                p.room.kind === "dm"
                  ? `@${p.room.other_username || ""}`
                  : `Участников: ${members.length}`
              }
            />
            {p.room.kind === "dm" && p.room.other_user_id && (
              <Row
                label="Заблокировать собеседника"
                danger
                onPress={() =>
                  Alert.alert(
                    "Заблокировать?",
                    "Личные сообщения будут недоступны",
                    [
                      { text: "Отмена" },
                      {
                        text: "Заблокировать",
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
                label="Разблокировать собеседника"
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
                      owner: "Владелец",
                      admin: "Администратор",
                      member: "Участник",
                    } as Record<string, string>
                  )[member.role] || "Участник"
                }
                onPress={() => {
                  if (!canManage || member.id === p.room!.created_by) return;
                  Alert.alert(member.username, "Управление участником", [
                    { text: "Отмена" },
                    {
                      text:
                        member.role === "admin"
                          ? "Снять права администратора"
                          : "Назначить администратором",
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
                      text: "Удалить",
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
                  label="Изменить аватар группы"
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
                {input(title, setTitle, "Название группы")}
                <Row
                  label="Сохранить название"
                  onPress={() =>
                    void run(async () => {
                      await api.updateRoom(p.room!.id, title);
                      await p.onRefresh();
                    })
                  }
                />
                {input(query, setQuery, "Username нового участника")}
                <Row
                  label="Добавить участника"
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
                  label="Создать приглашение"
                  onPress={() =>
                    void run(async () => {
                      const result = await api.invite(p.room!.id);
                      setInvite(result.token);
                      await Share.share({
                        message: `Приглашение в Messenger67: ${result.token}`,
                      });
                    })
                  }
                />
                {!!invite && (
                  <Row
                    label="Отозвать приглашение"
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
              Общие медиа и файлы
            </Text>
            <ScrollView horizontal contentContainerStyle={{ paddingHorizontal: 12 }}>
              {Object.entries({ all: "Все", image: "Фото", video: "Видео", file: "Файлы" }).map(([key, title]) => <Pressable key={key} onPress={() => setMediaKind(key)} accessibilityRole="button" accessibilityState={{ selected: mediaKind === key }} style={{ padding: 12, minHeight: 48 }}><Text style={{ color: mediaKind === key ? colors.text.blue : colors.text.secondary }}>{title}</Text></Pressable>)}
              <Pressable onPress={() => p.onMode("search-room")} style={{ padding: 12, minHeight: 48 }}><Text style={{ color: colors.text.blue }}>Ссылки · поиск</Text></Pressable>
            </ScrollView>
            {media.map((message) => (
              <Row
                key={message.id}
                label={message.text || message.media_mime || "Вложение"}
                detail={new Date(message.created_at).toLocaleDateString(
                  "ru-RU",
                )}
                onPress={() => p.onOpenMedia(message)}
              />
            ))}
            {moreMedia && <Row label={busy ? "Загрузка…" : "Загрузить ещё вложения"} onPress={() => { if (busy || !p.room) return; void run(async () => { const next = await api.media(p.room!.id, { kind: mediaKind, before: media.at(-1)!.id }); setMedia(prev => [...prev, ...next]); setMoreMedia(next.length === 100); }); }} />}
          </>
        )}
        {p.mode === "settings" && (
          <>
            <Row label="Профиль" onPress={p.onProfile} />
            <Row label="Папки чатов" onPress={() => p.onMode("folders")} />
            <Row label="Избранное" onPress={() => void run(async () => { const room = await api.saved(); await p.onRefresh(); await p.onOpen(await api.room(room.id)); })} />
            <Row
              label="Уведомления"
              detail={pushStatus.message}
              onPress={() => void Linking.openSettings()}
            />
            {pushNotifications.configured && (
              <>
                {pushStatus.status !== "enabled" && (
                  <Row label="Подключить уведомления" onPress={() => void run(() => pushNotifications.retry())} />
                )}
                <Row label="Проверить уведомления" onPress={() => void run(async () => {
                  await pushNotifications.test(p.baseUrl, p.token);
                  Alert.alert("67chat", "Проверочное уведомление отправлено");
                })} />
              </>
            )}
            {(["light", "dark", "system"] as ThemeMode[]).map((mode) => (
              <Row
                key={mode}
                label={`${theme.mode === mode ? "✓ " : ""}${{ light: "Светлая тема", dark: "Тёмная тема", system: "Как в системе" }[mode]}`}
                onPress={() => theme.setMode(mode)}
              />
            ))}
            <Row
              label={`Размер текста: ${theme.fontSize}`}
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
                Автозагрузка медиа
              </Text>
              <Switch
                value={autoDownload}
                onValueChange={(value) => {
                  setAutoDownload(value);
                  theme.setAutoDownload(value);
                }}
              />
            </View>
            <Row
              label={`Локальное хранилище: ${(cacheBytes / 1048576).toFixed(1)} МБ`}
            />
            <Row label={`Лимит кеша медиа: ${cacheLimit} МБ`} onPress={() => void run(async () => { const values = [64,128,256,512,1024], next = values[(values.indexOf(cacheLimit) + 1) % values.length]; await storage.set("media.cacheLimitMB", String(next)); setCacheLimit(next); await trimMediaCache(); if (p.repository) setCacheBytes((await mediaUsage(p.repository)).total); })} />
            <Row label="Очистить кеш медиа" detail="Черновики, записи и очередь сохранятся" onPress={() => Alert.alert("Очистить кеш медиа?", "Загруженные файлы можно будет открыть заново", [{ text: "Отмена" }, { text: "Очистить", onPress: () => void run(async () => { await clearMediaCache(); if (p.repository) setCacheBytes((await mediaUsage(p.repository)).total); }) }])} />
            <Row
              label="Очистить кеш истории"
              detail="Черновики и очередь отправки сохранятся"
              onPress={() =>
                Alert.alert(
                  "Очистить кеш?",
                  "Историю можно будет загрузить заново",
                  [
                    { text: "Отмена" },
                    {
                      text: "Очистить",
                      onPress: () => void run(() => p.repository!.clearCache()),
                    },
                  ],
                )
              }
            />
            <Text style={{ padding: 16, color: colors.text.secondary }}>
              Активные устройства
            </Text>
            {sessions.map((session) => (
              <Row
                key={session.id}
                label={String(session.deviceName || session.deviceType || 'Устройство').replace(/^Unknown on /,'').replace(/^Unknown(?: Device)?$/,'Неизвестное устройство')}
                detail={
                  session.isCurrent ? "Это устройство" : session.lastActivity ? new Date(session.lastActivity).toLocaleString('ru-RU') : 'Нет данных о последней активности'
                }
                onPress={() => {
                  if (session.isCurrent) return;
                  Alert.alert(
                    "Завершить сеанс?",
                    "Устройству потребуется войти заново",
                    [
                      { text: "Отмена" },
                      {
                        text: "Завершить",
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
              label="Удалить локальные данные"
              danger
              detail="Включая черновики и неотправленные сообщения"
              onPress={() =>
                Alert.alert(
                  "Удалить локальные данные?",
                  "Черновики и очередь будут потеряны. Потребуется войти заново.",
                  [
                    { text: "Отмена" },
                    {
                      text: "Удалить",
                      style: "destructive",
                      onPress: () => void run(p.onClearData),
                    },
                  ],
                )
              }
            />
            <Row
              label="Выйти"
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
        <MediaTray
          uploader={p.uploader}
          key={p.user.id + ":" + p.room?.id}
          initialCircle={p.mode === "circle"}
          baseUrl={p.baseUrl}
          token={p.token}
          repository={p.repository}
          roomId={p.room?.id || ""}
          onSent={p.onClose}
          onSend={p.onSendMedia}
        />
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
                Текст сообщения
              </Text>
              <Pressable
                onPress={onClose}
                style={{
                  minHeight: 48,
                  minWidth: 64,
                  alignItems: "center",
                  justifyContent: "center",
                }}
              >
                <Text style={{ color: colors.text.blue }}>Готово</Text>
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
                  label="Ответить"
                  onPress={() => action(() => onReply(first))}
                />
                <Row
                  label="Реакция"
                  onPress={() => action(() => onReact(first))}
                />
              </>
            )}
            <Row label="Выделить несколько" onPress={onSelectMore} />
            <Row
              label="Копировать"
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
                label="Выделить текст"
                onPress={() => setSelectingText(true)}
              />
            )}
            <Row
              label="Переслать"
              onPress={() => action(() => onForward(messages))}
            />
            {messages.length === 1 &&
              first.sender_id === meId &&
              first.type === "text" &&
              !first.delivery &&
              !first.meta?.e2e_group && (
                <Row
                  label="Изменить"
                  onPress={() => action(() => onEdit(first))}
                />
              )}
            {messages.every((m) => m.sender_id === meId && !m.delivery) && (
              <Row
                label="Удалить у всех"
                danger
                onPress={() =>
                  Alert.alert(
                    "Удалить сообщения?",
                    "Это действие нельзя отменить",
                    [
                      { text: "Отмена" },
                      {
                        text: "Удалить",
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
