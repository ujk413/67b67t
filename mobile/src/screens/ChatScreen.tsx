import {messageWindowState,setMessageViewport,messageWindowUnseenCount,consumeMessageWindowUnseen} from '../data/messages';
import {locale as uiLocale} from "../../../web/i18n.js";
import {t as uiText} from "../../../web/i18n.js";
import { presenceText, sameMessageGroup } from "../presentation";
import { groupAlbums } from "../data/albums";
import { Album } from "../components/Album";
import {isExpressiveControl,parseMessagePayload} from '../features/messagePayloads';
import {selectLoadedRange} from '../../../web/message-selection.js';
import { useEffect, useRef, useState } from "react";
import {
  ActivityIndicator,
  Alert,
  Animated,
  FlatList,
  Modal,
  PanResponder,
  Pressable,
  Text,
  View,
  type ViewToken,
} from "react-native";
import { ChatHeader } from "../components/ChatHeader";
import { Composer } from "../components/Composer";
import { MessageBubble } from "../components/MessageBubble";
import { dayLabel, participantsLabel } from "../format";
import { useTheme } from "../ThemeProvider";
import {WallpaperPattern} from '../features/WallpaperPattern';
import type { AccountRepository } from "../data/repository";
import type { Message, Room, User } from "../types";

type Props = {
  room: Room;
  messages: Message[];
  meId: string;
  baseUrl: string;
  composer: string;
  typingNames: string[];
  participants?: User[];
  peer?: User | null;
  connection?: string;
  refreshing: boolean;
  recording?: boolean;
  playingId: string | null;
  disabled?: boolean;
  onVoiceRecorded?: (uri: string, duration: number, waveform?:number[]) => Promise<void>;
  onBack: () => void;
  onRefresh: () => void;
  onChangeComposer: (text: string) => void;
  onSend: () => void;
  onAttach?: () => void;
  onVoice?: () => void;
  onVideoNote?: () => void;
  onAudioCall?: () => void;
  onVideoCall?: () => void;
  onPlayVoice: (message: Message) => void;
  onReact: (message: Message) => void;
  onOpenMedia: (message: Message) => void;
  mediaUrl: (base: string, id: string | null | undefined) => string | null;
  mediaThumbUrl: (base: string, id: string | null | undefined) => string | null;
  loading?: boolean;
  loadingOlder?: boolean;
  hasOlder?: boolean;
  hasNewer?: boolean;
  onNewer?: () => void;
  firstUnreadId?: string | null;
  focusId?: string | null;
  repository?: AccountRepository | null;
  onUsername?: (username: string) => void;
  onOlder?: () => void;
  onVisible?: (id: string) => void;
  onDetails?: () => void;
  onSearch?: () => void;
  searchMatches?: { index: number; total: number };
  onSearchNavigate?: (delta: number) => void;
  onCloseSearch?: () => void;
  onActions?: (items: Message[]) => void;
  selectionReset?: number;
  onReply?: (message: Message) => void;
  reply?: Message | null;
  editing?: boolean;
  onCancelReply?: () => void;
  onJump?: (id: string) => void;
  onUnpin?: () => void;
  onPins?: () => void;
  onRetry?: (id: string) => void;
  onCancelSend?: (id: string) => void;
};
export function ChatScreen(p: Props) {
  const { colors,wallpaper,dark } = useTheme();
  const [headerMenu, setHeaderMenu] = useState(false);
  const windowState=messageWindowState(p.room.id),hasOlder=!!p.hasOlder||windowState.hasOlder,hasNewer=!!p.hasNewer||windowState.hasNewer;
  const unseen=messageWindowUnseenCount(p.room.id,p.meId);
  const [,refreshWindow]=useState(0);
  const visibleMessages=p.messages.filter(message=>!isExpressiveControl(parseMessagePayload(message.text)));
  const grouped = groupAlbums(visibleMessages);
  const groupedRef = useRef(grouped);
  groupedRef.current = grouped;
  const list = useRef<FlatList<Message>>(null),
    nearBottom = useRef(true),
    restored = useRef(false),
    loadingFromGesture = useRef(false);
  const latest = useRef(p);
  latest.current = p;
  const [selected, setSelected] = useState<string[]>([]),
    [highlight, setHighlight] = useState<string | null>(null);
  useEffect(() => setSelected([]), [p.selectionReset]);
  const previousIds = useRef(new Set<string>()),
    anchor = useRef<string | null>(null),
    positionTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const jump = (id: string) => {
    const original = latest.current.messages.find((m) => m.id === id);
    const visibleId = original
      ? groupedRef.current.groupFor(original)?.[0].id || id
      : id;
    const index = groupedRef.current.display.findIndex(
      (m) => m.id === visibleId,
    );
    if (index < 0) {
      p.onJump?.(id);
      return;
    }
    list.current?.scrollToIndex({ index, animated: false, viewPosition: 0.4 });
    setHighlight(visibleId);
    setTimeout(() => setHighlight(null), 1800);
  };
  const visible = useRef(
    ({ viewableItems }: { viewableItems: ViewToken<Message>[] }) => {
      const items = viewableItems.filter((x) => x.item && !x.item.delivery);
      const lastItem = items.at(-1)?.item;
      const last = lastItem
        ? groupedRef.current.groupFor(lastItem)?.at(-1) || lastItem
        : undefined;
      if (!last||last.room_id!==latest.current.room.id) return;
      if(consumeMessageWindowUnseen(latest.current.room.id,last))refreshWindow(value=>value+1);
      const first = items[0]?.item;
      anchor.current = first?.id || null;
      if (positionTimer.current) clearTimeout(positionTimer.current);
      positionTimer.current = setTimeout(() => {
        const p = latest.current;
        if (anchor.current)
          void p.repository
            ?.set(`position:${p.room.id}`, anchor.current)
            .catch(() => {});
        p.onVisible?.(last.id);
      }, 500);
    },
  ).current;
  useEffect(() => {
    restored.current = false;
    nearBottom.current = true;
    previousIds.current = new Set();
    setSelected([]);
    return () => {
      if (positionTimer.current) clearTimeout(positionTimer.current);
    };
  }, [p.room.id]);
  useEffect(() => {
    if (p.focusId && p.messages.some((m) => m.id === p.focusId))
      requestAnimationFrame(() => jump(p.focusId!));
  }, [p.focusId, p.messages.length]);
  useEffect(() => {
    if (!p.messages.length) return;
    if (!restored.current && !p.loading) {
      restored.current = true;
      void (async () => {
        const saved = await p.repository?.get<string | null>(
          `position:${p.room.id}`,
          null,
        );
        const target = p.focusId || saved || p.firstUnreadId;
        requestAnimationFrame(() => {
          if (target && latest.current.messages.some((m) => m.id === target))
            jump(target);
          else list.current?.scrollToEnd({ animated: false });
        });
      })();
    }
    const additions = p.messages.filter((m) => !previousIds.current.has(m.id));
    if (previousIds.current.size && additions.length) {
      if (nearBottom.current && !hasNewer && !p.loadingOlder && !p.focusId)
        requestAnimationFrame(() =>
          list.current?.scrollToEnd({ animated: true }),
        );
    }
    previousIds.current = new Set(p.messages.map((m) => m.id));
  }, [p.messages, p.loading, p.loadingOlder]);
  const toggle = (id: string) =>
    setSelected((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id],
    );
  return (
    <View style={{ flex: 1, backgroundColor: colors.windowBackground }}>
      <View>
        <ChatHeader
          room={p.room}
          meId={p.meId}
          baseUrl={p.baseUrl}
          subtitle={
            p.typingNames.length
              ? p.typingNames.length === 1
                ? uiText("{0} печатает…",p.typingNames[0])
                : uiText("{0} печатают…",p.typingNames.join(", "))
              : p.room.kind === "dm"
                ? p.connection && p.connection !== "connected"
                  ? uiText("Подключение…")
                  : presenceText(p.peer)
                : p.room.kind === "channel"
                  ? uiText("Канал")
                  : p.participants && p.participants.length > 1
                    ? participantsLabel(p.participants.length)
                    : uiText("Группа")
          }
          online={
            !!p.typingNames.length ||
            (p.peer?.online === true &&
              (!p.connection || p.connection === "connected"))
          }
          onMenuPress={() => setHeaderMenu(true)}
          onBack={p.onBack}
          onDetails={p.onDetails}
          onAudioCall={p.onAudioCall}
          onVideoCall={p.onVideoCall}
          mediaUrl={p.mediaUrl}
        />
      </View>
      <View
        style={{
          flexDirection: "row",
          justifyContent: "space-between",
          backgroundColor: colors.windowBackground,
        }}
      >
        {!!selected.length && (
          <>
            <Pressable
              onPress={() =>
                p.onActions?.(p.messages.filter((m) => selected.includes(m.id)))
              }
              style={{ padding: 12, minHeight: 48 }}
            >
              <Text style={{ color: colors.text.blue }}>
                {uiText("Действия ·")}{selected.length}
              </Text>
            </Pressable>
            <Pressable
              onPress={() => setSelected([])}
              style={{ padding: 12, minHeight: 48 }}
            >
              <Text style={{ color: colors.text.blue }}>{uiText("Отмена")}</Text>
            </Pressable>
            <Pressable accessibilityRole="button" onPress={()=>Alert.alert(uiText("Диапазон выделения"),uiText("Только сообщения из загруженной истории, включая выбранное."),[{text:uiText("Выбрать до"),onPress:()=>setSelected(selectLoadedRange(visibleMessages,selected.at(-1)!,'before'))},{text:uiText("Выбрать после"),onPress:()=>setSelected(selectLoadedRange(visibleMessages,selected.at(-1)!,'after'))},{text:uiText("Все загруженные"),onPress:()=>setSelected(selectLoadedRange(visibleMessages,'','all'))}],{cancelable:true})} style={{padding:12,minHeight:48}}><Text style={{color:colors.text.blue}}>{uiText("Диапазон")}</Text></Pressable>
          </>
        )}
      </View>
      {!!selected.length&&<Text style={{paddingHorizontal:14,paddingVertical:4,color:colors.text.secondary,backgroundColor:colors.windowBackground}}>{(()=>{const chosen=p.messages.filter(message=>selected.includes(message.id));return chosen.length?uiText("Границы: {0} — {1}",new Date(chosen[0].created_at).toLocaleString(uiLocale()),new Date(chosen.at(-1)!.created_at).toLocaleString(uiLocale())):'';})()}</Text>}
      {p.searchMatches && (
        <View
          style={{
            flexDirection: "row",
            alignItems: "center",
            backgroundColor: colors.windowBackground,
          }}
        >
          <Pressable
            accessibilityLabel={uiText("Предыдущее совпадение")}
            disabled={p.searchMatches.index === 0}
            onPress={() => p.onSearchNavigate?.(-1)}
            style={{ padding: 14, minHeight: 48 }}
          >
            <Text style={{ color: colors.text.blue }}>{uiText("Назад")}</Text>
          </Pressable>
          <Text
            style={{
              flex: 1,
              textAlign: "center",
              color: colors.text.secondary,
            }}
          >
            {p.searchMatches.index + 1} {uiText("из")}{p.searchMatches.total}
          </Text>
          <Pressable
            accessibilityLabel={uiText("Следующее совпадение")}
            disabled={p.searchMatches.index === p.searchMatches.total - 1}
            onPress={() => p.onSearchNavigate?.(1)}
            style={{ padding: 14, minHeight: 48 }}
          >
            <Text style={{ color: colors.text.blue }}>{uiText("Далее")}</Text>
          </Pressable>
          <Pressable
            accessibilityLabel={uiText("Закрыть результаты поиска")}
            onPress={p.onCloseSearch}
            style={{ padding: 14, minHeight: 48 }}
          >
            <Text style={{ color: colors.text.blue }}>{uiText("Готово")}</Text>
          </Pressable>
        </View>
      )}
      <Modal
        visible={headerMenu}
        transparent
        animationType="fade"
        onRequestClose={() => setHeaderMenu(false)}
      >
        <Pressable
          onPress={() => setHeaderMenu(false)}
          style={{
            flex: 1,
            backgroundColor: colors.misc.backdrop,
            alignItems: "flex-end",
            paddingTop: 60,
            paddingRight: 10,
          }}
        >
          <View
            style={{
              backgroundColor: colors.windowBackground,
              borderRadius: 18,
              minWidth: 210,
              paddingVertical: 8,
            }}
          >
            <Pressable
              accessibilityRole="button"
              onPress={() => {
                setHeaderMenu(false);
                p.onSearch?.();
              }}
              style={{ padding: 16, minHeight: 48 }}
            >
              <Text style={{ color: colors.text.primary, fontSize: 16 }}>
                {uiText("Поиск в переписке")}</Text>
            </Pressable>
            <Pressable
              accessibilityRole="button"
              onPress={() => {
                setHeaderMenu(false);
                p.onDetails?.();
              }}
              style={{ padding: 16, minHeight: 48 }}
            >
              <Text style={{ color: colors.text.primary, fontSize: 16 }}>
                {uiText("Информация")}</Text>
            </Pressable>
          </View>
        </Pressable>
      </Modal>
      {p.loading && <ActivityIndicator color={colors.text.blue} />}
      {!!p.room.pinned_message_id && <View style={{flexDirection:'row',alignItems:'center',backgroundColor:colors.windowBackground}}>
        <Pressable accessibilityRole="button" accessibilityLabel={uiText("Перейти к закреплённому сообщению")} onPress={() => jump(p.room.pinned_message_id!)} style={{flex:1,padding:12,borderLeftWidth:3,borderColor:colors.text.blue}}>
          <Text style={{color:colors.text.blue,fontWeight:'600'}}>{uiText("Закреплённое сообщение")}</Text>
          <Text numberOfLines={1} style={{color:colors.text.secondary}}>{p.messages.find(m=>m.id===p.room.pinned_message_id)?.text || uiText("Нажмите, чтобы открыть")}</Text>
        </Pressable>
        {p.onPins && <Pressable accessibilityRole="button" accessibilityLabel={uiText("Все закрепления")} onPress={p.onPins} style={{padding:12}}><Text style={{color:colors.text.blue}}>{uiText("Все")}</Text></Pressable>}
        {p.onUnpin && <Pressable accessibilityRole="button" accessibilityLabel={uiText("Открепить сообщение")} onPress={p.onUnpin} style={{padding:16}}><Text style={{color:colors.text.secondary}}>✕</Text></Pressable>}
      </View>}
      <View style={{ flex: 1, backgroundColor: colors.chat.wallpaper }}>
        <WallpaperPattern mode={wallpaper} dark={dark}/>
        <FlatList
          ref={list}
          data={grouped.display}
          keyExtractor={(m) => m.id}
          keyboardShouldPersistTaps="handled"
          maintainVisibleContentPosition={{ minIndexForVisible: 0 }}
          onViewableItemsChanged={visible}
          viewabilityConfig={{
            itemVisiblePercentThreshold: 65,
            minimumViewTime: 350,
          }}
          onScrollBeginDrag={() => {
            loadingFromGesture.current = true;
          }}
          onScroll={(event) => {
            const { contentOffset, layoutMeasurement, contentSize } =
              event.nativeEvent;
            nearBottom.current =
              contentOffset.y + layoutMeasurement.height >=
              contentSize.height - 100;
            setMessageViewport(p.room.id,nearBottom.current);
            if (nearBottom.current&&!hasNewer&&consumeMessageWindowUnseen(p.room.id))refreshWindow(value=>value+1);
            if (
              loadingFromGesture.current &&
              contentOffset.y < 50 &&
              restored.current &&
              (latest.current.hasOlder||messageWindowState(latest.current.room.id).hasOlder) &&
              !latest.current.loadingOlder &&
              !latest.current.loading
            ) {
              loadingFromGesture.current = false;
              latest.current.onOlder?.();
            }
          }}
          scrollEventThrottle={100}
          onScrollToIndexFailed={(info) => {
            list.current?.scrollToOffset({
              offset: info.averageItemLength * info.index,
              animated: false,
            });
            setTimeout(() => {
              if (info.index < latest.current.messages.length)
                list.current?.scrollToIndex({
                  index: info.index,
                  animated: false,
                  viewPosition: 0.4,
                });
            }, 200);
          }}
          ListHeaderComponent={
            hasOlder ? (
              <Pressable
                onPress={p.onOlder}
                disabled={p.loadingOlder}
                style={{ padding: 16, minHeight: 48 }}
              >
                {p.loadingOlder ? (
                  <ActivityIndicator />
                ) : (
                  <Text
                    style={{ textAlign: "center", color: colors.text.blue }}
                  >
                    {uiText("Загрузить предыдущие сообщения")}</Text>
                )}
              </Pressable>
            ) : null
          }
          ListFooterComponent={
            hasNewer ? (
              <Pressable
                onPress={p.onNewer}
                style={{ padding: 16, minHeight: 48 }}
              >
                <Text style={{ color: colors.text.blue, textAlign: "center" }}>
                  {uiText("Загрузить следующие сообщения")}</Text>
              </Pressable>
            ) : null
          }
          ListEmptyComponent={
            !p.loading ? (
              <Text
                style={{
                  padding: 24,
                  textAlign: "center",
                  color: colors.text.secondary,
                }}
              >
                {uiText("Сообщений пока нет")}</Text>
            ) : null
          }
          contentContainerStyle={{ padding: 8 }}
          renderItem={({ item, index }) => {
            const previous = grouped.display[index - 1];
            const album = grouped.groupFor(item);
            const newDay =
              !previous ||
              new Date(previous.created_at).toDateString() !==
                new Date(item.created_at).toDateString();
            const group = p.room.kind !== "dm";
            const showAvatar =
              group &&
              (!previous || previous.sender_id !== item.sender_id || newDay);
            return (
              <View>
                {newDay && (
                  <Text
                    style={{
                      alignSelf: "center",
                      padding: 6,
                      margin: 8,
                      borderRadius: 22,
                      backgroundColor: colors.chat.serviceBackground,
                      color: colors.chat.serviceText,
                    }}
                  >
                    {dayLabel(item.created_at)}
                  </Text>
                )}
                {item.id === p.firstUnreadId && (
                  <Text
                    style={{
                      textAlign: "center",
                      padding: 8,
                      color: colors.text.blue,
                    }}
                  >
                    {uiText("Непрочитанные сообщения")}</Text>
                )}
                <SwipeReply
                  onReply={() => p.onReply?.(item)}
                  disabled={!!item.delivery}
                >
                  <Pressable
                    onLongPress={() => {
                      toggle(item.id);
                      if (!selected.length) p.onActions?.([item]);
                    }}
                    onPress={() => {
                      if (selected.length) toggle(item.id);
                      else p.onActions?.([item]);
                    }}
                    style={{
                      borderRadius: 12,
                      backgroundColor: selected.includes(item.id)
                        ? colors.chats.selectedBackground
                        : highlight === item.id
                          ? colors.chat.outBubble
                          : undefined,
                    }}
                  >
                    {album && album.length > 1 ? (
                      <Album
                        messages={album}
                        meId={p.meId}
                        baseUrl={p.baseUrl}
                        onOpen={p.onOpenMedia}
                      />
                    ) : (
                      <MessageBubble
                        message={item}
                        participants={p.participants}
                        joinedPrevious={sameMessageGroup(previous, item)}
                        joinedNext={sameMessageGroup(
                          item,
                          grouped.display[index + 1],
                        )}
                        onLongPress={() => {
                          toggle(item.id);
                          if (!selected.length) p.onActions?.([item]);
                        }}
                        meId={p.meId}
                        baseUrl={p.baseUrl}
                        playing={p.playingId === item.id}
                        showAvatar={showAvatar}
                        group={group}
                        onUsername={p.onUsername}
                        onPlayVoice={() => p.onPlayVoice(item)}
                        onReact={() => p.onReact(item)}
                        onOpenMedia={p.onOpenMedia}
                        onReplyPress={(id) => p.onJump?.(id)}
                        mediaUrl={p.mediaUrl}
                        mediaThumbUrl={p.mediaThumbUrl}
                      />
                    )}
                  </Pressable>
                </SwipeReply>
                {(item.delivery === "failed" || item.delivery === "queued") && (
                  <View
                    style={{ flexDirection: "row", justifyContent: "flex-end" }}
                  >
                    <Pressable
                      onPress={() => p.onRetry?.(item.client_message_id!)}
                      style={{ padding: 12, minHeight: 48 }}
                    >
                      <Text style={{ color: colors.misc.error }}>
                        {uiText("Повторить")}</Text>
                    </Pressable>
                    <Pressable
                      onPress={() => p.onCancelSend?.(item.client_message_id!)}
                      style={{ padding: 12, minHeight: 48 }}
                    >
                      <Text style={{ color: colors.text.secondary }}>
                        {uiText("Убрать")}</Text>
                    </Pressable>
                  </View>
                )}
              </View>
            );
          }}
        />
        {(unseen > 0||hasNewer) && (
          <Pressable
            onPress={() => {
              if(hasNewer)p.onNewer?.();
              list.current?.scrollToEnd({ animated: true });
            }}
            accessibilityLabel={uiText("Перейти к новым сообщениям")}
            style={{
              position: "absolute",
              right: 16,
              bottom: 12,
              borderRadius: 24,
              padding: 14,
              backgroundColor: colors.composer.background,
            }}
          >
            <Text style={{ color: colors.text.blue }}>{unseen?uiText("Вниз ·")+unseen+(windowState.unseenCapped?'+':''):uiText("Вниз")}</Text>
          </Pressable>
        )}
      </View>
      {!!p.reply && (
        <View
          style={{
            flexDirection: "row",
            padding: 12,
            backgroundColor: colors.windowBackgroundGray,
          }}
        >
          <Pressable
            onPress={() => p.onJump?.(p.reply!.id)}
            style={{ flex: 1 }}
          >
            <Text style={{ color: colors.text.blue }}>
              {p.editing
                ? uiText("Редактирование")
                : uiText("Ответ: {0}",p.reply.sender_display_name || p.reply.sender_username || "")}
            </Text>
            <Text numberOfLines={2} style={{ color: colors.text.primary }}>
              {p.reply.text || uiText("Вложение")}
            </Text>
          </Pressable>
          <Pressable
            onPress={p.onCancelReply}
            style={{ padding: 12, minWidth: 48 }}
          >
            <Text style={{ color: colors.text.secondary }}>{uiText("Отмена")}</Text>
          </Pressable>
        </View>
      )}
      <Composer
        onVoiceRecorded={p.onVoiceRecorded}
        repository={p.repository}
        roomId={p.room.id}
        value={p.composer}
        onChangeText={p.onChangeComposer}
        onSend={p.onSend}
        onAttach={p.onAttach}
        onVoice={p.onVoice}
        onVideoNote={p.onVideoNote}
        recording={p.recording}
        disabled={p.disabled}
      />
    </View>
  );
}
function SwipeReply({
  children,
  onReply,
  disabled,
}: {
  children: React.ReactNode;
  onReply: () => void;
  disabled: boolean;
}) {
  const offset = useRef(new Animated.Value(0)).current,
    latest = useRef({ onReply, disabled });
  latest.current = { onReply, disabled };
  const responder = useRef(
    PanResponder.create({
      onMoveShouldSetPanResponder: (_, g) =>
        !latest.current.disabled && Math.abs(g.dx) > 30 && Math.abs(g.dy) < 15,
      onPanResponderMove: (_, g) =>
        offset.setValue(Math.max(-80, Math.min(80, g.dx))),
      onPanResponderRelease: (_, g) => {
        if (Math.abs(g.dx) > 55) latest.current.onReply();
        Animated.spring(offset, { toValue: 0, useNativeDriver: true }).start();
      },
      onPanResponderTerminate: () =>
        Animated.spring(offset, { toValue: 0, useNativeDriver: true }).start(),
    }),
  ).current;
  return (
    <Animated.View
      {...responder.panHandlers}
      style={{ transform: [{ translateX: offset }] }}
    >
      {children}
    </Animated.View>
  );
}
