// Native screenshot fixture. The production entry remains mobile/index.ts.
import "react-native-get-random-values";
import { KeyboardAvoidingView, Platform } from "react-native";
import { registerRootComponent } from "expo";
import { StatusBar } from "expo-status-bar";
import { MessageActions } from "../src/screens/Workspace";
import { useState } from "react";
import { SafeAreaProvider, SafeAreaView } from "react-native-safe-area-context";
import { ThemeProvider, useTheme } from "../src/ThemeProvider";
import { AppNavigation } from "../src/components/AppNavigation";
import { ChatListScreen } from "../src/screens/ChatListScreen";
import { ChatScreen } from "../src/screens/ChatScreen";
import type { Message, Room, User } from "../src/types";

const user: User = {
  id: "demo-me",
  username: "alex",
  display_name: "Александр",
};
const time = (minutes: number) => {
  const date = new Date();
  date.setHours(9, 41 - minutes, 0, 0);
  return date.toISOString();
};
const peer: User = {
  id: "demo-mira",
  username: "mira",
  display_name: "Мира",
  online: true,
  last_seen: time(0),
};
const rooms: Room[] = [
  {
    id: "mira",
    kind: "dm",
    other_user_id: "demo-mira",
    other_username: "mira",
    other_display_name: "Мира",
    last_msg_text: "Отлично! Жду новую сборку ✨",
    last_msg_type: "text",
    last_msg_at: time(2),
    unread_count: 2,
    preferences: { pinned: true },
  },
  {
    id: "team",
    kind: "group",
    title: "Команда 67",
    last_msg_text: "Давайте созвонимся в 18:00",
    last_msg_sender_name: "Антон",
    last_msg_type: "text",
    last_msg_at: time(8),
    unread_count: 5,
    preferences: { pinned: true },
  },
  {
    id: "anton",
    kind: "dm",
    other_user_id: "demo-anton",
    other_username: "anton",
    other_display_name: "Антон",
    last_msg_text: "Да, всё получилось 👍",
    last_msg_type: "text",
    last_msg_at: time(18),
    unread_count: 0,
  },
  {
    id: "news",
    kind: "channel",
    title: "Новости Messenger67",
    last_msg_text: "Что нового в версии 1.1",
    last_msg_type: "text",
    last_msg_at: time(45),
    unread_count: 1,
  },
  {
    id: "design",
    kind: "group",
    title: "Дизайн и идеи",
    last_msg_text: "Новые иконки уже в приложении",
    last_msg_sender_name: "Мира",
    last_msg_type: "text",
    last_msg_at: time(72),
    unread_count: 0,
    preferences: { muted: true },
  },
  {
    id: "anna",
    kind: "dm",
    other_user_id: "demo-anna",
    other_username: "anna",
    other_display_name: "Анна",
    last_msg_text: "Спасибо! До завтра",
    last_msg_type: "text",
    last_msg_at: time(110),
    unread_count: 0,
  },
  {
    id: "weekend",
    kind: "group",
    title: "Планы на выходные",
    last_msg_text: "Встречаемся у парка?",
    last_msg_sender_name: "Антон",
    last_msg_type: "text",
    last_msg_at: time(240),
    unread_count: 0,
  },
];
const message = (
  id: string,
  own: boolean,
  text: string,
  minutes: number,
  extra: Partial<Message> = {},
): Message => ({
  id,
  room_id: "mira",
  sender_id: own ? user.id : "demo-mira",
  sender_username: own ? "alex" : "mira",
  sender_display_name: own ? "Александр" : "Мира",
  type: "text",
  text,
  created_at: time(minutes),
  read_by: [user.id, "demo-mira"],
  ...extra,
});
const history: Message[] = [
  message("1", false, "Привет! Как продвигается новая версия?", 36),
  message(
    "2",
    true,
    "Привет! Уже можно начинать переписку, искать сообщения и отвечать свайпом.",
    34,
  ),
  message("3", false, "Здорово! А светлая тема осталась?", 31),
  message(
    "4",
    true,
    "Конечно. Ещё добавили тёмную тему и настройку размера текста.",
    29,
    {
      reactions: { "👍": [peer.id] },
      reply_to: {
        id: "3",
        sender_username: "mira",
        sender_display_name: "Мира",
        text: "А светлая тема осталась?",
      },
    },
  ),
  message(
    "5",
    false,
    "Удобно, что черновики теперь сохраняются отдельно для каждого чата.",
    22,
    { reactions: { "❤️": [user.id, peer.id] } },
  ),
  message(
    "6",
    true,
    "Да! И сообщение останется в очереди, если пропадёт сеть.",
    18,
  ),
  message("7", false, "Отлично! Жду новую сборку ✨", 2),
];
const noMedia = () => null;
const noop = () => {};
function Screens() {
  const { colors, dark } = useTheme();
  const [actions, setActions] = useState<Message[]>([]),
    [reply, setReply] = useState<Message | null>(null),
    [selectionReset, setSelectionReset] = useState(0);
  const [room, setRoom] = useState<Room | null>(null),
    [composer, setComposer] = useState("");
  return (
    <SafeAreaView style={{ flex: 1, backgroundColor: colors.windowBackground }}>
      <StatusBar style={dark ? "light" : "dark"} />
      <KeyboardAvoidingView
        style={{ flex: 1 }}
        behavior={Platform.OS === "ios" ? "padding" : "height"}
      >
        <AppNavigation
          roomId={room?.id || null}
          onBack={() => setRoom(null)}
          onCloseWorkspace={noop}
          onCloseProfile={noop}
          list={() => (
            <ChatListScreen
              user={user}
              rooms={rooms}
              baseUrl=""
              refreshing={false}
              onRefresh={noop}
              onProfilePress={noop}
              onRoomPress={setRoom}
              onCompose={noop}
              onArchive={noop}
              onSearch={noop}
              onRoomMenu={noop}
              mediaUrl={noMedia}
            />
          )}
          chat={() =>
            room ? (
              <ChatScreen
                room={room}
                messages={history.map((m) => ({ ...m, room_id: room.id }))}
                meId={user.id}
                baseUrl=""
                composer={composer}
                onChangeComposer={setComposer}
                typingNames={[]}
                participants={[user, peer]}
                peer={peer}
                connection="connected"
                onActions={setActions}
                onReply={setReply}
                reply={reply}
                selectionReset={selectionReset}
                onCancelReply={() => setReply(null)}
                refreshing={false}
                playingId={null}
                onBack={() => setRoom(null)}
                onRefresh={noop}
                onSend={() => setComposer("")}
                onAttach={noop}
                onVoice={noop}
                onAudioCall={noop}
                onVideoCall={noop}
                onPlayVoice={noop}
                onReact={noop}
                onOpenMedia={noop}
                mediaUrl={noMedia}
                mediaThumbUrl={noMedia}
                onSearch={noop}
                onDetails={noop}
              />
            ) : null
          }
        />
      </KeyboardAvoidingView>
      <MessageActions onPin={async()=>{}} onHide={async()=>{}}
        messages={actions}
        meId={user.id}
        onClose={() => {
          setActions([]);
          setSelectionReset((n) => n + 1);
        }}
        onSelectMore={() => setActions([])}
        onReply={setReply}
        onReact={noop}
        onEdit={noop}
        onForward={noop}
        onDelete={async () => {}}
        onError={noop}
      />
    </SafeAreaView>
  );
}
function Preview() {
  return (
    <SafeAreaProvider>
      <ThemeProvider>
        <Screens />
      </ThemeProvider>
    </SafeAreaProvider>
  );
}
registerRootComponent(Preview);
