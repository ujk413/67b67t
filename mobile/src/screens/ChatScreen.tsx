import { FlatList, RefreshControl, StyleSheet, Text, View } from "react-native";
import { telegramColors } from "../theme";
import { ChatHeader } from "../components/ChatHeader";
import { Composer } from "../components/Composer";
import { MessageBubble } from "../components/MessageBubble";
import type { Message, Room, User } from "../types";

type ChatScreenProps = {
  room: Room;
  messages: Message[];
  meId: string;
  baseUrl: string;
  composer: string;
  typingNames: string[];
  refreshing: boolean;
  recording?: boolean;
  playingId: string | null;
  disabled?: boolean;
  onBack: () => void;
  onRefresh: () => void;
  onChangeComposer: (text: string) => void;
  onSend: () => void;
  onAttach?: () => void;
  onVoice?: () => void;
  onAudioCall?: () => void;
  onVideoCall?: () => void;
  onPlayVoice: (message: Message) => void;
  onReact: (message: Message) => void;
  onOpenMedia: (message: Message) => void;
  mediaUrl: (baseUrl: string, mediaId: string | null | undefined) => string | null;
  mediaThumbUrl: (baseUrl: string, mediaId: string | null | undefined) => string | null;
};

export function ChatScreen({
  room,
  messages,
  meId,
  baseUrl,
  composer,
  typingNames,
  refreshing,
  recording,
  playingId,
  disabled,
  onBack,
  onRefresh,
  onChangeComposer,
  onSend,
  onAttach,
  onVoice,
  onAudioCall,
  onVideoCall,
  onPlayVoice,
  onReact,
  onOpenMedia,
  mediaUrl,
  mediaThumbUrl,
}: ChatScreenProps) {
  const subtitle = room.kind === "dm" ? `@${room.other_username || "user"}` : room.title || "Group";

  return (
    <View style={styles.container}>
      <ChatHeader
        room={room}
        meId={meId}
        baseUrl={baseUrl}
        subtitle={subtitle}
        onBack={onBack}
        onAudioCall={onAudioCall}
        onVideoCall={onVideoCall}
        mediaUrl={mediaUrl}
      />

      <View style={styles.wallpaper}>
        <FlatList
          data={messages}
          keyExtractor={(item) => item.id}
          renderItem={({ item, index }) => {
            const prevMessage = index > 0 ? messages[index - 1] : null;
            const showAvatar = !prevMessage || prevMessage.sender_id !== item.sender_id;

            return (
              <MessageBubble
                message={item}
                meId={meId}
                baseUrl={baseUrl}
                playing={playingId === item.id}
                showAvatar={showAvatar}
                onPlayVoice={() => onPlayVoice(item)}
                onReact={() => onReact(item)}
                onOpenMedia={onOpenMedia}
                mediaUrl={mediaUrl}
                mediaThumbUrl={mediaThumbUrl}
              />
            );
          }}
          refreshControl={
            <RefreshControl
              refreshing={refreshing}
              onRefresh={onRefresh}
              tintColor={telegramColors.misc.progress}
            />
          }
          contentContainerStyle={styles.messagesList}
        />

        {typingNames.length > 0 && (
          <View style={styles.typingContainer}>
            <Text style={styles.typingText}>
              {typingNames.join(", ")} печатает…
            </Text>
          </View>
        )}
      </View>

      <Composer
        value={composer}
        onChangeText={onChangeComposer}
        onSend={onSend}
        onAttach={onAttach}
        onVoice={onVoice}
        recording={recording}
        disabled={disabled}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: telegramColors.windowBackground,
  },
  wallpaper: {
    flex: 1,
    backgroundColor: telegramColors.chat.wallpaper,
  },
  messagesList: {
    padding: 8,
  },
  typingContainer: {
    paddingHorizontal: 12,
    paddingVertical: 6,
  },
  typingText: {
    fontSize: 13,
    color: telegramColors.text.gray5,
    fontStyle: "italic",
  },
});
