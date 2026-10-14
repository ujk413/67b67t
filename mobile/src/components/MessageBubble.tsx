import { Image, Pressable, StyleSheet, Text, View } from "react-native";
import { Video, ResizeMode } from "expo-av";
import { telegramColors, telegramMetrics, nameColorFor } from "../theme";
import { Avatar } from "./Avatar";
import { shortTime } from "../format";
import type { Message } from "../types";

type MessageBubbleProps = {
  message: Message;
  meId: string;
  baseUrl: string;
  playing?: boolean;
  showAvatar?: boolean;
  onPlayVoice?: () => void;
  onReact?: () => void;
  onOpenMedia?: (message: Message) => void;
  mediaUrl: (baseUrl: string, mediaId: string | null | undefined) => string | null;
  mediaThumbUrl: (baseUrl: string, mediaId: string | null | undefined) => string | null;
};

export function MessageBubble({
  message,
  meId,
  baseUrl,
  playing,
  showAvatar,
  onPlayVoice,
  onReact,
  onOpenMedia,
  mediaUrl,
  mediaThumbUrl,
}: MessageBubbleProps) {
  const isOwn = message.sender_id === meId;
  const url = mediaUrl(baseUrl, message.media_id);
  const thumb = mediaThumbUrl(baseUrl, message.media_id) || url;
  const senderName = message.sender_display_name || message.sender_username || "User";
  const nameColor = nameColorFor(message.sender_id);
  const time = shortTime(message.created_at);
  const read = isOwn && message.read_by && message.read_by.length > 1;

  return (
    <View style={[styles.wrapper, isOwn ? styles.wrapperOwn : styles.wrapperOther]}>
      {showAvatar && !isOwn && (
        <Avatar
          user={{ id: message.sender_id, username: message.sender_username || "", display_name: message.sender_display_name }}
          baseUrl={baseUrl}
          size={telegramMetrics.bubble.avatarSize}
          mediaUrl={mediaUrl}
        />
      )}
      {showAvatar && !isOwn && <View style={{ width: telegramMetrics.bubble.avatarGap }} />}
      {!showAvatar && !isOwn && <View style={{ width: telegramMetrics.bubble.avatarSize + telegramMetrics.bubble.avatarGap }} />}

      <View
        style={[
          styles.bubble,
          isOwn ? styles.bubbleOwn : styles.bubbleIn,
          {
            maxWidth: `${telegramMetrics.bubble.maxWidthRatio * 100}%`,
          },
        ]}
      >
        {!isOwn && (
          <Text style={[styles.senderName, { color: nameColor }]} numberOfLines={1}>
            {senderName}
          </Text>
        )}

        {message.reply_to?.text && (
          <View style={[styles.replyContainer, isOwn ? styles.replyOwn : styles.replyIn]}>
            <View style={[styles.replyLine, isOwn ? styles.replyLineOwn : styles.replyLineIn]} />
            <View style={styles.replyContent}>
              <Text style={styles.replyName} numberOfLines={1}>
                {message.reply_to.sender_display_name || message.reply_to.sender_username || "User"}
              </Text>
              <Text style={styles.replyText} numberOfLines={2}>
                {message.reply_to.text}
              </Text>
            </View>
          </View>
        )}

        <MessageContent
          message={message}
          url={url}
          thumb={thumb}
          playing={playing}
          onPlayVoice={onPlayVoice}
          onOpenMedia={onOpenMedia}
          isOwn={isOwn}
        />

        <View style={styles.metaRow}>
          <Text style={[styles.time, isOwn ? styles.timeOwn : styles.timeIn]}>
            {time}
            {read ? " ✓✓" : isOwn ? " ✓" : ""}
          </Text>
          {onReact && (
            <Pressable onPress={onReact} hitSlop={8}>
              <Text style={styles.reactButton}>+</Text>
            </Pressable>
          )}
        </View>

        {message.reactions && <ReactionBar reactions={message.reactions} />}
      </View>
    </View>
  );
}

function MessageContent({
  message,
  url,
  thumb,
  playing,
  onPlayVoice,
  onOpenMedia,
  isOwn,
}: {
  message: Message;
  url: string | null;
  thumb: string | null;
  playing?: boolean;
  onPlayVoice?: () => void;
  onOpenMedia?: (message: Message) => void;
  isOwn: boolean;
}) {
  const textColor = isOwn ? telegramColors.chat.messageTextOut : telegramColors.chat.messageTextIn;

  if (message.type === "text") {
    return <Text style={[styles.text, { color: textColor }]}>{message.text || ""}</Text>;
  }

  if (message.type === "voice" && onPlayVoice) {
    return (
      <Pressable style={styles.voiceButton} onPress={onPlayVoice}>
        <Text style={[styles.voiceText, { color: textColor }]}>
          {playing ? "⏸ Stop" : "▶️ Play"}
          {message.duration_sec ? ` ${Math.round(message.duration_sec)}s` : ""}
        </Text>
      </Pressable>
    );
  }

  if (message.type === "video_note" && url && onOpenMedia) {
    return (
      <Pressable onPress={() => onOpenMedia(message)}>
        <Video
          source={{ uri: url }}
          style={styles.videoNote}
          useNativeControls={false}
          resizeMode={ResizeMode.COVER}
          isLooping
        />
      </Pressable>
    );
  }

  if (message.type === "media" && url && onOpenMedia) {
    const mime = message.media_mime || "";
    if (mime.startsWith("image/")) {
      return (
        <Pressable onPress={() => onOpenMedia(message)}>
          <Image source={{ uri: thumb || url }} style={styles.mediaImage} resizeMode="cover" />
        </Pressable>
      );
    }
    if (mime.startsWith("video/")) {
      return (
        <Pressable onPress={() => onOpenMedia(message)}>
          <Video
            source={{ uri: url }}
            style={styles.mediaVideo}
            useNativeControls
            resizeMode={ResizeMode.CONTAIN}
          />
        </Pressable>
      );
    }
    return (
      <Pressable onPress={() => onOpenMedia(message)} style={styles.fileCard}>
        <Text style={[styles.fileTitle, { color: textColor }]}>{message.text || "File"}</Text>
        <Text style={styles.fileMime}>{mime || "unknown"}</Text>
      </Pressable>
    );
  }

  return <Text style={[styles.text, { color: textColor }]}>[{message.type}]</Text>;
}

function ReactionBar({ reactions }: { reactions: Record<string, string[]> }) {
  const items = Object.entries(reactions).filter(([, users]) => users.length > 0);
  if (!items.length) return null;

  return (
    <View style={styles.reactions}>
      {items.map(([emoji, users]) => (
        <View key={emoji} style={styles.reactionChip}>
          <Text style={styles.reactionText}>
            {emoji} {users.length}
          </Text>
        </View>
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  wrapper: {
    width: "100%",
    flexDirection: "row",
    marginVertical: 1,
  },
  wrapperOwn: {
    justifyContent: "flex-end",
  },
  wrapperOther: {
    justifyContent: "flex-start",
  },
  bubble: {
    borderRadius: telegramMetrics.bubble.radius,
    paddingHorizontal: telegramMetrics.bubble.paddingH,
    paddingVertical: telegramMetrics.bubble.paddingV,
  },
  bubbleIn: {
    backgroundColor: telegramColors.chat.inBubble,
  },
  bubbleOwn: {
    backgroundColor: telegramColors.chat.outBubble,
  },
  senderName: {
    fontSize: telegramMetrics.font.messageName,
    fontWeight: "600",
    marginBottom: 2,
  },
  replyContainer: {
    flexDirection: "row",
    marginBottom: 6,
    borderRadius: 6,
    padding: 6,
  },
  replyIn: {
    backgroundColor: "rgba(255,255,255,0.05)",
  },
  replyOwn: {
    backgroundColor: "rgba(0,0,0,0.1)",
  },
  replyLine: {
    width: 2,
    borderRadius: 1,
    marginRight: 6,
  },
  replyLineIn: {
    backgroundColor: telegramColors.chat.inReplyLine,
  },
  replyLineOwn: {
    backgroundColor: telegramColors.chat.outReplyLine,
  },
  replyContent: {
    flex: 1,
  },
  replyName: {
    fontSize: 13,
    fontWeight: "600",
    color: telegramColors.chat.inReplyName,
    marginBottom: 2,
  },
  replyText: {
    fontSize: 13,
    color: telegramColors.chat.inReplyMessage,
  },
  text: {
    fontSize: telegramMetrics.font.messageText,
    lineHeight: 20,
  },
  metaRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    marginTop: 4,
  },
  time: {
    fontSize: telegramMetrics.font.messageTime,
  },
  timeIn: {
    color: telegramColors.chat.inTime,
  },
  timeOwn: {
    color: telegramColors.chat.outTime,
  },
  reactButton: {
    fontSize: 14,
    color: telegramColors.text.gray5,
    marginLeft: 8,
  },
  reactions: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 4,
    marginTop: 4,
  },
  reactionChip: {
    backgroundColor: "rgba(255,255,255,0.1)",
    borderRadius: 10,
    paddingHorizontal: 8,
    paddingVertical: 2,
  },
  reactionText: {
    fontSize: 12,
    color: telegramColors.text.primary,
  },
  voiceButton: {
    paddingVertical: 8,
  },
  voiceText: {
    fontSize: 14,
    fontWeight: "600",
  },
  videoNote: {
    width: 180,
    height: 180,
    borderRadius: 90,
    backgroundColor: "#000",
  },
  mediaImage: {
    width: 240,
    height: 240,
    borderRadius: 8,
    backgroundColor: "#000",
  },
  mediaVideo: {
    width: 280,
    height: 200,
    borderRadius: 8,
    backgroundColor: "#000",
  },
  fileCard: {
    padding: 10,
    borderRadius: 8,
    backgroundColor: "rgba(255,255,255,0.05)",
  },
  fileTitle: {
    fontSize: 14,
    fontWeight: "600",
  },
  fileMime: {
    fontSize: 12,
    color: telegramColors.text.gray5,
    marginTop: 2,
  },
});
