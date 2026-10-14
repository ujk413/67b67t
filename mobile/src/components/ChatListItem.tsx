import { Pressable, StyleSheet, Text, View } from "react-native";
import { telegramColors, telegramMetrics } from "../theme";
import { Avatar } from "./Avatar";
import { roomTitle, roomPreview, shortTime } from "../format";
import type { Room } from "../types";

type ChatListItemProps = {
  room: Room;
  meId: string;
  baseUrl: string;
  onPress: () => void;
  mediaUrl: (baseUrl: string, mediaId: string | null | undefined) => string | null;
};

export function ChatListItem({ room, meId, baseUrl, onPress, mediaUrl }: ChatListItemProps) {
  const title = roomTitle(room, meId);
  const preview = roomPreview(room, meId);
  const time = shortTime(room.last_msg_at);
  const unreadCount = room.unread_count || 0;

  return (
    <Pressable
      style={({ pressed }) => [
        styles.container,
        pressed && styles.pressed,
      ]}
      onPress={onPress}
    >
      <Avatar
        room={room}
        meId={meId}
        baseUrl={baseUrl}
        size={telegramMetrics.row.avatarSize}
        mediaUrl={mediaUrl}
      />
      <View style={styles.content}>
        <View style={styles.topRow}>
          <Text style={styles.name} numberOfLines={1}>
            {title}
          </Text>
          <Text style={styles.date}>{time}</Text>
        </View>
        <View style={styles.bottomRow}>
          <Text style={styles.message} numberOfLines={2}>
            {preview}
          </Text>
          {unreadCount > 0 && (
            <View style={styles.badge}>
              <Text style={styles.badgeText}>{unreadCount}</Text>
            </View>
          )}
        </View>
      </View>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  container: {
    height: telegramMetrics.row.height,
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: telegramMetrics.row.paddingH,
    paddingVertical: telegramMetrics.row.paddingV,
    backgroundColor: telegramColors.windowBackground,
  },
  pressed: {
    backgroundColor: telegramColors.chats.selectedBackground,
  },
  content: {
    flex: 1,
    marginLeft: telegramMetrics.row.textLeft,
    justifyContent: "center",
  },
  topRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    marginBottom: 4,
  },
  name: {
    flex: 1,
    fontSize: telegramMetrics.font.chatName,
    fontWeight: "600",
    color: telegramColors.chats.name,
  },
  date: {
    fontSize: telegramMetrics.font.chatDate,
    color: telegramColors.chats.date,
    marginLeft: 8,
  },
  bottomRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  message: {
    flex: 1,
    fontSize: telegramMetrics.font.chatMessage,
    color: telegramColors.chats.message,
    lineHeight: 20,
  },
  badge: {
    minWidth: 20,
    height: 20,
    borderRadius: 10,
    backgroundColor: telegramColors.chats.unreadCounter,
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: 6,
    marginLeft: 8,
  },
  badgeText: {
    fontSize: 12,
    fontWeight: "700",
    color: telegramColors.chats.unreadCounterText,
  },
});
