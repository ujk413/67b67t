import {t as uiText} from "../../../web/i18n.js";
import { useTheme } from "../ThemeProvider";
import { Pressable, StyleSheet, Text, View } from "react-native";
import { telegramColors, telegramMetrics } from "../theme";
import { Avatar } from "./Avatar";
import { roomTitle, roomPreview, listTime } from "../format";
import type { Room } from "../types";

type ChatListItemProps = {
  draft?:string;
  room: Room;
  meId: string;
  baseUrl: string;
  onPress: () => void;
  onLongPress?: () => void;
  mediaUrl: (
    baseUrl: string,
    mediaId: string | null | undefined,
  ) => string | null;
};

const AVATAR_SIZE = 50;

export function ChatListItem({
  draft,
  room,
  meId,
  baseUrl,
  onPress,
  onLongPress,
  mediaUrl,
}: ChatListItemProps) {
  const styles = useStyles();
  const { colors: telegramColors } = useTheme();

  const title = roomTitle(room, meId);
  const preview = roomPreview(room, meId);
  const time = listTime(room.last_msg_at);
  const muted = !!room.preferences?.muted;
  const unreadCount =
    room.unread_count || (room.preferences?.marked_unread ? 1 : 0);

  return (
    <Pressable
      style={({ pressed }) => [styles.container, pressed && styles.pressed]}
      onPress={onPress}
      onLongPress={onLongPress}
      accessibilityRole="button"
      accessibilityLabel={
        unreadCount > 0 ? uiText("{0}, непрочитанных: {1}",title,unreadCount) : title
      }
    >
      <Avatar
        room={room}
        meId={meId}
        baseUrl={baseUrl}
        size={AVATAR_SIZE}
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
          <Text style={styles.message} numberOfLines={1}>
            {draft?<><Text style={{color:telegramColors.misc.error}}>{uiText("Черновик:")}</Text>{draft}</>:preview}
          </Text>
          {unreadCount > 0 && (
            <View style={[styles.badge, muted && styles.badgeMuted]}>
              <Text style={styles.badgeText}>
                {unreadCount > 99 ? "99+" : unreadCount}
              </Text>
            </View>
          )}
        </View>
      </View>
    </Pressable>
  );
}

const useStyles = () => {
  const { colors: telegramColors } = useTheme();
  return StyleSheet.create({
    container: {
      minHeight: 68,
      flexDirection: "row",
      alignItems: "center",
      paddingLeft: 12,
      paddingVertical: 6,
      backgroundColor: telegramColors.windowBackground,
    },
    pressed: {
      backgroundColor: telegramColors.chats.selectedBackground,
    },
    content: {
      flex: 1,
      alignSelf: "stretch",
      marginLeft: 12,
      paddingRight: 16,
      justifyContent: "center",
      borderBottomWidth: StyleSheet.hairlineWidth,
      borderBottomColor: telegramColors.divider,
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
      fontSize: 14,
      color: telegramColors.chats.message,
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
    badgeMuted: {
      backgroundColor: telegramColors.chats.unreadCounterMuted,
    },
    badgeText: {
      fontSize: 11,
      fontWeight: "600",
      color: telegramColors.chats.unreadCounterText,
    },
  });
};
