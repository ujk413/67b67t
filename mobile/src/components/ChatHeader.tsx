import { Pressable, StyleSheet, Text, View } from "react-native";
import { telegramColors, telegramMetrics } from "../theme";
import { Avatar } from "./Avatar";
import type { Room } from "../types";

type ChatHeaderProps = {
  room: Room;
  meId: string;
  baseUrl: string;
  subtitle: string;
  onBack: () => void;
  onAudioCall?: () => void;
  onVideoCall?: () => void;
  mediaUrl: (baseUrl: string, mediaId: string | null | undefined) => string | null;
};

export function ChatHeader({
  room,
  meId,
  baseUrl,
  subtitle,
  onBack,
  onAudioCall,
  onVideoCall,
  mediaUrl,
}: ChatHeaderProps) {
  const title = room.title || room.other_display_name || room.other_username || "Chat";

  return (
    <View style={styles.container}>
      <Pressable onPress={onBack} hitSlop={8} style={styles.backButton}>
        <Text style={styles.backIcon}>←</Text>
      </Pressable>

      <Avatar
        room={room}
        meId={meId}
        baseUrl={baseUrl}
        size={40}
        mediaUrl={mediaUrl}
      />

      <View style={styles.info}>
        <Text style={styles.title} numberOfLines={1}>
          {title}
        </Text>
        <Text style={styles.subtitle} numberOfLines={1}>
          {subtitle}
        </Text>
      </View>

      {room.kind === "dm" && (
        <View style={styles.actions}>
          {onAudioCall && (
            <Pressable onPress={onAudioCall} hitSlop={8} style={styles.iconButton}>
              <Text style={styles.icon}>📞</Text>
            </Pressable>
          )}
          {onVideoCall && (
            <Pressable onPress={onVideoCall} hitSlop={8} style={styles.iconButton}>
              <Text style={styles.icon}>📹</Text>
            </Pressable>
          )}
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    height: telegramMetrics.header.height,
    backgroundColor: telegramColors.actionBar,
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 8,
    gap: 8,
  },
  backButton: {
    width: 40,
    height: 40,
    alignItems: "center",
    justifyContent: "center",
  },
  backIcon: {
    fontSize: 24,
    color: telegramColors.actionBarIcon,
  },
  info: {
    flex: 1,
    justifyContent: "center",
  },
  title: {
    fontSize: telegramMetrics.font.headerTitle,
    fontWeight: "600",
    color: telegramColors.actionBarTitle,
  },
  subtitle: {
    fontSize: telegramMetrics.font.headerSubtitle,
    color: telegramColors.actionBarSubtitle,
    marginTop: 1,
  },
  actions: {
    flexDirection: "row",
    gap: 4,
  },
  iconButton: {
    width: 40,
    height: 40,
    alignItems: "center",
    justifyContent: "center",
  },
  icon: {
    fontSize: 20,
  },
});
