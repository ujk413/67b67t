import { Pressable, StyleSheet, Text, View } from "react-native";
import { telegramColors, telegramMetrics } from "../theme";
import { Avatar } from "./Avatar";
import { displayName } from "../format";
import type { User } from "../types";

type MainHeaderProps = {
  user: User;
  baseUrl: string;
  onProfilePress: () => void;
  mediaUrl: (baseUrl: string, mediaId: string | null | undefined) => string | null;
};

export function MainHeader({ user, baseUrl, onProfilePress, mediaUrl }: MainHeaderProps) {
  return (
    <View style={styles.container}>
      <Pressable onPress={onProfilePress} style={styles.profile}>
        <Avatar user={user} baseUrl={baseUrl} size={40} mediaUrl={mediaUrl} />
        <View style={styles.info}>
          <Text style={styles.name} numberOfLines={1}>
            {displayName(user)}
          </Text>
          <Text style={styles.username} numberOfLines={1}>
            @{user.username}
          </Text>
        </View>
      </Pressable>

      <View style={styles.actions}>
        <Pressable hitSlop={8} style={styles.iconButton}>
          <Text style={styles.icon}>🔍</Text>
        </Pressable>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    height: telegramMetrics.header.height,
    backgroundColor: telegramColors.actionBar,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 8,
  },
  profile: {
    flex: 1,
    flexDirection: "row",
    alignItems: "center",
    gap: 12,
  },
  info: {
    flex: 1,
    justifyContent: "center",
  },
  name: {
    fontSize: telegramMetrics.font.headerTitle,
    fontWeight: "600",
    color: telegramColors.actionBarTitle,
  },
  username: {
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
    fontSize: 22,
    color: telegramColors.actionBarIcon,
  },
});
