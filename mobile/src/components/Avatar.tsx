import { useState } from "react";
import { useMediaHeaders } from "../MediaSession";
import { useTheme } from "../ThemeProvider";
import { Image, StyleSheet, Text, View } from "react-native";
import { avatarColorFor, telegramColors } from "../theme";
import { displayName, roomTitle } from "../format";
import type { Room, User } from "../types";

type AvatarProps = {
  user?: User | null;
  room?: Room | null;
  meId?: string | null;
  baseUrl: string;
  size: number;
  mediaUrl: (
    baseUrl: string,
    mediaId: string | null | undefined,
  ) => string | null;
};

export function Avatar({
  user,
  room,
  meId,
  baseUrl,
  size,
  mediaUrl,
}: AvatarProps) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const headers = useMediaHeaders();
  const styles = useStyles();
  const { colors: telegramColors } = useTheme();

  const mediaId =
    user?.avatar_id || room?.avatar_id || room?.other_avatar_id || null;
  const title = user ? displayName(user) : room ? roomTitle(room, meId) : "?";
  const url = mediaUrl(baseUrl, mediaId);
  const id =
    user?.id ||
    (room?.kind === "dm" ? room.other_user_id : room?.id) ||
    "fallback";

  if (url && failedUrl !== url) {
    return (
      <Image
        source={{ uri: url, headers }}
        onError={() => setFailedUrl(url)}
        style={[
          styles.avatar,
          { width: size, height: size, borderRadius: size / 2 },
        ]}
      />
    );
  }

  const bgColor = avatarColorFor(id);
  return (
    <View
      style={[
        styles.fallback,
        {
          width: size,
          height: size,
          borderRadius: size / 2,
          backgroundColor: bgColor,
        },
      ]}
    >
      <Text style={[styles.text, { fontSize: size * 0.4 }]}>
        {title.slice(0, 1).toUpperCase()}
      </Text>
    </View>
  );
}

const useStyles = () => {
  const { colors: telegramColors } = useTheme();
  return StyleSheet.create({
    avatar: {},
    fallback: {
      alignItems: "center",
      justifyContent: "center",
    },
    text: {
      color: telegramColors.text.onAccent,
      fontWeight: "700",
    },
  });
};
