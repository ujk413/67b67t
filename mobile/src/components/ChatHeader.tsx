import { useTheme } from "../ThemeProvider";
import { Icon } from "./Icon";
import { Pressable, StyleSheet, Text, View } from "react-native";
import { telegramColors, telegramMetrics } from "../theme";
import { Avatar } from "./Avatar";
import type { Room } from "../types";

type ChatHeaderProps = {
  room: Room;
  meId: string;
  baseUrl: string;
  subtitle: string;
  online?: boolean;
  onMenuPress?: () => void;
  onBack: () => void;
  onDetails?: () => void;
  onAudioCall?: () => void;
  onVideoCall?: () => void;
  mediaUrl: (
    baseUrl: string,
    mediaId: string | null | undefined,
  ) => string | null;
};

export function ChatHeader({
  room,
  meId,
  baseUrl,
  subtitle,
  online,
  onMenuPress,
  onBack,
  onDetails,
  onAudioCall,
  onVideoCall,
  mediaUrl,
}: ChatHeaderProps) {
  const styles = useStyles();
  const { colors: telegramColors } = useTheme();

  const title =
    room.is_saved ? 'Избранное' : room.title || room.other_display_name || room.other_username || "Переписка";

  return (
    <View style={styles.container}>
      <Pressable
        onPress={onBack}
        accessibilityRole="button"
        accessibilityLabel="Назад к чатам"
        hitSlop={8}
        style={styles.backButton}
      >
        <Icon name="back" />
      </Pressable>

      <Avatar
        room={room}
        meId={meId}
        baseUrl={baseUrl}
        size={36}
        mediaUrl={mediaUrl}
      />

      <Pressable
        style={styles.info}
        onPress={onDetails}
        accessibilityRole="button"
        accessibilityLabel="Сведения о переписке"
      >
        <Text style={styles.title} numberOfLines={1}>
          {title}
        </Text>
        <Text
          style={[
            styles.subtitle,
            online && { color: telegramColors.text.blue },
          ]}
          numberOfLines={1}
        >
          {subtitle}
        </Text>
      </Pressable>

      <View style={styles.actions}>
        {room.kind === "dm" && (
          <View style={styles.actions}>
            {onAudioCall && (
              <Pressable
                onPress={onAudioCall}
                accessibilityRole="button"
                accessibilityLabel="Аудиозвонок"
                hitSlop={8}
                style={styles.iconButton}
              >
                <View style={{ transform: [{ translateX: 4 }] }}>
                  <Icon name="phone" size={23} />
                </View>
              </Pressable>
            )}
            {onVideoCall && (
              <Pressable
                onPress={onVideoCall}
                accessibilityRole="button"
                accessibilityLabel="Видеозвонок"
                hitSlop={8}
                style={styles.iconButton}
              >
                <View style={{ transform: [{ translateX: -4 }] }}>
                  <Icon name="video" size={23} />
                </View>
              </Pressable>
            )}
          </View>
        )}
        {onMenuPress && (
          <Pressable
            onPress={onMenuPress}
            style={styles.iconButton}
            accessibilityRole="button"
            accessibilityLabel="Меню переписки"
          >
            <Icon name="more" size={22} />
          </Pressable>
        )}
      </View>
    </View>
  );
}

const useStyles = () => {
  const { colors: telegramColors } = useTheme();
  return StyleSheet.create({
    container: {
      height: telegramMetrics.header.height,
      backgroundColor: telegramColors.actionBar,
      flexDirection: "row",
      alignItems: "center",
      paddingHorizontal: 8,
      gap: 4,
    },
    backButton: {
      width: 48,
      height: 48,
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
      fontSize: 17,
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
      gap: 0,
    },
    iconButton: {
      width: 48,
      height: 48,
      alignItems: "center",
      justifyContent: "center",
    },
    icon: {
      fontSize: 20,
    },
  });
};
