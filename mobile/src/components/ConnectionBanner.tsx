import { Pressable, Text, View } from "react-native";
import { useTheme } from "../ThemeProvider";
export function ConnectionBanner({
  status,
  error,
  onDismiss,
}: {
  status: string;
  error: string | null;
  onDismiss: () => void;
}) {
  const { colors } = useTheme();
  if (!error && status === "connected") return null;
  return (
    <View
      accessibilityLiveRegion="polite"
      style={{
        padding: 10,
        backgroundColor: error
          ? colors.misc.error
          : colors.windowBackgroundGray,
        flexDirection: "row",
        alignItems: "center",
      }}
    >
      <Text style={{ flex: 1, color: error ? "white" : colors.text.primary }}>
        {error ||
          {
            connecting: "Подключение…",
            offline: "Нет соединения",
            connected: "",
          }[status] ||
          "Подключение…"}
      </Text>
      {error && (
        <Pressable
          onPress={onDismiss}
          accessibilityRole="button"
          accessibilityLabel="Закрыть ошибку"
          style={{
            minWidth: 48,
            minHeight: 48,
            alignItems: "center",
            justifyContent: "center",
          }}
        >
          <Text style={{ color: "white" }}>Закрыть</Text>
        </Pressable>
      )}
    </View>
  );
}
