import { ActivityIndicator, Pressable, StyleSheet, Text, View } from "react-native";
import { useTheme } from "../ThemeProvider";
import { Icon } from "./Icon";

/** Full-screen gate shown while the app is locked. */
export function LockScreen({
  label,
  busy,
  error,
  onUnlock,
}: {
  label: string;
  busy: boolean;
  error?: string | null;
  onUnlock: () => void;
}) {
  const { colors } = useTheme();
  return (
    <View style={[styles.container, { backgroundColor: colors.windowBackground }]}>
      <View style={[styles.badge, { backgroundColor: colors.text.blue }]}>
        <Text style={styles.badgeText}>67</Text>
      </View>
      <Text style={[styles.title, { color: colors.text.primary }]}>Приложение заблокировано</Text>
      <Text style={[styles.subtitle, { color: colors.text.secondary }]}>
        {error || "Подтвердите, что это вы"}
      </Text>
      <Pressable
        onPress={onUnlock}
        disabled={busy}
        accessibilityRole="button"
        accessibilityLabel={label}
        style={[
          styles.button,
          { backgroundColor: colors.text.blue, opacity: busy ? 0.6 : 1 },
        ]}
      >
        {busy ? (
          <ActivityIndicator color={colors.text.onAccent} />
        ) : (
          <>
            <Icon name="settings" size={18} color={colors.text.onAccent} />
            <Text style={[styles.buttonText, { color: colors.text.onAccent }]}>{label}</Text>
          </>
        )}
      </Pressable>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, alignItems: "center", justifyContent: "center", padding: 32, gap: 14 },
  badge: { width: 84, height: 84, borderRadius: 24, alignItems: "center", justifyContent: "center", marginBottom: 8 },
  badgeText: { color: "#FFFFFF", fontSize: 34, fontWeight: "700" },
  title: { fontSize: 20, fontWeight: "600", textAlign: "center" },
  subtitle: { fontSize: 14, textAlign: "center", marginBottom: 12 },
  button: { flexDirection: "row", alignItems: "center", gap: 10, minHeight: 52, paddingHorizontal: 26, borderRadius: 26 },
  buttonText: { fontSize: 16, fontWeight: "600" },
});
