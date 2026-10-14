import { Pressable, StyleSheet, Text, TextInput, View } from "react-native";
import { telegramColors, telegramMetrics } from "../theme";

type ComposerProps = {
  value: string;
  onChangeText: (text: string) => void;
  onSend: () => void;
  onAttach?: () => void;
  onVoice?: () => void;
  recording?: boolean;
  disabled?: boolean;
};

export function Composer({
  value,
  onChangeText,
  onSend,
  onAttach,
  onVoice,
  recording,
  disabled,
}: ComposerProps) {
  const hasText = value.trim().length > 0;

  return (
    <View style={styles.container}>
      {onAttach && (
        <Pressable onPress={onAttach} hitSlop={8} style={styles.iconButton} disabled={disabled}>
          <Text style={styles.icon}>📎</Text>
        </Pressable>
      )}

      <TextInput
        style={styles.input}
        value={value}
        onChangeText={onChangeText}
        placeholder="Message"
        placeholderTextColor={telegramColors.composer.hint}
        multiline
        editable={!disabled}
      />

      {hasText ? (
        <Pressable onPress={onSend} hitSlop={8} style={styles.sendButton} disabled={disabled}>
          <Text style={styles.sendIcon}>➤</Text>
        </Pressable>
      ) : onVoice ? (
        <Pressable onPress={onVoice} hitSlop={8} style={styles.iconButton} disabled={disabled}>
          <Text style={[styles.icon, recording && styles.recording]}>🎤</Text>
        </Pressable>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    minHeight: telegramMetrics.composer.minHeight,
    backgroundColor: telegramColors.composer.background,
    flexDirection: "row",
    alignItems: "flex-end",
    paddingHorizontal: telegramMetrics.composer.paddingH,
    paddingVertical: 8,
    gap: 8,
  },
  iconButton: {
    width: telegramMetrics.composer.buttonSize,
    height: telegramMetrics.composer.buttonSize,
    alignItems: "center",
    justifyContent: "center",
  },
  icon: {
    fontSize: 22,
    color: telegramColors.composer.icons,
  },
  recording: {
    color: "#FF0000",
  },
  input: {
    flex: 1,
    minHeight: telegramMetrics.composer.buttonSize,
    maxHeight: 120,
    backgroundColor: telegramColors.windowBackground,
    borderRadius: telegramMetrics.composer.radius,
    paddingHorizontal: 12,
    paddingVertical: 10,
    fontSize: telegramMetrics.font.messageText,
    color: telegramColors.composer.text,
  },
  sendButton: {
    width: telegramMetrics.composer.buttonSize,
    height: telegramMetrics.composer.buttonSize,
    backgroundColor: telegramColors.text.blue,
    borderRadius: telegramMetrics.composer.buttonSize / 2,
    alignItems: "center",
    justifyContent: "center",
  },
  sendIcon: {
    fontSize: 20,
    color: "#FFFFFF",
  },
});
