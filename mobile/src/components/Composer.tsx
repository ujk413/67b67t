import {t as uiText} from "../../../web/i18n.js";
import { useState } from "react";
import { VoiceRecorder } from "./VoiceRecorder";
import type { AccountRepository } from "../data/repository";
import { useTheme } from "../ThemeProvider";
import { Icon } from "./Icon";
import { Pressable, StyleSheet, Text, TextInput, View } from "react-native";
import { telegramColors, telegramMetrics } from "../theme";

type ComposerProps = {
  repository?: AccountRepository | null;
  roomId?: string;
  onVoiceRecorded?: (uri: string, duration: number, waveform?:number[]) => Promise<void>;
  value: string;
  onChangeText: (text: string) => void;
  onSend: () => void;
  onAttach?: () => void;
  onVideoNote?: () => void;
  onVoice?: () => void;
  recording?: boolean;
  disabled?: boolean;
};

export function Composer({
  repository,
  roomId,
  onVoiceRecorded,
  value,
  onChangeText,
  onSend,
  onAttach,
  onVideoNote,
  onVoice,
  recording,
  disabled,
}: ComposerProps) {
  const styles = useStyles();
  const { colors: telegramColors, fontSize } = useTheme();

  const [recordingExpanded, setRecordingExpanded] = useState(false);
  const hasText = value.trim().length > 0;

  return (
    <View style={styles.container}>
      {onAttach && !recordingExpanded && (
        <Pressable
          onPress={onAttach}
          accessibilityRole="button"
          accessibilityLabel={uiText("Добавить вложение")}
          hitSlop={8}
          style={styles.iconButton}
          disabled={disabled}
        >
          <Icon name="attach" />
        </Pressable>
      )}

      <TextInput
        style={[styles.input, { fontSize, display: recordingExpanded ? "none" : "flex" }]}
        value={value}
        onChangeText={onChangeText}
        placeholder={uiText("Сообщение")}
        placeholderTextColor={telegramColors.composer.hint}
        multiline
        editable={!disabled}
      />

      {hasText && !recordingExpanded ? (
        <Pressable
          onPress={onSend}
          accessibilityRole="button"
          accessibilityLabel={uiText("Отправить сообщение")}
          hitSlop={8}
          style={styles.sendButton}
          disabled={disabled}
        >
          <Icon name="send" color={telegramColors.text.onAccent} />
        </Pressable>
      ) : onVoiceRecorded && roomId ? (
        <VoiceRecorder
          key={`${repository?.userId}:${roomId}`}
          onModeChange={setRecordingExpanded}
          onSend={onVoiceRecorded}
          repository={repository}
          roomId={roomId}
          disabled={disabled}
        />
      ) : onVoice ? (
        <Pressable
          onPress={onVoice}
          hitSlop={8}
          style={styles.iconButton}
          disabled={disabled}
        >
          <Icon
            name="mic"
            color={
              recording
                ? telegramColors.misc.error
                : telegramColors.composer.icons
            }
          />
        </Pressable>
      ) : null}
      {!hasText && !recordingExpanded && onVideoNote && <Pressable onPress={onVideoNote} accessibilityRole="button" accessibilityLabel={uiText("Записать видеокружок")} style={styles.iconButton}><Icon name="video" /></Pressable>}
    </View>
  );
}

const useStyles = () => {
  const { colors: telegramColors } = useTheme();
  return StyleSheet.create({
    container: {
      minHeight: telegramMetrics.composer.minHeight,
      backgroundColor: telegramColors.composer.background,
      flexDirection: "row",
      alignItems: "flex-end",
      paddingHorizontal: telegramMetrics.composer.paddingH,
      paddingVertical: 4,
      gap: 2,
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
      color: telegramColors.misc.error,
    },
    input: {
      flex: 1,
      minHeight: 40,
      alignSelf: "center",
      maxHeight: 112,
      backgroundColor: telegramColors.composer.inputBackground,
      borderRadius: telegramMetrics.composer.radius,
      paddingHorizontal: 12,
      paddingVertical: 8,
      includeFontPadding: false,
      textAlignVertical: "center",
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
      color: telegramColors.text.onAccent,
    },
  });
};
