import { ActivityIndicator, Modal, Pressable, Text, View } from "react-native";
import { useTheme } from "../ThemeProvider";
import type { UpdateState } from "../hooks/useAppUpdate";

type Props = {
  state: UpdateState;
  onUpdate: () => void;
  onDismiss: () => void;
  onRetryInstall: () => void;
  onOpenPermissionSettings: () => void;
};

function percent(progress: number) {
  if (progress < 0) return "";
  return `${Math.round(progress * 100)}%`;
}

function megabytes(bytes: number) {
  if (!bytes) return "";
  return `${Math.round(bytes / (1024 * 1024))} МБ`;
}

/**
 * Non-blocking strip above the chat, plus a modal for download progress and
 * errors. Mirrors ConnectionBanner so the two read as one system.
 */
export function UpdateBanner({
  state,
  onUpdate,
  onDismiss,
  onRetryInstall,
  onOpenPermissionSettings,
}: Props) {
  const { colors } = useTheme();
  if (state.phase === "idle") return null;

  const release = state.release;
  const label =
    state.phase === "available"
      ? `Доступна версия ${release.version || release.versionCode}`
      : state.phase === "downloading"
        ? `Загрузка обновления… ${percent(state.progress)}`
        : state.phase === "ready" ? "Файл готов. Можно повторить установку."
        : state.phase === "installing"
          ? "Подтвердите установку в Android или вернитесь в приложение."
          : "Не удалось обновить";

  if (state.phase === "available") {
    return (
      <View
        accessibilityLiveRegion="polite"
        style={{
          flexDirection: "row",
          alignItems: "center",
          gap: 8,
          paddingHorizontal: 12,
          paddingVertical: 8,
          backgroundColor: colors.chats.searchBackground,
        }}
      >
        <Text style={{ flex: 1, color: colors.text.primary }} numberOfLines={2}>
          {label}
        </Text>
        <Pressable
          onPress={onUpdate}
          accessibilityRole="button"
          accessibilityLabel="Обновить приложение"
          style={{
            minHeight: 40,
            paddingHorizontal: 14,
            justifyContent: "center",
            borderRadius: 20,
            backgroundColor: colors.text.blue,
          }}
        >
          <Text style={{ color: colors.text.onAccent, fontWeight: "600" }}>
            Обновить
          </Text>
        </Pressable>
        {!release.mandatory && (
          <Pressable
            onPress={onDismiss}
            accessibilityRole="button"
            accessibilityLabel="Отложить обновление"
            style={{ minWidth: 40, minHeight: 40, alignItems: "center", justifyContent: "center" }}
          >
            <Text style={{ color: colors.text.secondary }}>✕</Text>
          </Pressable>
        )}
      </View>
    );
  }

  // Download / install / error: a modal so progress cannot be missed and the
  // installer intent is clearly attributed to a user action.
  return (
    <Modal transparent animationType="fade" visible onRequestClose={onDismiss}>
      <View
        style={{
          flex: 1,
          alignItems: "center",
          justifyContent: "center",
          padding: 24,
          backgroundColor: colors.misc.backdrop,
        }}
      >
        <View
          style={{
            width: "100%",
            maxWidth: 360,
            padding: 20,
            gap: 14,
            borderRadius: 18,
            backgroundColor: colors.windowBackground,
          }}
        >
          <Text style={{ fontSize: 17, fontWeight: "600", color: colors.text.primary }}>
            {state.phase === "error" ? "Обновление" : "Установка обновления"}
          </Text>

          <Text style={{ color: colors.text.secondary }}>
            {label}
            {state.phase === "downloading" && release.size
              ? ` · ${megabytes(release.size)}`
              : ""}
          </Text>

          {state.phase === "downloading" && (
            <ActivityIndicator color={colors.text.blue} />
          )}

          {state.phase === "error" && (
            <Text style={{ color: colors.misc.error }} accessibilityLiveRegion="polite">
              {state.needsPermission
                ? "Разрешите установку приложений из этого источника, затем повторите."
                : state.message === "checksum_mismatch"
                  ? "Проверка файла не пройдена. Повреждённый APK удалён. Скачайте обновление ещё раз."
                : state.message === "invalid_release"
                  ? "На сервере опубликованы неполные данные обновления. Повторите проверку позже."
                : state.message === "native_update_unavailable"
                  ? "Для этой сборки нужна ручная установка новой APK поверх приложения. Данные сохранятся."
                : state.message === "incomplete_download"
                  ? "Файл загрузился не полностью. Проверьте соединение и попробуйте снова."
                  : "Не удалось скачать обновление. Проверьте соединение и попробуйте снова."}
            </Text>
          )}

          <View style={{ flexDirection: "row", justifyContent: "flex-end", gap: 10 }}>
            <Pressable onPress={onDismiss} accessibilityRole="button" style={{minHeight:44,paddingHorizontal:12,justifyContent:'center'}}>
              <Text style={{color:colors.text.blue}}>{state.phase === 'downloading' ? 'Отменить' : 'Закрыть'}</Text>
            </Pressable>
            {state.phase === "error" && state.needsPermission && (
              <Pressable
                onPress={onOpenPermissionSettings}
                accessibilityRole="button"
                style={{ minHeight: 44, paddingHorizontal: 12, justifyContent: "center" }}
              >
                <Text style={{ color: colors.text.blue }}>Настройки</Text>
              </Pressable>
            )}
            {(state.phase === "error" || state.phase === "ready") && (
              <Pressable
                onPress={onRetryInstall}
                accessibilityRole="button"
                style={{
                  minHeight: 44,
                  paddingHorizontal: 16,
                  justifyContent: "center",
                  borderRadius: 22,
                  backgroundColor: colors.text.blue,
                }}
              >
                <Text style={{ color: colors.text.onAccent, fontWeight: "600" }}>
                  Повторить
                </Text>
              </Pressable>
            )}
          </View>
        </View>
      </View>
    </Modal>
  );
}
