import {
  ActivityIndicator,
  Modal,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import { telegramColors, telegramMetrics } from "../theme";
import { Avatar } from "../components/Avatar";
import { displayName } from "../format";
import type { User } from "../types";

type ProfileScreenProps = {
  visible: boolean;
  user: User;
  baseUrl: string;
  profileName: string;
  newPassword: string;
  busyUpload: boolean;
  onClose: () => void;
  onChangeName: (text: string) => void;
  onChangePassword: (text: string) => void;
  onSave: () => void;
  onPickAvatar: () => void;
  mediaUrl: (baseUrl: string, mediaId: string | null | undefined) => string | null;
};

export function ProfileScreen({
  visible,
  user,
  baseUrl,
  profileName,
  newPassword,
  busyUpload,
  onClose,
  onChangeName,
  onChangePassword,
  onSave,
  onPickAvatar,
  mediaUrl,
}: ProfileScreenProps) {
  return (
    <Modal visible={visible} animationType="slide" onRequestClose={onClose}>
      <View style={styles.container}>
        <View style={styles.header}>
          <Pressable onPress={onClose} hitSlop={8}>
            <Text style={styles.closeIcon}>✕</Text>
          </Pressable>
          <Text style={styles.title}>Профиль</Text>
          <View style={{ width: 32 }} />
        </View>

        <ScrollView contentContainerStyle={styles.content}>
          <Avatar user={user} baseUrl={baseUrl} size={96} mediaUrl={mediaUrl} />

          <Pressable onPress={onPickAvatar} disabled={busyUpload} style={styles.avatarButton}>
            {busyUpload ? (
              <ActivityIndicator size="small" color={telegramColors.text.blue} />
            ) : (
              <Text style={styles.avatarButtonText}>Изменить аватар</Text>
            )}
          </Pressable>

          <View style={styles.section}>
            <Text style={styles.label}>Имя</Text>
            <TextInput
              style={styles.input}
              value={profileName}
              onChangeText={onChangeName}
              placeholder={displayName(user)}
              placeholderTextColor={telegramColors.text.gray5}
            />
          </View>

          <View style={styles.section}>
            <Text style={styles.label}>Новый пароль</Text>
            <TextInput
              style={styles.input}
              value={newPassword}
              onChangeText={onChangePassword}
              placeholder="Оставьте пустым, чтобы не менять"
              placeholderTextColor={telegramColors.text.gray5}
              secureTextEntry
            />
          </View>

          <View style={styles.section}>
            <Text style={styles.username}>@{user.username}</Text>
          </View>

          <Pressable style={styles.saveButton} onPress={onSave}>
            <Text style={styles.saveButtonText}>Сохранить</Text>
          </Pressable>
        </ScrollView>
      </View>
    </Modal>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: telegramColors.windowBackground,
  },
  header: {
    height: telegramMetrics.header.height,
    backgroundColor: telegramColors.actionBar,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 16,
  },
  closeIcon: {
    fontSize: 24,
    color: telegramColors.actionBarIcon,
  },
  title: {
    fontSize: telegramMetrics.font.headerTitle,
    fontWeight: "600",
    color: telegramColors.actionBarTitle,
  },
  content: {
    alignItems: "center",
    padding: 24,
    gap: 20,
  },
  avatarButton: {
    paddingVertical: 10,
    paddingHorizontal: 16,
    minHeight: 40,
    alignItems: "center",
    justifyContent: "center",
  },
  avatarButtonText: {
    fontSize: 15,
    fontWeight: "600",
    color: telegramColors.text.blue,
  },
  section: {
    width: "100%",
    gap: 8,
  },
  label: {
    fontSize: 14,
    fontWeight: "600",
    color: telegramColors.text.gray5,
    marginLeft: 4,
  },
  input: {
    width: "100%",
    backgroundColor: telegramColors.composer.background,
    borderRadius: 8,
    paddingHorizontal: 16,
    paddingVertical: 14,
    fontSize: telegramMetrics.font.messageText,
    color: telegramColors.text.primary,
  },
  username: {
    fontSize: 16,
    color: telegramColors.text.gray5,
    textAlign: "center",
  },
  saveButton: {
    width: "100%",
    backgroundColor: telegramColors.text.blue,
    borderRadius: 8,
    paddingVertical: 14,
    alignItems: "center",
    marginTop: 8,
  },
  saveButtonText: {
    fontSize: 16,
    fontWeight: "700",
    color: telegramColors.text.primary,
  },
});
