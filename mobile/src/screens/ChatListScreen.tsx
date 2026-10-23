import {t as uiText} from "../../../web/i18n.js";
import { useRef, useState } from "react";
import {
  FlatList,
  ScrollView,
  Modal,
  Pressable,
  RefreshControl,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { useTheme } from "../ThemeProvider";
import { Icon } from "../components/Icon";
import { MainHeader } from "../components/MainHeader";
import { ChatListItem } from "../components/ChatListItem";
import type { Room, User } from "../types";

type Props = {
  drafts?:Record<string,string>;
  folders?: { id: string; title: string; room_ids: string[] }[];
  selectedFolder?: string | null;
  onFolder?: (id: string | null) => void;
  onFolders?: () => void;
  onSaved?: () => void;
  user: User;
  rooms: Room[];
  baseUrl: string;
  refreshing: boolean;
  onRefresh: () => void;
  onProfilePress: () => void;
  onRoomPress: (room: Room) => void;
  onLogout?: () => void;
  onCompose?: () => void;
  onSearch?: () => void;
  onArchive?: () => void;
  onRoomMenu?: (room: Room) => void;
  archivedCount?: number;
  mediaUrl: (base: string, id: string | null | undefined) => string | null;
};
export function ChatListScreen(p: Props) {
  const { colors } = useTheme();
  const [menuOpen, setMenuOpen] = useState(false);
  const list = useRef<FlatList<Room>>(null);
  const action = (run?: () => void) => {
    setMenuOpen(false);
    run?.();
  };
  return (
    <View style={{ flex: 1, backgroundColor: colors.windowBackground }}>
      <MainHeader
        title={uiText("Чаты")}
        onSearchPress={() => p.onSearch?.()}
        onMenuPress={() => setMenuOpen(true)}
      />
      {!!p.folders?.length && <ScrollView horizontal style={{ maxHeight: 48 }} contentContainerStyle={{ paddingHorizontal: 8 }}>{[{ id: null, title: uiText("Все чаты") }, ...p.folders].map(folder => <Pressable key={folder.id || "all"} onPress={() => p.onFolder?.(folder.id)} accessibilityRole="tab" accessibilityState={{ selected: p.selectedFolder === folder.id }} style={{ padding: 12, borderBottomWidth: p.selectedFolder === folder.id ? 2 : 0, borderColor: colors.text.blue }}><Text style={{ color: p.selectedFolder === folder.id ? colors.text.blue : colors.text.secondary }}>{folder.title}</Text></Pressable>)}</ScrollView>}
      <FlatList
        ref={list}
        data={p.rooms}
        keyExtractor={(room) => room.id}
        renderItem={({ item }) => (
          <ChatListItem
            draft={p.drafts?.[item.id]}
            room={item}
            meId={p.user.id}
            baseUrl={p.baseUrl}
            onPress={() => p.onRoomPress(item)}
            onLongPress={() => p.onRoomMenu?.(item)}
            mediaUrl={p.mediaUrl}
          />
        )}
        ListHeaderComponent={
          p.archivedCount ? (
            <Pressable
              onPress={p.onArchive}
              accessibilityRole="button"
              style={[styles.archive, { borderColor: colors.divider }]}
            >
              <Text style={{ color: colors.text.secondary, fontSize: 14 }}>
                {uiText("Архив")}</Text>
              <Text style={{ color: colors.text.secondary }}>
                {p.archivedCount}
              </Text>
            </Pressable>
          ) : null
        }
        ListEmptyComponent={
          <View style={styles.empty}>
            <Text
              style={{
                color: colors.text.primary,
                fontSize: 18,
                fontWeight: "600",
              }}
            >
              {uiText("Начните общение")}</Text>
            <Text
              style={{
                color: colors.text.secondary,
                textAlign: "center",
                marginTop: 8,
              }}
            >
              {uiText("Найдите человека по имени пользователя или создайте группу")}</Text>
            <Pressable
              onPress={p.onCompose}
              style={{ padding: 16, minHeight: 48 }}
            >
              <Text style={{ color: colors.text.blue, fontWeight: "600" }}>
                {uiText("Написать сообщение")}</Text>
            </Pressable>
          </View>
        }
        refreshControl={
          <RefreshControl
            refreshing={p.refreshing}
            onRefresh={p.onRefresh}
            tintColor={colors.misc.progress}
          />
        }
        contentContainerStyle={{ paddingBottom: 88 }}
        keyboardShouldPersistTaps="handled"
        style={{ flex: 1 }}
      />
      <Pressable
        onPress={p.onCompose}
        accessibilityRole="button"
        accessibilityLabel={uiText("Начать новую переписку")}
        style={({ pressed }) => [
          styles.compose,
          { backgroundColor: colors.text.blue, opacity: pressed ? 0.85 : 1 },
        ]}
      >
        <Icon name="compose" size={23} color={colors.text.onAccent} />
        <Text
          style={{
            color: colors.text.onAccent,
            fontWeight: "600",
            fontSize: 15,
          }}
        >
          {uiText("Написать")}</Text>
      </Pressable>
      <View
        style={[
          styles.tabs,
          {
            borderColor: colors.divider,
            backgroundColor: colors.windowBackground,
          },
        ]}
      >
        <Pressable
          onPress={() =>
            list.current?.scrollToOffset({ offset: 0, animated: true })
          }
          accessibilityRole="tab"
          accessibilityState={{ selected: true }}
          accessibilityLabel={uiText("Чаты")}
          style={styles.tab}
        >
          <Icon name="chats" size={23} color={colors.text.blue} />
          <Text
            style={{ color: colors.text.blue, fontSize: 11, fontWeight: "600" }}
          >
            {uiText("Чаты")}</Text>
        </Pressable>
        <Pressable
          onPress={p.onProfilePress}
          accessibilityRole="tab"
          accessibilityLabel={uiText("Настройки")}
          style={styles.tab}
        >
          <Icon name="settings" size={23} color={colors.text.secondary} />
          <Text style={{ color: colors.text.secondary, fontSize: 11 }}>
            {uiText("Настройки")}</Text>
        </Pressable>
      </View>
      <Modal
        visible={menuOpen}
        transparent
        animationType="fade"
        onRequestClose={() => setMenuOpen(false)}
      >
        <Pressable
          style={[styles.backdrop, { backgroundColor: colors.misc.backdrop }]}
          onPress={() => setMenuOpen(false)}
        >
          <View
            style={[styles.menu, { backgroundColor: colors.windowBackground }]}
          >
            <Pressable
              style={styles.menuItem}
              onPress={() => action(p.onCompose)}
            >
              <Text style={{ color: colors.text.primary, fontSize: 16 }}>
                {uiText("Новая переписка")}</Text>
            </Pressable>
            <Pressable style={styles.menuItem} onPress={() => action(p.onSaved)}><Text style={{ color: colors.text.primary, fontSize: 16 }}>{uiText("Избранное")}</Text></Pressable>
            <Pressable style={styles.menuItem} onPress={() => action(p.onFolders)}><Text style={{ color: colors.text.primary, fontSize: 16 }}>{uiText("Папки чатов")}</Text></Pressable>
            <Pressable
              style={styles.menuItem}
              onPress={() => action(p.onArchive)}
            >
              <Text style={{ color: colors.text.primary, fontSize: 16 }}>
                {uiText("Архив")}</Text>
            </Pressable>
            <Pressable
              style={styles.menuItem}
              onPress={() => action(p.onProfilePress)}
            >
              <Text style={{ color: colors.text.primary, fontSize: 16 }}>
                {uiText("Настройки и профиль")}</Text>
            </Pressable>
            {p.onLogout && (
              <Pressable
                style={styles.menuItem}
                onPress={() => action(p.onLogout)}
              >
                <Text style={{ color: colors.misc.error, fontSize: 16 }}>
                  {uiText("Выйти")}</Text>
              </Pressable>
            )}
          </View>
        </Pressable>
      </Modal>
    </View>
  );
}
const styles = StyleSheet.create({
  archive: {
    minHeight: 48,
    paddingHorizontal: 20,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    borderBottomWidth: StyleSheet.hairlineWidth,
  },
  empty: { alignItems: "center", paddingHorizontal: 32, paddingTop: 72 },
  compose: {
    position: "absolute",
    bottom: 76,
    right: 16,
    minHeight: 52,
    paddingHorizontal: 20,
    borderRadius: 28,
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
    elevation: 3,
    shadowColor: "#000",
    shadowOpacity: 0.12,
    shadowRadius: 8,
    shadowOffset: { width: 0, height: 3 },
  },
  tabs: {
    flexDirection: "row",
    borderTopWidth: StyleSheet.hairlineWidth,
    minHeight: 56,
  },
  tab: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    gap: 3,
    minHeight: 56,
    paddingVertical: 6,
  },
  backdrop: {
    flex: 1,
    alignItems: "flex-end",
    paddingTop: 54,
    paddingRight: 12,
  },
  menu: { minWidth: 210, borderRadius: 20, paddingVertical: 8 },
  menuItem: {
    minHeight: 48,
    justifyContent: "center",
    paddingHorizontal: 18,
    paddingVertical: 12,
  },
});
