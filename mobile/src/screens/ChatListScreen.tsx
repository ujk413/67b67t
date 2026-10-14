import { FlatList, RefreshControl, StyleSheet, View } from "react-native";
import { telegramColors } from "../theme";
import { MainHeader } from "../components/MainHeader";
import { ChatListItem } from "../components/ChatListItem";
import type { Room, User } from "../types";

type ChatListScreenProps = {
  user: User;
  rooms: Room[];
  baseUrl: string;
  refreshing: boolean;
  onRefresh: () => void;
  onProfilePress: () => void;
  onRoomPress: (room: Room) => void;
  mediaUrl: (baseUrl: string, mediaId: string | null | undefined) => string | null;
};

export function ChatListScreen({
  user,
  rooms,
  baseUrl,
  refreshing,
  onRefresh,
  onProfilePress,
  onRoomPress,
  mediaUrl,
}: ChatListScreenProps) {
  return (
    <View style={styles.container}>
      <MainHeader
        user={user}
        baseUrl={baseUrl}
        onProfilePress={onProfilePress}
        mediaUrl={mediaUrl}
      />
      <FlatList
        data={rooms}
        keyExtractor={(item) => item.id}
        renderItem={({ item }) => (
          <ChatListItem
            room={item}
            meId={user.id}
            baseUrl={baseUrl}
            onPress={() => onRoomPress(item)}
            mediaUrl={mediaUrl}
          />
        )}
        refreshControl={
          <RefreshControl
            refreshing={refreshing}
            onRefresh={onRefresh}
            tintColor={telegramColors.misc.progress}
          />
        }
        contentContainerStyle={rooms.length === 0 && styles.emptyContent}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: telegramColors.windowBackground,
  },
  emptyContent: {
    flexGrow: 1,
    alignItems: "center",
    justifyContent: "center",
  },
});
