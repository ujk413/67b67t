import type { ProtectedUploader } from "../protectedUpload";
import { useEffect, useRef, useState } from "react";
import {
  ActivityIndicator,
  Alert,
  Image,
  Linking,
  Pressable,
  ScrollView,
  Text,
  TextInput,
  View,
} from "react-native";
import * as Picker from "expo-image-picker";
import * as DocumentPicker from "expo-document-picker";
import * as Files from "expo-file-system/legacy";
import { Video, ResizeMode } from "./Video";
import { CircleCamera } from "./CircleCamera";
import { useTheme } from "../ThemeProvider";
import { newId, type AccountRepository } from "../data/repository";
type Attachment = {
  albumId?: string;
  caption?: string;
  id: string;
  uri: string;
  name: string;
  mime: string;
  kind: string;
  uploaded?: string;
  protectedMeta?: Record<string, unknown>;
};
export function MediaTray({
  uploader,
  initialCircle = false,
  baseUrl,
  token,
  roomId,
  repository,
  onSend,
  onSent,
}: {
  uploader: ProtectedUploader;
  initialCircle?: boolean;
  baseUrl: string;
  token: string;
  roomId: string;
  repository: AccountRepository | null;
  onSend: (
    caption: string,
    body: Record<string, unknown>,
    roomId: string,
  ) => Promise<void>;
  onSent: () => void;
}) {
  const { colors } = useTheme(),
    [items, setItems] = useState<Attachment[]>([]),
    [caption, setCaption] = useState(""),
    [progress, setProgress] = useState(0),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [circle, setCircle] = useState(initialCircle);
  const xhr = useRef<{abort():void} | null>(null),
    current = useRef(items);
  current.current = items;
  const stopped = useRef(false);
  useEffect(() => {
    let active = true;
    void repository
      ?.get<{
        items: Attachment[];
        caption: string;
      }>(`attachments:${roomId}`, { items: [], caption: "" })
      .then((value) => {
        if (active) {
          setItems(value.items);
          setCaption(value.caption);
        }
      });
    return () => {
      active = false;
      stopped.current = true;
      xhr.current?.abort();
    };
  }, [roomId]);
  const save = (next: Attachment[], text = caption) => {
    setItems(next);
    current.current = next;
    void repository?.set(`attachments:${roomId}`, {
      items: next,
      caption: text,
    });
  };
  const permissionError = () =>
    Alert.alert("Нет разрешения", "Разрешите доступ в настройках приложения", [
      { text: "Отмена" },
      { text: "Настройки", onPress: () => void Linking.openSettings() },
    ]);
  const add = async (values: Omit<Attachment, "id">[]) => {
    try {
      if (!repository) throw new Error("Хранилище ещё загружается. Повторите попытку.");
      const dir = `${Files.documentDirectory}draft-media/${repository.userId}/`;
      await Files.makeDirectoryAsync(dir, { intermediates: true });
      const additions = [];
      for (const value of values) {
        const id = newId(),
          uri = dir + id + "-" + value.name.replace(/[^a-zA-Z0-9._-]/g, "_");
        await Files.copyAsync({ from: value.uri, to: uri });
        additions.push({ ...value, id, uri });
      }
      save([...current.current, ...additions]);
    } catch (e) {
      setError(String(e));
    }
  };
  const gallery = async () => {
    try {
      if (!(await Picker.requestMediaLibraryPermissionsAsync()).granted) {
        permissionError();
        return;
      }
      const result = await Picker.launchImageLibraryAsync({
        mediaTypes: ["images", "videos"],
        allowsMultipleSelection: true,
        selectionLimit: 10,
        quality: 0.9,
      });
      if (!result.canceled)
        await add(
          result.assets.map((asset) => ({
            uri: asset.uri,
            name:
              asset.fileName ||
              `media.${asset.type === "video" ? "mp4" : "jpg"}`,
            mime:
              asset.mimeType ||
              `${asset.type === "video" ? "video/mp4" : "image/jpeg"}`,
            kind: asset.type === "video" ? "video" : "image",
          })),
        );
    } catch (e) {
      setError(String(e));
    }
  };
  const camera = async () => {
    try {
      if (!(await Picker.requestCameraPermissionsAsync()).granted) {
        permissionError();
        return;
      }
      const result = await Picker.launchCameraAsync({ quality: 0.9 });
      if (!result.canceled)
        await add(
          result.assets.map((asset) => ({
            uri: asset.uri,
            name: "camera.jpg",
            mime: "image/jpeg",
            kind: "image",
          })),
        );
    } catch (e) {
      setError(String(e));
    }
  };
  const file = async () => {
    try {
      const result = await DocumentPicker.getDocumentAsync({
        multiple: true,
        copyToCacheDirectory: true,
      });
      if (!result.canceled)
        await add(
          result.assets.map((asset) => ({
            uri: asset.uri,
            name: asset.name,
            mime: asset.mimeType || "application/octet-stream",
            kind: "file",
          })),
        );
    } catch (e) {
      setError(String(e));
    }
  };
  const upload = (item: Attachment) => {
    const task = uploader(roomId, item, setProgress);
    xhr.current = task.request;
    return task.promise;
  };
  const send = async () => {
    setBusy(true);
    setError("");
    stopped.current = false;
    const album = newId();
    try {
      let remaining = current.current.map((item, index) => ({
        ...item,
        albumId: item.albumId || album,
        caption:
          item.caption === undefined
            ? index === 0
              ? caption
              : ""
            : item.caption,
      }));
      save(remaining);
      for (const item of [...remaining]) {
        if (stopped.current) break;
        const mediaId = item.uploaded;
        if (stopped.current) break;
        await onSend(
          item.caption || "",
          {
            type: item.kind === "video_note" ? "video_note" : "media",
            media_id: mediaId,
            __queue_id:item.id,
            ...(!mediaId?{__upload:{uri:item.uri,name:item.name,mime:item.mime}}:{}),
            text: item.caption || "",
            meta: { album_id: item.albumId, filename: item.name, ...item.protectedMeta },
          },
          roomId,
        );
        remaining = remaining.filter((x) => x.id !== item.id);
        save(remaining);
        // The durable outbox owns this file until delivery/cancellation.
      }
      if (!remaining.length) {
        setCaption("");
        await repository?.set(`attachments:${roomId}`, {
          items: [],
          caption: "",
        });
        onSent();
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
      setProgress(0);
    }
  };
  const button = (label: string, action: () => void) => (
    <Pressable
      onPress={action}
      disabled={busy}
      style={{ padding: 14, minHeight: 48 }}
    >
      <Text style={{ color: colors.text.blue }}>{label}</Text>
    </Pressable>
  );
  return (
    <View
      style={{
        maxHeight: "85%",
        backgroundColor: colors.windowBackground,
        paddingBottom: 20,
      }}
    >
      {!!error && (
        <Text style={{ padding: 12, color: colors.misc.error }}>{error}</Text>
      )}
      {circle ? (
        <CircleCamera
          onCancel={() => setCircle(false)}
          onRecorded={async (uri) => {
            setCircle(false);
            await add([
              {
                uri,
                name: "circle.mp4",
                mime: "video/mp4",
                kind: "video_note",
              },
            ]);
          }}
        />
      ) : (
        <>
          <View style={{ flexDirection: "row", flexWrap: "wrap" }}>
            {button("Галерея", () => void gallery())}
            {button("Камера", () => void camera())}
            {button("Файл", () => void file())}
            {button("Видеокружок", () => setCircle(true))}
          </View>
          <ScrollView horizontal style={{ minHeight: items.length ? 170 : 0 }}>
            {items.map((item) => (
              <View key={item.id} style={{ width: 140, margin: 8 }}>
                {item.mime.startsWith("image/") ? (
                  <Image
                    source={{ uri: item.uri }}
                    style={{ width: 140, height: 120, borderRadius: 12 }}
                  />
                ) : item.mime.startsWith("video/") ? (
                  <Video
                    source={{ uri: item.uri }}
                    style={{ width: 140, height: 120 }}
                    resizeMode={ResizeMode.CONTAIN}
                    useNativeControls
                  />
                ) : (
                  <Text style={{ padding: 12, color: colors.text.primary }}>
                    {item.name}
                  </Text>
                )}
                <Pressable
                  onPress={() => {
                    save(current.current.filter((x) => x.id !== item.id));
                    void Files.deleteAsync(item.uri, { idempotent: true });
                  }}
                  disabled={busy}
                  style={{ minHeight: 48, padding: 10 }}
                >
                  <Text style={{ color: colors.misc.error }}>Убрать</Text>
                </Pressable>
              </View>
            ))}
          </ScrollView>
          {!!items.length && (
            <>
              <TextInput
                value={caption}
                onChangeText={(value) => {
                  setCaption(value);
                  save(current.current, value);
                }}
                placeholder="Подпись"
                placeholderTextColor={colors.text.secondary}
                editable={!busy}
                style={{ padding: 16, color: colors.text.primary }}
              />
              {busy ? (
                <>
                  <Text style={{ padding: 12, color: colors.text.secondary }}>
                    Загрузка: {Math.round(progress * 100)}%
                  </Text>
                  <Pressable
                    onPress={() => {
                      stopped.current = true;
                      xhr.current?.abort();
                    }}
                    style={{ padding: 16, minHeight: 48 }}
                  >
                    <Text style={{ color: colors.misc.error }}>Остановить</Text>
                  </Pressable>
                </>
              ) : (
                button("Отправить вложения", () => void send())
              )}
            </>
          )}
        </>
      )}
    </View>
  );
}
