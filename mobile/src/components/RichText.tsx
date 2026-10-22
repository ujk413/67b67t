import { useEffect, useRef, useState } from "react";
import { Image, Linking, Pressable, Text, View } from "react-native";
import { useTheme } from "../ThemeProvider";
export function RichText({
  text,
  onUsername,
  reserveMeta,
  onLongPress,
}: {
  text: string;
  reserveMeta?: number;
  onLongPress?: () => void;
  onUsername?: (username: string) => void;
}) {
  const { colors, fontSize } = useTheme();
  const [preview, setPreview] = useState<{
      url: string;
      title: string;
      image?: string;
    } | null>(null),
    [loading, setLoading] = useState(false);
  const controller = useRef<AbortController | null>(null);
  useEffect(() => () => controller.current?.abort(), []);
  const pieces = text.split(
    /(https?:\/\/[^\s<>]+|@[a-zA-Z0-9_]{3,}|\*\*[^*]+\*\*|`[^`]+`)/g,
  );
  const url = pieces.find((part) => /^https?:\/\//.test(part));
  const load = async () => {
    if (!url) return;
    setLoading(true);
    controller.current?.abort();
    const abort = new AbortController();
    controller.current = abort;
    const timer = setTimeout(() => abort.abort(), 5000);
    try {
      const response = await fetch(url, {
        signal: abort.signal,
        credentials: "omit",
        headers: { Accept: "text/html" },
      });
      const html = (await response.text()).slice(0, 1_000_000);
      const title =
        html.match(/<title[^>]*>([^<]*)<\/title>/i)?.[1]?.trim() ||
        new URL(url).hostname;
      const imageMatch = html.match(
        /<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)/i,
      )?.[1];
      const image = imageMatch
        ? new URL(imageMatch, url).toString()
        : undefined;
      setPreview({
        url,
        title,
        image: image && /^https?:/.test(image) ? image : undefined,
      });
    } catch {
      setPreview({ url, title: new URL(url).hostname });
    } finally {
      clearTimeout(timer);
      setLoading(false);
    }
  };
  return (
    <View>
      <Text
        selectable={!onLongPress}
        onLongPress={onLongPress}
        style={{
          fontSize,
          lineHeight: Math.round(fontSize * 1.3),
          color: colors.text.primary,
        }}
      >
        {pieces.map((part, i) => {
          if (/^https?:\/\//.test(part))
            return (
              <Text
                key={i}
                style={{ color: colors.text.blue }}
                onPress={() => {
                  void Linking.openURL(part);
                }}
              >
                {part}
              </Text>
            );
          if (part.startsWith("@"))
            return (
              <Text
                key={i}
                style={{ color: colors.text.blue }}
                onPress={() => onUsername?.(part.slice(1))}
              >
                {part}
              </Text>
            );
          if (part.startsWith("**") && part.endsWith("**"))
            return (
              <Text key={i} style={{ fontWeight: "700" }}>
                {part.slice(2, -2)}
              </Text>
            );
          if (part.startsWith("`") && part.endsWith("`"))
            return (
              <Text key={i} style={{ fontFamily: "monospace" }}>
                {part.slice(1, -1)}
              </Text>
            );
          return part;
        })}
        {reserveMeta ? "\u00a0".repeat(reserveMeta) : null}
      </Text>
      {url && !preview && (
        <Pressable
          onPress={() => void load()}
          style={{ paddingVertical: 12, minHeight: 48 }}
        >
          <Text style={{ color: colors.text.blue, fontSize: 13 }}>
            {loading ? "Загрузка…" : "Предпросмотр ссылки"}
          </Text>
        </Pressable>
      )}
      {preview && (
        <Pressable
          onPress={() => {
            void Linking.openURL(preview.url);
          }}
          style={{
            padding: 10,
            backgroundColor: colors.windowBackgroundGray,
            borderRadius: 10,
          }}
        >
          {preview.image && (
            <Image
              source={{ uri: preview.image }}
              style={{ width: 180, height: 100, borderRadius: 8 }}
            />
          )}
          <Text numberOfLines={2} style={{ color: colors.text.blue }}>
            {preview.title}
          </Text>
        </Pressable>
      )}
    </View>
  );
}
