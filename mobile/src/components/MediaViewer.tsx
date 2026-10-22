import { useEffect, useRef, useState } from "react";
import { ActivityIndicator, Animated, Modal, PanResponder, Pressable, Text, View, useWindowDimensions } from "react-native";
import { ResizeMode, Video } from "./Video";
import * as Sharing from "expo-sharing";
import { VoicePlayer } from "./VoicePlayer";
import { useTheme } from "../ThemeProvider";
import { useMediaHeaders } from "../MediaSession";
import { resolveMediaSource } from "../decryptedMedia";
import { openMediaFile } from "../mediaFiles";
import { userError } from "../errors";
import type { Message } from "../types";
export function MediaViewer({ message, messages, baseUrl, userId, decode, onChange, onClose }: {
  message: Message | null; messages: Message[]; baseUrl: string; userId: string; decode: (message: Message) => Promise<Message>; onChange: (message: Message) => void; onClose: () => void;
}) {
  const { colors } = useTheme(), headers = useMediaHeaders(), { width, height } = useWindowDimensions();
  const [source, setSource] = useState<{ uri: string; mime: string; name: string } | null>(null), [error, setError] = useState("");
  const scale = useRef(new Animated.Value(1)).current, pan = useRef(new Animated.ValueXY()).current, zoom = useRef(1), baseline = useRef(1), distance = useRef(0);
  const items = messages.filter(m => m.media_id), index = items.findIndex(m => m.id === message?.id);
  const latest = useRef({ items, index, onChange }); latest.current = { items, index, onChange };
  const setZoom = (value: number) => { zoom.current = Math.max(1, Math.min(4, value)); scale.setValue(zoom.current); if (zoom.current === 1) pan.setValue({ x: 0, y: 0 }); };
  useEffect(() => {
    let current = true; setSource(null); setError(""); setZoom(1);
    if (message) void resolveMediaSource(message, baseUrl, userId, headers, decode).then(result => { if (current) setSource(result); }).catch(e => { if (current) setError(userError(e)); });
    return () => { current = false; };
  }, [message?.id, userId, baseUrl]);
  const responder = useRef(PanResponder.create({
    onMoveShouldSetPanResponder: (event, g) => event.nativeEvent.touches.length === 2 || Math.abs(g.dx) > 35 || (zoom.current > 1 && Math.abs(g.dy) > 10),
    onPanResponderGrant: event => { baseline.current = zoom.current; const t = event.nativeEvent.touches; distance.current = t.length === 2 ? Math.hypot(t[0].pageX - t[1].pageX, t[0].pageY - t[1].pageY) : 0; },
    onPanResponderMove: (event, g) => { const t = event.nativeEvent.touches; if (t.length === 2 && distance.current) setZoom(baseline.current * Math.hypot(t[0].pageX - t[1].pageX, t[0].pageY - t[1].pageY) / distance.current); else if (zoom.current > 1) pan.setValue({ x: Math.max(-width, Math.min(width, g.dx)), y: Math.max(-height, Math.min(height, g.dy)) }); },
    onPanResponderRelease: (_, g) => { if (zoom.current === 1 && Math.abs(g.dx) > 70) { const p = latest.current, next = p.items[p.index + (g.dx < 0 ? 1 : -1)]; if (next) p.onChange(next); } },
  })).current;
  const action = (label: string, fn: () => void, disabled = false) => <Pressable accessibilityRole="button" accessibilityLabel={label} onPress={fn} disabled={disabled} style={{ padding: 12, minHeight: 48, opacity: disabled ? 0.3 : 1 }}><Text style={{ color: colors.text.blue }}>{label}</Text></Pressable>;
  return <Modal visible={!!message} animationType="fade" onRequestClose={onClose}><View style={{ flex: 1, paddingTop: 32, paddingBottom: 24, backgroundColor: colors.windowBackground }}>
    <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between" }}>{action("Закрыть", onClose)}<Text style={{ color: colors.text.secondary }}>{index >= 0 ? `${index + 1} / ${items.length}` : "Вложение"}</Text>{action("Поделиться", () => { if (source) void (source.uri.startsWith("file:") ? Sharing.shareAsync(source.uri, { mimeType: source.mime }) : openMediaFile(source.uri, headers, source.mime, userId)).catch(e => setError(userError(e))); }, !source)}</View>
    <View style={{ flex: 1, overflow: "hidden", justifyContent: "center", alignItems: "center" }}>
      {!source && !error && <ActivityIndicator color={colors.text.blue} />}{!!error && <Text style={{ padding: 20, color: colors.misc.error }}>{error}</Text>}
      {source?.mime.startsWith("image/") && <Animated.View {...responder.panHandlers} style={{ width, height: height - 180, transform: [{ translateX: pan.x }, { translateY: pan.y }, { scale }] }}><Animated.Image source={{ uri: source.uri, headers }} accessibilityLabel={message?.text || "Фотография"} resizeMode="contain" style={{ width: "100%", height: "100%" }} /></Animated.View>}
      {(source?.mime.startsWith("video/") || (source && message?.type === "video_note")) && <Video source={{ uri: source.uri, headers }} style={{ width, height: height - 190 }} useNativeControls resizeMode={ResizeMode.CONTAIN} />}
      {source && (message?.type === "voice" || source.mime.startsWith("audio/")) && <View style={{ width: width - 40 }}><VoicePlayer uri={source.uri} /></View>}
      {source && !/^(image|video|audio)\//.test(source.mime) && action(source.name || "Открыть файл", () => void (source.uri.startsWith("file:") ? Sharing.shareAsync(source.uri, { mimeType: source.mime }) : openMediaFile(source.uri, headers, source.mime, userId)).catch(e => setError(userError(e))))}
    </View>
    {!!message?.text && <Text numberOfLines={3} style={{ paddingHorizontal: 20, color: colors.text.primary }}>{message.text}</Text>}
    <View style={{ flexDirection: "row", justifyContent: "center", alignItems: "center" }}>{action("Назад", () => onChange(items[index - 1]), index <= 0)}{source?.mime.startsWith("image/") && <>{action("−", () => setZoom(zoom.current - 0.5))}{action("Сброс", () => setZoom(1))}{action("+", () => setZoom(zoom.current + 0.5))}</>}{action("Далее", () => onChange(items[index + 1]), index < 0 || index >= items.length - 1)}</View>
  </View></Modal>;
}
