import { useEffect, useRef, useState } from "react";
import { ActivityIndicator, AppState, Linking, Pressable, Text, View, useWindowDimensions } from "react-native";
import { CameraView, useCameraPermissions, useMicrophonePermissions } from "expo-camera";
import { useTheme } from "../ThemeProvider";
import { Icon } from "./Icon";
import { userError } from "../errors";
export function CircleCamera({ onRecorded, onCancel }: { onRecorded: (uri: string) => Promise<void>; onCancel: () => void }) {
  const { colors } = useTheme(), { width } = useWindowDimensions();
  const [permission, request] = useCameraPermissions(), [mic, requestMic] = useMicrophonePermissions();
  const [recording, setRecording] = useState(false), [ready, setReady] = useState(false), [preparing, setPreparing] = useState(true), [facing, setFacing] = useState<"front" | "back">("front"), [seconds, setSeconds] = useState(0), [error, setError] = useState("");
  const camera = useRef<CameraView>(null), cancelled = useRef(false), working = useRef(false), live = useRef(true);
  useEffect(() => { live.current=true; const sub=AppState.addEventListener("change",state=>{if(state!=="active" && working.current)camera.current?.stopRecording();}); void (async () => { await request(); await requestMic(); if (live.current) setPreparing(false); })().catch(e => { if (live.current) { setError(userError(e)); setPreparing(false); } }); return () => { sub.remove(); live.current = false; cancelled.current = true; camera.current?.stopRecording(); }; }, []);
  useEffect(() => { if (!recording) return; const timer = setInterval(() => setSeconds(n => n + 1), 1000); return () => clearInterval(timer); }, [recording]);
  const record = async () => {
    if (working.current) { camera.current?.stopRecording(); return; }
    if (!ready || !permission?.granted || !mic?.granted) return;
    working.current = true; cancelled.current = false; setSeconds(0); setRecording(true); setError("");
    try { const result = await camera.current!.recordAsync({ maxDuration: 60 }); if (result && !cancelled.current && live.current) await onRecorded(result.uri); }
    catch (e) { if (live.current && !cancelled.current) setError(userError(e)); }
    finally { working.current = false; if (live.current) setRecording(false); }
  };
  const allowed = permission?.granted && mic?.granted, size = Math.min(300, width - 48);
  return <View style={{ padding: 16, alignItems: "center", gap: 12, backgroundColor: colors.windowBackground }}>
    {allowed ? <View style={{ width: size, height: size, borderRadius: size / 2, overflow: "hidden", backgroundColor: "#000" }}><CameraView key={facing} ref={camera} mode="video" facing={facing} videoQuality="480p" videoBitrate={2000000} onCameraReady={() => {setReady(true);setError('');}} onMountError={() => { setReady(false); setError('Не удалось открыть камеру. Попробуйте сменить камеру или закройте другое приложение, которое её использует.'); }} style={{ flex: 1 }} /></View> : <Text style={{ color: colors.text.primary }}>Разрешите камеру и микрофон для записи кружка.</Text>}
    {(preparing || (allowed && !ready && !error)) && <ActivityIndicator color={colors.text.blue} />}
    {!!error && <Text accessibilityLiveRegion="polite" style={{ color: colors.misc.error }}>{error}</Text>}
    <Text style={{ color: colors.text.secondary }}>{recording ? `● ${seconds} с / 60 с` : "После записи можно просмотреть кружок и отправить его"}</Text>
    {!allowed && !preparing && <Pressable onPress={() => void Linking.openSettings()} style={{ padding: 14 }}><Text style={{ color: colors.text.blue }}>Открыть настройки разрешений</Text></Pressable>}
    <View style={{ flexDirection: "row", alignItems: "center", gap: 16 }}>
      <Pressable disabled={recording || !allowed} accessibilityRole="button" accessibilityLabel="Сменить камеру" onPress={() => { setReady(false);setError(''); setFacing(v => v === "front" ? "back" : "front"); }} style={{ padding: 12, minHeight: 48 }}><Icon name="camera-flip" /></Pressable>
      <Pressable disabled={!ready || !allowed} onPress={() => void record()} accessibilityRole="button" style={{ padding: 16, minHeight: 48, borderRadius: 24, backgroundColor: colors.text.blue, opacity: ready ? 1 : 0.4 }}><Text style={{ color: colors.text.onAccent }}>{recording ? "Остановить" : "Начать запись"}</Text></Pressable>
      <Pressable accessibilityRole="button" onPress={() => { cancelled.current = true; camera.current?.stopRecording(); onCancel(); }} style={{ padding: 12, minHeight: 48 }}><Text style={{ color: colors.misc.error }}>Отмена</Text></Pressable>
    </View>
  </View>;
}
