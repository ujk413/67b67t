import { useEffect, useRef, useState } from "react";
import {
  Alert,
  ActivityIndicator,
  Animated,
  AppState,
  Vibration,
  Linking,
  PanResponder,
  Pressable,
  Text,
  View,
} from "react-native";
import { Audio } from "expo-av";
import * as Files from "expo-file-system/legacy";
import { useTheme } from "../ThemeProvider";
import { userError } from "../errors";
import { Icon } from "./Icon";
import { VoicePlayer } from "./VoicePlayer";
import { newId, type AccountRepository } from "../data/repository";
type Clip = { uri: string; duration: number };
export function VoiceRecorder({
  onSend,
  repository,
  roomId,
  disabled,
  onModeChange,
}: {
  onSend: (uri: string, duration: number) => Promise<void>;
  repository?: AccountRepository | null;
  roomId: string;
  disabled?: boolean;
  onModeChange?: (value: boolean) => void;
}) {
  const { colors } = useTheme(),
    [clip, setClip] = useState<Clip | null>(null),
    [seconds, setSeconds] = useState(0),
    [active, setActive] = useState(false),
    [locked, setLocked] = useState(false),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [preparing, setPreparing] = useState(false);
  const recording = useRef<Audio.Recording | null>(null),
    held = useRef(false),
    lock = useRef(false),
    cancelled = useRef(false),
    mounted = useRef(true),
    starting = useRef(false);
  const latest = useRef({ onSend, repository, roomId, disabled });
  latest.current = { onSend, repository, roomId, disabled };
  const offset = useRef(new Animated.ValueXY()).current;
  useEffect(() => { onModeChange?.(active || preparing || !!clip || !!error); }, [active, preparing, clip, error]);
  useEffect(() => {
    mounted.current = true;
    let valid = true;
    void repository?.get<Clip | null>(`voice:${roomId}`, null).then((value) => {
      if (valid) setClip(value);
    });
    const sub = AppState.addEventListener("change", state => { if (state !== "active" && recording.current) void actions.current.stop(false); });
    return () => {
      sub.remove();
      onModeChange?.(false);
      valid = false;
      mounted.current = false;
      held.current = false;
      cancelled.current = true;
      const previous = recording.current; recording.current = null;
      void previous?.stopAndUnloadAsync().catch(() => {});
    };
  }, [roomId]);
  const stop = async (discard = false) => {
    const rec = recording.current;
    if (!rec) return;
    const owner = latest.current;
    recording.current = null;
    try {
      const status = await rec.getStatusAsync();
      await rec.stopAndUnloadAsync();
      const uri = rec.getURI();
      if (!discard && uri && (status.durationMillis || 0) >= 350) {
        if (!owner.repository) throw new Error("Хранилище записи ещё не готово");
        const directory = `${Files.documentDirectory}draft-media/${owner.repository.userId}/`;
        await Files.makeDirectoryAsync(directory, { intermediates: true });
        const target = `${directory}voice-${newId()}.m4a`;
        await Files.copyAsync({ from: uri, to: target });
        const next = { uri: target, duration: status.durationMillis || 0 };
        if (mounted.current && latest.current.roomId === owner.roomId)
          setClip(next);
        await owner.repository?.set(`voice:${owner.roomId}`, next);
      } else if (!discard && mounted.current) setError("Слишком короткая запись. Удерживайте микрофон чуть дольше.");
      if (uri) await Files.deleteAsync(uri, { idempotent: true }).catch(() => {});
    } catch (e) {
      if (mounted.current) setError("Не удалось сохранить запись. Попробуйте удерживать микрофон дольше.");
    } finally {
      if (mounted.current) {
        setActive(false);
        setLocked(false);
      }
      lock.current = false;
    }
  };
  const start = async (lockedMode = false) => {
    if (starting.current || latest.current.disabled || recording.current)
      return;
    held.current = true;
    cancelled.current = false;
    lock.current = lockedMode;
    setLocked(lockedMode);
    starting.current = true;
    setPreparing(true); setError(""); setSeconds(0);
    try {
      const previous = await Audio.getPermissionsAsync();
      const permission = previous.granted ? previous : await Audio.requestPermissionsAsync();
      if (!permission.granted) {
        Alert.alert(
          "Нет доступа к микрофону",
          "Разрешите микрофон для записи сообщений",
          [
            { text: "Отмена" },
            { text: "Настройки", onPress: () => void Linking.openSettings() },
          ],
        );
        return;
      }
      if (!previous.granted) { setError("Микрофон подключён. Удерживайте кнопку ещё раз, чтобы начать запись."); return; }
      if ((!held.current && !lockedMode) || cancelled.current || !mounted.current) return;
      await Audio.setAudioModeAsync({
        allowsRecordingIOS: true,
        playsInSilentModeIOS: true,
      });
      const rec = new Audio.Recording();
      await rec.prepareToRecordAsync(
        Audio.RecordingOptionsPresets.HIGH_QUALITY,
      );
      if (!mounted.current || cancelled.current || (!held.current && !lock.current)) { await rec.stopAndUnloadAsync(); return; }
      rec.setProgressUpdateInterval(200);
      rec.setOnRecordingStatusUpdate((status) => {
        if (mounted.current)
          setSeconds(Math.floor(status.durationMillis / 1000));
      });
      recording.current = rec;
      await rec.startAsync();
      if (mounted.current) { setActive(true); Vibration.vibrate(20); }
      if (!held.current || cancelled.current || !mounted.current)
        await stop(cancelled.current || !mounted.current);
    } catch (e) {
      if (mounted.current) setError(userError(e));
    } finally {
      starting.current = false;
      if (mounted.current) setPreparing(false);
    }
  };
  const actions = useRef({ start, stop }); actions.current = { start, stop };
  const gesture = useRef(
    PanResponder.create({
      onStartShouldSetPanResponder: () => !latest.current.disabled,
      onPanResponderGrant: () => void actions.current.start(),
      onPanResponderMove: (_, g) => {
        if (lock.current || cancelled.current) return;
        offset.setValue({ x: Math.max(-80, Math.min(0, g.dx)), y: Math.max(-60, Math.min(0, g.dy)) });
        if (g.dx < -80) {
          cancelled.current = true;
          held.current = false;
          Vibration.vibrate(20); void actions.current.stop(true);
        } else if (g.dy < -60) {
          lock.current = true;
          setLocked(true); offset.setValue({ x: 0, y: 0 }); Vibration.vibrate(20);
        }
      },
      onPanResponderRelease: () => {
        held.current = false;
        offset.setValue({ x: 0, y: 0 });
        if (!lock.current) void actions.current.stop(cancelled.current);
      },
      onPanResponderTerminationRequest: () => false,
      onPanResponderTerminate: () => {
        held.current = false;
        if (!lock.current) { cancelled.current = true; void actions.current.stop(true); }
      },
    }),
  ).current;
  const discard = async () => {
    if (clip) await Files.deleteAsync(clip.uri, { idempotent: true });
    setClip(null);
    await repository?.set(`voice:${roomId}`, null);
  };
  if (clip)
    return (
      <View style={{ flex: 1, padding: 8 }}>
        <VoicePlayer uri={clip.uri} initialDuration={clip.duration} />
        {!!error && <Text style={{ color: colors.misc.error }}>{error}</Text>}
        <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
          <Pressable
            disabled={busy}
            onPress={() => void discard()}
            style={{ padding: 14, minHeight: 48 }}
          >
            <Text style={{ color: colors.misc.error }}>Удалить запись</Text>
          </Pressable>
          <Pressable
            disabled={busy}
            onPress={() => {
              setBusy(true);
              void onSend(clip.uri, clip.duration)
                .then(discard)
                .catch((e) => setError(String(e)))
                .finally(() => setBusy(false));
            }}
            style={{ padding: 14, minHeight: 48 }}
          >
            <Text style={{ color: colors.text.blue }}>
              {busy ? "Отправка…" : "Отправить"}
            </Text>
          </Pressable>
        </View>
      </View>
    );
  return (
    <View style={{ alignItems: "center", flex: active || preparing || error ? 1 : undefined, paddingVertical: 4 }}>
      {preparing && <ActivityIndicator color={colors.text.blue} />}
      {!!error && <Text style={{ color: colors.misc.error }}>{error}</Text>}
      {active && (
        <Text style={{ color: colors.misc.error }}>
          {seconds} с ·{" "}
          {locked ? "Запись зафиксирована" : "Влево — отмена, вверх — фиксация"}
        </Text>
      )}
      {locked && (
        <View style={{ flexDirection: "row" }}>
          <Pressable
            onPress={() => void stop(false)}
            style={{ padding: 14, minHeight: 48 }}
          >
            <Text style={{ color: colors.text.blue }}>Завершить</Text>
          </Pressable>
          <Pressable
            onPress={() => void stop(true)}
            style={{ padding: 14, minHeight: 48 }}
          >
            <Text style={{ color: colors.misc.error }}>Отмена</Text>
          </Pressable>
        </View>
      )}
        <Animated.View
          {...gesture.panHandlers}
          accessible
          accessibilityRole="button"
          accessibilityActions={[{ name: "activate", label: "Начать запись" }]}
          onAccessibilityAction={() => {
            if (recording.current) void stop(false);
            else void start(true);
          }}
          accessibilityLabel="Удерживайте для записи голосового"
          style={{
            width: locked ? 1 : 48,
            opacity: locked ? 0 : 1,
            transform: offset.getTranslateTransform(),
            height: 48,
            alignItems: "center",
            justifyContent: "center",
          }}
        >
          <Icon
            name="mic"
            color={active ? colors.misc.error : colors.composer.icons}
          />
        </Animated.View>
    </View>
  );
}
