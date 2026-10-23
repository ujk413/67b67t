import {t as uiText} from "../../../web/i18n.js";
import { useEffect, useRef, useState } from "react";
import { ensurePlaybackMode, ensureRecordingMode } from "../audioSession";
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
import { onMediaCallStarted } from "../playbackOwnership";
import { runMediaTask, registerMediaCancellation } from "../mediaLifecycle";
import { VoicePlayer } from "./VoicePlayer";
import {mediaDetails,recordingGesture} from '../../../web/media-details.js';
import { newId, type AccountRepository } from "../data/repository";
type Clip = { uri: string; duration: number; waveform?:number[] };
export function VoiceRecorder({
  onSend,
  repository,
  roomId,
  disabled,
  onModeChange,
}: {
  onSend: (uri: string, duration: number, waveform?:number[]) => Promise<void>;
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
  const samples=useRef<number[]>([]);
  useEffect(() => { onModeChange?.(active || preparing || !!clip || !!error); }, [active, preparing, clip, error]);
  useEffect(() => {
    mounted.current = true;
    let valid = true;
    void repository?.get<Clip | null>(`voice:${roomId}`, null).then((value) => {
      if (valid) setClip(value);
    });
    const cancel=async()=>{held.current=false;cancelled.current=true;await actions.current.stop(true);};
    const interrupt=onMediaCallStarted(cancel),unsubscribe=repository?registerMediaCancellation(repository.userId,cancel):()=>{};
    const sub = AppState.addEventListener("change", state => { if (state !== "active") { held.current = false; cancelled.current = true; if (recording.current) void actions.current.stop(false); } });
    return () => {
      sub.remove();interrupt();unsubscribe();
      onModeChange?.(false);
      valid = false;
      mounted.current = false;
      held.current = false;
      cancelled.current = true;
      const previous = recording.current; recording.current = null;
      void (async () => { await previous?.stopAndUnloadAsync().catch(() => {}); const uri=previous?.getURI();if(uri)await Files.deleteAsync(uri,{idempotent:true}).catch(()=>{}); await ensurePlaybackMode(); })();
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
        await runMediaTask(owner.repository.userId, async task=>{
          const directory = `${Files.documentDirectory}draft-media/${owner.repository!.userId}/`;
          await Files.makeDirectoryAsync(directory, { intermediates: true });task.check();
          const target = `${directory}voice-${newId()}.m4a`;
          let saved=false;
          try{
            await Files.copyAsync({ from: uri, to: target });task.check();
            const next = { uri: target, duration: status.durationMillis || 0,waveform:mediaDetails({waveform:samples.current}).waveform };
            await owner.repository!.set(`voice:${owner.roomId}`, next);task.check();saved=true;
            if (mounted.current && latest.current.roomId === owner.roomId)setClip(next);
          }finally{if(!saved)await Files.deleteAsync(target,{idempotent:true}).catch(()=>{});}
        });
      } else if (!discard && mounted.current) setError(uiText("Слишком короткая запись. Удерживайте микрофон чуть дольше."));
    } catch (e) {
      if (mounted.current) setError(uiText("Не удалось сохранить запись. Попробуйте удерживать микрофон дольше."));
    } finally {
      const temporary=rec.getURI();if(temporary)await Files.deleteAsync(temporary,{idempotent:true}).catch(()=>{});
      if (mounted.current) {
        setActive(false);
        setLocked(false);
      }
      lock.current = false;
      // Hand the audio session back: leaving recording mode on is what made
      // later playback quiet and routed to the earpiece.
      void ensurePlaybackMode();
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
    const work=async()=>{
    let prepared: Audio.Recording | null = null;
    try {
      const previous = await Audio.getPermissionsAsync();
      const permission = previous.granted ? previous : await Audio.requestPermissionsAsync();
      if (!permission.granted) {
        Alert.alert(
          uiText("Нет доступа к микрофону"),
          uiText("Разрешите микрофон для записи сообщений"),
          [
            { text: uiText("Отмена") },
            { text: uiText("Настройки"), onPress: () => void Linking.openSettings() },
          ],
        );
        return;
      }
      if (!previous.granted) { setError(uiText("Микрофон подключён. Удерживайте кнопку ещё раз, чтобы начать запись.")); return; }
      if ((!held.current && !lockedMode) || cancelled.current || !mounted.current) return;
      await ensureRecordingMode();
      if (!mounted.current || cancelled.current || (!held.current && !lock.current)) return;
      const rec = new Audio.Recording(); prepared = rec;
      samples.current=[];
      await rec.prepareToRecordAsync({...Audio.RecordingOptionsPresets.HIGH_QUALITY,isMeteringEnabled:true});
      if (!mounted.current || cancelled.current || (!held.current && !lock.current)) return;
      rec.setProgressUpdateInterval(200);
      rec.setOnRecordingStatusUpdate((status) => {
        if(status.isRecording && typeof status.metering==='number')samples.current.push(Math.pow(10,Math.max(-160,Math.min(0,status.metering))/20));
        if(samples.current.length>4000)samples.current=mediaDetails({waveform:samples.current}).waveform || [];
        if (mounted.current)
          setSeconds(Math.floor(status.durationMillis / 1000));
      });
      recording.current = rec;
      await rec.startAsync();
      prepared = null;
      if (mounted.current) { setActive(true); Vibration.vibrate(20); }
      if (!held.current || cancelled.current || !mounted.current)
        await stop(cancelled.current || !mounted.current);
    } catch (e) {
      if (mounted.current) setError(userError(e));
    } finally {
      if (prepared) { if (recording.current === prepared) recording.current = null; await prepared.stopAndUnloadAsync().catch(() => {});const uri=prepared.getURI();if(uri)await Files.deleteAsync(uri,{idempotent:true}).catch(()=>{}); }
      if (!recording.current) await ensurePlaybackMode();
      starting.current = false;
      if (mounted.current) setPreparing(false);
    }
    };
    try{
      const owner=latest.current.repository;
      if(owner)await runMediaTask(owner.userId,async task=>{
        const remove=task.onCancel(()=>{held.current=false;cancelled.current=true;return stop(true);});
        try{await work();task.check();}finally{remove();}
      });else await work();
    }catch(error){if(mounted.current && !cancelled.current)setError(userError(error));}
    finally{starting.current=false;if(mounted.current)setPreparing(false);}
  };
  const actions = useRef({ start, stop }); actions.current = { start, stop };
  const gesture = useRef(
    PanResponder.create({
      onStartShouldSetPanResponder: () => !latest.current.disabled,
      onPanResponderGrant: () => void actions.current.start(),
      onPanResponderMove: (_, g) => {
        if (lock.current || cancelled.current) return;
        offset.setValue({ x: Math.max(-80, Math.min(0, g.dx)), y: Math.max(-60, Math.min(0, g.dy)) });
        const action=recordingGesture({dx:g.dx,dy:g.dy,locked:lock.current});
        if (action==='cancel') {
          cancelled.current = true;
          held.current = false;
          Vibration.vibrate(20); void actions.current.stop(true);
        } else if (action==='lock') {
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
        <VoicePlayer uri={clip.uri} initialDuration={clip.duration} waveform={clip.waveform} />
        {!!error && <Text style={{ color: colors.misc.error }}>{error}</Text>}
        <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
          <Pressable
            disabled={busy}
            onPress={() => void discard()}
            style={{ padding: 14, minHeight: 48 }}
          >
            <Text style={{ color: colors.misc.error }}>{uiText("Удалить запись")}</Text>
          </Pressable>
          <Pressable
            disabled={busy}
            onPress={() => {
              setBusy(true);
              void onSend(clip.uri, clip.duration,clip.waveform)
                .then(discard)
                .catch((e) => setError(String(e)))
                .finally(() => setBusy(false));
            }}
            style={{ padding: 14, minHeight: 48 }}
          >
            <Text style={{ color: colors.text.blue }}>
              {busy ? uiText("Отправка…") : uiText("Отправить")}
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
          {seconds} {uiText("с ·")}{" "}
          {locked ? uiText("Запись зафиксирована") : uiText("Влево — отмена, вверх — фиксация")}
        </Text>
      )}
      {locked && (
        <View style={{ flexDirection: "row" }}>
          <Pressable
            onPress={() => void stop(false)}
            style={{ padding: 14, minHeight: 48 }}
          >
            <Text style={{ color: colors.text.blue }}>{uiText("Завершить")}</Text>
          </Pressable>
          <Pressable
            onPress={() => void stop(true)}
            style={{ padding: 14, minHeight: 48 }}
          >
            <Text style={{ color: colors.misc.error }}>{uiText("Отмена")}</Text>
          </Pressable>
        </View>
      )}
        <Animated.View
          {...gesture.panHandlers}
          accessible
          accessibilityRole="button"
          accessibilityActions={[{ name: "activate", label: uiText("Начать запись") }]}
          onAccessibilityAction={() => {
            if (recording.current) void stop(false);
            else void start(true);
          }}
          accessibilityLabel={uiText("Удерживайте для записи голосового")}
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
