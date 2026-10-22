import { useEffect, useRef, useState } from "react";
import { Audio, type AVPlaybackStatus } from "expo-av";
import { Pressable, Text, View } from "react-native";
import { useTheme } from "../ThemeProvider";
import { ensurePlaybackMode } from "../audioSession";
import { useMediaHeaders } from "../MediaSession";
import { Icon } from "./Icon";
let current: Audio.Sound | null = null;

// Stable pseudo-waveform per media URI: same message always draws the same
// bars, without shipping or decoding an actual waveform.
const WAVE_BARS = 30;
function waveformFor(seed: string): number[] {
  let hash = 2166136261;
  for (let i = 0; i < seed.length; i += 1) {
    hash ^= seed.charCodeAt(i);
    hash = Math.imul(hash, 16777619) >>> 0;
  }
  const bars: number[] = [];
  for (let i = 0; i < WAVE_BARS; i += 1) {
    hash ^= hash << 13;
    hash ^= hash >>> 17;
    hash ^= hash << 5;
    hash >>>= 0;
    // 0.18..1 keeps the shape readable but never a flat line.
    bars.push(0.18 + (hash % 1000) / 1000 * 0.82);
  }
  return bars;
}

export function VoicePlayer({ uri, initialDuration=0 }: { uri: string; initialDuration?:number }) {
  const { colors } = useTheme(),
    headers = useMediaHeaders();
  const sound = useRef<Audio.Sound | null>(null),
    width = useRef(160);
  const [playing, setPlaying] = useState(false),
    [position, setPosition] = useState(0),
    [duration, setDuration] = useState(initialDuration),
    [rate, setRate] = useState(1),
    [error, setError] = useState("");
  const waveform = useRef(waveformFor(uri)).current;
  const update = (status: AVPlaybackStatus) => {
    if (!status.isLoaded) {
      setPlaying(false);
      return;
    }
    setPlaying(status.isPlaying);
    setPosition(status.positionMillis);
    setDuration(status.durationMillis || 0);
  };
  const busy=useRef(false),generation=useRef(0);
  useEffect(
    () => () => {
      generation.current++;
      const instance = sound.current;
      sound.current = null;
      if (current === instance) current = null;
      void instance?.unloadAsync().catch(() => {});
    },
    [uri],
  );
  const toggle = async () => {
    if(busy.current)return;
    busy.current=true;const mine=generation.current;
    try {
      setError("");
      if (playing) {
        await sound.current?.pauseAsync();
        return;
      }
      if (current && current !== sound.current) {
        await current.unloadAsync().catch(() => {});
        current = null;
      }
      // A previous call or recording may have left the communication route on.
      await ensurePlaybackMode();
      if (!sound.current) {
        const created = await Audio.Sound.createAsync(
          { uri, headers },
          {
            shouldPlay: true,
            progressUpdateIntervalMillis: 200,
            rate,
            shouldCorrectPitch: true,
          },
          update,
        );
        if(mine!==generation.current){await created.sound.unloadAsync();return;}
        sound.current = created.sound;
      } else {
        const status = await sound.current.getStatusAsync();
        if (!status.isLoaded) {
          sound.current = null;
          busy.current=false;
          await toggle();
          return;
        }
        if (
          status.durationMillis &&
          status.positionMillis >= status.durationMillis
        )
          await sound.current.setPositionAsync(0);
        await sound.current.playAsync();
      }
      current = sound.current;
    } catch (e) {
      setError("Не удалось воспроизвести");
      sound.current = null;
    }finally{busy.current=false;}
  };
  const time = (ms: number) =>
    `${Math.floor(ms / 60000)}:${String(Math.floor(ms / 1000) % 60).padStart(2, "0")}`;
  return (
    <View style={{ minWidth: 180, paddingVertical: 4 }}>
      <View style={{ flexDirection: "row", alignItems: "center" }}>
        <Pressable
          onPress={() => void toggle()}
          accessibilityRole="button"
          accessibilityLabel={
            playing ? "Приостановить голосовое" : "Воспроизвести голосовое"
          }
          style={{
            minWidth: 48,
            minHeight: 48,
            justifyContent: "center",
            alignItems: "center",
          }}
        >
          <Icon name={playing ? "pause" : "play"} color={colors.text.blue} />
        </Pressable>
        <Pressable
          accessibilityRole="adjustable"
          accessibilityLabel="Перемотка голосового"
          onLayout={(e) => {
            width.current = e.nativeEvent.layout.width;
          }}
          onPress={(event) => {
            void sound.current?.setPositionAsync(
              Math.round(
                (event.nativeEvent.locationX / width.current) * duration,
              ),
            );
          }}
          style={{ flex: 1, paddingVertical: 18, marginHorizontal: 8 }}
        >
          {/* Waveform instead of a flat bar: bars are derived from the media URI
              so every message keeps a stable shape, and the played part is
              tinted — the way Telegram draws a voice message. */}
          <View style={{ flexDirection: "row", alignItems: "center", height: 26, gap: 2 }}>
            {waveform.map((height, index) => {
              const played = duration ? index / waveform.length <= position / duration : false;
              return (
                <View
                  key={index}
                  style={{
                    flex: 1,
                    height: Math.max(3, Math.round(height * 26)),
                    borderRadius: 2,
                    backgroundColor: played ? colors.text.blue : colors.divider,
                  }}
                />
              );
            })}
          </View>
        </Pressable>
        <Pressable
          onPress={() => {
            const next = rate === 1 ? 1.5 : rate === 1.5 ? 2 : 1;
            setRate(next);
            void sound.current?.setRateAsync(next, true);
          }}
          style={{ minWidth: 48, minHeight: 48, justifyContent: "center" }}
        >
          <Text style={{ color: colors.text.blue }}>{rate}×</Text>
        </Pressable>
      </View>
      <Text style={{ color: colors.text.secondary, fontSize: 12 }}>
        {error || `${time(position)} / ${time(duration)}`}
      </Text>
    </View>
  );
}
