import { useEffect, useRef, useState } from "react";
import { Audio, type AVPlaybackStatus } from "expo-av";
import { Pressable, Text, View } from "react-native";
import { useTheme } from "../ThemeProvider";
import { useMediaHeaders } from "../MediaSession";
import { Icon } from "./Icon";
let current: Audio.Sound | null = null;
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
          style={{ flex: 1, paddingVertical: 20, marginHorizontal: 8 }}
        >
          <View
            style={{
              height: 5,
              borderRadius: 3,
              backgroundColor: colors.divider,
            }}
          >
            <View
              style={{
                height: 5,
                width: `${duration ? (position / duration) * 100 : 0}%`,
                backgroundColor: colors.text.blue,
                borderRadius: 3,
              }}
            />
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
