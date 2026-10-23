import {t as uiText} from "../../../web/i18n.js";
import { useEffect, useRef, useState } from "react";
import { Audio, type AVPlaybackStatus } from "expo-av";
import { Pressable, Text, View } from "react-native";
import { useTheme } from "../ThemeProvider";
import { ensurePlaybackMode } from "../audioSession";
import { useMediaHeaders } from "../MediaSession";
import { Icon } from "./Icon";
import { registerPlayback, type PlaybackOwner } from "../playbackOwnership";

export function VoicePlayer({ uri, initialDuration=0, waveform:actualWaveform, onListened }: { uri: string; initialDuration?:number;waveform?:number[];onListened?:()=>void }) {
  const { colors } = useTheme(),
    headers = useMediaHeaders();
  const sound = useRef<Audio.Sound | null>(null),
    width = useRef(160);
  const [playing, setPlaying] = useState(false),
    [position, setPosition] = useState(0),
    [duration, setDuration] = useState(initialDuration),
    [rate, setRate] = useState(1),
    [error, setError] = useState("");
  const waveform=actualWaveform?.length?actualWaveform:Array(30).fill(0.12);
  const peak=Math.max(0.001,...waveform);
  const listened=useRef(false),latestListen=useRef(onListened);latestListen.current=onListened;
  const update = (status: AVPlaybackStatus) => {
    if (!status.isLoaded) {
      setPlaying(false);
      return;
    }
    setPlaying(status.isPlaying);
    setPosition(status.positionMillis);
    setDuration(status.durationMillis || 0);
    if(status.isPlaying && status.positionMillis>250 && !listened.current){listened.current=true;latestListen.current?.();}
  };
  const busy=useRef(false),generation=useRef(0),owner=useRef<PlaybackOwner|null>(null);
  useEffect(() => {
    const lease=registerPlayback(async()=>{generation.current++;await sound.current?.pauseAsync().catch(()=>{});});owner.current=lease;
    return()=>{generation.current++;owner.current=null;lease.dispose();const instance=sound.current;sound.current=null;void instance?.unloadAsync().catch(()=>{});};
  },[uri]);
  const toggle = async () => {
    if(busy.current)return;
    busy.current=true;let mine=generation.current;
    try {
      setError("");
      if(playing){await sound.current?.pauseAsync();owner.current?.release();return;}
      const lease=owner.current;if(!lease || !await lease.claim())return;
      mine=generation.current;
      const valid=()=>mine===generation.current && lease.isOwner();
      await ensurePlaybackMode();if(!valid())return;
      if(!sound.current){
        const created=await Audio.Sound.createAsync({uri,headers},{shouldPlay:false,progressUpdateIntervalMillis:200,rate,shouldCorrectPitch:true},status=>{if(mine===generation.current)update(status);});
        if(!valid()){await created.sound.unloadAsync();return;}sound.current=created.sound;
      }
      const instance=sound.current,status=await instance.getStatusAsync();if(!valid())return;
      if(!status.isLoaded){sound.current=null;await instance.unloadAsync().catch(()=>{});return;}
      if(status.durationMillis && status.positionMillis>=status.durationMillis){await instance.setPositionAsync(0);if(!valid())return;}
      await instance.playAsync();if(!valid())await instance.pauseAsync().catch(()=>{});
    }catch(e){if(mine===generation.current)setError(uiText("Не удалось воспроизвести"));}
    finally{busy.current=false;}
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
            playing ? uiText("Приостановить голосовое") : uiText("Воспроизвести голосовое")
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
          accessibilityLabel={uiText("Перемотка голосового")}
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
          {/* Recorded amplitudes; old messages without samples use a flat track. */}
          <View style={{ flexDirection: "row", alignItems: "center", height: 26, gap: 2 }}>
            {waveform.map((height, index) => {
              const played = duration ? index / waveform.length <= position / duration : false;
              return (
                <View
                  key={index}
                  style={{
                    flex: 1,
                    height: Math.max(3, Math.round(Math.sqrt(height/peak) * 26)),
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
