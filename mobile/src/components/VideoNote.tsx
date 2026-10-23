import { useEffect, useRef, useState } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";
import { useVideoPlayer, VideoView } from "expo-video";
import Svg, { Circle } from "react-native-svg";
import { useTheme } from "../ThemeProvider";
import { useMediaHeaders } from "../MediaSession";
import { Icon } from "./Icon";

const SIZE = 200;
const RADIUS = SIZE / 2 - 4;
const CIRCUMFERENCE = 2 * Math.PI * RADIUS;

function clock(seconds: number) {
  const total = Math.max(0, Math.round(seconds || 0));
  return `${Math.floor(total / 60)}:${String(total % 60).padStart(2, "0")}`;
}

/**
 * Video note (кружок) as Telegram draws it: a circle, tap to play/pause,
 * a progress ring along the edge, the duration until the first play and a
 * mute toggle. Tapping no longer opens the full-screen viewer — that only
 * happens on the viewer button, so a circle behaves like a voice message.
 */
export function VideoNote({
  uri,
  durationSec,
  onOpenViewer,
  onLongPress,
}: {
  uri: string;
  durationSec?: number | null;
  onOpenViewer?: () => void;
  onLongPress?: () => void;
}) {
  const { colors } = useTheme();
  const headers = useMediaHeaders();
  const player = useVideoPlayer({ uri, headers }, (instance) => {
    instance.timeUpdateEventInterval = 0.1;
    // Start muted: a tap should not blast audio unexpectedly, and the mute
    // button is the explicit opt-in, as in Telegram.
    instance.muted = true;
    instance.loop = true;
  });
  const [playing, setPlaying] = useState(false);
  const [muted, setMuted] = useState(true);
  const [progress, setProgress] = useState(0);
  const started = useRef(false);

  useEffect(() => {
    const timeSub = player.addListener("timeUpdate", ({ currentTime }) => {
      const total = player.duration || 0;
      setProgress(total > 0 ? Math.min(1, currentTime / total) : 0);
    });
    const playingSub = player.addListener("playingChange", ({ isPlaying }) => {
      setPlaying(isPlaying);
      if (isPlaying) started.current = true;
    });
    return () => {
      timeSub.remove();
      playingSub.remove();
    };
  }, [player]);

  const toggle = () => {
    if (playing) player.pause();
    else player.play();
  };

  const shownDuration = durationSec && durationSec > 0 ? clock(durationSec) : "";

  return (
    <View style={styles.wrapper}>
      <Pressable
        onPress={toggle}
        onLongPress={onLongPress}
        accessibilityRole="button"
        accessibilityLabel={`Видеокружок${shownDuration ? `, ${shownDuration}` : ""}`}
        style={styles.circle}
      >
        <VideoView
          player={player}
          style={styles.video}
          nativeControls={false}
          contentFit="cover"
          surfaceType="textureView"
        />

        <Svg width={SIZE} height={SIZE} style={StyleSheet.absoluteFill} pointerEvents="none">
          <Circle
            cx={SIZE / 2}
            cy={SIZE / 2}
            r={RADIUS}
            stroke="rgba(255,255,255,0.25)"
            strokeWidth={3}
            fill="none"
          />
          <Circle
            cx={SIZE / 2}
            cy={SIZE / 2}
            r={RADIUS}
            stroke="#FFFFFF"
            strokeWidth={3}
            fill="none"
            strokeLinecap="round"
            strokeDasharray={CIRCUMFERENCE}
            strokeDashoffset={CIRCUMFERENCE * (1 - progress)}
            transform={`rotate(-90 ${SIZE / 2} ${SIZE / 2})`}
            opacity={progress > 0 ? 1 : 0}
          />
        </Svg>

        {!playing && !started.current && shownDuration ? (
          <View style={styles.badge} pointerEvents="none">
            <Text style={styles.badgeText}>{shownDuration}</Text>
          </View>
        ) : null}

        {!playing && (
          <View style={styles.playOverlay} pointerEvents="none">
            <Icon name="play" size={26} color="#FFFFFF" />
          </View>
        )}
      </Pressable>

      <View style={styles.sideControls}>
        <Pressable
          onPress={() => {
            player.muted = !muted;
            setMuted(!muted);
          }}
          accessibilityRole="button"
          accessibilityLabel={muted ? "Включить звук" : "Выключить звук"}
          accessibilityState={{ selected: !muted }}
          hitSlop={8}
          style={[styles.sideButton, !muted && { backgroundColor: colors.text.blue }]}
        >
          <Icon name="speaker" size={16} color="#FFFFFF" />
        </Pressable>
        {onOpenViewer ? (
          <Pressable
            onPress={onOpenViewer}
            accessibilityRole="button"
            accessibilityLabel="Открыть на весь экран"
            hitSlop={8}
            style={styles.sideButton}
          >
            <Icon name="search" size={16} color="#FFFFFF" />
          </Pressable>
        ) : null}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  wrapper: { flexDirection: "row", alignItems: "flex-end", gap: 6 },
  circle: {
    width: SIZE,
    height: SIZE,
    borderRadius: SIZE / 2,
    overflow: "hidden",
    backgroundColor: "#000",
    alignItems: "center",
    justifyContent: "center",
  },
  video: { ...StyleSheet.absoluteFillObject, borderRadius: SIZE / 2 },
  badge: {
    position: "absolute",
    bottom: 16,
    paddingHorizontal: 9,
    paddingVertical: 2,
    borderRadius: 11,
    backgroundColor: "rgba(0,0,0,0.55)",
  },
  badgeText: { color: "#FFFFFF", fontSize: 11, fontVariant: ["tabular-nums"] },
  playOverlay: {
    position: "absolute",
    width: 56,
    height: 56,
    borderRadius: 28,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "rgba(0,0,0,0.35)",
  },
  sideControls: { gap: 6, paddingBottom: 10 },
  sideButton: {
    width: 32,
    height: 32,
    borderRadius: 16,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "rgba(0,0,0,0.45)",
  },
});
