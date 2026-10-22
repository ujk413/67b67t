// Single owner of the audio session.
//
// Android keeps one global audio mode per app. Three features fight over it:
// the voice recorder (needs recording + MODE_IN_COMMUNICATION), playback of
// voice messages / кружки (wants media routing) and WebRTC calls (needs
// communication routing). When the recorder or a call set the mode and nothing
// put it back, every later sound was routed like a call — quiet, through the
// earpiece. This module is the only place allowed to change the mode, so each
// feature asks for what it needs and the previous owner always hands it back.
//
// On Android the native ActiveCallService also resets the route on destroy;
// this layer covers the JS side (recorder, player) and iOS.

import { Audio } from "expo-av";

export type AudioPhase = "playback" | "recording" | "call";

type Mode = {
  allowsRecordingIOS: boolean;
  playsInSilentModeIOS: boolean;
  staysActiveInBackground: boolean;
  interruptionModeIOS: number;
  shouldDuckAndroid: boolean;
  interruptionModeAndroid: number;
  playThroughEarpieceAndroid: boolean;
};

let phase: AudioPhase = "playback";

const playbackMode: Mode = {
  allowsRecordingIOS: false,
  playsInSilentModeIOS: true,
  staysActiveInBackground: false,
  interruptionModeIOS: 1, // DoNotMix: pause other apps instead of talking over them
  shouldDuckAndroid: false,
  interruptionModeAndroid: 1, // DoNotMix
  playThroughEarpieceAndroid: false,
};

const recordingMode: Mode = {
  ...playbackMode,
  allowsRecordingIOS: true,
  playThroughEarpieceAndroid: false,
};

const callMode: Mode = {
  ...playbackMode,
  allowsRecordingIOS: true,
  // A call is the one case that must use the communication route.
  playThroughEarpieceAndroid: true,
};

async function apply(mode: Mode) {
  try {
    await Audio.setAudioModeAsync(mode as Parameters<typeof Audio.setAudioModeAsync>[0]);
  } catch {
    // Older platforms ignore individual keys; the next call retries.
  }
}

/** Media playback: music, voice messages, video notes. */
export async function ensurePlaybackMode(): Promise<void> {
  if (phase === "playback") {
    // Still re-apply once: another app or a crashed call may have changed it.
    await apply(playbackMode);
    return;
  }
  phase = "playback";
  await apply(playbackMode);
}

/** Recording a voice message or a circle. */
export async function ensureRecordingMode(): Promise<void> {
  if (phase === "recording") return;
  phase = "recording";
  await apply(recordingMode);
}

/**
 * A call started. Recording is not allowed to keep the session afterwards, so
 * callers must pair this with `endCall()` in a finally block.
 */
export async function beginCall(): Promise<void> {
  phase = "call";
  await apply(callMode);
}

/**
 * The call is over — whatever the outcome (hang up, decline, network drop,
 * timeout). Restores media routing so the next voice message is audible.
 */
export async function endCall(): Promise<void> {
  phase = "playback";
  await apply(playbackMode);
}

export function currentPhase(): AudioPhase {
  return phase;
}
