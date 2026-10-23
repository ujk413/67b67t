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
import { setMediaCallActive, pauseAllMedia } from "./playbackOwnership";

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
let version = 0, callOwner = 0, queue: Promise<void> = Promise.resolve();

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

function apply(mode: Mode): Promise<void> {
  const mine = ++version;
  const job = queue.catch(() => {}).then(async () => {
    if (mine !== version) return;
    try { await Audio.setAudioModeAsync(mode as Parameters<typeof Audio.setAudioModeAsync>[0]); } catch {}
  });
  queue = job;
  return job;
}

/** Media playback: music, voice messages, video notes. */
export async function ensurePlaybackMode(): Promise<void> {
  if (phase === "call") return;
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
  if (phase === "call") throw Error("Завершите звонок перед записью сообщения");
  await pauseAllMedia();
  if ((phase as AudioPhase) === "call") throw Error("Завершите звонок перед записью сообщения");
  if (phase === "recording") return;
  phase = "recording";
  await apply(recordingMode);
  if ((phase as AudioPhase) === "call") throw Error("Завершите звонок перед записью сообщения");
}

/**
 * A call started. Recording is not allowed to keep the session afterwards, so
 * callers must pair this with `endCall()` in a finally block.
 */
export async function beginCall(): Promise<number> {
  const owner = ++callOwner; phase = "call";
  await setMediaCallActive(true);
  if (owner === callOwner && phase === "call") await apply(callMode);
  return owner;
}

/**
 * The call is over — whatever the outcome (hang up, decline, network drop,
 * timeout). Restores media routing so the next voice message is audible.
 */
export async function endCall(owner?: number): Promise<void> {
  if (owner !== undefined && owner !== callOwner) return;
  callOwner++; phase = "playback";
  await setMediaCallActive(false);
  if (phase === "playback") await apply(playbackMode);
}

export function currentPhase(): AudioPhase {
  return phase;
}
