import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url),
  ts = require("../mobile/node_modules/typescript");

// Load the audio session module with expo-av stubbed, recording every mode that
// is applied. The bug this guards: leaving recording/communication mode on
// after a call or a voice message made all later audio play "like a call".
function loadAudioSession() {
  const applied = [];
  const source = fs.readFileSync(
    new URL("../mobile/src/audioSession.ts", import.meta.url),
    "utf8",
  );
  const exports = {};
  const js = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2022,
    },
  }).outputText;
  const fakeRequire = (name) => {
    if (name === "expo-av") {
      return {
        Audio: {
          setAudioModeAsync: async (mode) => {
            applied.push(mode);
          },
        },
      };
    }
    throw new Error(`Unexpected import ${name}`);
  };
  new Function("exports", "require", js)(exports, fakeRequire);
  return { ...exports, applied };
}

test("recording hands the audio session back to playback when it ends", async () => {
  const audio = loadAudioSession();
  await audio.ensurePlaybackMode();
  assert.equal(audio.applied.at(-1).allowsRecordingIOS, false);

  await audio.ensureRecordingMode();
  assert.equal(audio.applied.at(-1).allowsRecordingIOS, true);

  await audio.ensurePlaybackMode();
  assert.equal(audio.applied.at(-1).allowsRecordingIOS, false);
  assert.equal(audio.currentPhase(), "playback");
});

test("a finished call restores media routing instead of leaving call mode on", async () => {
  const audio = loadAudioSession();
  await audio.beginCall();
  const during = audio.applied.at(-1);
  assert.equal(during.allowsRecordingIOS, true);
  assert.equal(during.playThroughEarpieceAndroid, true);
  assert.equal(audio.currentPhase(), "call");

  await audio.endCall();
  const after = audio.applied.at(-1);
  // The regression: these two must go back to media values, otherwise the next
  // voice message is quiet and routed to the earpiece.
  assert.equal(after.playThroughEarpieceAndroid, false);
  assert.equal(after.allowsRecordingIOS, false);
  assert.equal(after.playsInSilentModeIOS, true);
  assert.equal(audio.currentPhase(), "playback");
});

test("playback never ducks other apps and never stays in the background", async () => {
  const audio = loadAudioSession();
  await audio.ensurePlaybackMode();
  const mode = audio.applied.at(-1);
  assert.equal(mode.shouldDuckAndroid, false);
  assert.equal(mode.staysActiveInBackground, false);
});

test("a failing setAudioModeAsync does not throw into the caller", async () => {
  const source = fs.readFileSync(
    new URL("../mobile/src/audioSession.ts", import.meta.url),
    "utf8",
  );
  const js = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2022,
    },
  }).outputText;
  const exports = {};
  const fakeRequire = () => ({
    Audio: {
      setAudioModeAsync: async () => {
        throw new Error("unsupported");
      },
    },
  });
  new Function("exports", "require", js)(exports, fakeRequire);
  await assert.doesNotReject(exports.endCall());
  assert.equal(exports.currentPhase(), "playback");
});
