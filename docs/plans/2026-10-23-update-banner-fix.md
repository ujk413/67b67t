# Android update banner regression

- [x] Trace skipped request to version detection and inspect Expo native exports.
- [x] Reproduce with Expo 55-shaped native-module fixtures (no version fields on Constants). Five tests failed, including the missing HTTP request.
- [x] Read installed version from expo-application; keep Expo Go unsupported.
- [x] Run mobile/API suites and TypeScript checks: 73 mobile, 48 API, TypeScript clean.
- [x] Build signed arm64 bootstrap APK and install on connected device without clearing data.
- [x] Publish a higher versionCode through CI and verify public manifest/APK.
- [ ] Visually verify an older fixed build offers the newer published build (blocked by a concurrent device update, see below).

Scope: missing update banner only. No unrelated messenger changes. Existing installed builds contain the broken check, so they require one bootstrap installation that preserves data.

Bootstrap: assembled a signed arm64 build 12 with the fix and installed using `adb install -r` (success). Device previously had build 11 and reported 12 after installation. Certificate SHA-256 matches `1a218f859a412f623104d83aaaad0a44abdad3ed665060ac8085902891dc9ab8`. The device was locked at that time; no attempt was made to bypass the lock screen.

Publication: commit `d40950d`, Android CI run `39903918699` succeeded. Production serves 1.3.2 / code 13. Downloaded the public APK and verified its SHA-256 against the manifest: `3142a5df889d37433c615812bd5e7712d88a8d6ec97bef8b2901f673339196b7`; package code/name, arm64-only ABI and release certificate all match.

Device verification limitation: during CI, concurrent work installed 1.3.3 / code 14 on the phone (device lastUpdateTime 2026-10-23 08:20:34). The phone was unlocked afterward; restarting the app issued GET /app/version, with server HTTP 200 at 05:22:30 UTC. No banner was present, correctly for installed 14 > published 13. This is not proof of the older-to-newer banner transition. Do not downgrade the phone or overwrite parallel working-tree changes to force that test.
