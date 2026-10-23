# Android update banner regression

- [x] Trace skipped request to version detection and inspect Expo native exports.
- [x] Reproduce with Expo 55-shaped native-module fixtures (no version fields on Constants). Five tests failed, including the missing HTTP request.
- [x] Read installed version from expo-application; keep Expo Go unsupported.
- [x] Run mobile/API suites and TypeScript checks: 73 mobile, 48 API, TypeScript clean.
- [ ] Build signed arm64 bootstrap APK and verify banner on connected device against the published release.
- [ ] Publish a higher versionCode through CI and verify public manifest/APK.

Scope: missing update banner only. No unrelated messenger changes. Existing installed builds contain the broken check, so they require one bootstrap installation that preserves data.

Bootstrap: assembled a signed arm64 build 12 with the fix and installed using `adb install -r` (success). Device previously had build 11; now reports 12. Certificate SHA-256 matches `1a218f859a412f623104d83aaaad0a44abdad3ed665060ac8085902891dc9ab8`. Device lock screen currently prevents visual confirmation; do not bypass it. Publish build 13 to test real 12→13 discovery.
