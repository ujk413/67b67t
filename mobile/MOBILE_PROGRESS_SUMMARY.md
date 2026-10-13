# Mobile App Development - Summary

## Completed (2026-04-10)

### Backend (API Server)
- ✅ Added FCM_SERVER_KEY environment variable
- ✅ Added `sendPushNotification()` function for FCM
- ✅ Added POST `/me/fcm-token` endpoint for token registration
- ✅ Integrated push notifications into message sending
- ✅ Created migration `003_add_fcm_token.sql` for users table
- ✅ Push notifications sent only to offline users
- ✅ Notifications include room info and message preview

### Mobile App
- ✅ Updated package.json with dependencies:
  - @react-native-firebase/app & messaging
  - @privacyresearch/libsignal-protocol-typescript
  - expo-notifications
  - expo-device
  - react-native-webrtc
- ✅ Updated app.json with:
  - Firebase plugins
  - Android permissions (CAMERA, RECORD_AUDIO, etc.)
  - iOS permissions (NSCameraUsageDescription, etc.)
  - Google Services file paths
- ✅ Created `src/crypto.ts` - Signal Protocol E2E encryption
- ✅ Created `src/notifications.ts` - FCM push notifications
- ✅ Created `src/webrtc.ts` - Voice/Video calls
- ✅ Updated App.tsx:
  - Added imports for crypto, notifications, webrtc
  - Added call state management
  - Integrated E2E initialization in bootstrap
  - Integrated push notifications in bootstrap
  - Added registerFCMToken to API client
- ✅ Created FIREBASE_SETUP.md with setup instructions

## Next Steps

### 1. Complete App.tsx Integration (2-3 hours)
- [ ] Add WebSocket handlers for call signaling
- [ ] Add call UI components (incoming call modal, active call screen)
- [ ] Integrate E2E encryption for messages
- [ ] Add call buttons to chat header
- [ ] Handle call lifecycle (answer, reject, end)

### 2. Testing & Debugging (1-2 hours)
- [ ] Install dependencies: `cd mobile && npm install`
- [ ] Setup Firebase project and download config files
- [ ] Run prebuild: `npx expo prebuild`
- [ ] Test on real Android device: `npx expo run:android`
- [ ] Test E2E encryption
- [ ] Test push notifications
- [ ] Test voice/video calls

### 3. Database Migration
- [ ] Run migration on server: `psql $DATABASE_URL < db/migrations/003_add_fcm_token.sql`
- [ ] Set FCM_SERVER_KEY in .env

## Architecture

### E2E Encryption Flow
1. User logs in → `e2e.initialize(userId)` generates keys
2. Keys stored in AsyncStorage with Signal Protocol
3. Messages encrypted before sending
4. Messages decrypted on receive
5. Server never sees plaintext

### Push Notifications Flow
1. App starts → FCM token generated
2. Token sent to server via `/me/fcm-token`
3. User receives message while offline
4. Server sends FCM notification
5. User taps notification → app opens to room

### WebRTC Calls Flow
1. Caller initiates → WebSocket signaling
2. Offer/Answer exchange via WebSocket
3. ICE candidates exchanged
4. DTLS-SRTP encrypted media stream
5. Call UI shows local/remote video

## Files Modified
- api/server.js (+60 lines)
- mobile/package.json
- mobile/app.json
- mobile/App.tsx (+30 lines)
- mobile/src/api.ts (+6 lines)
- mobile/src/crypto.ts (NEW, 350 lines)
- mobile/src/notifications.ts (NEW, 120 lines)
- mobile/src/webrtc.ts (NEW, 150 lines)
- db/migrations/003_add_fcm_token.sql (NEW)
- .env.example (+3 lines)

## Progress: 25% → 40% (after completing App.tsx integration)

**Estimated time to completion: 3-5 hours**
