# Build Instructions - Complete Guide

## Prerequisites

1. **Node.js & npm** installed
2. **Android Studio** installed (for Android development)
3. **Java JDK 17** installed
4. **Android SDK** configured

## Step 1: Install Dependencies

```bash
cd mobile
npm install
```

## Step 2: Configure Firebase

### A. Create Firebase Project

1. Go to https://console.firebase.google.com/
2. Click "Add project"
3. Name: "Messenger67" (or any name)
4. Disable Google Analytics (optional)
5. Click "Create project"

### B. Add Android App

1. In Firebase Console, click "Add app" → Android
2. Android package name: `ru.b67t.messenger.mobile`
3. App nickname: "Messenger67 Mobile"
4. Click "Register app"

### C. Download Config File

1. Download `google-services.json`
2. Place it in: `mobile/google-services.json`

### D. Get Server Key

1. Firebase Console → Project Settings → Cloud Messaging
2. Copy "Server key"
3. Add to `.env` file in project root:

```bash
FCM_SERVER_KEY=your_server_key_here
```

## Step 3: Prebuild Native Projects

```bash
npx expo prebuild --clean
```

This will generate `android/` and `ios/` directories.

## Step 4: Build for Android

### Option A: Development Build (Recommended)

```bash
npx expo run:android
```

This will:
- Build the app
- Install on connected device/emulator
- Start Metro bundler

### Option B: Release Build

```bash
cd android
./gradlew assembleRelease
```

APK will be in: `android/app/build/outputs/apk/release/app-release.apk`

## Step 5: Run Database Migration

On your server:

```bash
psql $DATABASE_URL < db/migrations/003_add_fcm_token.sql
```

Or manually:

```sql
ALTER TABLE users ADD COLUMN IF NOT EXISTS fcm_token text;
CREATE INDEX IF NOT EXISTS users_fcm_token_idx ON users(fcm_token) WHERE fcm_token IS NOT NULL;
```

## Step 6: Start Server with FCM

Make sure your API server has FCM_SERVER_KEY in .env:

```bash
cd api
# Add to .env:
# FCM_SERVER_KEY=your_server_key

# Restart server
docker-compose restart api
```

## Troubleshooting

### Build Errors

**Error: "SDK location not found"**
```bash
# Create local.properties in android/
echo "sdk.dir=/Users/YOUR_USERNAME/Library/Android/sdk" > android/local.properties
# Or on Windows:
echo "sdk.dir=C:\\Users\\YOUR_USERNAME\\AppData\\Local\\Android\\Sdk" > android/local.properties
```

**Error: "Execution failed for task ':app:processDebugGoogleServices'"**
- Make sure `google-services.json` is in `mobile/` directory
- Check package name matches: `ru.b67t.messenger.mobile`

**Error: "Could not resolve all files for configuration"**
```bash
cd android
./gradlew clean
cd ..
npx expo prebuild --clean
```

### Runtime Errors

**WebRTC not working:**
- Must test on real device (not emulator)
- Check camera/microphone permissions granted
- Check network connectivity

**Push notifications not working:**
- Check FCM_SERVER_KEY in server .env
- Check google-services.json is correct
- Check app is in background when testing
- Check device has Google Play Services

**E2E encryption errors:**
- Check both users have initialized keys (login once)
- Check console logs for crypto errors
- Try deleting app data and re-login

### Logs

**View Android logs:**
```bash
npx react-native log-android
```

**View Metro bundler logs:**
```bash
npx expo start
```

**View native logs:**
```bash
adb logcat | grep -i "messenger"
```

## Testing Checklist

### Basic Features
- [ ] Login with existing account
- [ ] Register new account
- [ ] View room list
- [ ] Open a room
- [ ] Send text message
- [ ] Receive text message
- [ ] Upload image
- [ ] Upload video
- [ ] Record voice message
- [ ] Record video note
- [ ] Add reaction
- [ ] Edit profile
- [ ] Change avatar

### E2E Encryption
- [ ] Send message in DM chat
- [ ] Receive encrypted message in DM
- [ ] Verify message decrypts correctly
- [ ] Check console for "E2E encryption initialized"

### Push Notifications
- [ ] Close app completely
- [ ] Send message from another device
- [ ] Receive push notification
- [ ] Tap notification → app opens to room
- [ ] Check badge count updates

### Voice Calls
- [ ] Start voice call from DM chat
- [ ] Answer incoming voice call
- [ ] Mute/unmute microphone
- [ ] End call
- [ ] Reject incoming call

### Video Calls
- [ ] Start video call from DM chat
- [ ] Answer incoming video call
- [ ] Toggle video on/off
- [ ] Switch camera (front/back)
- [ ] Mute/unmute microphone
- [ ] End call

## Performance Tips

### Optimize Build Time
```bash
# Use Gradle daemon
echo "org.gradle.daemon=true" >> android/gradle.properties
echo "org.gradle.parallel=true" >> android/gradle.properties
echo "org.gradle.configureondemand=true" >> android/gradle.properties
```

### Reduce APK Size
```bash
# Enable ProGuard in android/app/build.gradle
android {
    buildTypes {
        release {
            minifyEnabled true
            shrinkResources true
            proguardFiles getDefaultProguardFile('proguard-android-optimize.txt'), 'proguard-rules.pro'
        }
    }
}
```

## Next Steps

After successful build and testing:

1. **Generate signed APK** for distribution
2. **Set up CI/CD** for automated builds
3. **Configure app signing** for Play Store
4. **Add crash reporting** (Sentry, Firebase Crashlytics)
5. **Add analytics** (optional)

## Support

If you encounter issues:

1. Check logs: `npx react-native log-android`
2. Clean build: `cd android && ./gradlew clean`
3. Rebuild: `npx expo prebuild --clean`
4. Check Firebase configuration
5. Verify server is running with FCM_SERVER_KEY

## Build Complete! 🎉

Your app is ready to test. Run:

```bash
npx expo run:android
```

And start testing all features!
