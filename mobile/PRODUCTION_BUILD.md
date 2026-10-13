# Production Build Guide

## Prerequisites

1. **Keystore for signing** (Android)
2. **Firebase configured** (google-services.json)
3. **Server deployed** with FCM_SERVER_KEY

## Step 1: Generate Keystore

```bash
keytool -genkeypair -v -storetype PKCS12 -keystore messenger67.keystore -alias messenger67 -keyalg RSA -keysize 2048 -validity 10000
```

**Save these values:**
- Keystore password: ___________
- Key alias: messenger67
- Key password: ___________

**Store keystore securely!** Never commit to git.

## Step 2: Configure Signing

Create `android/gradle.properties`:

```properties
MESSENGER_UPLOAD_STORE_FILE=messenger67.keystore
MESSENGER_UPLOAD_KEY_ALIAS=messenger67
MESSENGER_UPLOAD_STORE_PASSWORD=your_keystore_password
MESSENGER_UPLOAD_KEY_PASSWORD=your_key_password
```

Update `android/app/build.gradle`:

```gradle
android {
    ...
    signingConfigs {
        release {
            if (project.hasProperty('MESSENGER_UPLOAD_STORE_FILE')) {
                storeFile file(MESSENGER_UPLOAD_STORE_FILE)
                storePassword MESSENGER_UPLOAD_STORE_PASSWORD
                keyAlias MESSENGER_UPLOAD_KEY_ALIAS
                keyPassword MESSENGER_UPLOAD_KEY_PASSWORD
            }
        }
    }
    buildTypes {
        release {
            signingConfig signingConfigs.release
            minifyEnabled true
            shrinkResources true
            proguardFiles getDefaultProguardFile('proguard-android-optimize.txt'), 'proguard-rules.pro'
        }
    }
}
```

## Step 3: Update Version

Update `app.json`:

```json
{
  "expo": {
    "version": "1.0.0",
    "android": {
      "versionCode": 1
    }
  }
}
```

Increment `versionCode` for each release.

## Step 4: Build Release APK

```bash
cd android
./gradlew assembleRelease
```

APK location: `android/app/build/outputs/apk/release/app-release.apk`

## Step 5: Build Release AAB (for Play Store)

```bash
cd android
./gradlew bundleRelease
```

AAB location: `android/app/build/outputs/bundle/release/app-release.aab`

## Step 6: Test Release Build

```bash
# Install release APK
adb install android/app/build/outputs/apk/release/app-release.apk

# Test all features
# See TESTING_GUIDE.md
```

## Step 7: Optimize APK Size

### Enable ProGuard

Already configured in build.gradle above.

### Remove unused resources

Add to `android/app/build.gradle`:

```gradle
android {
    buildTypes {
        release {
            shrinkResources true
        }
    }
}
```

### Split APKs by ABI

```gradle
android {
    splits {
        abi {
            enable true
            reset()
            include 'armeabi-v7a', 'arm64-v8a', 'x86', 'x86_64'
            universalApk true
        }
    }
}
```

## Step 8: Prepare for Play Store

### Create Store Listing

1. **App name:** Messenger67
2. **Short description:** Secure messenger with E2E encryption
3. **Full description:**

```
Messenger67 - Secure Messaging with End-to-End Encryption

Features:
• End-to-end encryption using Signal Protocol
• Voice and video calls
• Send photos, videos, and files
• Voice messages and video notes
• Message reactions
• Push notifications
• Modern, clean interface

Privacy First:
• Messages encrypted on your device
• Server cannot read your messages
• Open source

Perfect for:
• Private conversations
• Secure group chats
• Voice/video calls with friends
```

4. **Category:** Communication
5. **Content rating:** Everyone
6. **Privacy policy:** (required - create one)

### Screenshots Required

- At least 2 screenshots (1080x1920 recommended)
- Feature graphic (1024x500)
- App icon (512x512)

### Upload AAB

1. Go to Play Console
2. Create new release
3. Upload `app-release.aab`
4. Fill in release notes
5. Submit for review

## Step 9: Post-Release

### Monitor Crashes

Set up crash reporting:

```bash
npm install @sentry/react-native
```

Or use Firebase Crashlytics.

### Monitor Performance

- Check Play Console vitals
- Monitor ANR (Application Not Responding) rate
- Check crash-free users percentage

### Update Strategy

- Patch releases: 1.0.1, 1.0.2 (bug fixes)
- Minor releases: 1.1.0, 1.2.0 (new features)
- Major releases: 2.0.0 (breaking changes)

## Troubleshooting

### Build fails with ProGuard

Add to `android/app/proguard-rules.pro`:

```
-keep class com.facebook.react.** { *; }
-keep class com.oney.WebRTCModule.** { *; }
-keep class org.webrtc.** { *; }
```

### APK too large

- Enable APK splits by ABI
- Remove unused resources
- Compress images
- Use WebP format for images

### Signing errors

- Verify keystore path is correct
- Check passwords in gradle.properties
- Ensure keystore file exists

## Security Checklist

- [ ] Keystore stored securely (not in git)
- [ ] Passwords not hardcoded
- [ ] ProGuard enabled
- [ ] No debug logs in release
- [ ] API keys secured
- [ ] HTTPS only
- [ ] Certificate pinning (optional)

## Release Checklist

- [ ] Version incremented
- [ ] Changelog updated
- [ ] All tests passing
- [ ] No critical bugs
- [ ] Performance acceptable
- [ ] APK/AAB signed
- [ ] Release notes written
- [ ] Screenshots updated
- [ ] Store listing reviewed

## Automated Release (CI/CD)

### GitHub Actions Example

Create `.github/workflows/release.yml`:

```yaml
name: Release

on:
  push:
    tags:
      - 'v*'

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Node
        uses: actions/setup-node@v3
        with:
          node-version: 18
      
      - name: Install dependencies
        run: |
          cd mobile
          npm install
      
      - name: Build AAB
        run: |
          cd mobile/android
          ./gradlew bundleRelease
        env:
          MESSENGER_UPLOAD_STORE_FILE: ${{ secrets.KEYSTORE_FILE }}
          MESSENGER_UPLOAD_STORE_PASSWORD: ${{ secrets.KEYSTORE_PASSWORD }}
          MESSENGER_UPLOAD_KEY_ALIAS: ${{ secrets.KEY_ALIAS }}
          MESSENGER_UPLOAD_KEY_PASSWORD: ${{ secrets.KEY_PASSWORD }}
      
      - name: Upload to Play Store
        uses: r0adkll/upload-google-play@v1
        with:
          serviceAccountJsonPlainText: ${{ secrets.SERVICE_ACCOUNT_JSON }}
          packageName: ru.b67t.messenger.mobile
          releaseFiles: mobile/android/app/build/outputs/bundle/release/app-release.aab
          track: production
```

## Support

After release:
- Monitor Play Console reviews
- Respond to user feedback
- Fix critical bugs quickly
- Plan feature updates

## Success Metrics

Track:
- Daily active users (DAU)
- Retention rate (Day 1, Day 7, Day 30)
- Crash-free users %
- Average session duration
- Feature usage (calls, E2E, etc.)

---

**Ready for production!** 🚀
