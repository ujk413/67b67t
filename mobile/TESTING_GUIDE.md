# Messenger67 Mobile - Testing Guide

## Pre-Testing Setup

### 1. Build and Install
```bash
cd mobile
npm install
npx expo prebuild --clean
npx expo run:android
```

### 2. Create Test Accounts

Create 2 accounts for testing:
- User A: `testuser1` / `password123`
- User B: `testuser2` / `password123`

## Test Scenarios

### ✅ Basic Functionality

#### 1. Authentication
- [ ] Register new account
- [ ] Login with credentials
- [ ] Logout
- [ ] Login again (session persistence)

#### 2. Room List
- [ ] View all rooms
- [ ] Pull to refresh
- [ ] See unread counts
- [ ] See last message preview

#### 3. Messaging
- [ ] Send text message
- [ ] Receive text message (real-time)
- [ ] See typing indicator (if implemented)
- [ ] See read receipts (✓✓)
- [ ] Message timestamps

#### 4. Media
- [ ] Upload image
- [ ] Upload video
- [ ] Record voice message
- [ ] Record video note (circle video)
- [ ] Upload file
- [ ] View media in full screen

#### 5. Reactions
- [ ] Add reaction to message
- [ ] Remove reaction
- [ ] See others' reactions
- [ ] Multiple reactions on same message

#### 6. Profile
- [ ] View own profile
- [ ] Edit display name
- [ ] Change avatar
- [ ] Change password

---

### 🔐 E2E Encryption Testing

**Setup:** Use 2 devices or 1 device + web app

#### Test 1: First Message (Key Exchange)
1. User A sends message to User B in DM
2. Check console logs for "E2E encryption initialized"
3. User B receives and decrypts message
4. Verify message text is correct

**Expected:**
- Message encrypted on User A device
- Server stores encrypted_content (not plaintext)
- User B decrypts successfully
- No "[Encrypted message]" error

#### Test 2: Subsequent Messages
1. User B replies to User A
2. Both users exchange 5-10 messages
3. All messages decrypt correctly

**Expected:**
- Fast encryption/decryption
- No decryption errors
- Messages appear instantly

#### Test 3: Group Chat (Unencrypted)
1. Create group chat with 3+ users
2. Send messages in group
3. Verify messages are NOT encrypted

**Expected:**
- Group messages sent as plaintext
- No encryption overhead
- All members see messages

#### Test 4: Encryption Failure Fallback
1. Clear app data for User A
2. User A logs in again (new keys)
3. User B sends message to User A
4. User A should see "[Encrypted message]" or fallback

**Expected:**
- Graceful handling of key mismatch
- No app crash
- User can still send new messages

---

### 🔔 Push Notifications Testing

**Setup:** Close app completely (swipe away from recent apps)

#### Test 1: Basic Notification
1. Close app on Device A
2. Send message from Device B
3. Device A receives push notification

**Expected:**
- Notification appears within 5 seconds
- Shows sender name and message preview
- Notification sound plays
- Badge count updates

#### Test 2: Notification Tap
1. Receive notification
2. Tap notification
3. App opens to correct room

**Expected:**
- App opens immediately
- Correct room is opened
- Message is visible

#### Test 3: Multiple Notifications
1. Close app
2. Receive 3 messages from different users
3. Check notification tray

**Expected:**
- 3 separate notifications
- Each shows correct sender
- Badge shows total count

#### Test 4: Encrypted Message Notification
1. Close app
2. Receive encrypted DM message
3. Check notification content

**Expected:**
- Notification shows "New message" (not decrypted content)
- Privacy preserved in notification
- Message decrypts when app opened

---

### 📞 Voice Call Testing

**Setup:** 2 devices with microphone access

#### Test 1: Outgoing Call
1. User A opens DM with User B
2. User A taps phone icon (📞)
3. User B receives incoming call notification

**Expected:**
- Call UI appears on User A (calling...)
- User B sees incoming call modal
- Caller name displayed correctly

#### Test 2: Answer Call
1. User B taps "Answer"
2. Call connects
3. Both users can hear each other

**Expected:**
- Call status changes to "Connected"
- Audio works both ways
- Low latency (<500ms)

#### Test 3: Call Controls
1. During call, User A mutes microphone
2. User B should not hear User A
3. User A unmutes
4. User B hears User A again

**Expected:**
- Mute button toggles (🔊 ↔ 🔇)
- Audio mutes/unmutes instantly
- Visual feedback on button

#### Test 4: End Call
1. User A taps end call button (✕)
2. Call ends for both users

**Expected:**
- Call UI closes
- Both users return to chat
- No hanging connections

#### Test 5: Reject Call
1. User A calls User B
2. User B taps "Reject"
3. User A sees call ended

**Expected:**
- Call ends immediately
- User A returns to chat
- No error messages

---

### 📹 Video Call Testing

**Setup:** 2 devices with camera access

#### Test 1: Start Video Call
1. User A taps video icon (📹)
2. User B receives video call notification
3. User B answers

**Expected:**
- Local video preview appears
- Remote video appears after answer
- Both videos display correctly

#### Test 2: Video Toggle
1. During call, User A taps video button
2. Video turns off (audio continues)
3. User A taps again, video turns on

**Expected:**
- Video toggles on/off
- Remote user sees black screen when off
- Audio continues working

#### Test 3: Camera Switch
1. During call, User A taps camera switch (🔄)
2. Camera switches front ↔ back

**Expected:**
- Camera switches smoothly
- No call interruption
- Remote user sees new camera view

#### Test 4: Video Quality
1. Make video call
2. Check video quality and framerate
3. Move around, test different lighting

**Expected:**
- Smooth video (20-30 fps)
- Acceptable quality (480p+)
- Adapts to network conditions

---

### 🐛 Error Handling Testing

#### Test 1: Network Loss
1. Start call
2. Turn off WiFi/data
3. Wait 10 seconds
4. Turn network back on

**Expected:**
- Call disconnects gracefully
- Reconnection attempt (optional)
- No app crash

#### Test 2: Permission Denied
1. Revoke camera permission
2. Try to start video call

**Expected:**
- Error message shown
- Prompt to enable permission
- No app crash

#### Test 3: Server Offline
1. Stop API server
2. Try to send message

**Expected:**
- Error message shown
- Message queued (optional)
- App remains functional

#### Test 4: Invalid Encryption
1. Manually corrupt encryption keys
2. Try to decrypt message

**Expected:**
- Shows "[Encrypted message]"
- No app crash
- Can still send new messages

---

## Performance Testing

### Memory Usage
- [ ] Monitor memory during normal use
- [ ] Check for memory leaks during calls
- [ ] Verify app doesn't crash after 30+ minutes

### Battery Usage
- [ ] Monitor battery drain during calls
- [ ] Check background battery usage
- [ ] Verify push notifications don't drain battery

### Network Usage
- [ ] Monitor data usage during calls
- [ ] Check message sync efficiency
- [ ] Verify media uploads are optimized

---

## Regression Testing

After any code changes, re-test:
- [ ] Login/logout
- [ ] Send/receive messages
- [ ] E2E encryption
- [ ] Push notifications
- [ ] Voice calls
- [ ] Video calls

---

## Bug Reporting Template

```
**Bug:** [Short description]

**Steps to Reproduce:**
1. 
2. 
3. 

**Expected:** [What should happen]

**Actual:** [What actually happened]

**Device:** [Android version, device model]

**Logs:** [Paste relevant logs from `npx react-native log-android`]

**Screenshots:** [If applicable]
```

---

## Test Results

Date: ___________  
Tester: ___________

| Feature | Status | Notes |
|---------|--------|-------|
| Authentication | ⬜ Pass ⬜ Fail | |
| Messaging | ⬜ Pass ⬜ Fail | |
| E2E Encryption | ⬜ Pass ⬜ Fail | |
| Push Notifications | ⬜ Pass ⬜ Fail | |
| Voice Calls | ⬜ Pass ⬜ Fail | |
| Video Calls | ⬜ Pass ⬜ Fail | |
| Media Upload | ⬜ Pass ⬜ Fail | |
| Reactions | ⬜ Pass ⬜ Fail | |
| Profile | ⬜ Pass ⬜ Fail | |

**Overall Status:** ⬜ Ready for Production ⬜ Needs Fixes

**Critical Issues:**
- 

**Minor Issues:**
- 

**Notes:**
- 
