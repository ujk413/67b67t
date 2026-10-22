// SDK 55.0.27 keeps a rejected native-token promise forever after an offline
// first launch. Clear it in finally so a later foreground/network retry works.
const fs = require('node:fs');
const path = require('node:path');
const root = path.dirname(require.resolve('expo-notifications/package.json'));
const version = require(path.join(root, 'package.json')).version;
if (version !== '55.0.27') throw new Error('Review the Expo token retry patch before changing expo-notifications');
for (const relative of ['src/getDevicePushTokenAsync.ts', 'build/getDevicePushTokenAsync.js']) {
  const file = path.join(root, relative);
  const source = fs.readFileSync(file, 'utf8');
  if (source.includes('// 67chat: clear failed token requests too')) continue;
  const pattern = /(\s*)nativeTokenPromise = PushTokenManager\.getDevicePushTokenAsync\(\);\r?\n\s*devicePushToken = await nativeTokenPromise;\r?\n\s*nativeTokenPromise = null;/;
  if (!pattern.test(source)) throw new Error('Expo token implementation changed: ' + relative);
  fs.writeFileSync(file, source.replace(pattern, (_, space) => `${space}nativeTokenPromise = PushTokenManager.getDevicePushTokenAsync();${space}// 67chat: clear failed token requests too${space}try { devicePushToken = await nativeTokenPromise; }${space}finally { nativeTokenPromise = null; }`));
}
