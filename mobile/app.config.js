const fs = require("fs");
module.exports = ({ config }) => {
  const fcm = fs.existsSync("./google-services.json");
  const plist = fs.existsSync("./GoogleService-Info.plist");
  const ios = { ...config.ios };
  if (!plist) delete ios.googleServicesFile;
  return {
    ...config,
    userInterfaceStyle: "automatic",
    ios,
    extra: { ...config.extra, fcmConfigured: fcm },
    android: {
      ...config.android,
      ...(fcm ? { googleServicesFile: "./google-services.json" } : {}),
    },
    plugins: [
      ...(config.plugins || []),
      "expo-sharing",
      // Only arm64-v8a: a universal APK carries four ABIs and weighs ~168 MB,
      // which is where the flaky update downloads came from. This cuts it to
      // roughly a third. It is also what keeps the setting alive across
      // `expo prebuild --clean`, unlike gradle.properties.
      ["expo-build-properties", { android: { buildArchs: ["arm64-v8a"] } }],
      ["expo-video", { supportsBackgroundPlayback: false, supportsPictureInPicture: false }],
      ["expo-sqlite", { useSQLCipher: true, enableFTS: true }],
      "expo-secure-store",
      [
        "expo-camera",
        {
          cameraPermission: "Камера нужна для фото, видеокружков и звонков",
          microphonePermission: "Микрофон нужен для записи сообщений и звонков",
        },
      ],
      ["expo-notifications", { color: "#0FA37F", defaultChannel: "messages", icon: "./assets/icons/chats@3x.png" }],
    ],
  };
};
