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
