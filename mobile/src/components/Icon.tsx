import { Image, View } from "react-native";
import { useTheme } from "../ThemeProvider";
const icons = {
  attach: require("../../assets/icons/attach.png"),
  back: require("../../assets/icons/back.png"),
  "camera-flip": require("../../assets/icons/camera-flip.png"),
  chats: require("../../assets/icons/chats.png"),
  check: require("../../assets/icons/check.png"),
  checks: require("../../assets/icons/checks.png"),
  close: require("../../assets/icons/close.png"),
  compose: require("../../assets/icons/compose.png"),
  down: require("../../assets/icons/down.png"),
  mic: require("../../assets/icons/mic.png"),
  more: require("../../assets/icons/more.png"),
  pause: require("../../assets/icons/pause.png"),
  phone: require("../../assets/icons/phone.png"),
  play: require("../../assets/icons/play.png"),
  search: require("../../assets/icons/search.png"),
  send: require("../../assets/icons/send.png"),
  settings: require("../../assets/icons/settings.png"),
  speaker: require("../../assets/icons/speaker.png"),
  video: require("../../assets/icons/video.png"),
};
export type IconName = keyof typeof icons;
export function Icon({
  name,
  size = 24,
  color,
}: {
  name: IconName;
  size?: number;
  color?: string;
}) {
  const { colors } = useTheme();
  return (
    <View style={{width:size,height:size,overflow:'hidden',alignItems:'center',justifyContent:'center'}}><Image
      source={icons[name]}
      style={{
        width: size * 1.4,
        height: size * 1.4,
        tintColor: color || colors.text.primary,
      }}
      resizeMode="contain"
      accessible={false}
    /></View>
  );
}
