import {locale as uiLocale} from "../../../web/i18n.js";
import {t as uiText} from "../../../web/i18n.js";
import {
  Image,
  Pressable,
  Text,
  View,
  useWindowDimensions,
} from "react-native";
import { useTheme } from "../ThemeProvider";
import { useMediaHeaders, useProtectedMedia } from "../MediaSession";
import {useMediaAutoDownload} from '../features/qualityPolicy';
import { RichText } from "./RichText";
import type { Message } from "../types";
export function Album({
  messages,
  meId,
  baseUrl,
  onOpen,
}: {
  messages: Message[];
  meId: string;
  baseUrl: string;
  onOpen: (message: Message) => void;
}) {
  const { colors } = useTheme(),
    headers = useMediaHeaders(),
    { width } = useWindowDimensions();
  const first = messages[0],
    size = Math.min(130, Math.floor((width * 0.78 - 24) / 2));
  return (
    <View
      style={{
        alignSelf: first.sender_id === meId ? "flex-end" : "flex-start",
        backgroundColor:
          first.sender_id === meId
            ? colors.chat.outBubble
            : colors.chat.inBubble,
        borderRadius: 22,
        padding: 6,
        maxWidth: width * 0.8,
      }}
    >
      <View
        style={{
          flexDirection: "row",
          flexWrap: "wrap",
          gap: 4,
          width: size * 2 + 4,
        }}
      >
        {messages.map((message) => (
          <Pressable
            key={message.id}
            onPress={() => onOpen(message)}
            accessibilityRole="button"
            accessibilityLabel={uiText("Открыть фото из альбома")}
          >
            <AlbumImage message={message} baseUrl={baseUrl} size={size} meId={meId}/>
          </Pressable>
        ))}
      </View>
      {!!first.text && <RichText text={first.text} />}
      <Text
        style={{
          textAlign: "right",
          color: colors.text.secondary,
          fontSize: 12,
          paddingTop: 4,
        }}
      >
        {new Date(first.created_at).toLocaleTimeString(uiLocale(), {
          hour: "2-digit",
          minute: "2-digit",
        })}{" "}
        · {messages.length} {uiText("фото")}</Text>
    </View>
  );
}

function AlbumImage({message,baseUrl,size,meId}:{message:Message;baseUrl:string;size:number;meId:string}) {
  const {colors}=useTheme(),autoDownload=useMediaAutoDownload('image',meId),headers=useMediaHeaders(),uri=useProtectedMedia(message,autoDownload);
  if(message.meta?.media_encryption && !uri)return <View style={{width:size,height:size,borderRadius:12,backgroundColor:colors.windowBackgroundGray,alignItems:'center',justifyContent:'center'}}><Text style={{color:colors.text.secondary}}>{uiText("Фото · открыть")}</Text></View>;
  return <Image source={{uri:uri || `${baseUrl}/media/${message.media_id}/thumb`,headers}} style={{width:size,height:size,borderRadius:12}} />;
}
