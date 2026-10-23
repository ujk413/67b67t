import {t as uiText} from "../../../web/i18n.js";
import { VoicePlayer } from "./VoicePlayer";
import { VideoNote } from "./VideoNote";
import { useTheme } from "../ThemeProvider";
import { Alert, Image, Pressable, StyleSheet, Text, View } from "react-native";
import {useContext} from 'react';
import { Video, ResizeMode } from "./Video";
import { telegramColors, telegramMetrics, nameColorFor } from "../theme";
import { Avatar } from "./Avatar";
import { shortTime } from "../format";
import { reactionPeople } from "../presentation";
import { Icon } from "./Icon";
import { messageStatus } from "../data/messages";
import { RichText } from "./RichText";
import { useMediaHeaders, useProtectedMedia,MediaDecoder } from "../MediaSession";
import type { Message, User } from "../types";
import {FeatureProvider} from '../features/FeatureProvider';
import {ExpressiveMessage,ProtectedExpressiveMedia} from '../features/ExpressiveFeatures';
import {parseMessagePayload} from '../features/messagePayloads';
import {useMediaAutoDownload} from '../features/qualityPolicy';

const MAX_MEDIA_SIDE = 280;
const MIN_MEDIA_SIDE = 96;

/**
 * Photo box from the dimensions the server stored. Falls back to a square when
 * they are missing, and keeps extreme panoramas readable by clamping the ratio.
 */
function imageBox(width?: number | null, height?: number | null) {
  const w = Number(width) || 0;
  const h = Number(height) || 0;
  if (w <= 0 || h <= 0) return { width: MAX_MEDIA_SIDE, height: MAX_MEDIA_SIDE };
  const ratio = Math.min(2.4, Math.max(0.42, w / h));
  const boxWidth = ratio >= 1 ? MAX_MEDIA_SIDE : Math.round(MAX_MEDIA_SIDE * ratio);
  const boxHeight = ratio >= 1 ? Math.round(MAX_MEDIA_SIDE / ratio) : MAX_MEDIA_SIDE;
  return {
    width: Math.max(MIN_MEDIA_SIDE, boxWidth),
    height: Math.max(MIN_MEDIA_SIDE, boxHeight),
  };
}

type MessageBubbleProps = {
  message: Message;
  meId: string;
  baseUrl: string;
  participants?: User[];
  joinedPrevious?: boolean;
  joinedNext?: boolean;
  onLongPress?: () => void;
  playing?: boolean;
  showAvatar?: boolean;
  group?: boolean;
  onReplyPress?: (id: string) => void;
  onUsername?: (username: string) => void;
  onPlayVoice?: () => void;
  onReact?: () => void;
  onOpenMedia?: (message: Message) => void;
  mediaUrl: (
    baseUrl: string,
    mediaId: string | null | undefined,
  ) => string | null;
  mediaThumbUrl: (
    baseUrl: string,
    mediaId: string | null | undefined,
  ) => string | null;
};

export function MessageBubble({
  message,
  meId,
  baseUrl,
  playing,
  participants = [],
  joinedPrevious,
  joinedNext,
  onLongPress,
  showAvatar,
  group = true,
  onReplyPress,
  onUsername,
  onPlayVoice,
  onReact,
  onOpenMedia,
  mediaUrl,
  mediaThumbUrl,
}: MessageBubbleProps) {
  const styles = useStyles();
  const feature=useContext(FeatureProvider);
  const { colors: telegramColors } = useTheme();

  const isOwn = message.sender_id === meId;
  const url = mediaUrl(baseUrl, message.media_id);
  const thumb = mediaThumbUrl(baseUrl, message.media_id) || url;
  const senderName =
    message.sender_display_name || message.sender_username || uiText("Пользователь");
  const nameColorStyle = { color: nameColorFor(message.sender_id) };
  const time = shortTime(message.created_at);
  const read = !!message.read_by?.some((id) => id !== message.sender_id);
  const people = [...participants];
  if (!people.some((user) => user.id === message.sender_id))
    people.push({
      id: message.sender_id,
      username: message.sender_username || "",
      display_name: message.sender_display_name,
      avatar_id: message.sender_avatar_id,
    });
  const status = message.delivery ? messageStatus(message) : "";
  const reserve = status ? 25 : message.edited_at ? 23 : isOwn ? 13 : 9;

  return (
    <View
      style={[
        styles.wrapper,
        isOwn ? styles.wrapperOwn : styles.wrapperOther,
        { marginTop: joinedPrevious ? 1 : 5 },
      ]}
    >
      {showAvatar && !isOwn && (
        <Avatar
          user={{
            id: message.sender_id,
            username: message.sender_username || "",
            display_name: message.sender_display_name,
            avatar_id: message.sender_avatar_id,
          }}
          baseUrl={baseUrl}
          size={telegramMetrics.bubble.avatarSize}
          mediaUrl={mediaUrl}
        />
      )}
      {showAvatar && !isOwn && <View style={styles.avatarGap} />}
      {!showAvatar && !isOwn && group && <View style={styles.avatarSpacer} />}

      <View
        style={[
          styles.bubble,
          isOwn ? styles.bubbleOwn : styles.bubbleIn,
          isOwn
            ? {
                borderTopRightRadius: joinedPrevious ? 8 : 22,
                borderBottomRightRadius: joinedNext ? 8 : 22,
              }
            : {
                borderTopLeftRadius: joinedPrevious ? 8 : 22,
                borderBottomLeftRadius: joinedNext ? 8 : 22,
              },
        ]}
      >
        {!isOwn && group && showAvatar && (
          <Text style={[styles.senderName, nameColorStyle]} numberOfLines={1}>
            {senderName}
          </Text>
        )}

        {message.reply_to?.text && (
          <Pressable
            onLongPress={onLongPress}
            onPress={() =>
              message.reply_to?.id && onReplyPress?.(message.reply_to.id)
            }
            style={[
              styles.replyContainer,
              isOwn ? styles.replyOwn : styles.replyIn,
            ]}
          >
            <View
              style={[
                styles.replyLine,
                isOwn ? styles.replyLineOwn : styles.replyLineIn,
              ]}
            />
            <View style={styles.replyContent}>
              <Text style={styles.replyName} numberOfLines={1}>
                {message.reply_to.sender_display_name ||
                  message.reply_to.sender_username ||
                  uiText("Пользователь")}
              </Text>
              <Text style={styles.replyText} numberOfLines={2}>
                {message.reply_to.text}
              </Text>
            </View>
          </Pressable>
        )}

        <View
          style={{
            position: "relative",
            paddingBottom: message.type === "text" ? 0 : 18,
          }}
        >
          {feature && parseMessagePayload(message.text) ? <ExpressiveMessage message={message} events={feature.events} context={feature.context}/> : <MessageContent
            message={message}
            url={url}
            thumb={thumb}
            playing={playing}
            onPlayVoice={onPlayVoice}
            onOpenMedia={onOpenMedia}
            isOwn={isOwn}
            onUsername={onUsername}
            onLongPress={onLongPress}
            reserveMeta={reserve}
          />}
          {message.type !== "text" && !!message.text && <RichText text={message.text} onUsername={onUsername} onLongPress={onLongPress} reserveMeta={reserve} />}
          <View
            style={styles.metaRow}
            accessible
            accessibilityLabel={`${time}, ${isOwn ? messageStatus(message) : ""}`}
          >
            <Text style={[styles.time, isOwn ? styles.timeOwn : styles.timeIn]}>
              {message.edited_at ? (""+uiText("изменено ·")+" ") : ""}
              {time}
              {status ? ` · ${status}` : ""}
            </Text>
            {isOwn && !message.delivery && (
              <Icon
                name={read ? "checks" : "check"}
                size={read ? 16 : 14}
                color={
                  read
                    ? telegramColors.chat.sentCheckRead
                    : telegramColors.chat.outTime
                }
              />
            )}
          </View>
        </View>
        {message.reactions && (
          <ReactionBar
            reactions={message.reactions}
            people={people}
            group={group}
            meId={meId}
            baseUrl={baseUrl}
            mediaUrl={mediaUrl}
            onReact={onReact}
          />
        )}
      </View>
    </View>
  );
}

function MessageContent({
  message,
  url,
  thumb,
  playing,
  onPlayVoice,
  onOpenMedia,
  isOwn,
  onUsername,
  onLongPress,
  reserveMeta,
}: {
  onLongPress?: () => void;
  reserveMeta?: number;
  message: Message;
  url: string | null;
  thumb: string | null;
  playing?: boolean;
  onPlayVoice?: () => void;
  onOpenMedia?: (message: Message) => void;
  isOwn: boolean;
  onUsername?: (username: string) => void;
}) {
  const headers = useMediaHeaders();
  const session=useContext(MediaDecoder);
  const contentMime=String((message.meta?.media_encryption as {mime?:string}|undefined)?.mime||message.media_mime||'');
  const mediaType=message.type==='voice'?'voice':contentMime.startsWith('image/')?'image':message.type==='video_note'||contentMime.startsWith('video/')?'video':'file';
  const autoDownload=useMediaAutoDownload(mediaType,session?.userId);
  const protectedUri = useProtectedMedia(message, autoDownload);
  const styles = useStyles();
  const { colors: telegramColors } = useTheme();

  const textColor = isOwn
    ? telegramColors.chat.messageTextOut
    : telegramColors.chat.messageTextIn;

  if (message.meta?.media_encryption) {
    if(protectedUri) {
      const info=message.meta.media_encryption as {mime:string;name:string};
      if(message._mediaDetails?.expressive)return <Pressable accessibilityRole="button" onPress={()=>onOpenMedia?.(message)} onLongPress={onLongPress}><ProtectedExpressiveMedia uri={protectedUri} mime={info.mime} label={message._mediaDetails.expressive.label}/></Pressable>;
      return <MessageContent message={{...message,media_mime:info.mime,meta:{...message.meta,media_encryption:undefined,filename:info.name}}} url={protectedUri} thumb={protectedUri} isOwn={isOwn} onOpenMedia={()=>onOpenMedia?.(message)} onLongPress={onLongPress} />;
    }
    const file=message.meta.media_encryption as {name?:string;mime?:string};
    return <Pressable accessibilityRole="button" accessibilityLabel={(""+uiText("Скачать и открыть")+" ")+(file.name||uiText("вложение"))} onPress={() => onOpenMedia?.(message)} onLongPress={onLongPress} style={{ padding: 14, minHeight: 48 }}><Text style={{ color: textColor }}>{file.name||uiText("Вложение")}</Text><Text style={{color:textColor,opacity:.7,fontSize:12,marginTop:5}}>{file.mime||uiText("Файл")}{message.media_size?uiText(" · {0} МБ",(message.media_size/1048576).toFixed(1)):''} {uiText("· Скачать и открыть")}</Text></Pressable>;
  }

  if(message.type==='call'){
    const status=String(message.meta?.status||'completed'),labels:Record<string,string>={completed:uiText("Звонок завершён"),missed:uiText("Пропущенный звонок"),rejected:uiText("Звонок отклонён"),busy:uiText("Абонент занят"),cancelled:uiText("Вызов отменён"),interrupted:uiText("Звонок прерван")};
    const duration=Math.floor(Number(message.meta?.duration_ms||0)/1000);
    return <View style={{padding:8,gap:5}}><Text style={{color:textColor}}>{message.meta?.is_video?uiText("Видеозвонок"):uiText("Аудиозвонок")}</Text><Text style={{color:textColor,fontSize:13}}>{labels[status]||uiText("Звонок")}{duration>0?` · ${Math.floor(duration/60)}:${String(duration%60).padStart(2,'0')}`:''}</Text></View>;
  }
  if (message.type === "text") {
    return (
      <RichText
        text={message.text || ""}
        onUsername={onUsername}
        onLongPress={onLongPress}
        reserveMeta={reserveMeta}
      />
    );
  }

  if (message.type === "voice" && url) return <View><VoicePlayer uri={url} initialDuration={message._mediaDetails?.duration_ms || Number(message.meta?.duration_ms) || (message.duration_sec || 0)*1000} waveform={message._mediaDetails?.waveform} onListened={()=>session?.onListened?.(message)} />{!!message.listened_by?.some(id=>id!==message.sender_id) && <Text style={{fontSize:11,color:textColor}}>{uiText("Прослушано")}</Text>}</View>;

  if (
    (message.type === "video_note" ||
      (message.type === "media" && message.media_mime?.startsWith("video/"))) &&
    url &&
    onOpenMedia &&
    !autoDownload
  )
    return (
      <Pressable
        onPress={() => onOpenMedia(message)}
        onLongPress={onLongPress}
        style={{ padding: 16, minHeight: 48 }}
      >
        <Text style={{ color: textColor }}>{uiText("Видео · Нажмите для просмотра")}</Text>
      </Pressable>
    );

  if (message.type === "video_note" && url) {
    return (
      <VideoNote
        uri={url}
        durationSec={message.duration_sec}
        onLongPress={onLongPress}
        onOpenViewer={onOpenMedia ? () => onOpenMedia(message) : undefined}
      />
    );
  }

  if (message.type === "media" && url && onOpenMedia) {
    const mime = message.media_mime || "";
    if (mime.startsWith("image/")) {
      // Honour the real aspect ratio the server reported instead of cropping
      // every photo to a square. Telegram sizes a photo by its own dimensions.
      const natural = imageBox(message.width, message.height);
      return (
        <Pressable
          onPress={() => onOpenMedia(message)}
          onLongPress={onLongPress}
        >
          <Image
            source={{ uri: autoDownload ? url : thumb || url, headers }}
            style={[styles.mediaImage, natural]}
            resizeMode="cover"
          />
        </Pressable>
      );
    }
    if (mime.startsWith("video/")) {
      return (
        <Pressable
          onPress={() => onOpenMedia(message)}
          onLongPress={onLongPress}
        >
          <Video
            source={{ uri: url, headers }}
            style={styles.mediaVideo}
            useNativeControls
            resizeMode={ResizeMode.CONTAIN}
          />
        </Pressable>
      );
    }
    return (
      <Pressable
        onPress={() => onOpenMedia(message)}
        onLongPress={onLongPress}
        style={styles.fileCard}
      >
        <Text style={[styles.fileTitle, { color: textColor }]}>
          {String(message.meta?.filename || uiText("Файл"))}
        </Text>
        <Text style={styles.fileMime}>{mime || uiText("Вложение")}</Text>
      </Pressable>
    );
  }

  return (
    <Text style={[styles.text, { color: textColor }]}>[{message.type}]</Text>
  );
}

function ReactionBar({
  reactions,
  people,
  group,
  meId,
  baseUrl,
  mediaUrl,
  onReact,
}: {
  reactions: Record<string, string[]>;
  people: User[];
  group: boolean;
  meId: string;
  baseUrl: string;
  mediaUrl: (base: string, id: string | null | undefined) => string | null;
  onReact?: () => void;
}) {
  const styles = useStyles();
  const { colors } = useTheme();
  const items = Object.entries(reactions).filter(([, ids]) => ids.length);
  if (!items.length) return null;
  return (
    <View style={styles.reactions}>
      {items.map(([emoji, ids]) => {
        const users = reactionPeople(ids, people);
        const names = users.map((user) =>
          user.id === meId
            ? uiText("Вы")
            : user.display_name || user.username || uiText("Участник"),
        );
        return (
          <Pressable
            key={emoji}
            onPress={onReact}
            onLongPress={() =>
              Alert.alert(uiText("Реакция {0}",emoji), names.join("\n"))
            }
            accessibilityRole="button"
            accessibilityLabel={`${emoji}: ${names.join(", ")}`}
            style={[
              styles.reactionChip,
              {
                borderColor: ids.includes(meId)
                  ? colors.text.blue
                  : "transparent",
              },
            ]}
          >
            <Text style={styles.reactionText}>{emoji}</Text>
            {group ? (
              <Text style={{ fontSize: 12, color: colors.text.secondary }}>
                {users.length}
              </Text>
            ) : (
              <View style={{ flexDirection: "row", marginLeft: 2 }}>
                {users.map((user, index) => (
                  <View
                    key={user.id}
                    style={{
                      marginLeft: index ? -4 : 0,
                      borderRadius: 12,
                      borderWidth: 1,
                      borderColor: colors.chat.inBubble,
                    }}
                  >
                    <Avatar
                      user={user}
                      baseUrl={baseUrl}
                      size={18}
                      mediaUrl={mediaUrl}
                    />
                  </View>
                ))}
              </View>
            )}
          </Pressable>
        );
      })}
    </View>
  );
}

const useStyles = () => {
  const { colors: telegramColors } = useTheme();
  return StyleSheet.create({
    wrapper: {
      width: "100%",
      flexDirection: "row",
      marginVertical: 1,
    },
    wrapperOwn: {
      justifyContent: "flex-end",
    },
    wrapperOther: {
      justifyContent: "flex-start",
    },
    avatarGap: {
      width: telegramMetrics.bubble.avatarGap,
    },
    avatarSpacer: {
      width:
        telegramMetrics.bubble.avatarSize + telegramMetrics.bubble.avatarGap,
    },
    bubble: {
      maxWidth: `${telegramMetrics.bubble.maxWidthRatio * 100}%`,
      borderRadius: telegramMetrics.bubble.radius,
      paddingHorizontal: telegramMetrics.bubble.paddingH,
      paddingVertical: telegramMetrics.bubble.paddingV,
    },
    bubbleIn: {
      backgroundColor: telegramColors.chat.inBubble,
    },
    bubbleOwn: {
      backgroundColor: telegramColors.chat.outBubble,
    },
    senderName: {
      fontSize: telegramMetrics.font.messageName,
      fontWeight: "600",
      marginBottom: 2,
    },
    replyContainer: {
      flexDirection: "row",
      marginBottom: 6,
      borderRadius: 12,
      padding: 6,
    },
    replyIn: {
      backgroundColor: telegramColors.chat.replyBackgroundIn,
    },
    replyOwn: {
      backgroundColor: telegramColors.chat.replyBackgroundOut,
    },
    replyLine: {
      width: 2,
      borderRadius: 1,
      marginRight: 6,
    },
    replyLineIn: {
      backgroundColor: telegramColors.chat.inReplyLine,
    },
    replyLineOwn: {
      backgroundColor: telegramColors.chat.outReplyLine,
    },
    replyContent: {
      flex: 1,
    },
    replyName: {
      fontSize: 13,
      fontWeight: "600",
      color: telegramColors.chat.inReplyName,
      marginBottom: 2,
    },
    replyText: {
      fontSize: 13,
      color: telegramColors.chat.inReplyMessage,
    },
    text: {
      fontSize: telegramMetrics.font.messageText,
      lineHeight: 20,
    },
    metaRow: {
      position: "absolute",
      right: 0,
      bottom: 0,
      flexDirection: "row",
      alignItems: "center",
      gap: 3,
    },
    time: {
      fontSize: 11,
      lineHeight: 14,
    },
    timeIn: {
      color: telegramColors.chat.inTime,
    },
    timeOwn: {
      color: telegramColors.chat.outTime,
    },
    reactButton: {
      fontSize: 14,
      color: telegramColors.text.gray5,
      marginLeft: 8,
    },
    reactions: {
      flexDirection: "row",
      flexWrap: "wrap",
      gap: 4,
      marginTop: 4,
    },
    reactionChip: {
      backgroundColor: telegramColors.chat.reactionChip,
      borderRadius: 14,
      borderWidth: 1,
      flexDirection: "row",
      alignItems: "center",
      gap: 3,
      paddingHorizontal: 6,
      paddingVertical: 3,
      minHeight: 26,
    },
    reactionText: {
      fontSize: 14,
      color: telegramColors.text.primary,
    },
    voiceButton: {
      paddingVertical: 8,
    },
    voiceText: {
      fontSize: 14,
      fontWeight: "600",
    },
    videoNote: {
      width: 180,
      height: 180,
      borderRadius: 90,
      backgroundColor: telegramColors.windowBackgroundGray,
    },
    mediaImage: {
      width: 240,
      height: 240,
      borderRadius: 14,
      backgroundColor: telegramColors.windowBackgroundGray,
    },
    mediaVideo: {
      width: 280,
      height: 200,
      borderRadius: 14,
      backgroundColor: telegramColors.windowBackgroundGray,
    },
    fileCard: {
      padding: 10,
      borderRadius: 8,
      backgroundColor: telegramColors.chat.fileCard,
    },
    fileTitle: {
      fontSize: 14,
      fontWeight: "600",
    },
    fileMime: {
      fontSize: 12,
      color: telegramColors.text.gray5,
      marginTop: 2,
    },
  });
};
